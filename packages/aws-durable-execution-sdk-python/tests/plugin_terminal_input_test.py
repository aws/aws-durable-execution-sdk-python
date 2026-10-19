"""Visible terminal input updates notify plugins before cached user paths skip them."""

import contextvars
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Event

import pytest
from datetime import UTC, datetime

from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationOutput,
    InvocationStatus,
)
from aws_durable_execution_sdk_python.lambda_service import (
    CallbackDetails,
    ErrorObject,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationUpdate,
    OperationType,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    PluginExecutor,
)


def _callback(operation_id, status=OperationStatus.SUCCEEDED, error=None):
    return Operation(
        operation_id=operation_id,
        operation_type=OperationType.CALLBACK,
        status=status,
        name=operation_id,
        callback_details=CallbackDetails(
            callback_id=operation_id, result="saved", error=error
        ),
    )


def _start(executor, operations, updated_ids):
    executor.on_invocation_start(
        execution_arn="execution/input-delivery",
        is_first_invocation=False,
        execution_start_time=datetime.now(UTC),
        lambda_context=None,
        operations_provider=lambda: operations,
        updated_operation_ids=updated_ids,
    )


def test_input_delivery_follows_all_starts_and_shares_actual_notification_ledger():
    marker = contextvars.ContextVar("input-owner", default="host")
    events = []

    class Observe(DurableInstrumentationPlugin):
        def __init__(self, name):
            self.name = name

        def on_invocation_start(self, info):
            marker.set(self.name)
            events.append(("start", self.name))

        def on_operation_end(self, info):
            events.append(
                ("end", self.name, info.operation_id, marker.get(), info.result)
            )

    b, active = _callback("b"), _callback("active", OperationStatus.STARTED)
    operations = {"b": b, "active": active}
    executor = PluginExecutor([Observe("first"), Observe("second")])

    def invoke():
        with executor.run():
            _start(executor, operations, ["b", "active", "not-visible"])
            assert events == [
                ("start", "first"),
                ("start", "second"),
                ("end", "first", "b", "second", "saved"),
                ("end", "second", "b", "second", "saved"),
            ]
            executor.on_operation_update(b)
            assert len(events) == 4
            # Missing input metadata did not claim an event that arrives later.
            executor.on_operation_update(_callback("not-visible"))
            assert [event[2] for event in events if event[0] == "end"] == [
                "b",
                "b",
                "not-visible",
                "not-visible",
            ]
            executor.on_invocation_end(
                DurableExecutionInvocationOutput(status=InvocationStatus.PENDING)
            )

    contextvars.Context().run(invoke)
    assert marker.get() == "host"
    events.clear()
    # A new invocation can receive the same explicit update after redelivery;
    # the ledger is not a cross-invocation exporter acknowledgment.
    contextvars.Context().run(invoke)


@pytest.mark.parametrize("deferred", [False, True])
def test_failed_start_and_snapshot_mutation_cannot_lose_input_notification(deferred):
    marker = contextvars.ContextVar("input-failed-start", default="host")
    error = ErrorObject(
        message="saved error", type="TestError", data=None, stack_trace=["original"]
    )
    operation = _callback("b", OperationStatus.FAILED, error)
    operations = {"b": operation}
    if deferred:
        operation = replace(operation, parent_id="unvisited-parent")
        operations = {"b": operation}
    seen = []

    class MutatingFailure(DurableInstrumentationPlugin):
        def on_invocation_start(self, info):
            marker.set("poison")
            info.updated_operations["b"].error.stack_trace.append("mutated")
            info.updated_operations.clear()
            raise RuntimeError("intentional Start failure")

    class Observe(DurableInstrumentationPlugin):
        def on_invocation_start(self, info):
            assert marker.get() == "host"
            marker.set("survivor")

        def on_operation_end(self, info):
            seen.append((info.operation_id, marker.get(), list(info.error.stack_trace)))

    def invoke():
        executor = PluginExecutor([MutatingFailure(), Observe()])
        with executor.run():
            _start(executor, operations, ["b"])
            if deferred:
                assert seen == []
            executor.on_invocation_end(
                DurableExecutionInvocationOutput(status=InvocationStatus.PENDING)
            )

    contextvars.Context().run(invoke)
    assert seen == [("b", "survivor", ["original"])]
    assert operation.callback_details.error.stack_trace == ["original"]
    assert marker.get() == "host"


@pytest.mark.parametrize("start_kind", ["replay", "start"])
def test_input_waits_for_the_real_parent_chain_and_concurrent_starts(start_kind):
    parent_entered = Event()
    parent_returned = Event()
    delivered = []
    error = ErrorObject(message="saved", type="External", data=None, stack_trace=None)
    grandparent = Operation(
        operation_id="grandparent",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
        sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
    )
    parent = replace(grandparent, operation_id="parent", parent_id="grandparent")
    child = replace(
        _callback("child", OperationStatus.FAILED, error), parent_id="parent"
    )
    operations = {op.operation_id: op for op in (grandparent, parent, child)}

    class Observe(DurableInstrumentationPlugin):
        def on_user_function_start(self, info):
            if info.operation_id == "parent":
                parent_entered.set()
                assert parent_returned.wait(5)

        def on_operation_end(self, info):
            assert parent_returned.is_set()
            delivered.append(info)

    executor = PluginExecutor([Observe()])
    for invocation in range(2):
        parent_entered.clear()
        parent_returned.clear()
        with executor.run(), ThreadPoolExecutor(max_workers=8) as workers:
            _start(executor, operations, ["child"])
            assert len(delivered) == invocation
            executor.on_operation_replay(grandparent)
            assert len(delivered) == invocation
            executor.on_user_function_start(
                OperationIdentifier(
                    operation_id="grandparent",
                    parent_id=None,
                    name="grandparent",
                    operation_type=OperationType.CONTEXT,
                    sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
                )
            )
            if start_kind == "replay":
                executor.on_operation_replay(parent)
            else:
                executor.on_operation_action(
                    OperationUpdate(
                        operation_id="parent",
                        operation_type=OperationType.CONTEXT,
                        action=OperationAction.START,
                        parent_id="grandparent",
                        sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
                    ),
                    parent,
                )
            assert len(delivered) == invocation
            start = workers.submit(
                executor.on_user_function_start,
                OperationIdentifier(
                    operation_id="parent",
                    parent_id="grandparent",
                    name="parent",
                    operation_type=OperationType.CONTEXT,
                    sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
                ),
            )
            assert parent_entered.wait(5)
            try:
                # A checkpoint may repeat the input while Start is still running.
                list(workers.map(executor.on_operation_update, [child] * 16))
                assert len(delivered) == invocation
            finally:
                parent_returned.set()
            start.result(timeout=5)
            executor.on_operation_update(child)
            assert len(delivered) == invocation + 1
            assert delivered[-1].error is error
            assert delivered[-1].parent_id == "parent"


def test_cached_terminal_parent_does_not_require_a_synthetic_start():
    parent = Operation(
        operation_id="cached-parent",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.SUCCEEDED,
        sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
    )
    child = replace(_callback("child"), parent_id=parent.operation_id)
    events = []

    class Observe(DurableInstrumentationPlugin):
        def on_operation_start(self, info):
            events.append(("start", info.operation_id))

        def on_operation_end(self, info):
            events.append(("end", info.operation_id))

    executor = PluginExecutor([Observe()])
    with executor.run():
        _start(
            executor,
            {parent.operation_id: parent, child.operation_id: child},
            ["child"],
        )
        assert events == [("end", "child")]
        executor.on_operation_update(child)
        assert events == [("end", "child")]


def test_unvisited_live_parent_flushes_input_before_invocation_end():
    parent = Operation(
        operation_id="unvisited-parent",
        operation_type=OperationType.CONTEXT,
        status=OperationStatus.STARTED,
        sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
    )
    child = replace(_callback("child"), parent_id=parent.operation_id)
    events = []

    class Observe(DurableInstrumentationPlugin):
        def on_operation_end(self, info):
            events.append(("operation-end", info.operation_id, info.parent_id))

        def on_invocation_end(self, info):
            events.append(("invocation-end", info.status.value))

    executor = PluginExecutor([Observe()])
    with executor.run():
        _start(
            executor,
            {parent.operation_id: parent, child.operation_id: child},
            ["child"],
        )
        assert events == []
        executor.on_operation_update(child)
        assert events == []
        executor.on_invocation_end(
            DurableExecutionInvocationOutput(status=InvocationStatus.PENDING)
        )
        assert events == [
            ("operation-end", "child", "unvisited-parent"),
            ("invocation-end", "PENDING"),
        ]
