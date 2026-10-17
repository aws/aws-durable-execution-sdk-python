"""Visible terminal input updates notify plugins before cached user paths skip them."""

import contextvars
from datetime import UTC, datetime

from aws_durable_execution_sdk_python.execution import (
    DurableExecutionInvocationOutput,
    InvocationStatus,
)
from aws_durable_execution_sdk_python.lambda_service import (
    CallbackDetails,
    ErrorObject,
    Operation,
    OperationStatus,
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


def test_failed_start_and_snapshot_mutation_cannot_lose_input_notification():
    marker = contextvars.ContextVar("input-failed-start", default="host")
    error = ErrorObject(
        message="saved error", type="TestError", data=None, stack_trace=["original"]
    )
    operation = _callback("b", OperationStatus.FAILED, error)
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
            _start(executor, {"b": operation}, ["b"])
            executor.on_invocation_end(
                DurableExecutionInvocationOutput(status=InvocationStatus.PENDING)
            )

    contextvars.Context().run(invoke)
    assert seen == [("b", "survivor", ["original"])]
    assert operation.callback_details.error.stack_trace == ["original"]
    assert marker.get() == "host"
