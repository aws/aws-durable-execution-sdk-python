"""Integration tests for context propagation into the user-function thread.

The SDK runs the handler body on a worker thread it submits to a pool. A
submitted callable is given a context of its own, so anything a contextvar
carries -- set by a plugin in ``on_invocation_start``, or by customer code
around the decorator -- is only visible to top-level handler code if the
invocation thread's context is carried into that worker.

Log-correlating plugins depend on this: a plugin that claims the invocation for
the calling thread at invocation start has no hook that runs on the worker
before the handler body, so the claim reaches top-level handler code by context
propagation or not at all.
"""

from __future__ import annotations

import contextvars
import threading
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from unittest.mock import Mock, patch

from aws_durable_execution_sdk_python.context import DurableContext, durable_step
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    Operation,
    OperationAction,
    OperationStatus,
    OperationType,
    StepDetails,
)
from aws_durable_execution_sdk_python.plugin import DurableInstrumentationPlugin
from tests.test_helpers import plugin_factory


UNSET = "unset"
_probe: contextvars.ContextVar[str] = contextvars.ContextVar(
    "handler_context_propagation_probe", default=UNSET
)


def _lambda_context() -> Mock:
    ctx = Mock()
    ctx.aws_request_id = "test-request-id"
    ctx.client_context = None
    ctx.identity = None
    ctx._epoch_deadline_time_in_ms = 0  # noqa: SLF001
    ctx.invoked_function_arn = "test-arn"
    ctx.tenant_id = None
    return ctx


def _event() -> dict:
    return {
        "DurableExecutionArn": "test-arn/execution-1",
        "CheckpointToken": "test-token",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "execution-1",
                    "Type": "EXECUTION",
                    "Status": "STARTED",
                    "ExecutionDetails": {"InputPayload": "{}"},
                }
            ],
            "NextMarker": "",
        },
        "LocalRunner": True,
    }


def _tracking_checkpoint():
    """Checkpoint mock that accumulates operations, as the service would.

    SUCCEED actions are recorded as SUCCEEDED so the SDK dispatches
    operation-end hooks, which is how a hook reaches the background
    checkpointing thread.
    """
    operations: dict[str, Operation] = {}

    def mock_checkpoint(
        durable_execution_arn,  # noqa: ARG001
        checkpoint_token,  # noqa: ARG001
        updates,
        client_token="token",  # noqa: S107, ARG001
    ) -> CheckpointOutput:
        for update in updates:
            previous = operations.get(update.operation_id)
            base = previous or Operation(
                operation_id=update.operation_id,
                operation_type=update.operation_type,
                status=OperationStatus.STARTED,
                parent_id=update.parent_id,
                name=update.name,
                sub_type=update.sub_type,
                start_timestamp=datetime.now(UTC),
            )
            if update.action is OperationAction.SUCCEED:
                operations[update.operation_id] = replace(
                    base,
                    status=OperationStatus.SUCCEEDED,
                    end_timestamp=datetime.now(UTC),
                    step_details=(
                        StepDetails(result=update.payload, attempt=1)
                        if update.operation_type is OperationType.STEP
                        else base.step_details
                    ),
                )
            else:
                operations[update.operation_id] = base
        return CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=list(operations.values())
            ),
        )

    return mock_checkpoint


def _run(handler, event: dict | None = None) -> dict:
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client.checkpoint = _tracking_checkpoint()
        mock_client_class.initialize_client.return_value = mock_client

        return handler(event if event is not None else _event(), _lambda_context())


class _ContextvarSettingPlugin(DurableInstrumentationPlugin):
    """Sets a contextvar on the invocation thread, as a log-correlating plugin does."""

    def __init__(self, value: str) -> None:
        self._value = value
        self.operation_end_observations: list[tuple[str, str]] = []
        self._lock = threading.Lock()

    def on_invocation_start(self, info) -> None:  # noqa: ARG002
        _probe.set(self._value)

    def on_operation_end(self, info) -> None:  # noqa: ARG002
        # Dispatched from the background checkpointing thread, which reads back
        # the terminal status of a checkpointed operation.
        with self._lock:
            self.operation_end_observations.append(
                (threading.current_thread().name, _probe.get())
            )


def test_handler_body_sees_a_contextvar_set_by_a_plugin_at_invocation_start():
    """The invocation-start hook's contextvar reaches top-level handler code.

    The hook runs on the invocation thread and the handler body runs on a worker
    thread, and no hook runs on that worker before the handler's first
    statement, so this only holds if the invocation's context is propagated.
    """
    plugin = _ContextvarSettingPlugin("claimed-by-plugin")
    seen: list[str] = []

    @durable_execution(plugins=[plugin_factory(plugin)])
    def my_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        seen.append(_probe.get())
        return "ok"

    result = _run(my_handler)

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert seen == ["claimed-by-plugin"]


def test_handler_body_sees_a_contextvar_set_by_the_caller():
    """A contextvar set before the handler is visible inside the handler body.

    This matches ``asyncio.to_thread``, which also runs the callable in a copy
    of the caller's context.
    """
    seen: list[str] = []

    @durable_execution
    def my_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        seen.append(_probe.get())
        return "ok"

    token = _probe.set("set-by-caller")
    try:
        result = _run(my_handler)
    finally:
        _probe.reset(token)

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    assert seen == ["set-by-caller"]


def test_handler_body_contextvar_writes_do_not_leak_to_the_caller():
    """The worker mutates its own copy, so the caller's context is untouched.

    A worker thread has a context of its own whether it starts empty or from a
    copy, so propagation adds no path from the handler back to the invocation
    thread.
    """
    observed_in_step: list[str] = []

    @durable_step
    def read_probe(_step_context) -> str:
        observed_in_step.append(_probe.get())
        return _probe.get()

    @durable_execution
    def my_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        _probe.set("set-inside-handler")
        return context.step(read_probe(), name="read-probe")

    token = _probe.set("set-by-caller")
    try:
        result = _run(my_handler)
        after = _probe.get()
    finally:
        _probe.reset(token)

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    # The write is visible to code the handler drives, and nowhere else.
    assert observed_in_step == ["set-inside-handler"]
    assert after == "set-by-caller"


def test_checkpointing_thread_does_not_carry_the_invocation_context():
    """The background checkpointing thread is submitted without the context.

    It runs SDK checkpointing rather than user code, so it is left starting from
    an empty context. This test pins that choice: a plugin hook dispatched from
    that thread sees the contextvar's default, not the value the invocation
    thread set.
    """
    plugin = _ContextvarSettingPlugin("claimed-by-plugin")
    handler_thread_names: list[str] = []

    @durable_step
    def a_step(_step_context) -> str:
        return "stepped"

    @durable_execution(plugins=[plugin_factory(plugin)])
    def my_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        handler_thread_names.append(threading.current_thread().name)
        return context.step(a_step(), name="a-step")

    result = _run(my_handler)

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    off_handler_thread = [
        value
        for thread_name, value in plugin.operation_end_observations
        if thread_name not in handler_thread_names
    ]
    assert off_handler_thread, plugin.operation_end_observations
    assert set(off_handler_thread) == {UNSET}
