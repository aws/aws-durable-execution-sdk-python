"""Integration tests for the plugin invocation payload surfaces.

Exercises `InvocationInfo.execution_input` / `InvocationEndInfo.execution_result`
through complete `durable_execution()` invocations -- across the decorator, the
plugin executor, and the invocation hooks -- including a suspend/replay pair
where the suspending invocation has no result and the replay carries the
terminal one, and the isolation guarantee between the plugin view and the user
handler's event.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock, patch

from aws_durable_execution_sdk_python.config import Duration, StepConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    Operation,
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python.plugin import DurableInstrumentationPlugin
from aws_durable_execution_sdk_python.retries import RetryDecision
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from tests.test_helpers import operation_id_sequence


class _PayloadRecordingPlugin(DurableInstrumentationPlugin):
    """Records the payload surfaces seen on each invocation hook."""

    def __init__(self) -> None:
        self.starts: list[Any] = []
        self.ends: list[tuple[str, Any, str | None]] = []

    def on_invocation_start(self, info) -> None:
        self.starts.append(info.execution_input)

    def on_invocation_end(self, info) -> None:
        self.ends.append(
            (info.status.value, info.execution_input, info.execution_result)
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


def _event(input_payload: str, extra_operations: list[dict] | None = None) -> dict:
    """Build an invocation event carrying the given execution input payload."""
    execution_operation = {
        "Id": "execution-1",
        "Type": "EXECUTION",
        "Status": "STARTED",
        "ExecutionDetails": {"InputPayload": input_payload},
    }
    return {
        "DurableExecutionArn": "test-arn/execution-1",
        "CheckpointToken": "test-token",
        "InitialExecutionState": {
            "Operations": [execution_operation, *(extra_operations or [])],
            "NextMarker": "",
        },
        "LocalRunner": True,
    }


def _tracking_checkpoint(initial_operations: list[Operation] | None = None):
    """Checkpoint mock that accumulates operations, as the service would.

    A stub returning an empty execution state is not enough for suspending
    paths: after the WAIT START is checkpointed the SDK re-reads the operation
    from the returned state, so it must be present.
    """
    operations: list[Operation] = list(initial_operations or [])

    def mock_checkpoint(
        durable_execution_arn,  # noqa: ARG001
        checkpoint_token,  # noqa: ARG001
        updates,
        client_token="token",  # noqa: S107, ARG001
    ) -> CheckpointOutput:
        for update in updates:
            operations.append(
                Operation(
                    operation_id=update.operation_id,
                    operation_type=update.operation_type,
                    status=OperationStatus.STARTED,
                    parent_id=update.parent_id,
                    name=update.name,
                    sub_type=update.sub_type,
                )
            )
        return CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=operations.copy()
            ),
        )

    return mock_checkpoint


def test_plugin_sees_execution_input_and_result_end_to_end():
    """A completing invocation surfaces the input on both hooks and the result."""
    plugin = _PayloadRecordingPlugin()

    @durable_execution(plugins=[plugin])
    def my_handler(event: Any, context: DurableContext) -> dict:  # noqa: ARG001
        return {"greeting": f"Hello, {event['name']}!"}

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client.checkpoint = _tracking_checkpoint()
        mock_client_class.initialize_client.return_value = mock_client

        result = my_handler(_event('{"name": "World"}'), _lambda_context())

    assert result["Status"] == InvocationStatus.SUCCEEDED.value

    # Start hook: the deserialized input, not the raw payload string.
    assert plugin.starts == [{"name": "World"}]

    # End hook: the same input, plus the serialized result.
    assert len(plugin.ends) == 1
    status, end_input, end_result = plugin.ends[0]
    assert status == InvocationStatus.SUCCEEDED.value
    assert end_input == {"name": "World"}
    assert end_result == '{"greeting": "Hello, World!"}'


def test_plugin_payload_surfaces_on_suspending_invocation():
    """A suspending invocation carries the input but no execution result."""
    plugin = _PayloadRecordingPlugin()

    @durable_execution(plugins=[plugin])
    def my_handler(event: Any, context: DurableContext) -> str:
        context.wait(Duration.from_seconds(60))
        return f"done-{event['name']}"

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client.checkpoint = _tracking_checkpoint()
        mock_client_class.initialize_client.return_value = mock_client

        result = my_handler(_event('{"name": "World"}'), _lambda_context())

    assert result["Status"] == InvocationStatus.PENDING.value
    assert plugin.starts == [{"name": "World"}]

    status, end_input, end_result = plugin.ends[0]
    assert status == InvocationStatus.PENDING.value
    # The input is still reported on a non-terminal invocation-end.
    assert end_input == {"name": "World"}
    # But a suspending invocation produced no execution result.
    assert end_result is None


def test_plugin_payload_surfaces_on_replay_invocation():
    """A replay past a completed wait carries the input and the terminal result."""
    plugin = _PayloadRecordingPlugin()

    @durable_execution(plugins=[plugin])
    def my_handler(event: Any, context: DurableContext) -> str:
        context.wait(Duration.from_seconds(60))
        return f"done-{event['name']}"

    # The wait completed externally while the execution was suspended.
    completed_wait = {
        "Id": next(operation_id_sequence()),
        "Type": OperationType.WAIT.value,
        "SubType": "Wait",
        "Status": OperationStatus.SUCCEEDED.value,
    }

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client.checkpoint = _tracking_checkpoint()
        mock_client_class.initialize_client.return_value = mock_client

        result = my_handler(
            _event('{"name": "World"}', extra_operations=[completed_wait]),
            _lambda_context(),
        )

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
    # The input is carried identically across invocations of one execution.
    assert plugin.starts == [{"name": "World"}]

    status, end_input, end_result = plugin.ends[0]
    assert status == InvocationStatus.SUCCEEDED.value
    assert end_input == {"name": "World"}
    assert end_result == '"done-World"'


def test_plugin_execution_input_is_isolated_from_handler_end_to_end():
    """The plugin's input view and the handler's event must not alias.

    durable_execution() hands one mutable object to both, so the plugin view is
    deep-copied. Without that a plugin could alter execution behaviour, and a
    handler could retroactively change what the frozen hook info reports.
    """

    class _MutatingPlugin(DurableInstrumentationPlugin):
        def __init__(self) -> None:
            self.end_inputs: list[Any] = []

        def on_invocation_start(self, info) -> None:
            info.execution_input["injected_by_plugin"] = True
            info.execution_input["nested"]["items"].append("from_plugin")

        def on_invocation_end(self, info) -> None:
            self.end_inputs.append(info.execution_input)

    plugin = _MutatingPlugin()
    handler_saw: dict[str, Any] = {}

    @durable_execution(plugins=[plugin])
    def my_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        handler_saw.update(
            {"top": dict(event), "nested_items": list(event["nested"]["items"])}
        )
        event["injected_by_handler"] = True
        return "ok"

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client.checkpoint = _tracking_checkpoint()
        mock_client_class.initialize_client.return_value = mock_client

        my_handler(
            _event('{"name": "World", "nested": {"items": ["original"]}}'),
            _lambda_context(),
        )

    # The plugin's mutations never reached the handler, at any depth.
    assert "injected_by_plugin" not in handler_saw["top"]
    assert handler_saw["nested_items"] == ["original"]
    # The handler's mutation never reached the end hook.
    assert "injected_by_handler" not in plugin.end_inputs[0]


class _OutcomeRecordingPlugin(DurableInstrumentationPlugin):
    """Records the result and error payloads seen on each hook that carries one."""

    def __init__(self) -> None:
        self.operation_ends: list[Any] = []
        self.user_function_ends: list[Any] = []
        self.invocation_ends: list[Any] = []

    def on_operation_end(self, info) -> None:
        self.operation_ends.append(info)

    def on_user_function_end(self, info) -> None:
        self.user_function_ends.append(info)

    def on_invocation_end(self, info) -> None:
        self.invocation_ends.append(info)


def _run_with_plugin(handler: Any, plugin: _OutcomeRecordingPlugin) -> Any:
    wrapped = durable_execution(handler, plugins=[plugin])
    with DurableFunctionTestRunner(handler=wrapped) as runner:
        return runner.run(input="{}", execution_timeout=60)


def test_plugin_sees_operation_result_and_no_error_on_success_end_to_end():
    """A succeeding step reports its serialized result and no error on its hooks."""
    plugin = _OutcomeRecordingPlugin()

    def my_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        return context.step(lambda _: "step-value", name="work")

    result = _run_with_plugin(my_handler, plugin)

    assert result.status is InvocationStatus.SUCCEEDED
    step_ends = [info for info in plugin.operation_ends if info.name == "work"]
    assert len(step_ends) == 1
    assert step_ends[0].status is OperationStatus.SUCCEEDED
    assert step_ends[0].result == '"step-value"'
    assert step_ends[0].error is None
    assert [info.error for info in plugin.user_function_ends] == [None]
    assert plugin.invocation_ends[0].error is None
    assert plugin.invocation_ends[0].execution_result == '"step-value"'


def test_plugin_sees_error_payloads_on_failing_step_end_to_end():
    """A failing step reports its error on the operation, function and invocation hooks."""
    plugin = _OutcomeRecordingPlugin()

    def failing_step(_):
        msg = "step-boom"
        raise ValueError(msg)

    def my_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        return context.step(
            failing_step,
            name="work",
            config=StepConfig(
                retry_strategy=lambda _error, _attempt: RetryDecision.no_retry()
            ),
        )

    result = _run_with_plugin(my_handler, plugin)

    assert result.status is InvocationStatus.FAILED
    step_ends = [info for info in plugin.operation_ends if info.name == "work"]
    assert len(step_ends) == 1
    assert step_ends[0].status is OperationStatus.FAILED
    assert step_ends[0].error is not None
    assert step_ends[0].error.message == "step-boom"
    assert step_ends[0].result is None

    assert len(plugin.user_function_ends) == 1
    assert plugin.user_function_ends[0].error is not None
    assert plugin.user_function_ends[0].error.message == "step-boom"

    invocation_end = plugin.invocation_ends[0]
    assert invocation_end.status.value == InvocationStatus.FAILED.value
    assert invocation_end.error is not None
    assert invocation_end.error.message == "step-boom"
    assert invocation_end.execution_result is None
