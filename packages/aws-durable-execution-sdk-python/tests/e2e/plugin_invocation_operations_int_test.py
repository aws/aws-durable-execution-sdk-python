"""Integration tests for the plugin invocation operation maps.

Exercises `InvocationInfo.operations` and
`InvocationStartInfo.updated_operations` through complete
`durable_execution()` invocations -- across the decorator, invocation-input
parsing, the checkpoint path and the invocation hooks -- including a
suspend/replay pair where the replay reports the externally-completed wait via
`UpdatedOperationIds`.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock, patch

from aws_durable_execution_sdk_python.config import Duration
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
from tests.test_helpers import operation_id_sequence


class _MapRecordingPlugin(DurableInstrumentationPlugin):
    """Records the operation maps seen on each invocation hook."""

    def __init__(self) -> None:
        self.starts: list[tuple[list[str], list[str]]] = []
        self.ends: list[tuple[str, list[str]]] = []

    def on_invocation_start(self, info) -> None:
        self.starts.append((sorted(info.operations), sorted(info.updated_operations)))

    def on_invocation_end(self, info) -> None:
        self.ends.append((info.status.value, sorted(info.operations)))


def _lambda_context() -> Mock:
    ctx = Mock()
    ctx.aws_request_id = "test-request-id"
    ctx.client_context = None
    ctx.identity = None
    ctx._epoch_deadline_time_in_ms = 0  # noqa: SLF001
    ctx.invoked_function_arn = "test-arn"
    ctx.tenant_id = None
    return ctx


def _event(
    extra_operations: list[dict] | None = None,
    updated_operation_ids: list[str] | None = None,
) -> dict:
    execution_operation = {
        "Id": "execution-1",
        "Type": "EXECUTION",
        "Status": "STARTED",
        "ExecutionDetails": {"InputPayload": '{"name": "World"}'},
    }
    event: dict[str, Any] = {
        "DurableExecutionArn": "test-arn/execution-1",
        "CheckpointToken": "test-token",
        "InitialExecutionState": {
            "Operations": [execution_operation, *(extra_operations or [])],
            "NextMarker": "",
        },
        "LocalRunner": True,
    }
    if updated_operation_ids is not None:
        event["UpdatedOperationIds"] = updated_operation_ids
    return event


def _tracking_checkpoint(initial_operations: list[Operation] | None = None):
    """Checkpoint mock that accumulates operations, as the service would."""
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


def test_operation_maps_on_a_completing_invocation():
    """The start map holds the prior state; the end map sees the step added."""
    plugin = _MapRecordingPlugin()

    @durable_execution(plugins=[plugin])
    def my_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        return context.step(lambda _ctx: "stepped", name="greet")

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client.checkpoint = _tracking_checkpoint()
        mock_client_class.initialize_client.return_value = mock_client

        result = my_handler(_event(), _lambda_context())

    assert result["Status"] == InvocationStatus.SUCCEEDED.value

    # First invocation: only the EXECUTION operation exists at start, and
    # nothing was updated externally.
    (start_operations, start_updated) = plugin.starts[0]
    assert start_operations == ["execution-1"]
    assert start_updated == []

    # The end hook re-reads the map, so it sees the step checkpointed during
    # this invocation -- that is the point of re-reading rather than reusing
    # the start snapshot.
    status, end_operations = plugin.ends[0]
    assert status == InvocationStatus.SUCCEEDED.value
    assert len(end_operations) > len(start_operations)
    assert "execution-1" in end_operations


def test_operation_maps_across_suspend_and_replay():
    """The replay start hook reports the externally-completed wait.

    Invocation 1 suspends on a wait. Invocation 2 replays with the wait already
    SUCCEEDED and its id in ``UpdatedOperationIds``, which is exactly what
    ``updated_operations`` is derived from.
    """
    wait_id = next(operation_id_sequence())

    # --- Invocation 1: the wait starts and the execution suspends.
    first = _MapRecordingPlugin()

    @durable_execution(plugins=[first])
    def suspending_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        context.wait(Duration.from_seconds(60))
        return "done"

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client.checkpoint = _tracking_checkpoint()
        mock_client_class.initialize_client.return_value = mock_client

        first_result = suspending_handler(_event(), _lambda_context())

    assert first_result["Status"] == InvocationStatus.PENDING.value
    start_operations, start_updated = first.starts[0]
    assert start_operations == ["execution-1"]
    assert start_updated == []
    # The suspending end hook already sees the wait that was just checkpointed.
    _, end_operations = first.ends[0]
    assert wait_id in end_operations

    # --- Invocation 2: replay with the wait completed externally.
    replay = _MapRecordingPlugin()

    @durable_execution(plugins=[replay])
    def replayed_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        context.wait(Duration.from_seconds(60))
        return "done"

    completed_wait = {
        "Id": wait_id,
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

        replay_result = replayed_handler(
            _event(extra_operations=[completed_wait], updated_operation_ids=[wait_id]),
            _lambda_context(),
        )

    assert replay_result["Status"] == InvocationStatus.SUCCEEDED.value

    start_operations, start_updated = replay.starts[0]
    # The replay start map carries the prior state, including the wait.
    assert sorted(["execution-1", wait_id]) == start_operations
    # And updated_operations is the UpdatedOperationIds subset of it.
    assert start_updated == [wait_id]

    status, end_operations = replay.ends[0]
    assert status == InvocationStatus.SUCCEEDED.value
    assert wait_id in end_operations


def test_updated_operations_ignores_ids_absent_from_the_map():
    """An id the execution state does not carry must not appear in the subset."""
    plugin = _MapRecordingPlugin()

    @durable_execution(plugins=[plugin])
    def my_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        return "ok"

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client.checkpoint = _tracking_checkpoint()
        mock_client_class.initialize_client.return_value = mock_client

        my_handler(
            _event(updated_operation_ids=["execution-1", "never-checkpointed"]),
            _lambda_context(),
        )

    _, start_updated = plugin.starts[0]
    assert start_updated == ["execution-1"]


def test_plugin_free_execution_still_completes():
    """The provider gate must not disturb an execution without plugins."""

    @durable_execution
    def my_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        return "ok"

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client.checkpoint = _tracking_checkpoint()
        mock_client_class.initialize_client.return_value = mock_client

        result = my_handler(_event(), _lambda_context())

    assert result["Status"] == InvocationStatus.SUCCEEDED.value
