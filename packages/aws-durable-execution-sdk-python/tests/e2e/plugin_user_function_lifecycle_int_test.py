"""End-to-end coverage for user-function plugin lifecycle callbacks."""

from __future__ import annotations

import dataclasses
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
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
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    UserFunctionEndInfo,
    UserFunctionOutcome,
    UserFunctionStartInfo,
)


@dataclass(frozen=True)
class _LifecycleEvent:
    phase: str
    operation_id: str
    name: str | None
    outcome: UserFunctionOutcome | None
    thread_id: int


class _LifecycleRecordingPlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        self.events: list[_LifecycleEvent] = []

    def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
        self.events.append(
            _LifecycleEvent(
                phase="start",
                operation_id=info.operation_id,
                name=info.name,
                outcome=None,
                thread_id=threading.get_ident(),
            )
        )

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        self.events.append(
            _LifecycleEvent(
                phase="end",
                operation_id=info.operation_id,
                name=info.name,
                outcome=info.outcome,
                thread_id=threading.get_ident(),
            )
        )


def _lambda_context() -> Mock:
    context = Mock()
    context.aws_request_id = "test-request-id"
    context.client_context = None
    context.identity = None
    context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
    context.invoked_function_arn = "test-arn"
    context.tenant_id = None
    return context


def _event(
    extra_operations: Sequence[Mapping[str, Any]] | None = None,
    updated_operation_ids: list[str] | None = None,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "DurableExecutionArn": "test-arn/execution-1",
        "CheckpointToken": "test-token",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "execution-1",
                    "Type": OperationType.EXECUTION.value,
                    "Status": OperationStatus.STARTED.value,
                    "ExecutionDetails": {"InputPayload": "{}"},
                },
                *(extra_operations or []),
            ],
            "NextMarker": "",
        },
        "LocalRunner": True,
    }
    if updated_operation_ids is not None:
        event["UpdatedOperationIds"] = updated_operation_ids
    return event


def _tracking_checkpoint(
    initial_operations: list[Operation] | None = None,
) -> tuple[Any, list[Operation]]:
    operations = list(initial_operations or [])
    if not operations:
        operations.append(
            Operation(
                operation_id="execution-1",
                operation_type=OperationType.EXECUTION,
                status=OperationStatus.STARTED,
            )
        )

    def checkpoint(
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
            checkpoint_token="new-token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=operations.copy()
            ),
        )

    return checkpoint, operations


def test_child_user_function_lifecycle_across_suspend_and_replay() -> None:
    """A child reports INCOMPLETE on suspend and SUCCEEDED after replay."""
    plugin = _LifecycleRecordingPlugin()
    child_threads: list[int] = []

    def child_function(context: DurableContext) -> str:
        child_threads.append(threading.get_ident())
        context.wait(Duration.from_seconds(60))
        return "charged"

    def user_handler(event: Any, context: DurableContext) -> str:  # noqa: ARG001
        return context.run_in_child_context(child_function, name="charge")

    handler = durable_execution(user_handler, plugins=[plugin])

    first_checkpoint, first_operations = _tracking_checkpoint()
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client.checkpoint = first_checkpoint
        mock_client_class.initialize_client.return_value = mock_client
        first_result = handler(_event(), _lambda_context())

    assert first_result["Status"] == InvocationStatus.PENDING.value
    assert [(event.phase, event.name, event.outcome) for event in plugin.events] == [
        ("start", "charge", None),
        ("end", "charge", UserFunctionOutcome.INCOMPLETE),
    ]
    first_start, first_end = plugin.events
    assert first_start.operation_id == first_end.operation_id
    assert first_start.thread_id == first_end.thread_id == child_threads[0]

    child_operation = next(
        operation
        for operation in first_operations
        if operation.operation_type is OperationType.CONTEXT
    )
    wait_operation = next(
        operation
        for operation in first_operations
        if operation.operation_type is OperationType.WAIT
    )
    assert wait_operation.parent_id == child_operation.operation_id

    replay_operations = [
        dataclasses.replace(operation, status=OperationStatus.SUCCEEDED)
        if operation.operation_type is OperationType.WAIT
        else operation
        for operation in first_operations
    ]
    replay_event_operations = [
        operation.to_dict()
        for operation in replay_operations
        if operation.operation_type is not OperationType.EXECUTION
    ]
    replay_checkpoint, _ = _tracking_checkpoint(replay_operations)

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client.checkpoint = replay_checkpoint
        mock_client_class.initialize_client.return_value = mock_client
        replay_result = handler(
            _event(
                extra_operations=replay_event_operations,
                updated_operation_ids=[wait_operation.operation_id],
            ),
            _lambda_context(),
        )

    assert replay_result["Status"] == InvocationStatus.SUCCEEDED.value
    assert [(event.phase, event.name, event.outcome) for event in plugin.events] == [
        ("start", "charge", None),
        ("end", "charge", UserFunctionOutcome.INCOMPLETE),
        ("start", "charge", None),
        ("end", "charge", UserFunctionOutcome.SUCCEEDED),
    ]
    replay_start, replay_end = plugin.events[2:]
    assert replay_start.operation_id == first_start.operation_id
    assert replay_end.operation_id == first_start.operation_id
    assert replay_start.thread_id == replay_end.thread_id == child_threads[1]
    assert len(child_threads) == 2
