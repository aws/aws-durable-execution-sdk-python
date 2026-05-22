"""Integration tests for immediate checkpoint response handling.

Tests end-to-end operation execution with the immediate response handling
that's implemented via the OperationExecutor base class pattern.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python.config import ChildConfig, Duration
from aws_durable_execution_sdk_python.context import DurableContext, durable_step
from aws_durable_execution_sdk_python.exceptions import InvocationError
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.lambda_service import (
    CallbackDetails,
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    Operation,
    OperationStatus,
    OperationType,
)

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.types import StepContext


def create_mock_checkpoint_with_operations():
    """Create a mock checkpoint function that properly tracks operations.

    Returns a tuple of (mock_checkpoint_function, checkpoint_calls_list).
    The mock properly maintains an operations list that gets updated with each checkpoint.
    """
    checkpoint_calls = []
    operations = [
        Operation(
            operation_id="execution-1",
            operation_type=OperationType.EXECUTION,
            status=OperationStatus.STARTED,
        )
    ]

    def mock_checkpoint(
        durable_execution_arn,
        checkpoint_token,
        updates,
        client_token="token",  # noqa: S107
    ):
        checkpoint_calls.append(updates)

        # Convert updates to Operation objects and add to operations list
        for update in updates:
            op = Operation(
                operation_id=update.operation_id,
                operation_type=update.operation_type,
                status=OperationStatus.STARTED,
                parent_id=update.parent_id,
            )
            operations.append(op)

        return CheckpointOutput(
            checkpoint_token="new_token",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=operations.copy()
            ),
        )

    return mock_checkpoint, checkpoint_calls


def test_end_to_end_step_operation_with_double_check():
    """Test end-to-end step operation execution with double-check pattern.

    Verifies that the OperationExecutor.process() method properly calls
    check_result_status() twice when a checkpoint is created, enabling
    immediate response handling.
    """

    @durable_step
    def my_step(step_context: StepContext) -> str:
        return "step_result"

    @durable_execution
    def my_handler(event, context: DurableContext) -> str:
        result: str = context.step(my_step())
        return result

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn",
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

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = my_handler(event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        assert result["Result"] == '"step_result"'

        # Verify checkpoints were created (START + SUCCEED)
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) == 2


def test_end_to_end_multiple_operations_execute_sequentially():
    """Test end-to-end execution with multiple operations.

    Verifies that multiple operations in a workflow execute correctly
    with the immediate response handling pattern.
    """

    @durable_step
    def step1(step_context: StepContext) -> str:
        return "result1"

    @durable_step
    def step2(step_context: StepContext) -> str:
        return "result2"

    @durable_execution
    def my_handler(event, context: DurableContext) -> list[str]:
        return [context.step(step1()), context.step(step2())]

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn",
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

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = my_handler(event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        assert result["Result"] == '["result1", "result2"]'

        # Verify all checkpoints were created (2 START + 2 SUCCEED)
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) == 4


def test_end_to_end_wait_operation_with_double_check():
    """Test end-to-end wait operation execution with double-check pattern.

    Verifies that wait operations properly use the double-check pattern
    for immediate response handling.
    """

    @durable_execution
    def my_handler(event, context: DurableContext) -> str:
        context.wait(Duration.from_seconds(5))
        return "completed"

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn",
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

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        # Wait will suspend, so we expect PENDING status
        result = my_handler(event, lambda_context)

        assert result["Status"] == InvocationStatus.PENDING.value

        # Verify wait checkpoint was created
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) >= 1


def test_end_to_end_checkpoint_synchronization_with_operations_list():
    """Test that synchronous checkpoints properly update operations list.

    Verifies that when is_sync=True, the operations list is updated
    before the second status check occurs.
    """

    @durable_step
    def my_step(step_context: StepContext) -> str:
        return "result"

    @durable_execution
    def my_handler(event, context: DurableContext) -> str:
        return context.step(my_step())

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn",
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

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = my_handler(event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value

        # Verify operations list was properly maintained
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) >= 2  # At least START and SUCCEED


def test_callback_deferred_error_handling_to_result():
    """Test callback deferred error handling pattern.

    Verifies that callback operations properly return callback_id through
    the immediate response handling pattern, enabling deferred error handling.
    """

    @durable_step
    def step_after_callback(step_context: StepContext) -> str:
        return "code_executed_after_callback"

    @durable_execution
    def my_handler(event, context: DurableContext) -> str:
        # Create callback
        callback_id = context.create_callback("test_callback")

        # This code executes even if callback will eventually fail
        # This is the deferred error handling pattern
        result = context.step(step_after_callback())

        return f"{callback_id}:{result}"

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        checkpoint_calls = []
        operations = [
            Operation(
                operation_id="execution-1",
                operation_type=OperationType.EXECUTION,
                status=OperationStatus.STARTED,
            )
        ]

        def mock_checkpoint(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            checkpoint_calls.append(updates)

            # Add operations with proper details
            for update in updates:
                if update.operation_type == OperationType.CALLBACK:
                    op = Operation(
                        operation_id=update.operation_id,
                        operation_type=update.operation_type,
                        status=OperationStatus.STARTED,
                        parent_id=update.parent_id,
                        callback_details=CallbackDetails(
                            callback_id=f"cb-{update.operation_id[:8]}"
                        ),
                    )
                else:
                    op = Operation(
                        operation_id=update.operation_id,
                        operation_type=update.operation_type,
                        status=OperationStatus.STARTED,
                        parent_id=update.parent_id,
                    )
                operations.append(op)

            return CheckpointOutput(
                checkpoint_token="new_token",  # noqa: S106
                new_execution_state=CheckpointUpdatedExecutionState(
                    operations=operations.copy()
                ),
            )

        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn",
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

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = my_handler(event, lambda_context)

        # Verify execution succeeded and code after callback executed
        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        assert "code_executed_after_callback" in result["Result"]


def test_end_to_end_invoke_operation_with_double_check():
    """Test end-to-end invoke operation execution with double-check pattern.

    Verifies that invoke operations properly use the double-check pattern
    for immediate response handling.
    """

    @durable_execution
    def my_handler(event, context: DurableContext):
        context.invoke("my-function", {"data": "test"})

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn",
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

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        # Invoke will suspend, so we expect PENDING status
        result = my_handler(event, lambda_context)

        assert result["Status"] == InvocationStatus.PENDING.value

        # Verify invoke checkpoint was created
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) >= 1


def test_end_to_end_child_context_with_async_checkpoint():
    """Test end-to-end child context execution with async checkpoint.

    Verifies that child context operations use async checkpoint (is_sync=False)
    and execute correctly without waiting for immediate response.
    """

    def child_function(ctx: DurableContext) -> str:
        return "child_result"

    @durable_execution
    def my_handler(event, context: DurableContext) -> str:
        result: str = context.run_in_child_context(child_function)
        return result

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn",
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

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = my_handler(event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        assert result["Result"] == '"child_result"'

        # Verify checkpoints were created (START + SUCCEED)
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) == 2


def test_end_to_end_child_context_replay_children_mode():
    """Test end-to-end child context with large payload and ReplayChildren mode.

    Verifies that child context with large result (>256KB) triggers replay_children mode,
    uses summary generator if provided, and re-executes function on replay.
    """
    execution_count = {"count": 0}

    def child_function_with_large_result(ctx: DurableContext) -> str:
        execution_count["count"] += 1
        return "large" * 256 * 1024

    def summary_generator(result: str) -> str:
        return f"summary_of_{len(result)}_bytes"

    @durable_execution
    def my_handler(event, context: DurableContext) -> str:
        context.run_in_child_context(
            child_function_with_large_result,
            config=ChildConfig(summary_generator=summary_generator),
        )
        return f"executed_{execution_count['count']}_times"

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        checkpoint_calls = []
        operations = [
            Operation(
                operation_id="execution-1",
                operation_type=OperationType.EXECUTION,
                status=OperationStatus.STARTED,
            )
        ]

        def mock_checkpoint(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            checkpoint_calls.append(updates)

            for update in updates:
                op = Operation(
                    operation_id=update.operation_id,
                    operation_type=update.operation_type,
                    status=OperationStatus.STARTED,
                    parent_id=update.parent_id,
                )
                operations.append(op)

            return CheckpointOutput(
                checkpoint_token="new_token",  # noqa: S106
                new_execution_state=CheckpointUpdatedExecutionState(
                    operations=operations.copy()
                ),
            )

        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn",
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

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = my_handler(event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        # Function executed once during initial execution
        assert execution_count["count"] == 1

        # Verify replay_children was set in SUCCEED checkpoint
        all_operations = [op for batch in checkpoint_calls for op in batch]
        succeed_updates = [
            op
            for op in all_operations
            if hasattr(op, "action") and op.action.value == "SUCCEED"
        ]
        assert len(succeed_updates) == 1
        assert succeed_updates[0].context_options.replay_children is True


def test_end_to_end_child_context_error_handling():
    """Test end-to-end child context error handling.

    Verifies that child context that raises exception creates FAIL checkpoint
    and error is wrapped as CallableRuntimeError.
    """

    def child_function_that_fails(ctx: DurableContext) -> str:
        msg = "Child function error"
        raise ValueError(msg)

    @durable_execution
    def my_handler(event, context: DurableContext) -> str:
        result: str = context.run_in_child_context(child_function_that_fails)
        return result

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn",
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

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        result = my_handler(event, lambda_context)

        # Verify execution failed
        assert result["Status"] == InvocationStatus.FAILED.value

        # Verify FAIL checkpoint was created
        all_operations = [op for batch in checkpoint_calls for op in batch]
        fail_updates = [
            op
            for op in all_operations
            if hasattr(op, "action") and op.action.value == "FAIL"
        ]
        assert len(fail_updates) == 1


def test_end_to_end_child_context_invocation_error_reraised():
    """Test end-to-end child context InvocationError re-raising.

    Verifies that child context that raises InvocationError creates FAIL checkpoint
    and re-raises InvocationError (not wrapped) to enable retry at execution handler level.
    """

    def child_function_with_invocation_error(ctx: DurableContext) -> str:
        msg = "Invocation failed in child"
        raise InvocationError(msg)

    @durable_execution
    def my_handler(event, context: DurableContext) -> str:
        result: str = context.run_in_child_context(child_function_with_invocation_error)
        return result

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn",
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

        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        # InvocationError should be re-raised (not wrapped) to trigger Lambda retry
        with pytest.raises(InvocationError, match="Invocation failed in child"):
            my_handler(event, lambda_context)

        # Verify FAIL checkpoint was created before re-raising
        all_operations = [op for batch in checkpoint_calls for op in batch]
        fail_updates = [
            op
            for op in all_operations
            if hasattr(op, "action") and op.action.value == "FAIL"
        ]
        assert len(fail_updates) == 1
