"""Integration tests for running handler end to end."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)

# LambdaContext no longer needed - using duck typing
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    OperationAction,
    OperationType,
)
from aws_durable_execution_sdk_python.logger import LoggerInterface
from tests.test_helpers import operation_id_sequence

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.types import StepContext


def test_step_different_ways_to_pass_args():
    def step_plain(step_context: StepContext) -> str:
        return "from step plain"

    @durable_step
    def step_no_args(step_context: StepContext) -> str:
        return "from step no args"

    @durable_step
    def step_with_args(step_context: StepContext, a: int, b: str) -> str:
        return f"from step {a} {b}"

    @durable_execution
    def my_handler(event, context: DurableContext) -> list[str]:
        results: list[str] = []
        result: str = context.step(step_with_args(a=123, b="str"))
        assert result == "from step 123 str"
        results.append(result)

        result = context.step(step_no_args())
        assert result == "from step no args"
        results.append(result)

        # note this won't work:
        # result: str = context.step(step_no_args)

        result = context.step(step_plain)
        assert result == "from step plain"
        results.append(result)

        return results

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_local_runner_client.return_value = mock_client

        # Mock the checkpoint method to track calls
        checkpoint_calls = []

        def mock_checkpoint(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            checkpoint_calls.append(updates)

            return CheckpointOutput(
                checkpoint_token="new_token",  # noqa: S106
                new_execution_state=CheckpointUpdatedExecutionState(),
            )

        mock_client.checkpoint = mock_checkpoint

        # Create test event
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

        # Create mock lambda context
        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        # Execute the handler
        result = my_handler(event, lambda_context)

        assert result["Status"] == InvocationStatus.SUCCEEDED.value
        assert (
            result["Result"]
            == '["from step 123 str", "from step no args", "from step plain"]'
        )

        # 3 START checkpoint, 3 SUCCEED checkpoint (batched together)
        # Flatten all operations from all batches
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) == 6

        # Check the last operation
        last_checkpoint = all_operations[-1]
        assert last_checkpoint.operation_type is OperationType.STEP
        assert last_checkpoint.action is OperationAction.SUCCEED
        assert last_checkpoint.payload == '"from step plain"'


def test_step_with_logger():
    my_logger = Mock(spec=LoggerInterface)

    @durable_step
    def mystep(step_context: StepContext, a: int, b: str) -> str:
        step_context.logger.info("from step %s %s", a, b)
        return "result"

    @durable_execution
    def my_handler(event, context: DurableContext):
        context.set_logger(my_logger)
        result: str = context.step(mystep(a=123, b="str"))
        assert result == "result"

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_local_runner_client.return_value = mock_client

        # Mock the checkpoint method to track calls
        checkpoint_calls = []

        def mock_checkpoint(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            checkpoint_calls.append(updates)

            return CheckpointOutput(
                checkpoint_token="new_token",  # noqa: S106
                new_execution_state=CheckpointUpdatedExecutionState(),
            )

        mock_client.checkpoint = mock_checkpoint

        # Create test event
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

        # Create mock lambda context
        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        # Execute the handler
        result = my_handler(event, lambda_context)

        my_logger.info.assert_called_once_with(
            "from step %s %s",
            123,
            "str",
            extra={"execution_arn": "test-arn", "name": "mystep"},
        )

        assert result["Status"] == InvocationStatus.SUCCEEDED.value

        # 1 START checkpoint, 1 SUCCEED checkpoint (batched together)
        # Flatten all operations from all batches
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) == 2
        operation_id = next(operation_id_sequence())

        # Check the START operation
        start_op = all_operations[0]
        assert start_op.operation_type == OperationType.STEP
        assert start_op.action == OperationAction.START
        assert start_op.operation_id == operation_id
        # Check the SUCCEED operation
        succeed_op = all_operations[1]
        assert succeed_op.operation_type == OperationType.STEP
        assert succeed_op.action == OperationAction.SUCCEED
        assert succeed_op.operation_id == operation_id


def test_wait_inside_run_in_childcontext():
    """A wait inside a child context should suspend the execution."""

    mock_inside_child = Mock()

    @durable_with_child_context
    def func(child_context: DurableContext, a: int, b: int):
        mock_inside_child(a, b)
        child_context.wait(Duration.from_seconds(1))

    @durable_execution
    def my_handler(event, context):
        context.run_in_child_context(func(10, 20))

    # Mock the lambda client
    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_local_runner_client.return_value = mock_client

        # Mock the checkpoint method to track calls
        checkpoint_calls = []

        def mock_checkpoint(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            checkpoint_calls.append(updates)

            return CheckpointOutput(
                checkpoint_token="new_token",  # noqa: S106
                new_execution_state=CheckpointUpdatedExecutionState(),
            )

        mock_client.checkpoint = mock_checkpoint

        # Create test event
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

        # Create mock lambda context
        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        # Execute the handler
        result = my_handler(event, lambda_context)

        # Assert the execution returns PENDING status
        assert result["Status"] == InvocationStatus.PENDING.value

        # Assert that checkpoints were created (may be batched together)
        # Flatten all operations from all batches
        all_operations = [op for batch in checkpoint_calls for op in batch]
        assert len(all_operations) == 2  # One for child context start, one for wait

        expected_parent_id = next(operation_id_sequence())
        expected_child_id = next(operation_id_sequence(expected_parent_id))

        # Check first operation (child context start)
        first_checkpoint = all_operations[0]
        assert first_checkpoint.operation_type is OperationType.CONTEXT
        assert first_checkpoint.action is OperationAction.START
        assert first_checkpoint.operation_id == expected_parent_id

        # Check second operation (wait operation)
        second_checkpoint = all_operations[1]
        assert second_checkpoint.operation_type is OperationType.WAIT
        assert second_checkpoint.action is OperationAction.START
        assert second_checkpoint.operation_id == expected_child_id
        assert second_checkpoint.wait_options.wait_seconds == 1

        assert second_checkpoint.operation_id != first_checkpoint.operation_id

        mock_inside_child.assert_called_once_with(10, 20)


class CustomError(Exception):
    """Custom exception for testing."""


def test_step_checkpoint_failure_propagates_error():
    """Test that errors during checkpoint invocation propagate correctly from background thread.

    This test demonstrates a bug: when a checkpoint fails in the background thread,
    the user code thread is blocked waiting on completion_event.wait() with no timeout.
    The background thread exception is raised, but the user thread never completes,
    causing the execution to hang indefinitely.
    """

    @durable_step
    def failing_step(step_context: StepContext) -> str:
        return "this should checkpoint but fail"

    @durable_execution
    def my_handler(event, context: DurableContext):
        # This step will trigger a checkpoint that fails
        result: str = context.step(failing_step())
        return result

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_local_runner_client.return_value = mock_client

        # Mock the checkpoint method to raise an error (using RuntimeError as a generic exception)
        def mock_checkpoint_failure(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            # Simulate a failure during checkpoint invocation
            msg = "Checkpoint service unavailable"
            raise RuntimeError(msg)

        mock_client.checkpoint = mock_checkpoint_failure

        # Create test event
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

        # Create mock lambda context
        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        # Execute the handler - should propagate the checkpoint error
        # The background thread error should propagate and raise
        with pytest.raises(RuntimeError, match="Checkpoint service unavailable"):
            my_handler(event, lambda_context)


def test_wait_not_caught_by_exception():
    """Do not catch Suspend exceptions."""

    @durable_execution
    def my_handler(event: Any, context: DurableContext):
        try:
            context.wait(Duration.from_seconds(1))
        except Exception as err:
            msg = "This should not be caught"
            raise CustomError(msg) from err

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_local_runner_client.return_value = mock_client

        # Mock the checkpoint method to track calls
        checkpoint_calls = []

        def mock_checkpoint(
            durable_execution_arn,
            checkpoint_token,
            updates,
            client_token="token",  # noqa: S107
        ):
            checkpoint_calls.append(updates)

            return CheckpointOutput(
                checkpoint_token="new_token",  # noqa: S106
                new_execution_state=CheckpointUpdatedExecutionState(),
            )

        mock_client.checkpoint = mock_checkpoint

        # Create test event
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

        # Create mock lambda context
        lambda_context = Mock()
        lambda_context.aws_request_id = "test-request-id"
        lambda_context.client_context = None
        lambda_context.identity = None
        lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
        lambda_context.invoked_function_arn = "test-arn"
        lambda_context.tenant_id = None

        # Execute the handler
        result = my_handler(event, lambda_context)
        operation_ids = operation_id_sequence()

        # Assert the execution returns PENDING status
        assert result["Status"] == InvocationStatus.PENDING.value

        # Assert that only 1 checkpoint was created for the wait operation
        assert len(checkpoint_calls) == 1

        # Check the wait checkpoint
        checkpoint = checkpoint_calls[0][0]
        assert checkpoint.operation_type is OperationType.WAIT
        assert checkpoint.action is OperationAction.START
        assert checkpoint.operation_id == next(operation_ids)
        assert checkpoint.wait_options.wait_seconds == 1
