"""Integration tests for running handler end to end."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import Mock, patch

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_handler,
)
from aws_durable_execution_sdk_python.lambda_context import LambdaContext
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    LambdaClient,
    OperationAction,
    OperationType,
)
from aws_durable_execution_sdk_python.logger import LoggerInterface

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

    @durable_handler
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
        lambda_context = Mock(spec=LambdaContext)
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

        # 3 START checkpoint, 3 SUCCEED checkpoint
        assert len(checkpoint_calls) == 6

        checkpoint = checkpoint_calls[-1][0]
        assert checkpoint.operation_type is OperationType.STEP
        assert checkpoint.action is OperationAction.SUCCEED
        assert checkpoint.payload == '"from step plain"'


def test_step_with_logger():
    my_logger = Mock(spec=LoggerInterface)

    @durable_step
    def mystep(step_context: StepContext, a: int, b: str) -> str:
        step_context.logger.info("from step %s %s", a, b)
        return "result"

    @durable_handler
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
        lambda_context = Mock(spec=LambdaContext)
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

        # 1 START checkpoint, 1 SUCCEED checkpoint
        assert len(checkpoint_calls) == 2

        checkpoint = checkpoint_calls[0][0]
        assert checkpoint.operation_type == OperationType.STEP
        assert checkpoint.action == OperationAction.START
        assert checkpoint.operation_id == "1"
        # Check the wait checkpoint
        checkpoint = checkpoint_calls[1][0]
        assert checkpoint.operation_type == OperationType.STEP
        assert checkpoint.action == OperationAction.SUCCEED
        assert checkpoint.operation_id == "1"


def test_wait_inside_run_in_childcontext():
    """A wait inside a child context should suspend the execution."""

    mock_inside_child = Mock()

    @durable_with_child_context
    def func(child_context: DurableContext, a: int, b: int):
        mock_inside_child(a, b)
        child_context.wait(1)

    @durable_handler
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
        lambda_context = Mock(spec=LambdaContext)
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

        # Assert that checkpoints were created
        assert len(checkpoint_calls) == 2  # One for child context start, one for wait

        # Check first checkpoint (child context start)
        first_checkpoint = checkpoint_calls[0][0]
        assert first_checkpoint.operation_type is OperationType.CONTEXT
        assert first_checkpoint.action is OperationAction.START
        assert first_checkpoint.operation_id == "1"

        # Check second checkpoint (wait operation)
        second_checkpoint = checkpoint_calls[1][0]
        assert second_checkpoint.operation_type is OperationType.WAIT
        assert second_checkpoint.action is OperationAction.START
        assert second_checkpoint.operation_id == "1-1"
        assert second_checkpoint.wait_options.wait_seconds == 1

        mock_inside_child.assert_called_once_with(10, 20)


class CustomError(Exception):
    """Custom exception for testing."""


def test_wait_not_caught_by_exception():
    """Do not catch Suspend exceptions."""

    @durable_handler
    def my_handler(event: Any, context: DurableContext):
        try:
            context.wait(1)
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
        lambda_context = Mock(spec=LambdaContext)
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

        # Assert that only 1 checkpoint was created for the wait operation
        assert len(checkpoint_calls) == 1

        # Check the wait checkpoint
        checkpoint = checkpoint_calls[0][0]
        assert checkpoint.operation_type is OperationType.WAIT
        assert checkpoint.action is OperationAction.START
        assert checkpoint.operation_id == "1"
        assert checkpoint.wait_options.wait_seconds == 1


def test_lambda_client_initialization():
    """Test initialization of real Lambda client with specified endpoint and region."""
    endpoint = "https://durable.durable-functions.devo.us-west-2.lambda.aws.a2z.com"
    region = "us-west-2"

    client = LambdaClient.initialize_from_endpoint_and_region(endpoint, region)

    assert client is not None
    assert client.client is not None
