"""Integration tests for the typed per-operation error hierarchy.

These exercise the full handler flow end-to-end (rather than mocking the
executor seams) to cover the two directions unit tests mock away:

- serialization: a failing operation checkpoints a FAIL carrying the typed
  discriminator, and the invocation result surfaces the typed error.
- reconstruction: a pre-seeded FAILED checkpoint resurfaces as the typed error
  on replay without re-executing the operation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import pytest

from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)
from tests.e2e.checkpoint_response_int_test import (
    create_mock_checkpoint_with_operations,
)
from tests.test_helpers import operation_id_sequence

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.types import StepContext


def _lambda_context() -> Mock:
    lambda_context = Mock()
    lambda_context.aws_request_id = "test-request-id"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 0  # noqa: SLF001
    lambda_context.invoked_function_arn = "test-arn"
    lambda_context.tenant_id = None
    return lambda_context


def test_step_failure_surfaces_typed_step_error():
    """A failing step records the raw error at its own level and surfaces a
    typed StepError to the caller (serialization path).

    The step's FAIL checkpoint records the raw escaping error ("ValueError");
    the StepError wrapper is recorded one level up (the execution result).
    """

    def failing_step(_step_context: StepContext) -> str:
        msg = "step boom"
        raise ValueError(msg)

    @durable_execution
    def my_handler(event, context: DurableContext) -> str:
        # Single attempt so the step fails without scheduling retries.
        config = StepConfig(
            retry_strategy=create_retry_strategy(RetryStrategyConfig(max_attempts=1))
        )
        return context.step(failing_step, name="charge", config=config)

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        mock_checkpoint, checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
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

        result = my_handler(event, _lambda_context())

        # Execution failed; the result (one level up from the step) carries the
        # StepError wrapper, and StepError.error_type is the original error type.
        assert result["Status"] == InvocationStatus.FAILED.value
        assert result["Error"]["ErrorType"] == "StepError"
        assert result["Error"]["ErrorMessage"] == "step boom"

        # The step's own FAIL checkpoint records the raw escaping error type.
        all_operations = [op for batch in checkpoint_calls for op in batch]
        fail_updates = [
            op
            for op in all_operations
            if hasattr(op, "action") and op.action.value == "FAIL"
        ]
        assert len(fail_updates) == 1
        assert fail_updates[0].error is not None
        assert fail_updates[0].error.type == "ValueError"


def test_failed_step_reconstructs_step_error_on_replay():
    """A pre-seeded FAILED step checkpoint resurfaces as a typed StepError on
    replay without re-executing the step (reconstruction path)."""
    executed = {"count": 0}

    def maybe_step(_step_context: StepContext) -> str:
        executed["count"] += 1
        return "should-not-run"

    @durable_execution
    def my_handler(event, context: DurableContext) -> str:
        return context.step(maybe_step, name="charge")

    # The id the first top-level step will look up on replay.
    step_id = next(operation_id_sequence())

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        mock_checkpoint, _checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    },
                    {
                        "Id": step_id,
                        "Type": "STEP",
                        "Status": "FAILED",
                        "SubType": "Step",
                        "ParentId": "execution-1",
                        # A failed step records the raw escaping error type.
                        "StepDetails": {
                            "Error": {
                                "ErrorType": "ValueError",
                                "ErrorMessage": "prior boom",
                            }
                        },
                    },
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        result = my_handler(event, _lambda_context())

        # Reconstructed as a StepError with the checkpointed message; the step
        # body is never re-executed.
        assert result["Status"] == InvocationStatus.FAILED.value
        assert result["Error"]["ErrorType"] == "StepError"
        assert result["Error"]["ErrorMessage"] == "prior boom"
        assert executed["count"] == 0


@pytest.mark.parametrize(
    ("checkpointed_type", "message"),
    [
        ("CallbackExternalError", "external boom"),
        ("CallbackTimeoutError", "timed out"),
        ("CallbackError", "internal callback boom"),
    ],
)
def test_failed_callback_reconstructs_typed_error_on_replay(
    checkpointed_type: str, message: str
):
    """A pre-seeded FAILED wait_for_callback context resurfaces the typed callback
    error on replay (the CallbackError family passes through unchanged), without
    re-running the submitter (reconstruction path)."""
    submitter_runs = {"count": 0}

    def submitter(_callback_id, _ctx) -> None:
        submitter_runs["count"] += 1

    @durable_execution
    def my_handler(event, context: DurableContext) -> str:
        return context.wait_for_callback(submitter, name="await-external")

    # The wait_for_callback child context is the first top-level operation.
    wfc_id = next(operation_id_sequence())

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        mock_checkpoint, _checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    },
                    {
                        "Id": wfc_id,
                        "Type": "CONTEXT",
                        "Status": "FAILED",
                        "SubType": "WaitForCallback",
                        "ParentId": "execution-1",
                        "ContextDetails": {
                            "Error": {
                                "ErrorType": checkpointed_type,
                                "ErrorMessage": message,
                            }
                        },
                    },
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        result = my_handler(event, _lambda_context())

        assert result["Status"] == InvocationStatus.FAILED.value
        assert result["Error"]["ErrorType"] == checkpointed_type
        assert result["Error"]["ErrorMessage"] == message
        # Replay short-circuits from the FAILED checkpoint; the submitter never runs.
        assert submitter_runs["count"] == 0


def test_failed_submitter_reconstructs_callback_submitter_error_on_replay():
    """A failed submitter records StepError at the context level but surfaces
    CallbackSubmitterError to the caller on replay (console-parity contract)."""
    submitter_runs = {"count": 0}

    def submitter(_callback_id, _ctx) -> None:
        submitter_runs["count"] += 1

    @durable_execution
    def my_handler(event, context: DurableContext) -> str:
        return context.wait_for_callback(submitter, name="await-external")

    wfc_id = next(operation_id_sequence())

    with patch(
        "aws_durable_execution_sdk_python.execution.LambdaClient"
    ) as mock_client_class:
        mock_client = Mock()
        mock_client_class.initialize_client.return_value = mock_client

        mock_checkpoint, _checkpoint_calls = create_mock_checkpoint_with_operations()
        mock_client.checkpoint = mock_checkpoint

        event = {
            "DurableExecutionArn": "test-arn/execution-1",
            "CheckpointToken": "test-token",
            "InitialExecutionState": {
                "Operations": [
                    {
                        "Id": "execution-1",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "ExecutionDetails": {"InputPayload": "{}"},
                    },
                    {
                        "Id": wfc_id,
                        "Type": "CONTEXT",
                        "Status": "FAILED",
                        "SubType": "WaitForCallback",
                        "ParentId": "execution-1",
                        "ContextDetails": {
                            "Error": {
                                "ErrorType": "StepError",
                                "ErrorMessage": "submitter boom",
                            }
                        },
                    },
                ],
                "NextMarker": "",
            },
            "LocalRunner": True,
        }

        result = my_handler(event, _lambda_context())

        assert result["Status"] == InvocationStatus.FAILED.value
        assert result["Error"]["ErrorType"] == "CallbackSubmitterError"
        assert result["Error"]["ErrorMessage"] == "submitter boom"
        assert submitter_runs["count"] == 0
