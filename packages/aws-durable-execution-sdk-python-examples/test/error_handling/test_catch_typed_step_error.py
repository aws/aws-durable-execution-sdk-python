"""Tests for catch_typed_step_error."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import (
    OperationStatus,
    OperationType,
)

from src.error_handling import catch_typed_step_error
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=catch_typed_step_error.handler,
    lambda_function_name="Catch Typed Step Error",
)
def test_catch_typed_step_error(durable_runner):
    """A failed step surfaces as a catchable StepError, identical across replay.

    The handler catches the StepError and then crosses a wait/replay boundary, so
    the returned ``outcome`` is produced on the resume (replay) invocation, while
    the STEP FAILED checkpoint was written on the first invocation. Asserting the
    surfaced error matches the persisted checkpoint error shows the typed error is
    reconstructed identically across replays.
    """
    with durable_runner:
        result = durable_runner.run(input=None, timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    # The error caught on the replay pass (returned as the execution result).
    result_data = deserialize_operation_payload(result.result)
    assert result_data == {
        "handled": True,
        "caught": "StepError",
        "error_type": "ValueError",
        "message": "payment declined",
    }

    # A WAIT operation proves the execution suspended and replayed after the
    # error was caught, so the result above reflects the replay invocation.
    wait_ops = [
        op for op in result.operations if op.operation_type == OperationType.WAIT
    ]
    assert len(wait_ops) >= 1

    # The step's own checkpoint (written on the first run) is FAILED and records
    # the raw escaping error type.
    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) == 1
    checkpoint_error = step_ops[0].error
    assert step_ops[0].status is OperationStatus.FAILED
    assert checkpoint_error is not None

    # Cross-replay identity: the error surfaced on the replay pass (result_data)
    # matches the error persisted on the first run (the checkpoint).
    assert result_data["error_type"] == checkpoint_error.type == "ValueError"
    assert result_data["message"] == checkpoint_error.message == "payment declined"
