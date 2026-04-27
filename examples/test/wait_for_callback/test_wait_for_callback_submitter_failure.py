"""Tests for wait_for_callback_submitter_retry_success."""

import json

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.wait_for_callback import (
    wait_for_callback_submitter_failure,
)


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=wait_for_callback_submitter_failure.handler,
    lambda_function_name="Wait For Callback Submitter Failure",
)
def test_fail_after_exhausting_retries_when_submitter_always_fails(durable_runner):
    """Test that execution fails after exhausting retries when submitter always fails."""
    test_payload = {"shouldFail": True}

    with durable_runner:
        execution_arn = durable_runner.run_async(input=test_payload, timeout=30)
        result = durable_runner.wait_for_result(execution_arn=execution_arn)

    # Execution should fail after retries are exhausted
    assert result.status is InvocationStatus.FAILED

    # Verify error details
    error = result.error
    assert error is not None
    assert "Simulated submitter failure" in error.message
