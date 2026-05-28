"""Tests for handler_error."""

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus

from src.handler_error import handler_error


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=handler_error.handler,
    lambda_function_name="handler error",
)
def test_handle_handler_errors_gracefully_and_capture_error_details(durable_runner):
    """Test that handler errors are handled gracefully and error details are captured."""
    test_payload = {"test": "error-case"}

    with durable_runner:
        result = durable_runner.run(input=test_payload, timeout=10)

    # Verify execution failed
    assert result.status is InvocationStatus.FAILED

    # Check that error was captured in the result
    error = result.error
    assert error is not None

    assert error.message == "Intentional handler failure"
    assert error.type == "Exception"

    # Verify no operations were completed due to early error
    assert len(result.operations) == 0
