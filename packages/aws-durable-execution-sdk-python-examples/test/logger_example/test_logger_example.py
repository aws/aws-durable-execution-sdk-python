"""Tests for logger_example."""

import logging

import pytest

from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationType
from src.logger_example import logger_example
from test.conftest import deserialize_operation_payload


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=logger_example.handler,
    lambda_function_name="logger example",
)
def test_logger_example(durable_runner):
    """Test logger example."""
    with durable_runner:
        result = durable_runner.run(input={"id": "test-123"}, timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == "processed-child-processed"

    # Verify step operations exist (process_data at top level)
    # Note: child_step is nested inside the CONTEXT operation, not at top level
    step_ops = [
        op for op in result.operations if op.operation_type == OperationType.STEP
    ]
    assert len(step_ops) >= 1

    # Verify context operation exists (child_workflow)
    context_ops = [
        op for op in result.operations if op.operation_type.value == "CONTEXT"
    ]
    assert len(context_ops) >= 1


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=logger_example.handler,
    lambda_function_name="logger example",
)
def test_logger_example_emits_expected_logs(durable_runner, caplog):
    """Verify the durable logger emits the expected messages and enriched extras.

    The SDK logger wraps the stdlib root logger, so ``caplog`` captures every
    record emitted from both the top-level context and from inside steps. Step
    logs are enriched with the operation name and attempt number, which we
    assert on via the LogRecord attributes.

    Log capture only works in local mode (the handler runs in-process); in cloud
    mode the handler runs in a deployed Lambda, so this test is skipped there.
    """
    if durable_runner.mode != "local":
        pytest.skip("Log capture is only available in local (in-process) mode")

    with caplog.at_level(logging.INFO):
        with durable_runner:
            result = durable_runner.run(input={"id": "test-123"}, timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED

    messages = [record.getMessage() for record in caplog.records]
    # Top-level context logs (no step_id) and in-step logs both appear.
    assert "Starting workflow" in messages
    assert "Hello from my_step" in messages
    assert "Workflow completed" in messages

    # The warning emitted inside my_step is enriched with the step's extra
    # (my_arg) and with the operation name the SDK attaches automatically.
    warning = next(
        record
        for record in caplog.records
        if record.getMessage() == "Warning from my_step"
    )
    assert warning.levelno == logging.WARNING
    assert warning.my_arg == 123
    assert warning.operationName == "my_step"
