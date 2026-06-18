"""Tests for the OTel-enriched logger example."""

import time
from datetime import UTC, datetime

import pytest

from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import OperationType
from src.otel import otel_logger_example
from test.conftest import deserialize_operation_payload


# X-Ray ingestion is eventually consistent; wait before querying so the backend
# has received and indexed the exported spans.
_XRAY_INGESTION_DELAY_SECONDS = 20


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=otel_logger_example.handler,
    lambda_function_name="Otel Logger Example",
)
def test_otel_logger_example(durable_runner):
    """Verify the OTel logger example runs and produces the expected result."""
    with durable_runner:
        result = durable_runner.run(input="{}", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == "hello world | hello nested"

    # The top-level step is named "top-greet".
    top_step = result.get_step("top-greet")
    assert deserialize_operation_payload(top_step.result) == "hello world"

    # The child context wraps a nested step, so a CONTEXT operation exists.
    context_ops = [
        op for op in result.operations if op.operation_type is OperationType.CONTEXT
    ]
    assert len(context_ops) >= 1


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=otel_logger_example.handler,
    lambda_function_name="Otel Logger Example",
)
def test_otel_logger_example_spans_in_xray(durable_runner, xray_spans):
    """Single-invocation example: spans land in one X-Ray trace.

    Runs only in cloud mode;
    """
    start_time = datetime.now(UTC)

    with durable_runner:
        result = durable_runner.run(input="{}", timeout=60)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == "hello world | hello nested"

    # Allow X-Ray time to ingest the exported spans.
    time.sleep(_XRAY_INGESTION_DELAY_SECONDS)

    _trace_id, segment_text = xray_spans.fetch_trace_with_span(
        start_time, datetime.now(UTC), marker_span="top-greet"
    )

    # Expected spans for the single-invocation example.
    assert "invocation" in segment_text
    assert "top-greet" in segment_text
    assert "child-context" in segment_text
    assert "child-greet" in segment_text
