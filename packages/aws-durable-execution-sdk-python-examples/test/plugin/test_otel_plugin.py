"""Tests for the OTel plugin example (execution_with_otel)."""

import time
from datetime import UTC, datetime

import pytest

from aws_durable_execution_sdk_python.execution import InvocationStatus
from src.plugin import execution_with_otel
from test.conftest import deserialize_operation_payload


# X-Ray ingestion is eventually consistent; wait before querying so the backend
# has received and indexed the exported spans.
_XRAY_INGESTION_DELAY_SECONDS = 20


def _count_occurrences(text: str, substring: str) -> int:
    """Count non-overlapping occurrences of ``substring`` in ``text``."""
    count = 0
    index = 0
    while (index := text.find(substring, index)) != -1:
        count += 1
        index += len(substring)
    return count


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=execution_with_otel.handler,
    lambda_function_name="Otel Plugin",
)
def test_plugin(durable_runner):
    """Test basic step example."""
    with durable_runner:
        result = durable_runner.run(input="{}", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == 23

    step_result = result.get_step("final-step")
    assert deserialize_operation_payload(step_result.result) == 23


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=execution_with_otel.handler,
    lambda_function_name="Otel Plugin",
)
def test_plugin_spans_in_xray_across_invocations(durable_runner, xray_spans):
    """Multi-invocation example: spans from all invocations share one trace."""
    start_time = datetime.now(UTC)

    with durable_runner:
        result = durable_runner.run(input="{}", timeout=120)

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result) == 23

    # Multi-invocation executions take longer to fully export; give extra time.
    time.sleep(_XRAY_INGESTION_DELAY_SECONDS + 5)

    trace_id, segment_text = xray_spans.fetch_trace_with_span(
        start_time, datetime.now(UTC), marker_span="final-step"
    )

    # Spans from every child context plus the final top-level step.
    for i in range(3):
        assert f"context-{i}" in segment_text, f"missing span context-{i}"
        assert f"step-{i}" in segment_text, f"missing span step-{i}"
        assert f"wait-{i}" in segment_text, f"missing span wait-{i}"
    assert "final-step" in segment_text

    # The waits force multiple Lambda invocations -> multiple invocation spans.
    invocation_count = _count_occurrences(segment_text, "invocation")
    assert invocation_count >= 2, (
        f"Expected at least 2 invocation spans (multi-invocation), "
        f"got {invocation_count}"
    )

    # All segments belong to one trace -> deterministic trace ID worked.
    assert trace_id, "Expected a single unified trace ID across invocations"
