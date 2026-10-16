"""Shared status regressions through both public plugin lifecycles."""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationStatus,
    OperationSubType,
)
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
    OperationEndInfo,
    OperationStartInfo,
    OperationType,
)
from opentelemetry import context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


START = datetime(2026, 10, 1, tzinfo=UTC)
END = START + timedelta(seconds=1)
ARN = (
    "arn:aws:lambda:us-west-2:123456789012:function:status:1/durable-execution/test/id"
)


InstrumentedView = tuple[
    ExecutionOtelPlugin | InvocationOtelPlugin, InMemorySpanExporter
]


@pytest.fixture(params=[ExecutionOtelPlugin, InvocationOtelPlugin])
def instrumented_view(request: pytest.FixtureRequest) -> Iterator[InstrumentedView]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = request.param(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    original = context.get_current()
    yield plugin, exporter
    assert context.get_current() == original
    provider.shutdown()


@pytest.mark.parametrize(
    "status",
    [
        OperationStatus.SUCCEEDED,
        OperationStatus.FAILED,
        OperationStatus.CANCELLED,
        OperationStatus.TIMED_OUT,
        OperationStatus.STOPPED,
    ],
)
@pytest.mark.parametrize("with_error", [False, True])
@pytest.mark.parametrize("started_here", [False, True])
def test_operation_status_and_exception(
    instrumented_view: InstrumentedView,
    status: OperationStatus,
    with_error: bool,
    started_here: bool,
) -> None:
    """A delivered terminal hook must not label an unsuccessful operation OK."""
    plugin, exporter = instrumented_view
    invocation: dict[str, Any] = dict(
        request_id="first",
        execution_arn=ARN,
        execution_start_time=START,
        is_first_invocation=True,
    )
    plugin.on_invocation_start(InvocationStartInfo(**invocation))
    operation: dict[str, Any] = dict(
        operation_id="wait",
        name="wait",
        parent_id=None,
        operation_type=OperationType.WAIT,
        sub_type=OperationSubType.WAIT,
        start_time=START,
        is_replayed=False,
    )
    if started_here:
        plugin.on_operation_start(
            OperationStartInfo(**operation, status=OperationStatus.STARTED)
        )
    error = (
        ErrorObject(
            message="deadline exceeded",
            type="TimeoutError",
            data=None,
            stack_trace=None,
        )
        if with_error
        else None
    )
    plugin.on_operation_end(
        OperationEndInfo(**operation, end_time=END, status=status, error=error)
    )
    plugin.on_invocation_end(
        InvocationEndInfo(**invocation, status=InvocationStatus.SUCCEEDED)
    )
    span = next(s for s in exporter.get_finished_spans() if s.name == "wait")
    assert span.attributes is not None
    assert span.attributes["durable.operation.status"] == status.value
    expected = (
        StatusCode.ERROR
        if with_error
        else (
            StatusCode.OK if status is OperationStatus.SUCCEEDED else StatusCode.UNSET
        )
    )
    assert span.status.status_code is expected
    if with_error:
        assert span.status.description == "deadline exceeded"
        assert len(span.events) == 1
        assert span.events[0].name == "exception"
        assert span.events[0].attributes is not None
        assert span.events[0].attributes["exception.message"] == "deadline exceeded"
    else:
        assert not span.events


def test_retry_then_resume_exports_retrying(
    instrumented_view: InstrumentedView,
) -> None:
    """Telemetry normalizes RETRY without changing the core enum or next invocation."""
    plugin, exporter = instrumented_view
    original = context.get_current()
    for index, status in enumerate(
        [InvocationStatus.RETRY, InvocationStatus.SUCCEEDED]
    ):
        invocation: dict[str, Any] = dict(
            request_id=f"request-{index}",
            execution_arn=ARN,
            execution_start_time=START,
            is_first_invocation=index == 0,
        )
        plugin.on_invocation_start(InvocationStartInfo(**invocation))
        plugin.on_invocation_end(InvocationEndInfo(**invocation, status=status))
        assert context.get_current() == original
    spans = [s for s in exporter.get_finished_spans() if s.name == "Invocation"]
    assert [
        s.attributes["durable.invocation.status"]
        for s in spans
        if s.attributes is not None
    ] == [
        "RETRYING",
        "SUCCEEDED",
    ]
    assert [s.status.status_code for s in spans] == [StatusCode.UNSET, StatusCode.OK]
    assert InvocationStatus.RETRY.value == "RETRY"
    assert spans[0].context is not None
    assert spans[1].context is not None
    assert spans[0].context.trace_id == spans[1].context.trace_id
