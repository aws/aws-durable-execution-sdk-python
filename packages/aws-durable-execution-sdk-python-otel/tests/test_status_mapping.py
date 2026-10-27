"""Shared status regressions through both public plugin lifecycles."""

from collections.abc import Iterator
from dataclasses import replace
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
    UserFunctionEndInfo,
    UserFunctionOutcome,
    UserFunctionStartInfo,
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
from aws_durable_execution_sdk_python_otel.exception_events import (
    exception_from_error_object,
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
    list(OperationStatus),
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
        or status
        in {OperationStatus.FAILED, OperationStatus.TIMED_OUT, OperationStatus.STOPPED}
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


@pytest.mark.parametrize("start_time", [None, START, END])
@pytest.mark.parametrize("end_time", [END, None])
def test_terminal_exception_uses_normalized_span_end(
    instrumented_view: InstrumentedView,
    start_time: datetime | None,
    end_time: datetime | None,
) -> None:
    """Delayed and checkpointless completions retain bounded diagnostic times."""
    plugin, exporter = instrumented_view
    _start_invocation(plugin)
    plugin.on_operation_end(
        replace(
            _operation_end_info(
                ErrorObject("stored failure", "ValueError", None, None)
            ),
            operation_type=OperationType.CONTEXT,
            sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
            start_time=start_time,
            end_time=end_time,
        )
    )
    _end_invocation(plugin)
    span = next(s for s in exporter.get_finished_spans() if s.name == "wait")
    (event,) = span.events
    assert span.start_time is not None and span.end_time is not None
    assert span.start_time <= event.timestamp == span.end_time


@pytest.mark.parametrize("with_error", [False, True])
@pytest.mark.parametrize("end_time", [START, END, None])
def test_failed_attempt_exception_uses_normalized_span_end(
    instrumented_view: InstrumentedView,
    with_error: bool,
    end_time: datetime | None,
) -> None:
    """An attempt failure reports its outcome without fabricating diagnostics."""
    plugin, exporter = instrumented_view
    _start_invocation(plugin)
    operation: dict[str, Any] = dict(
        operation_id="attempt",
        operation_type=OperationType.STEP,
        sub_type=OperationSubType.STEP,
        name="attempt",
        parent_id=None,
        start_time=START,
        is_replayed=False,
        status=OperationStatus.STARTED,
        is_replay_children=False,
        attempt=1,
    )
    plugin.on_user_function_start(UserFunctionStartInfo(**operation))
    plugin.on_user_function_end(
        UserFunctionEndInfo(
            **operation,
            outcome=UserFunctionOutcome.FAILED,
            end_time=end_time,
            error=ErrorObject("stored failure", "ValueError", None, None)
            if with_error
            else None,
        )
    )
    _end_invocation(plugin)
    span = next(
        s for s in exporter.get_finished_spans() if s.name == "attempt attempt 1"
    )
    assert span.status.status_code is StatusCode.ERROR
    if with_error:
        (event,) = span.events
        assert span.start_time is not None and span.end_time is not None
        assert span.start_time <= event.timestamp == span.end_time
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


def _start_invocation(plugin: Any) -> None:
    plugin.on_invocation_start(
        InvocationStartInfo(
            request_id="request-1",
            execution_arn=ARN,
            execution_start_time=START,
            is_first_invocation=True,
        )
    )


def _end_invocation(plugin: Any) -> None:
    plugin.on_invocation_end(
        InvocationEndInfo(
            request_id="request-1",
            execution_arn=ARN,
            execution_start_time=START,
            is_first_invocation=True,
            status=InvocationStatus.SUCCEEDED,
        )
    )


def _operation_end_info(
    error: ErrorObject | None, *, is_replayed: bool = False
) -> OperationEndInfo:
    return OperationEndInfo(
        operation_id="wait",
        name="wait",
        parent_id=None,
        operation_type=OperationType.WAIT,
        sub_type=OperationSubType.WAIT,
        start_time=START,
        is_replayed=is_replayed,
        end_time=END,
        status=OperationStatus.FAILED,
        error=error,
    )


def _run_failed_step_attempt(
    plugin: Any, error: ErrorObject | None, *, is_replayed: bool = False
) -> None:
    operation: dict[str, Any] = dict(
        operation_id="step-1",
        operation_type=OperationType.STEP,
        sub_type=OperationSubType.STEP,
        name="step-1",
        parent_id=None,
        start_time=START,
        is_replayed=is_replayed,
        status=OperationStatus.STARTED,
        is_replay_children=False,
        attempt=1,
    )
    plugin.on_user_function_start(UserFunctionStartInfo(**operation))
    plugin.on_user_function_end(
        UserFunctionEndInfo(
            **operation,
            outcome=UserFunctionOutcome.FAILED,
            end_time=END,
            error=error,
        )
    )


def _exception_attributes(span: Any) -> Any:
    assert len(span.events) == 1
    assert span.events[0].name == "exception"
    assert span.events[0].attributes is not None
    return span.events[0].attributes


CASES = [
    ("ValueError", "example failure"),
    ("KeyError", "missing key"),
    ("TimeoutError", "deadline exceeded"),
    ("custompkg.NotARealError", "boom"),
    ("ExceptionGroup", "group failure (1 sub-exception)"),
    ("BaseExceptionGroup", "base group failure (1 sub-exception)"),
]


@pytest.mark.parametrize("is_replayed", [False, True])
@pytest.mark.parametrize(("error_type", "message"), CASES)
def test_operation_exception_event_preserves_original_type(
    instrumented_view: InstrumentedView,
    error_type: str,
    message: str,
    is_replayed: bool,
) -> None:
    """The operation exception event must keep the SDK-supplied error type."""
    plugin, exporter = instrumented_view
    _start_invocation(plugin)
    stack_trace = ["Traceback from checkpoint", f"{error_type}: {message}"]
    plugin.on_operation_end(
        _operation_end_info(
            ErrorObject(message, error_type, None, stack_trace),
            is_replayed=is_replayed,
        )
    )
    _end_invocation(plugin)
    if is_replayed and isinstance(plugin, ExecutionOtelPlugin):
        # The execution view suppresses duplicate logical operation spans.
        assert all(s.name != "wait" for s in exporter.get_finished_spans())
        return
    span = next(s for s in exporter.get_finished_spans() if s.name == "wait")
    attributes = _exception_attributes(span)
    assert attributes["exception.type"] == error_type
    assert attributes["exception.message"] == message
    assert attributes["exception.stacktrace"] == "\n".join(stack_trace)


@pytest.mark.parametrize("is_replayed", [False, True])
@pytest.mark.parametrize(("error_type", "message"), CASES)
def test_attempt_exception_event_preserves_original_type(
    instrumented_view: InstrumentedView,
    error_type: str,
    message: str,
    is_replayed: bool,
) -> None:
    """A failed STEP attempt event must keep the SDK-supplied error type."""
    plugin, exporter = instrumented_view
    _start_invocation(plugin)
    stack_trace = ["Traceback from checkpoint", f"{error_type}: {message}"]
    _run_failed_step_attempt(
        plugin,
        ErrorObject(message, error_type, None, stack_trace),
        is_replayed=is_replayed,
    )
    _end_invocation(plugin)
    span = next(
        s for s in exporter.get_finished_spans() if s.name == "step-1 attempt 1"
    )
    attributes = _exception_attributes(span)
    assert attributes["exception.type"] == error_type
    assert attributes["exception.message"] == message
    assert attributes["exception.stacktrace"] == "\n".join(stack_trace)


def test_replay_attempt_event_uses_stored_error_metadata(
    instrumented_view: InstrumentedView,
) -> None:
    """A replayed attempt must recover type/message/stack from checkpoint data."""
    plugin, exporter = instrumented_view
    _start_invocation(plugin)
    stack_trace = [
        "Traceback (most recent call last):",
        '  File "handler.py", line 1, in handler',
        "ValueError: example failure",
    ]
    _run_failed_step_attempt(
        plugin,
        ErrorObject("example failure", "ValueError", None, stack_trace),
        is_replayed=True,
    )
    _end_invocation(plugin)
    span = next(
        s for s in exporter.get_finished_spans() if s.name == "step-1 attempt 1"
    )
    attributes = _exception_attributes(span)
    assert attributes["exception.type"] == "ValueError"
    assert attributes["exception.message"] == "example failure"
    assert attributes["exception.stacktrace"] == "\n".join(stack_trace)


def test_failed_operation_without_type_falls_back(
    instrumented_view: InstrumentedView,
) -> None:
    """Without type metadata the event still carries the original message."""
    plugin, exporter = instrumented_view
    _start_invocation(plugin)
    plugin.on_operation_end(
        _operation_end_info(ErrorObject("no type here", None, None, None))
    )
    _end_invocation(plugin)
    span = next(s for s in exporter.get_finished_spans() if s.name == "wait")
    attributes = _exception_attributes(span)
    assert attributes["exception.message"] == "no type here"
    assert attributes["exception.type"] == "Exception"


@pytest.mark.parametrize("error_type", [None, ""])
@pytest.mark.parametrize("message", [None, ""])
def test_exception_from_error_object_uses_fallback_message(
    error_type: str | None,
    message: str | None,
) -> None:
    """A missing or empty type with an empty message uses the default."""
    error = ErrorObject(message, error_type, None, None)
    assert str(exception_from_error_object(error)) == "Unknown error"


def test_failed_operation_without_type_or_message_uses_fallback(
    instrumented_view: InstrumentedView,
) -> None:
    """Without type or message metadata the event uses the default message."""
    plugin, exporter = instrumented_view
    _start_invocation(plugin)
    plugin.on_operation_end(_operation_end_info(ErrorObject(None, None, None, None)))
    _end_invocation(plugin)
    span = next(s for s in exporter.get_finished_spans() if s.name == "wait")
    attributes = _exception_attributes(span)
    assert attributes["exception.message"] == "Unknown error"
    assert attributes["exception.type"] == "Exception"


class ConstructorSideEffectError(Exception):
    constructor_calls = 0

    def __init__(self, message: str) -> None:
        type(self).constructor_calls += 1
        super().__init__(message)


@pytest.mark.parametrize("is_replayed", [False, True])
@pytest.mark.parametrize("event_kind", ["operation", "attempt"])
def test_exception_event_does_not_repeat_custom_constructor(
    instrumented_view: InstrumentedView,
    monkeypatch: pytest.MonkeyPatch,
    is_replayed: bool,
    event_kind: str,
) -> None:
    """Exporting stored error metadata must not rerun application code."""
    monkeypatch.setattr(ConstructorSideEffectError, "constructor_calls", 0)
    original = ConstructorSideEffectError("custom failure")
    error_type = f"{__name__}.ConstructorSideEffectError"
    stack_trace = ["Traceback from checkpoint", f"{error_type}: {original}"]
    error = ErrorObject(str(original), error_type, None, stack_trace)
    plugin, exporter = instrumented_view
    _start_invocation(plugin)
    if event_kind == "operation":
        plugin.on_operation_end(_operation_end_info(error, is_replayed=is_replayed))
        span_name = "wait"
    else:
        _run_failed_step_attempt(plugin, error, is_replayed=is_replayed)
        span_name = "step-1 attempt 1"
    _end_invocation(plugin)
    assert ConstructorSideEffectError.constructor_calls == 1
    if (
        is_replayed
        and event_kind == "operation"
        and isinstance(plugin, ExecutionOtelPlugin)
    ):
        assert all(s.name != span_name for s in exporter.get_finished_spans())
        return
    span = next(s for s in exporter.get_finished_spans() if s.name == span_name)
    attributes = _exception_attributes(span)
    assert attributes["exception.type"] == error_type
    assert attributes["exception.message"] == str(original)
    assert attributes["exception.stacktrace"] == "\n".join(stack_trace)
