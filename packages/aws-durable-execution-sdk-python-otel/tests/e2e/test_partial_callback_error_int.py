"""A partial callback error must not gain invented OTel exception fields."""

from __future__ import annotations

import json
import time
from typing import Any

import pytest
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.exceptions import CallbackError
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPluginFactory,
    InvocationOtelPluginFactory,
    OtelPluginConfig,
)


CALLBACK_NAME = "partial-error"
CALLBACK_SPAN = f"{CALLBACK_NAME} create callback id"
SUBMITTER_SPAN = f"{CALLBACK_NAME} submitter"

VIEWS = [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]

# Each case supplies exactly one of the two fields the exception event carries.
PARTIAL_ERRORS: dict[str, tuple[ErrorObject, dict[str, str]]] = {
    "message-only": (
        ErrorObject(message="rejected", type=None, data=None, stack_trace=None),
        {"exception.message": "rejected"},
    ),
    "type-only": (
        ErrorObject(message=None, type="RejectedError", data=None, stack_trace=None),
        {"exception.type": "RejectedError"},
    ),
}


def _exception_events(spans: list[ReadableSpan], name: str) -> list[Any]:
    """Return the exception events recorded on every segment of a span."""
    matching = [span for span in spans if span.name == name]
    assert matching, f"no {name!r} span was exported"
    events = [event for span in matching for event in span.events]
    assert events, f"no exception event on {name!r}"
    assert all(event.name == "exception" for event in events)
    assert all(
        span.status.status_code is StatusCode.ERROR for span in matching if span.events
    )
    return events


def _run(
    view: type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory],
    error: ErrorObject,
    *,
    complete_in_submitter: bool,
    catch: bool,
) -> tuple[Any, list[ReadableSpan], list[str]]:
    """Fail one callback with a partial error and return the exported spans."""
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    factory = view(
        OtelPluginConfig(
            tracer_provider=provider,
            enrich_logger=False,
            context_extractor=lambda _: None,
        )
    )
    submitter_calls: list[str] = []
    # The submitter needs the runner, which only exists once the handler is built.
    runner_box: list[DurableFunctionTestRunner] = []

    def saved_step(_context: Any) -> str:
        return "saved"

    def submitter(callback_id: str, _context: Any) -> None:
        submitter_calls.append(callback_id)
        if complete_in_submitter:
            # The failure is already stored when the callback is awaited, so the
            # invocation that created the callback also observes its outcome.
            runner_box[0].send_callback_failure(callback_id, error)

    def body(_event: Any, context: DurableContext) -> str:
        saved = context.step(saved_step, name="saved-step")
        try:
            context.wait_for_callback(submitter, name=CALLBACK_NAME)
        except CallbackError:
            if not catch:
                raise
            return saved
        raise AssertionError("Expected the callback failure")

    store = InMemoryExecutionStore()
    try:
        with DurableFunctionTestRunner(
            handler=durable_execution(body, plugins=[factory]),
            store=store,
            poll_interval=0.01,
        ) as runner:
            runner_box.append(runner)
            arn = runner.run_async(input="{}")
            if not complete_in_submitter:
                callback_id = runner.wait_for_callback(
                    arn, name=CALLBACK_SPAN, timeout=10
                )
                deadline = time.monotonic() + 10
                while not store.load(arn).invocation_completions:
                    assert time.monotonic() < deadline, "Invocation did not suspend"
                    time.sleep(0.01)
                runner.send_callback_failure(callback_id, error)
            result = runner.wait_for_result(arn, timeout=20)
        return result, list(exporter.get_finished_spans()), submitter_calls
    finally:
        provider.shutdown()


@pytest.mark.parametrize("view", VIEWS)
@pytest.mark.parametrize("error_kind", list(PARTIAL_ERRORS))
@pytest.mark.parametrize("complete_in_submitter", [False, True])
def test_partial_callback_error_omits_the_absent_field(
    view: type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory],
    error_kind: str,
    complete_in_submitter: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The event carries the supplied field and nothing invented in its place."""
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    error, expected = PARTIAL_ERRORS[error_kind]
    result, spans, submitter_calls = _run(
        view, error, complete_in_submitter=complete_in_submitter, catch=True
    )

    # A handler that catches the failure still succeeds.
    assert result.status.value == "SUCCEEDED"
    assert result.result is not None and json.loads(result.result) == "saved"
    # The submitter is checkpointed, so it runs once even across a resume.
    assert len(submitter_calls) == 1
    submitter_spans = [s for s in spans if s.name.startswith(SUBMITTER_SPAN)]
    assert submitter_spans
    assert all(s.status.status_code is not StatusCode.ERROR for s in submitter_spans)

    for event in _exception_events(spans, CALLBACK_SPAN):
        assert event.attributes is not None
        assert dict(event.attributes) == expected


@pytest.mark.parametrize("view", VIEWS)
@pytest.mark.parametrize("error_kind", list(PARTIAL_ERRORS))
def test_partial_callback_error_preserves_failure_statuses(
    view: type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory],
    error_kind: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Omitting a field must not soften the statuses an uncaught failure sets."""
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    error, expected = PARTIAL_ERRORS[error_kind]
    result, spans, _ = _run(view, error, complete_in_submitter=False, catch=False)

    assert result.status.value == "FAILED"
    for event in _exception_events(spans, CALLBACK_SPAN):
        assert event.attributes is not None
        assert dict(event.attributes) == expected
    workflow = [s for s in spans if s.name == "Workflow"]
    assert workflow
    assert all(s.status.status_code is StatusCode.ERROR for s in workflow)
