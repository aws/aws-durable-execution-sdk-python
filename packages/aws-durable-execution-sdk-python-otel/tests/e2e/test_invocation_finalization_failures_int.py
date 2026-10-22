"""A deferred completion failure must not discard unrelated invocation telemetry."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
import logging
from threading import Event
from typing import Any
from unittest.mock import Mock, patch

import pytest
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.lambda_service import (
    CallbackDetails,
    ErrorObject,
    ExecutionDetails,
    Operation,
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
)
from opentelemetry import context as otel_context
from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, Span, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPluginFactory,
    InvocationOtelPluginFactory,
    OtelPluginConfig,
)


class _FailCallbackProcessor(SimpleSpanProcessor):
    def __init__(
        self,
        exporter: InMemorySpanExporter,
        invocation_ending: Event,
        permanent: bool,
    ) -> None:
        super().__init__(exporter)
        self.invocation_ending = invocation_ending
        self.permanent = permanent
        self.error = RuntimeError("callback A processor failed before export")
        self.a_starts: list[Span] = []
        self.a_attempts: list[ReadableSpan] = []
        self.failed_after_close: list[bool] = []
        self.during_invocation_end: list[bool] = []

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        if span.name == "a":
            self.a_starts.append(span)
        super().on_start(span, parent_context)

    def on_end(self, span: ReadableSpan) -> None:
        if span.name == "a":
            self.a_attempts.append(span)
            self.during_invocation_end.append(self.invocation_ending.is_set())
            if self.permanent or len(self.a_attempts) == 1:
                current = self.a_starts[-1]
                self.failed_after_close.append(
                    not current.is_recording() and current.end_time is not None
                )
                raise self.error
        super().on_end(span)


def _contains_error(error: BaseException, expected: BaseException) -> bool:
    if error is expected:
        return True
    return isinstance(error, BaseExceptionGroup) and any(
        _contains_error(nested, expected) for nested in error.exceptions
    )


@pytest.mark.parametrize(
    "factory_type", [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]
)
@pytest.mark.parametrize("permanent", [False, True])
def test_deferred_export_failure_preserves_other_completions_and_roots(
    factory_type: type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory],
    permanent: bool,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    caplog.set_level(logging.ERROR)
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    invocation_ending = Event()
    processor = _FailCallbackProcessor(exporter, invocation_ending, permanent)
    provider.add_span_processor(processor)
    invocation_ends: list[InvocationEndInfo] = []

    class Observe(DurableInstrumentationPlugin):
        def on_invocation_end(self, info: InvocationEndInfo) -> None:
            invocation_ends.append(info)
            invocation_ending.set()

    class ObserveFactory:
        def create_plugin(self, info: InvocationStartInfo) -> Observe:
            return Observe()

    factory = factory_type(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    start = datetime.now(UTC) - timedelta(seconds=10)
    callback_error = ErrorObject(
        type="RemoteCallbackError",
        message="callback A failed",
        data="callback details",
        stack_trace=["callback frame"],
    )
    operations = [
        Operation(
            operation_id="execution-id",
            operation_type=OperationType.EXECUTION,
            status=OperationStatus.STARTED,
            start_timestamp=start,
            execution_details=ExecutionDetails(input_payload='{"result":"winner"}'),
        ),
        Operation(
            operation_id="a",
            operation_type=OperationType.CALLBACK,
            name="a",
            status=OperationStatus.FAILED,
            start_timestamp=start + timedelta(seconds=1),
            end_timestamp=start + timedelta(seconds=2),
            callback_details=CallbackDetails(
                callback_id="callback-a", error=callback_error
            ),
        ),
        Operation(
            operation_id="b",
            operation_type=OperationType.CALLBACK,
            name="b",
            status=OperationStatus.SUCCEEDED,
            start_timestamp=start + timedelta(seconds=1),
            end_timestamp=start + timedelta(seconds=2),
            callback_details=CallbackDetails(callback_id="callback-b", result='"B"'),
        ),
    ]
    invocation: dict[str, Any] = {
        "DurableExecutionArn": (
            "arn:aws:lambda:us-west-2:123456789012:function:completion:1"
            "/durable-execution/finalization/execution-id"
        ),
        "CheckpointToken": "resume-token",
        "InitialExecutionState": {
            "Operations": [operation.to_json_dict() for operation in operations],
            "NextMarker": "",
        },
        "UpdatedOperationIds": ["a", "b"],
    }
    original_invocation = deepcopy(invocation)
    lambda_context = Mock(
        aws_request_id="request",
        client_context=None,
        identity=None,
        _epoch_deadline_time_in_ms=0,
        invoked_function_arn="completion:1",
        tenant_id=None,
    )
    handler_calls: list[dict[str, str]] = []

    def body(event: dict[str, str], _context: DurableContext) -> str:
        handler_calls.append(dict(event))
        assert processor.a_attempts == []
        assert not any(
            span.name in ("a", "b") for span in exporter.get_finished_spans()
        )
        return event["result"]

    handler = durable_execution(body, plugins=[ObserveFactory(), factory])
    service = Mock()
    before = otel_context.get_current()
    try:
        with patch(
            "aws_durable_execution_sdk_python.execution.LambdaClient.initialize_client",
            return_value=service,
        ):
            result = handler(invocation, lambda_context)

        assert result == {"Status": "SUCCEEDED", "Result": '"winner"'}
        assert handler_calls == [{"result": "winner"}]
        assert invocation == original_invocation
        service.checkpoint.assert_not_called()
        (end_info,) = invocation_ends
        assert end_info.status is InvocationStatus.SUCCEEDED
        assert end_info.operations["a"].error == callback_error
        assert end_info.operations["b"].result == '"B"'
        spans = exporter.get_finished_spans()
        names = [span.name for span in spans]
        assert (
            names.count("b")
            == names.count("Invocation")
            == names.count("Workflow")
            == 1
        )
        assert names.count("a") == int(not permanent)
        assert len(processor.a_attempts) == 2
        assert processor.during_invocation_end == [True, True]
        assert processor.failed_after_close == [True] * (2 if permanent else 1)

        first_a, retried_a = processor.a_attempts
        assert retried_a.context == first_a.context
        assert retried_a.parent == first_a.parent
        assert retried_a.start_time == first_a.start_time
        assert retried_a.end_time == first_a.end_time
        assert retried_a.attributes == first_a.attributes
        for attempt in processor.a_attempts:
            assert attempt.status.status_code is StatusCode.ERROR
            assert attempt.attributes is not None
            assert attempt.attributes["durable.operation.status"] == "FAILED"
            assert len(attempt.events) == 1
            assert attempt.events[0].attributes is not None
            assert (
                attempt.events[0].attributes["exception.message"]
                == callback_error.message
            )
        if not permanent:
            assert [span for span in spans if span.name == "a"] == [retried_a]

        (b,) = [span for span in spans if span.name == "b"]
        (invocation_span,) = [span for span in spans if span.name == "Invocation"]
        (workflow_span,) = [span for span in spans if span.name == "Workflow"]
        assert b.attributes is not None
        assert b.attributes["durable.operation.id"] == "b"
        assert b.attributes["durable.operation.status"] == "SUCCEEDED"
        assert b.status.status_code is StatusCode.OK
        assert b.events == ()
        assert invocation_span.attributes is not None
        assert invocation_span.attributes["durable.invocation.status"] == "SUCCEEDED"
        assert workflow_span.attributes is not None
        assert workflow_span.attributes["durable.execution.status"] == "SUCCEEDED"
        assert invocation_span.status.status_code is StatusCode.OK
        assert workflow_span.status.status_code is StatusCode.OK
        expected_parent = (
            workflow_span
            if factory_type is ExecutionOtelPluginFactory
            else invocation_span
        )
        assert expected_parent.context is not None
        for callback in (b, retried_a):
            assert callback.parent == expected_parent.context
            assert callback.context is not None
            assert callback.context.trace_id == expected_parent.context.trace_id
        assert any(
            record.exc_info is not None
            and record.exc_info[1] is not None
            and _contains_error(record.exc_info[1], processor.error)
            for record in caplog.records
        )
        assert otel_context.get_current() == before
    finally:
        provider.shutdown()
