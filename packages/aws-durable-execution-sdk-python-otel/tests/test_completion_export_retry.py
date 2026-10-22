"""A failed completion remains retryable until its span successfully ends."""

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
    OperationChangeInfo,
    OperationEndInfo,
    OperationStartInfo,
    OperationStatus,
    OperationType,
    UserFunctionEndInfo,
    UserFunctionStartInfo,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


class _FailOnceProcessor(SimpleSpanProcessor):
    def __init__(self, exporter, operation_id):
        super().__init__(exporter)
        self.operation_id = operation_id
        self.error = RuntimeError("one-time processor failure")
        self.failed = False

    def on_end(self, span):
        if (
            span.attributes.get("durable.operation.id") == self.operation_id
            and not self.failed
        ):
            self.failed = True
            raise self.error
        super().on_end(span)


def _setup(view, failing_operation=None):
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    processor = (
        _FailOnceProcessor(exporter, failing_operation)
        if failing_operation is not None
        else SimpleSpanProcessor(exporter)
    )
    provider.add_span_processor(processor)
    plugin = view(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    invocation = dict(
        request_id="retry-completion",
        execution_arn="execution/retry-completion",
        execution_start_time=datetime.now(UTC),
        is_first_invocation=False,
    )
    plugin.on_invocation_start(InvocationStartInfo(**invocation))
    return plugin, provider, exporter, invocation, processor


def _completion(
    operation_id="operation",
    parent_id=None,
    operation_type=OperationType.CALLBACK,
    error=None,
    end_time=None,
):
    now = end_time or datetime.now(UTC)
    return OperationEndInfo(
        operation_id=operation_id,
        operation_type=operation_type,
        sub_type=None,
        name=operation_id,
        parent_id=parent_id,
        start_time=now - timedelta(seconds=1),
        end_time=now,
        is_replayed=False,
        status=OperationStatus.FAILED if error else OperationStatus.SUCCEEDED,
        error=error,
    )


def _spans(exporter, operation_id="operation"):
    return [
        span
        for span in exporter.get_finished_spans()
        if span.attributes.get("durable.operation.id") == operation_id
    ]


@pytest.mark.parametrize("view", [InvocationOtelPlugin, ExecutionOtelPlugin])
@pytest.mark.parametrize("failure", ["start", "end", "processor"])
@pytest.mark.parametrize("retry", ["notification", "invocation-end"])
def test_failed_completion_retries_without_poisoning_success_ledger(
    view, failure, retry, monkeypatch
):
    plugin, provider, exporter, invocation, processor = _setup(
        view, "operation" if failure == "processor" else None
    )
    error = (
        processor.error
        if failure == "processor"
        else RuntimeError("one-time span failure")
    )
    info = _completion(
        error=ErrorObject(
            type="ValueError",
            message="operation failed",
            data=None,
            stack_trace=["frame"],
        )
    )
    original_start = plugin._start_span
    injected = False
    attempted_end_times = []

    def fail_once_start(*args, **kwargs):
        nonlocal injected
        if failure == "start" and not injected:
            injected = True
            raise error
        span = original_start(*args, **kwargs)
        if failure == "end":
            original_end = span.end

            def fail_once_end(*end_args, **end_kwargs):
                nonlocal injected
                attempted_end_times.append(end_kwargs["end_time"])
                if not injected:
                    injected = True
                    raise error
                return original_end(*end_args, **end_kwargs)

            monkeypatch.setattr(span, "end", fail_once_end)
        return span

    monkeypatch.setattr(plugin, "_start_span", fail_once_start)
    try:
        with pytest.raises(RuntimeError) as caught:
            plugin.on_operation_end(info)
        assert caught.value is error
        assert "operation" not in plugin._ended_operation_ids
        assert "operation" not in plugin._ending_operation_ids
        assert plugin._pending_completions["operation"] == info
        assert _spans(exporter) == []
        original_end_time = (
            attempted_end_times[0]
            if failure == "end"
            else plugin._get_span("operation").end_time
            if failure == "processor"
            else None
        )
        if original_end_time is not None:
            # An unrelated completion advances invocation-local ordering. It
            # must not inflate the failed operation's retained duration.
            plugin.on_operation_end(
                _completion("unrelated", end_time=info.end_time + timedelta(seconds=5))
            )
        if retry == "notification":
            plugin.on_operation_end(info)
            assert "operation" in plugin._ended_operation_ids
            assert "operation" not in plugin._pending_completions
            plugin.on_operation_end(info)
            plugin.on_operation_change(
                OperationChangeInfo(
                    execution_arn=invocation["execution_arn"],
                    operations={"operation": info},
                    updated_operations={"operation": info},
                )
            )
        plugin.on_invocation_end(
            InvocationEndInfo(**invocation, status=InvocationStatus.SUCCEEDED)
        )
        spans = _spans(exporter)
        assert len(spans) == 1
        if original_end_time is not None:
            assert spans[0].end_time == original_end_time
        assert spans[0].status.status_code is StatusCode.ERROR
        assert spans[0].attributes["durable.operation.status"] == "FAILED"
        exceptions = [event for event in spans[0].events if event.name == "exception"]
        assert len(exceptions) == 1
        assert exceptions[0].attributes["exception.message"] == "operation failed"
        assert spans[0].start_time <= exceptions[0].timestamp <= spans[0].end_time
        assert not plugin._pending_completions
        assert not plugin._ending_operation_ids
        assert not plugin._completion_end_timestamps
    finally:
        if plugin._tracing_enabled:
            plugin.on_invocation_end(
                InvocationEndInfo(**invocation, status=InvocationStatus.SUCCEEDED)
            )
        provider.shutdown()


@pytest.mark.parametrize("view", [InvocationOtelPlugin, ExecutionOtelPlugin])
def test_concurrent_completion_cannot_overtake_failed_attempt_then_retry(
    view, monkeypatch
):
    plugin, provider, exporter, invocation, _ = _setup(view)
    entered, release = threading.Event(), threading.Event()
    original_start = plugin._start_span
    attempts = []
    error = RuntimeError("first attempt failed")

    def gated_start(*args, **kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            entered.set()
            assert release.wait(5)
            raise error
        return original_start(*args, **kwargs)

    monkeypatch.setattr(plugin, "_start_span", gated_start)
    info = _completion()
    try:
        with ThreadPoolExecutor(max_workers=2) as callers:
            first = callers.submit(plugin.on_operation_end, info)
            try:
                assert entered.wait(5)
                duplicate = callers.submit(plugin.on_operation_end, info)
                duplicate.result(timeout=5)
                assert attempts == [1]
            finally:
                release.set()
            with pytest.raises(RuntimeError) as caught:
                first.result(timeout=5)
            assert caught.value is error
        assert "operation" not in plugin._ended_operation_ids
        assert not plugin._ending_operation_ids
        plugin.on_operation_end(info)
        plugin.on_operation_end(info)
        assert attempts == [1, 1]
        assert len(_spans(exporter)) == 1
    finally:
        release.set()
        plugin.on_invocation_end(
            InvocationEndInfo(**invocation, status=InvocationStatus.SUCCEEDED)
        )
        provider.shutdown()


@pytest.mark.parametrize("view", [InvocationOtelPlugin, ExecutionOtelPlugin])
def test_parent_processor_retry_preserves_context_for_already_exported_child(view):
    plugin, provider, exporter, invocation, _ = _setup(view, "parent")
    parent_end = _completion("parent", operation_type=OperationType.CONTEXT)
    start_fields = dict(
        operation_id="parent",
        operation_type=OperationType.CONTEXT,
        sub_type=None,
        name="parent",
        parent_id=None,
        start_time=parent_end.start_time,
        status=OperationStatus.STARTED,
        is_replayed=False,
    )
    try:
        plugin.on_operation_start(OperationStartInfo(**start_fields))
        start = UserFunctionStartInfo(**start_fields)
        plugin.on_user_function_start(start)
        original_context = plugin._get_span("parent").get_span_context()
        plugin.on_user_function_end(UserFunctionEndInfo.from_start_info(start, None))
        with pytest.raises(RuntimeError, match="processor failure"):
            plugin.on_operation_end(parent_end)
        failed_parent = plugin._get_span("parent")
        assert failed_parent is not None
        original_start_ns = failed_parent.start_time
        plugin.on_operation_end(_completion(parent_id="parent"))
        assert len(_spans(exporter)) == 1
        plugin.on_operation_end(parent_end)
        parent = _spans(exporter, "parent")
        child = _spans(exporter)[0]
        assert len(parent) == 1
        assert parent[0].context.span_id == original_context.span_id
        assert child.parent.span_id == original_context.span_id
        assert parent[0].start_time == original_start_ns
        plugin.on_operation_end(parent_end)
        assert len(_spans(exporter, "parent")) == 1
    finally:
        plugin.on_invocation_end(
            InvocationEndInfo(**invocation, status=InvocationStatus.SUCCEEDED)
        )
        provider.shutdown()


@pytest.mark.parametrize("failure", ["end", "processor"])
@pytest.mark.parametrize("retry", ["notification", "invocation-end"])
def test_failed_replay_segment_remains_retryable_after_logical_export(
    failure, retry, monkeypatch
):
    plugin, provider, exporter, invocation, processor = _setup(
        InvocationOtelPlugin, "operation" if failure == "processor" else None
    )
    if failure == "processor":
        processor.failed = True
    first_end = _completion(operation_type=OperationType.CONTEXT)
    start_fields = dict(
        operation_id="operation",
        operation_type=OperationType.CONTEXT,
        sub_type=None,
        name="operation",
        parent_id=None,
        start_time=first_end.start_time,
        status=OperationStatus.STARTED,
        is_replayed=False,
    )
    try:
        plugin.on_operation_start(OperationStartInfo(**start_fields))
        start = UserFunctionStartInfo(**start_fields)
        plugin.on_user_function_start(start)
        plugin.on_user_function_end(UserFunctionEndInfo.from_start_info(start, None))
        plugin.on_operation_end(first_end)
        assert len(_spans(exporter)) == 1

        replay_start = UserFunctionStartInfo(**start_fields, is_replay_children=True)
        plugin.on_user_function_start(replay_start)
        failed_span = plugin._get_span("operation")
        if failure == "end":
            original_end = failed_span.end
            injected = False

            def fail_once_end(*args, **kwargs):
                nonlocal injected
                if not injected:
                    injected = True
                    raise RuntimeError("replay end failure")
                return original_end(*args, **kwargs)

            monkeypatch.setattr(failed_span, "end", fail_once_end)
        else:
            processor.failed = False
        plugin.on_user_function_end(
            UserFunctionEndInfo.from_start_info(replay_start, None)
        )
        replay_end = OperationEndInfo(
            **{
                **start_fields,
                "start_time": None,
                "is_replayed": True,
                "status": OperationStatus.SUCCEEDED,
            },
            end_time=datetime.now(UTC),
        )
        with pytest.raises(RuntimeError):
            plugin.on_operation_end(replay_end)
        assert "operation" in plugin._ended_operation_ids
        assert plugin._pending_completions["operation"] == replay_end
        assert not plugin._ending_operation_ids

        # The old terminal checkpoint must neither finalize nor overwrite the
        # failed fresh segment's retained replay End.
        plugin.on_operation_end(first_end)
        plugin.on_operation_change(
            OperationChangeInfo(
                execution_arn=invocation["execution_arn"],
                operations={"operation": first_end},
                updated_operations={"operation": first_end},
            )
        )
        assert plugin._pending_completions["operation"] == replay_end
        assert len(_spans(exporter)) == 1
        if retry == "notification":
            plugin.on_operation_end(replay_end)
            plugin.on_operation_end(replay_end)
        plugin.on_invocation_end(
            InvocationEndInfo(**invocation, status=InvocationStatus.SUCCEEDED)
        )
        spans = _spans(exporter)
        assert len(spans) == 2
        assert spans[1].context.span_id == failed_span.get_span_context().span_id
        assert spans[1].status.status_code is StatusCode.OK
        assert spans[1].attributes["durable.operation.status"] == "SUCCEEDED"
    finally:
        if plugin._tracing_enabled:
            plugin.on_invocation_end(
                InvocationEndInfo(**invocation, status=InvocationStatus.SUCCEEDED)
            )
        provider.shutdown()


@pytest.mark.parametrize("failure", ["end", "processor"])
def test_failed_first_segment_finishes_before_next_context_segment_starts(
    failure, monkeypatch
):
    plugin, provider, exporter, invocation, _ = _setup(
        InvocationOtelPlugin, "operation" if failure == "processor" else None
    )
    first_end = _completion(operation_type=OperationType.CONTEXT)
    fields = dict(
        operation_id="operation",
        operation_type=OperationType.CONTEXT,
        sub_type=None,
        name="operation",
        parent_id=None,
        start_time=first_end.start_time,
        status=OperationStatus.STARTED,
        is_replayed=False,
    )
    try:
        plugin.on_operation_start(OperationStartInfo(**fields))
        first_start = UserFunctionStartInfo(**fields)
        plugin.on_user_function_start(first_start)
        first_span = plugin._get_span("operation")
        if failure == "end":
            original_end = first_span.end
            injected = False

            def fail_once_end(*args, **kwargs):
                nonlocal injected
                if not injected:
                    injected = True
                    raise RuntimeError("first segment end failure")
                return original_end(*args, **kwargs)

            monkeypatch.setattr(first_span, "end", fail_once_end)
        plugin.on_user_function_end(
            UserFunctionEndInfo.from_start_info(first_start, None)
        )
        with pytest.raises(RuntimeError):
            plugin.on_operation_end(first_end)
        assert _spans(exporter) == []

        # No new End notification arrives for the failed first segment. The
        # next user-function Start must finish it before replacing its registry
        # entry or treating it as an incomplete segment to continue.
        second_start = UserFunctionStartInfo(**fields, is_replay_children=True)
        plugin.on_user_function_start(second_start)
        assert len(_spans(exporter)) == 1
        assert (
            _spans(exporter)[0].context.span_id == first_span.get_span_context().span_id
        )
        second_span = plugin._get_span("operation")
        assert (
            second_span.get_span_context().span_id
            != first_span.get_span_context().span_id
        )
        plugin.on_user_function_end(
            UserFunctionEndInfo.from_start_info(second_start, None)
        )
        second_end = OperationEndInfo(
            **{
                **fields,
                "start_time": None,
                "is_replayed": True,
                "status": OperationStatus.SUCCEEDED,
            },
            end_time=datetime.now(UTC),
        )
        plugin.on_operation_end(second_end)
        plugin.on_operation_end(first_end)
        assert len(_spans(exporter)) == 2
        assert (
            _spans(exporter)[1].context.span_id
            == second_span.get_span_context().span_id
        )
        assert all(
            span.status.status_code is StatusCode.OK for span in _spans(exporter)
        )
    finally:
        plugin.on_invocation_end(
            InvocationEndInfo(**invocation, status=InvocationStatus.SUCCEEDED)
        )
        provider.shutdown()
