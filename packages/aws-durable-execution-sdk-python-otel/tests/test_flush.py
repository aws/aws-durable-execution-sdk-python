"""Bound provider I/O without spawning replacement workers for a stuck call."""

import contextvars
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import _flush


def _wait_idle() -> None:
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        with _flush._lock:
            if not _flush._active:
                return
        time.sleep(0.001)
    raise AssertionError("Provider worker did not actually complete")


@pytest.fixture(autouse=True)
def idle_flush(monkeypatch):
    _wait_idle()
    monkeypatch.setattr(_flush, "_FLUSH_TIMEOUT_SECONDS", 1.0)
    yield
    _wait_idle()


def test_timeout_retains_one_worker_and_rejects_new_work(monkeypatch):
    monkeypatch.setattr(_flush, "_FLUSH_TIMEOUT_SECONDS", 0.02)
    started = threading.Event()
    release = threading.Event()
    calls = []

    def blocked():
        calls.append(threading.get_ident())
        started.set()
        assert release.wait(2)
        return True

    provider = Mock(spec=TracerProvider)
    provider.force_flush.side_effect = blocked
    try:
        began = time.monotonic()
        assert _flush.flush_provider(provider) is False
        assert time.monotonic() - began < 1
        assert started.wait(1)
        for _ in range(5):
            assert _flush.flush_provider(provider) is False
        assert len(calls) == 1
        workers = [t for t in threading.enumerate() if t.name == "durable-otel-flush"]
        assert len(workers) == 1
        assert workers[0].daemon
    finally:
        release.set()
        _wait_idle()
    monkeypatch.setattr(_flush, "_FLUSH_TIMEOUT_SECONDS", 1.0)
    provider.force_flush.side_effect = None
    provider.force_flush.return_value = True
    assert _flush.flush_provider(provider) is True


@pytest.mark.parametrize("first_timed_out", [False, True])
def test_distinct_provider_flushes_while_first_provider_is_blocked(
    first_timed_out, monkeypatch
):
    exporting = threading.Event()
    release = threading.Event()

    class GatedExporter(InMemorySpanExporter):
        def export(self, spans):
            exporting.set()
            assert release.wait(3)
            return super().export(spans)

    class UnhashableProvider(TracerProvider):
        def __eq__(self, other):
            return isinstance(other, UnhashableProvider)

    first_exporter = GatedExporter()
    second_exporter = InMemorySpanExporter()
    first_provider = UnhashableProvider()
    second_provider = UnhashableProvider()
    first_provider.add_span_processor(
        BatchSpanProcessor(first_exporter, schedule_delay_millis=60000)
    )
    second_provider.add_span_processor(
        BatchSpanProcessor(second_exporter, schedule_delay_millis=60000)
    )
    with first_provider.get_tracer("first").start_as_current_span("first"):
        pass
    with second_provider.get_tracer("second").start_as_current_span("second"):
        pass

    try:
        with ThreadPoolExecutor(max_workers=2) as callers:
            monkeypatch.setattr(
                _flush, "_FLUSH_TIMEOUT_SECONDS", 0.02 if first_timed_out else 3.0
            )
            first = callers.submit(_flush.flush_provider, first_provider)
            try:
                assert exporting.wait(1)
                if first_timed_out:
                    assert first.result(timeout=1) is False
                monkeypatch.setattr(_flush, "_FLUSH_TIMEOUT_SECONDS", 1.0)
                # Complete B's real export while A's exporter is still blocked,
                # including after A's caller has exhausted its wait budget.
                second = callers.submit(_flush.flush_provider, second_provider)
                assert second.result(timeout=1.5) is True
                assert not first_exporter.get_finished_spans()
                assert [span.name for span in second_exporter.get_finished_spans()] == [
                    "second"
                ]
            finally:
                release.set()
            assert first.result(timeout=2) is (not first_timed_out)
        _wait_idle()
        assert [span.name for span in first_exporter.get_finished_spans()] == ["first"]
    finally:
        release.set()
        first_provider.shutdown()
        second_provider.shutdown()


def test_same_provider_flushes_spans_queued_after_active_attempt(monkeypatch):
    first_flushed = threading.Event()
    release_first = threading.Event()
    waiting_for_provider = threading.Event()
    calls = []
    original_wait = _flush._lock.wait

    def admission_wait(timeout=None):
        waiting_for_provider.set()
        return original_wait(timeout)

    monkeypatch.setattr(_flush._lock, "wait", admission_wait)

    class GatedProvider(TracerProvider):
        def force_flush(self, timeout_millis=30000):
            calls.append(threading.get_ident())
            result = super().force_flush(timeout_millis)
            if len(calls) == 1:
                first_flushed.set()
                assert release_first.wait(3)
            return result

    exporter = InMemorySpanExporter()
    provider = GatedProvider()
    provider.add_span_processor(
        BatchSpanProcessor(exporter, schedule_delay_millis=60000)
    )
    tracer = provider.get_tracer("shared")
    with tracer.start_as_current_span("first"):
        pass

    try:
        with ThreadPoolExecutor(max_workers=2) as callers:
            first = callers.submit(_flush.flush_provider, provider)
            try:
                assert first_flushed.wait(1)
                with tracer.start_as_current_span("second"):
                    pass
                second = callers.submit(_flush.flush_provider, provider)
                assert waiting_for_provider.wait(1)
                assert len(calls) == 1
                assert [span.name for span in exporter.get_finished_spans()] == [
                    "first"
                ]
            finally:
                release_first.set()
            assert first.result(timeout=2) is True
            assert second.result(timeout=2) is True
        assert len(calls) == 2
        assert [span.name for span in exporter.get_finished_spans()] == [
            "first",
            "second",
        ]
    finally:
        release_first.set()
        provider.shutdown()


def test_admission_and_execution_share_the_same_deadline():
    first_entered = threading.Event()
    first_release = threading.Event()
    second_called = threading.Event()
    second_entered = threading.Event()
    second_release = threading.Event()

    def first_io():
        first_entered.set()
        assert first_release.wait(3)
        return True

    def second_io():
        second_entered.set()
        assert second_release.wait(3)
        return True

    def second_call():
        second_called.set()
        return _flush.flush_provider(provider)

    provider = Mock(spec=TracerProvider)
    provider.force_flush.side_effect = lambda: (
        second_io() if first_entered.is_set() else first_io()
    )
    with ThreadPoolExecutor(max_workers=2) as callers:
        first = callers.submit(_flush.flush_provider, provider)
        try:
            assert first_entered.wait(1)
            second = callers.submit(second_call)
            assert second_called.wait(1)
            # Spend most of the existing one-second budget on admission.
            time.sleep(0.6)
            first_release.set()
            assert first.result(timeout=1) is True
            assert second_entered.wait(1)
            # A reset one-second I/O budget would still be blocked here.
            assert second.result(timeout=0.65) is False
        finally:
            first_release.set()
            second_release.set()
    _wait_idle()


@pytest.mark.parametrize("result", [True, False, None])
def test_completed_provider_result(result):
    provider = Mock(spec=TracerProvider)
    provider.force_flush.return_value = result
    assert _flush.flush_provider(provider) is (result is not False)


@pytest.mark.parametrize(
    "error", [RuntimeError("ordinary"), SystemExit("fatal"), KeyboardInterrupt()]
)
def test_fast_error_preserves_identity_on_the_caller(error):
    owner = threading.get_ident()
    worker = []

    def fail():
        worker.append(threading.get_ident())
        raise error

    provider = Mock(spec=TracerProvider)
    provider.force_flush.side_effect = fail
    with pytest.raises(type(error)) as caught:
        _flush.flush_provider(provider)
    assert caught.value is error
    assert worker[0] != owner
    assert threading.get_ident() == owner


@pytest.mark.parametrize(
    "error", [RuntimeError("late ordinary"), SystemExit("late fatal")]
)
def test_late_error_is_reported_without_poisoning_next_call(error, monkeypatch, caplog):
    monkeypatch.setattr(_flush, "_FLUSH_TIMEOUT_SECONDS", 0.02)
    release = threading.Event()

    def fail_late():
        assert release.wait(2)
        raise error

    provider = Mock(spec=TracerProvider)
    provider.force_flush.side_effect = fail_late
    try:
        assert _flush.flush_provider(provider) is False
    finally:
        release.set()
        _wait_idle()
    messages = [
        r for r in caplog.records if "after the caller stopped waiting" in r.message
    ]
    assert len(messages) == 1
    assert messages[0].exc_info[1] is error
    monkeypatch.setattr(_flush, "_FLUSH_TIMEOUT_SECONDS", 1.0)
    provider.force_flush.side_effect = None
    provider.force_flush.return_value = True
    assert _flush.flush_provider(provider) is True


def test_start_failure_releases_capacity(monkeypatch):
    def fail_start(_thread):
        raise RuntimeError("start failed")

    provider = Mock(spec=TracerProvider)
    provider.force_flush.return_value = True
    with monkeypatch.context() as patch:
        patch.setattr(threading.Thread, "start", fail_start)
        with pytest.raises(RuntimeError, match="start failed"):
            _flush.flush_provider(provider)
    assert _flush.flush_provider(provider) is True


def test_only_provider_io_gets_post_cleanup_context_snapshot():
    value = contextvars.ContextVar("flush-test", default="unset")
    token = value.set("owner-after-cleanup")
    try:

        def flush():
            assert value.get() == "owner-after-cleanup"
            value.set("provider-only")
            return True

        provider = Mock(spec=TracerProvider)
        provider.force_flush.side_effect = flush
        assert _flush.flush_provider(provider)
        assert value.get() == "owner-after-cleanup"
    finally:
        value.reset(token)
