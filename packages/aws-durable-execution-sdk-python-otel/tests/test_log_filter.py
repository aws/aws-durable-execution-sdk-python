"""Tests for the OTel context logging filter."""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    OperationStatus,
)
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
    OperationType,
    UserFunctionStartInfo,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import log_filter as log_filter_module
from aws_durable_execution_sdk_python_otel.log_filter import (
    OtelContextLogFilter,
    install_log_filter,
)
from aws_durable_execution_sdk_python_otel.invocation_plugin import InvocationOtelPlugin
from aws_durable_execution_sdk_python_otel.otel_plugin_config import OtelPluginConfig


START_TIME = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
EXECUTION_ARN = "arn:aws:lambda:us-west-2:123456789012:function:workflow:$LATEST"


@pytest.fixture(autouse=True)
def _isolated_invocation_registry():
    """Run each test against an empty open-invocation registry.

    The registry is process-global by design -- one installed filter serves every
    invocation in the environment -- so a test that starts an invocation without
    ending it would otherwise change what later tests resolve.
    """
    saved = list(log_filter_module._open_invocations)
    log_filter_module._open_invocations.clear()
    token = log_filter_module._current_invocation.set(None)
    try:
        yield
    finally:
        log_filter_module._current_invocation.reset(token)
        log_filter_module._open_invocations.clear()
        log_filter_module._open_invocations.update(saved)


def _create_plugin(
    enrich_logger: bool = True,
) -> tuple[InvocationOtelPlugin, InMemorySpanExporter]:
    """Create a plugin wired to an in-memory span exporter."""
    exporter = InMemorySpanExporter()
    trace_provider = TracerProvider()
    trace_provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = InvocationOtelPlugin(
        OtelPluginConfig(
            tracer_provider=trace_provider,
            context_extractor=lambda _: None,
            enrich_logger=enrich_logger,
        )
    )
    return plugin, exporter


def _invocation_start_info(suffix: str = "") -> InvocationStartInfo:
    """Create standard invocation start info for tests."""
    return InvocationStartInfo(
        request_id=f"request-1{suffix}",
        execution_arn=f"{EXECUTION_ARN}{suffix}",
        execution_start_time=START_TIME,
        is_first_invocation=True,
    )


def _invocation_end_info(suffix: str = "") -> InvocationEndInfo:
    """Create standard invocation end info for tests."""
    return InvocationEndInfo(
        request_id=f"request-1{suffix}",
        execution_arn=f"{EXECUTION_ARN}{suffix}",
        is_first_invocation=True,
        status=InvocationStatus.SUCCEEDED,
    )


def _user_function_start_info(operation_id: str) -> UserFunctionStartInfo:
    """Create standard user function start info for tests."""
    return UserFunctionStartInfo(
        operation_id=operation_id,
        operation_type=OperationType.STEP,
        sub_type=None,
        name="fetch-user",
        parent_id=None,
        start_time=START_TIME,
        is_replayed=False,
        status=OperationStatus.STARTED,
        is_replay_children=False,
        attempt=1,
    )


def _make_record() -> logging.LogRecord:
    """Create a bare LogRecord for filtering."""
    return logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )


def _stamped(record: logging.LogRecord) -> tuple[str | None, str | None]:
    """Return the trace and span identifiers a filter stamped on a record."""
    return getattr(record, "traceId", None), getattr(record, "spanId", None)


def _own_identifiers(plugin: InvocationOtelPlugin) -> tuple[str, str]:
    """Return the trace and span identifiers of the plugin's current span."""
    span_context = plugin.get_current_span_context()
    assert span_context is not None
    return (
        format(span_context.trace_id, "032x"),
        format(span_context.span_id, "016x"),
    )


def _remove_otel_filters(handler: logging.Handler) -> None:
    """Remove any OtelContextLogFilter from a handler (test cleanup)."""
    for log_filter in [
        f for f in handler.filters if isinstance(f, OtelContextLogFilter)
    ]:
        handler.removeFilter(log_filter)


def test_filter_always_returns_true():
    """The filter never drops a record, even with no active span."""
    log_filter = OtelContextLogFilter()

    assert log_filter.filter(_make_record()) is True


def test_filter_does_not_set_fields_without_active_span():
    """With no invocation open, the filter leaves the record unmodified."""
    _create_plugin()
    log_filter = OtelContextLogFilter()

    record = _make_record()
    log_filter.filter(record)

    assert not hasattr(record, "traceId")
    assert not hasattr(record, "spanId")
    assert not hasattr(record, "otelTraceSampled")


def test_filter_injects_trace_context_from_invocation_span():
    """The filter stamps the invocation span context for top-level code."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    log_filter = OtelContextLogFilter()

    record = _make_record()
    log_filter.filter(record)

    assert len(record.traceId) == 32
    assert len(record.spanId) == 16
    assert isinstance(record.otelTraceSampled, bool)


def test_filter_uses_attempt_span_inside_user_function():
    """spanId reflects the active attempt span during user code."""
    plugin, _ = _create_plugin()
    plugin.on_invocation_start(_invocation_start_info())
    operation_id = "step-1"
    plugin.on_user_function_start(_user_function_start_info(operation_id))
    try:
        record = _make_record()
        OtelContextLogFilter().filter(record)

        attempt_span = plugin._get_span("step-1:attempt:1")
        assert attempt_span is not None
        expected_span_id = format(attempt_span.get_span_context().span_id, "016x")
        assert record.spanId == expected_span_id
    finally:
        # Ends the invocation, which detaches the attempt scope this test
        # attached to the running thread's OTel context. Left attached, it would
        # stay current for every later test on this thread.
        plugin.on_invocation_end(_invocation_end_info())


def test_concurrent_invocations_each_stamp_their_own_span_context():
    """Two invocations open at once each correlate to their own trace.

    Logging handlers are process-global, so both invocations are served by one
    filter instance. Each record must carry the trace and span identifiers of the
    invocation that emitted it, not of whichever invocation started most
    recently.
    """
    shared_filter = OtelContextLogFilter()
    both_started = threading.Barrier(2, timeout=10)
    stamped: dict[str, tuple[str | None, str | None]] = {}
    own: dict[str, tuple[str, str]] = {}
    failures: list[BaseException] = []
    lock = threading.Lock()

    def invocation(owner: str) -> None:
        try:
            plugin, _ = _create_plugin(enrich_logger=False)
            plugin.on_invocation_start(_invocation_start_info(suffix=owner))
            try:
                with lock:
                    own[owner] = _own_identifiers(plugin)
                # Emit only once both invocations are open, so a filter holding
                # one mutable plugin reference is guaranteed to have been
                # overwritten by the other invocation.
                both_started.wait()
                record = _make_record()
                shared_filter.filter(record)
                with lock:
                    stamped[owner] = _stamped(record)
            finally:
                plugin.on_invocation_end(_invocation_end_info())
        except BaseException as error:  # noqa: BLE001
            with lock:
                failures.append(error)
            both_started.abort()

    threads = [
        threading.Thread(target=invocation, args=(owner,), name=f"invocation-{owner}")
        for owner in ("a", "b")
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert not failures
    assert own["a"] != own["b"]
    assert stamped["a"] == own["a"]
    assert stamped["b"] == own["b"]


def test_record_on_an_unclaimed_thread_uses_the_only_open_invocation():
    """A thread the plugin never ran on still correlates to the one invocation.

    The SDK runs the handler body on a worker thread it creates after the
    invocation-start hook has run, and Python does not propagate context into a
    new thread, so that thread carries no claim. With a single invocation open
    there is no ambiguity to resolve.
    """
    plugin, _ = _create_plugin(enrich_logger=False)
    plugin.on_invocation_start(_invocation_start_info())
    try:
        expected = _own_identifiers(plugin)
        stamped: list[tuple[str | None, str | None]] = []

        def emit() -> None:
            record = _make_record()
            OtelContextLogFilter().filter(record)
            stamped.append(_stamped(record))

        worker = threading.Thread(target=emit, name="unclaimed")
        worker.start()
        worker.join(timeout=10)

        assert stamped == [expected]
    finally:
        plugin.on_invocation_end(_invocation_end_info())


def test_record_on_an_unclaimed_thread_is_left_alone_when_two_are_open():
    """An unattributable record is not correlated to an arbitrary invocation."""
    first, _ = _create_plugin(enrich_logger=False)
    second, _ = _create_plugin(enrich_logger=False)
    first.on_invocation_start(_invocation_start_info(suffix="first"))
    second.on_invocation_start(_invocation_start_info(suffix="second"))
    try:
        records: list[logging.LogRecord] = []

        def emit() -> None:
            record = _make_record()
            OtelContextLogFilter().filter(record)
            records.append(record)

        worker = threading.Thread(target=emit, name="unclaimed")
        worker.start()
        worker.join(timeout=10)

        assert len(records) == 1
        assert not hasattr(records[0], "traceId")
        assert not hasattr(records[0], "spanId")
    finally:
        first.on_invocation_end(_invocation_end_info())
        second.on_invocation_end(_invocation_end_info())


def test_finished_invocation_does_not_correlate_later_records():
    """A thread claimed by an invocation stops correlating once it ends.

    A pooled thread can outlive the invocation that claimed it, so liveness is
    checked at emit time rather than assumed from the claim.
    """
    plugin, _ = _create_plugin(enrich_logger=False)
    plugin.on_invocation_start(_invocation_start_info())
    plugin.on_invocation_end(_invocation_end_info())

    record = _make_record()
    OtelContextLogFilter().filter(record)

    assert not hasattr(record, "traceId")
    assert not hasattr(record, "spanId")


def test_install_log_filter_attaches_to_handlers():
    """install_log_filter adds the filter to each handler on the target logger."""
    target = logging.getLogger("test.install")
    handler = logging.NullHandler()
    target.addHandler(handler)
    try:
        installed = install_log_filter(target_logger=target)

        assert isinstance(installed, OtelContextLogFilter)
        assert any(isinstance(f, OtelContextLogFilter) for f in handler.filters)
    finally:
        target.removeHandler(handler)


def test_install_log_filter_is_idempotent():
    """Repeated installs do not stack duplicate filters on a handler."""
    target = logging.getLogger("test.idempotent")
    handler = logging.NullHandler()
    target.addHandler(handler)
    try:
        install_log_filter(target_logger=target)
        install_log_filter(target_logger=target)

        otel_filters = [
            f for f in handler.filters if isinstance(f, OtelContextLogFilter)
        ]
        assert len(otel_filters) == 1
    finally:
        target.removeHandler(handler)


def test_concurrent_first_time_installs_attach_one_filter():
    """Two invocations installing at once cannot both add a filter.

    The handler holds the first install open at the moment it attaches, so a
    second caller that was not serialized behind it still sees a filterless
    handler and attaches a second filter.
    """

    class HandlerRacingOnAttach(logging.NullHandler):
        """Holds the first attach open until a second caller reaches it."""

        def __init__(self) -> None:
            super().__init__()
            self._barrier = threading.Barrier(2, timeout=0.2)
            self._released = False

        def addFilter(self, filter) -> None:  # noqa: A002 - stdlib signature
            if not self._released:
                try:
                    self._barrier.wait()
                except threading.BrokenBarrierError:
                    # Installation was serialized, so no second caller arrived.
                    pass
                self._released = True
            super().addFilter(filter)

    target = logging.getLogger("test.install.race")
    handler = HandlerRacingOnAttach()
    target.addHandler(handler)
    try:
        threads = [
            threading.Thread(target=install_log_filter, args=(target,))
            for _ in range(2)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)

        otel_filters = [
            f for f in handler.filters if isinstance(f, OtelContextLogFilter)
        ]
        assert len(otel_filters) == 1
    finally:
        target.removeHandler(handler)


def test_a_later_invocation_takes_over_log_correlation():
    """The invocation that is open now owns correlation, not the first one.

    A logging handler lives as long as the Lambda environment while a plugin
    instance lives for one invocation, so a filter tied to the first
    invocation's plugin would stop correlating logs after that invocation ended.
    """
    first, _ = _create_plugin()
    second, _ = _create_plugin()
    target = logging.getLogger("test.rebind")
    handler = logging.NullHandler()
    target.addHandler(handler)
    try:
        installed = install_log_filter(target_logger=target)
        assert installed is not None
        assert install_log_filter(target_logger=target) is installed

        first.on_invocation_start(_invocation_start_info(suffix="first"))
        first.on_invocation_end(_invocation_end_info())
        second.on_invocation_start(_invocation_start_info(suffix="second"))

        record = _make_record()
        installed.filter(record)

        assert _stamped(record) == _own_identifiers(second)
    finally:
        second.on_invocation_end(_invocation_end_info())
        target.removeHandler(handler)


def test_install_log_filter_reuses_single_instance_across_handlers():
    """A single filter instance is shared across all handlers."""
    target = logging.getLogger("test.shared")
    handler_a = logging.NullHandler()
    handler_b = logging.NullHandler()
    target.addHandler(handler_a)
    target.addHandler(handler_b)
    try:
        installed = install_log_filter(target_logger=target)

        filter_a = next(
            f for f in handler_a.filters if isinstance(f, OtelContextLogFilter)
        )
        filter_b = next(
            f for f in handler_b.filters if isinstance(f, OtelContextLogFilter)
        )
        assert filter_a is filter_b is installed
    finally:
        target.removeHandler(handler_a)
        target.removeHandler(handler_b)


def test_install_log_filter_returns_none_without_handlers():
    """With no handlers, install_log_filter has nothing to attach to."""
    target = logging.getLogger("test.nohandlers")

    assert install_log_filter(target_logger=target) is None


def test_plugin_installs_filter_on_root_logger_at_construction():
    """The plugin installs the filter on the root logger when constructed."""
    root = logging.getLogger()
    handler = logging.NullHandler()
    root.addHandler(handler)
    try:
        _create_plugin(enrich_logger=True)

        assert any(isinstance(f, OtelContextLogFilter) for f in handler.filters)
    finally:
        for h in root.handlers:
            _remove_otel_filters(h)
        root.removeHandler(handler)


def test_plugin_skips_filter_when_disabled():
    """No filter is installed when enrich_logger is disabled."""
    root = logging.getLogger()
    handler = logging.NullHandler()
    root.addHandler(handler)
    try:
        _create_plugin(enrich_logger=False)

        assert not any(isinstance(f, OtelContextLogFilter) for f in handler.filters)
    finally:
        for h in root.handlers:
            _remove_otel_filters(h)
        root.removeHandler(handler)
