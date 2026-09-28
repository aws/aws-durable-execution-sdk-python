"""Tests for the OTel context logging filter."""

from __future__ import annotations

import contextvars
import gc
import logging
import threading
import weakref
from datetime import UTC, datetime

import opentelemetry.context as otel_context
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
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    TraceFlags,
    TraceState,
)

from aws_durable_execution_sdk_python_otel import log_filter as log_filter_module
from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    _to_otel_trace_id,
)
from aws_durable_execution_sdk_python_otel.execution_plugin import ExecutionOtelPlugin
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


def test_record_on_an_unclaimed_thread_is_not_stamped_with_the_open_invocation():
    """A thread carrying no claim is not correlated, even to a lone invocation.

    A thread that carries no claim -- one customer code started itself, since
    ``threading.Thread`` does not copy the starting thread's context -- has no
    invocation of its own. Resolving it to the single open invocation would be
    right only when that invocation is the one that emitted the record, and two
    orderings make it the wrong one: a thread still carrying a finished
    invocation's claim, and an invocation's own thread before its start hook has
    run. Correlation for such records is given up so neither ordering can stamp
    one execution's record with another's trace.
    """
    plugin, _ = _create_plugin(enrich_logger=False)
    plugin.on_invocation_start(_invocation_start_info())
    try:
        open_identifiers = _own_identifiers(plugin)
        stamped: list[tuple[str | None, str | None]] = []

        def emit() -> None:
            record = _make_record()
            OtelContextLogFilter().filter(record)
            stamped.append(_stamped(record))

        worker = threading.Thread(target=emit, name="unclaimed")
        worker.start()
        worker.join(timeout=10)

        assert stamped == [(None, None)]
        assert stamped[0] != open_identifiers
    finally:
        plugin.on_invocation_end(_invocation_end_info())


def test_a_stale_claim_is_not_resolved_to_the_only_open_invocation():
    """A thread still claimed by a finished invocation is not given a live one.

    ``unbind_invocation`` cannot reset the claim on any thread but its own, so a
    thread the first invocation claimed still names that invocation after it
    ends. A second invocation is then the only open one. Resolving by the number
    of open invocations would stamp the first invocation's record with the
    second's trace, which is one customer execution's identifiers on another's
    record.
    """
    first, _ = _create_plugin(enrich_logger=False)
    first.on_invocation_start(_invocation_start_info(suffix="first"))
    first_identifiers = _own_identifiers(first)

    # A worker running in a copy of the claiming thread's context, which is what
    # the SDK submits the handler body as, so the claim reaches it.
    claimed_context = contextvars.copy_context()
    second_is_open = threading.Event()
    stamped: list[tuple[str | None, str | None]] = []

    def emit() -> None:
        assert second_is_open.wait(timeout=10)
        record = _make_record()
        OtelContextLogFilter().filter(record)
        stamped.append(_stamped(record))

    worker = threading.Thread(
        target=claimed_context.run, args=(emit,), name="claimed-by-first"
    )
    worker.start()
    try:
        first.on_invocation_end(_invocation_end_info(suffix="first"))
        second, _ = _create_plugin(enrich_logger=False)
        second.on_invocation_start(_invocation_start_info(suffix="second"))
        try:
            second_identifiers = _own_identifiers(second)
            second_is_open.set()
            worker.join(timeout=10)

            assert len(stamped) == 1
            assert stamped[0] != second_identifiers
            assert stamped[0] != first_identifiers
            assert stamped[0] == (None, None)
        finally:
            second.on_invocation_end(_invocation_end_info(suffix="second"))
    finally:
        second_is_open.set()
        worker.join(timeout=10)


def test_a_claimed_worker_does_not_pin_the_finished_invocation():
    """A claim left on a pool thread does not keep the plugin alive.

    ``unbind_invocation`` resets the claim only on the thread that ends the
    invocation, so a pool thread that is never used again keeps the claim it was
    given. The claim is a weak reference, so what it keeps is nothing: once the
    invocation ends and the SDK releases the plugin, the plugin is collectable
    even while the claimed thread is still running.
    """
    plugin, _ = _create_plugin(enrich_logger=False)
    plugin.on_invocation_start(_invocation_start_info(suffix="only"))

    # A worker running in a copy of the claiming thread's context, held alive for
    # the length of the test, as a pooled worker would be.
    claimed_context = contextvars.copy_context()
    release_worker = threading.Event()
    resolved_while_open: list[bool] = []

    def hold_the_claim() -> None:
        resolved_while_open.append(log_filter_module._resolve_provider() is not None)
        assert release_worker.wait(timeout=10)

    worker = threading.Thread(
        target=claimed_context.run, args=(hold_the_claim,), name="claimed-worker"
    )
    worker.start()
    try:
        collected = threading.Event()
        weakref.finalize(plugin, collected.set)
        plugin.on_invocation_end(_invocation_end_info(suffix="only"))

        del plugin
        gc.collect()

        assert resolved_while_open == [True], "the claim must resolve while open"
        assert collected.is_set(), "the claimed worker must not pin the plugin"
    finally:
        release_worker.set()
        worker.join(timeout=10)


def test_a_record_emitted_before_its_invocation_binds_is_not_given_the_open_one():
    """A record emitted before its own invocation binds is not correlated.

    An invocation emits records on its own thread before its invocation-start
    hook runs -- while the SDK fetches initial state, for instance -- and that
    thread carries no claim yet. Another invocation can be open at that moment.
    Resolving by the number of open invocations would stamp the starting
    invocation's record with the open invocation's trace.
    """
    first, _ = _create_plugin(enrich_logger=False)
    first.on_invocation_start(_invocation_start_info(suffix="first"))
    second, _ = _create_plugin(enrich_logger=False)
    stamped: list[tuple[str | None, str | None]] = []
    try:
        first_identifiers = _own_identifiers(first)

        def start_second_invocation() -> None:
            # This thread belongs to the second invocation, which has not bound
            # itself yet, so nothing here carries a claim.
            record = _make_record()
            OtelContextLogFilter().filter(record)
            stamped.append(_stamped(record))
            second.on_invocation_start(_invocation_start_info(suffix="second"))

        worker = threading.Thread(target=start_second_invocation, name="second-wrapper")
        worker.start()
        worker.join(timeout=10)

        assert len(stamped) == 1
        assert stamped[0] != first_identifiers
        assert stamped[0] != _own_identifiers(second)
        assert stamped[0] == (None, None)
    finally:
        first.on_invocation_end(_invocation_end_info(suffix="first"))
        second.on_invocation_end(_invocation_end_info(suffix="second"))


def test_context_propagated_into_a_worker_resolves_the_claiming_invocation():
    """A worker started from a copy of the claiming thread's context resolves it.

    This is what the SDK does for the thread it runs the handler body on: the
    invocation-start hook claims the invocation thread, and the handler body runs
    in a copy of that thread's context. Both invocations are open when either
    record is emitted, so the number of open invocations cannot resolve them and
    only the propagated claim can.
    """
    both_emitted = threading.Barrier(2, timeout=10)
    stamped: dict[str, tuple[str | None, str | None]] = {}
    own: dict[str, tuple[str, str]] = {}
    failures: list[BaseException] = []
    lock = threading.Lock()

    def invocation(owner: str) -> None:
        """Run one invocation the way the SDK does, on its own thread."""
        try:
            plugin, _ = _create_plugin(enrich_logger=False)
            plugin.on_invocation_start(_invocation_start_info(suffix=owner))
            try:
                with lock:
                    own[owner] = _own_identifiers(plugin)

                def emit() -> None:
                    record = _make_record()
                    # Emit only once both invocations are open, so the
                    # single-open-invocation fallback cannot resolve the record.
                    both_emitted.wait()
                    OtelContextLogFilter().filter(record)
                    with lock:
                        stamped[owner] = _stamped(record)

                # A fresh copy per submission: one Context cannot be entered
                # twice concurrently, which is why the SDK copies at each submit.
                worker = threading.Thread(
                    target=contextvars.copy_context().run,
                    args=(emit,),
                    name=f"worker-{owner}",
                )
                worker.start()
                worker.join(timeout=10)
            finally:
                plugin.on_invocation_end(_invocation_end_info())
        except BaseException as error:  # noqa: BLE001
            with lock:
                failures.append(error)
            both_emitted.abort()

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
    assert stamped == own


def test_concurrent_invocations_with_enclosing_ambient_spans_stay_separate():
    """Excluding the enclosing ambient span does not blur two open invocations.

    Each invocation runs with its own ADOT-style enclosing span current, on its
    own execution trace, and both are open when either record is emitted. Each
    record must carry its own invocation's Invocation span: not the other
    invocation's span, and not its own enclosing span.
    """
    both_started = threading.Barrier(2, timeout=10)
    stamped: dict[str, tuple[str | None, str | None]] = {}
    invocation_spans: dict[str, tuple[str, str]] = {}
    ambient_span_ids: dict[str, str] = {}
    failures: list[BaseException] = []
    lock = threading.Lock()
    ambient_span_id_by_owner = {
        "a": int("1111aaaa1111aaaa", 16),
        "b": int("2222bbbb2222bbbb", 16),
    }

    def invocation(owner: str) -> None:
        try:
            plugin, _ = _create_plugin(enrich_logger=False)
            # The enclosing span sits on this execution's own trace, which is
            # what the ADOT layer produces under X-Ray active tracing.
            ambient_context = SpanContext(
                trace_id=_to_otel_trace_id(f"{EXECUTION_ARN}{owner}", START_TIME),
                span_id=ambient_span_id_by_owner[owner],
                is_remote=True,
                trace_flags=TraceFlags(TraceFlags.SAMPLED),
                trace_state=TraceState(),
            )
            token = otel_context.attach(
                trace.set_span_in_context(NonRecordingSpan(ambient_context), Context())
            )
            try:
                plugin.on_invocation_start(_invocation_start_info(suffix=owner))
                try:
                    invocation_span = plugin._get_span(None)
                    assert invocation_span is not None
                    span_context = invocation_span.get_span_context()
                    with lock:
                        invocation_spans[owner] = (
                            format(span_context.trace_id, "032x"),
                            format(span_context.span_id, "016x"),
                        )
                        ambient_span_ids[owner] = format(
                            ambient_context.span_id, "016x"
                        )
                    both_started.wait()
                    record = _make_record()
                    OtelContextLogFilter().filter(record)
                    with lock:
                        stamped[owner] = _stamped(record)
                finally:
                    plugin.on_invocation_end(_invocation_end_info(suffix=owner))
            finally:
                otel_context.detach(token)
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

    assert not failures, failures
    assert invocation_spans["a"] != invocation_spans["b"]
    assert stamped == invocation_spans
    for owner in ("a", "b"):
        assert stamped[owner][1] != ambient_span_ids[owner]


def test_record_on_a_customer_thread_is_not_attributed_to_another_invocation():
    """A thread customer code starts itself is never given the wrong invocation.

    ``threading.Thread`` does not copy the starting thread's context, so a thread
    a handler creates directly carries no claim. This case is not the SDK's
    handler worker, which is submitted with a copy of the invocation's context.
    With two invocations open there is nothing to resolve such a record against,
    and it is left uncorrelated rather than attributed to either invocation.
    """
    first, _ = _create_plugin(enrich_logger=False)
    second, _ = _create_plugin(enrich_logger=False)
    first.on_invocation_start(_invocation_start_info(suffix="first"))
    second.on_invocation_start(_invocation_start_info(suffix="second"))
    try:
        wrong_identifiers = {_own_identifiers(first), _own_identifiers(second)}
        stamped: list[tuple[str | None, str | None]] = []

        def emit() -> None:
            record = _make_record()
            OtelContextLogFilter().filter(record)
            stamped.append(_stamped(record))

        worker = threading.Thread(target=emit, name="customer-created")
        worker.start()
        worker.join(timeout=10)

        assert len(stamped) == 1
        # The rule that matters: never another execution's trace.
        assert stamped[0] not in wrong_identifiers
        # And with no claim and no single open invocation, nothing is stamped.
        assert stamped[0] == (None, None)
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


class _SpanProcessorFailingOnEnd(SpanProcessor):
    """Raises when a span ends, standing in for customer span-processor code."""

    def on_start(self, span, parent_context=None) -> None:
        pass

    def on_end(self, span) -> None:
        raise RuntimeError("span processor on_end failed")


@pytest.mark.parametrize(
    "plugin_class",
    [InvocationOtelPlugin, ExecutionOtelPlugin],
    ids=lambda c: c.__name__,
)
def test_invocation_is_released_when_span_shutdown_fails(plugin_class):
    """A failing span shutdown still releases the invocation.

    Ending a span calls the configured span processors, which are customer code
    and can raise. The plugin executor contains that exception, so the invocation
    survives it. An invocation left registered would stay open for the life of
    the environment, and a thread still carrying its claim would keep correlating
    records to its finished spans.
    """
    provider = TracerProvider()
    provider.add_span_processor(_SpanProcessorFailingOnEnd())
    plugin = plugin_class(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    plugin.on_invocation_start(_invocation_start_info())

    with pytest.raises(RuntimeError, match="span processor on_end failed"):
        plugin.on_invocation_end(_invocation_end_info())

    assert plugin not in log_filter_module._open_invocations
    # This thread carried the claim, so it is the thread a later record would be
    # mis-attributed on.
    assert log_filter_module._resolve_provider() is not plugin
    record = _make_record()
    OtelContextLogFilter().filter(record)
    assert _stamped(record) == (None, None)
    # The OTel context stack belongs to the thread, so a scope the plugin left
    # attached would stay current after the invocation returned.
    assert plugin._context_tokens == {}


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
