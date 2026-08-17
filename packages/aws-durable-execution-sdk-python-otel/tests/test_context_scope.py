"""Tests for balanced OTel context scopes across the plugin lifecycles.

Covers :mod:`context_scope` directly plus the lifecycle paths the plugins rely
on it for: a suspended operation whose end hook never runs, hooks that execute
on a worker thread, and both plugins attaching on the same thread.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import opentelemetry.context as otel_context
import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    InvocationStatus,
    OperationStatus,
    OperationSubType,
    OperationType,
)
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStartInfo,
    UserFunctionEndInfo,
    UserFunctionOutcome,
    UserFunctionStartInfo,
)
from opentelemetry import baggage, trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import context_scope
from aws_durable_execution_sdk_python_otel.execution_plugin import ExecutionOtelPlugin
from aws_durable_execution_sdk_python_otel.invocation_plugin import InvocationOtelPlugin
from aws_durable_execution_sdk_python_otel.otel_plugin_config import (
    OtelPluginConfig,
    ProviderSource,
)


START_TIME = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
END_TIME = datetime(2024, 1, 2, 3, 4, 6, tzinfo=UTC)
EXECUTION_ARN = "arn:aws:lambda:us-west-2:123456789012:function:workflow:$LATEST"


@pytest.fixture(autouse=True)
def _assert_otel_context_balanced():
    """Fail any test that leaves an OTel context scope attached."""
    before = otel_context.get_current()
    before_depth = context_scope.depth()
    yield
    assert context_scope.depth() == before_depth
    assert otel_context.get_current() is before


class _Owner:
    """Stand-in for a plugin instance; context_scope only uses its identity."""


def _span_context(name: str) -> Context:
    """Return a context carrying a non-recording span named after ``name``."""
    return trace.set_span_in_context(
        trace.NonRecordingSpan(
            trace.SpanContext(
                trace_id=abs(hash(name)) % (1 << 128) or 1,
                span_id=abs(hash(name)) % (1 << 64) or 1,
                is_remote=False,
                trace_flags=trace.TraceFlags(trace.TraceFlags.SAMPLED),
            )
        ),
        otel_context.get_current(),
    )


# ---------------------------------------------------------------------------
# context_scope helper
# ---------------------------------------------------------------------------
def test_enter_then_exit_restores_the_previous_context():
    owner = _Owner()
    before = otel_context.get_current()

    context_scope.enter_scope(owner, "a", lambda: _span_context("a"))
    assert otel_context.get_current() is not before
    assert context_scope.depth(owner) == 1

    context_scope.exit_scope(owner, "a")
    assert otel_context.get_current() is before
    assert context_scope.depth(owner) == 0


def test_nested_scopes_pop_in_lifo_order():
    owner = _Owner()
    before = otel_context.get_current()

    context_scope.enter_scope(owner, "outer", lambda: _span_context("outer"))
    outer = otel_context.get_current()
    context_scope.enter_scope(owner, "inner", lambda: _span_context("inner"))

    context_scope.exit_scope(owner, "inner")
    assert otel_context.get_current() is outer
    context_scope.exit_scope(owner, "outer")
    assert otel_context.get_current() is before


def test_exit_unwinds_scopes_stacked_above_the_target():
    """Popping an outer scope must also drop anything left above it.

    ``ContextVar.reset`` writes back the token's captured value unconditionally,
    so resetting out of order would revive a stale context. Unwinding downwards
    keeps the underlying variable strictly LIFO.
    """
    owner = _Owner()
    before = otel_context.get_current()

    context_scope.enter_scope(owner, "outer", lambda: _span_context("outer"))
    context_scope.enter_scope(owner, "orphan", lambda: _span_context("orphan"))

    context_scope.exit_scope(owner, "outer")

    assert otel_context.get_current() is before
    assert context_scope.depth(owner) == 0


def test_exit_with_unknown_key_is_a_noop():
    owner = _Owner()
    before = otel_context.get_current()

    context_scope.exit_scope(owner, "never-pushed")

    assert otel_context.get_current() is before
    assert context_scope.depth(owner) == 0


def test_reentering_the_same_key_replaces_the_previous_scope():
    """Re-entering a key inside one invocation must not stack a second scope.

    A suspended operation is re-entered when its branch is resubmitted, and its
    first scope is still attached because the suspending path had no end hook to
    pop it. The epoch is unchanged, so only the ancestry check catches this.
    """
    owner = _Owner()
    before = otel_context.get_current()

    context_scope.enter_scope(owner, "wfc-1", lambda: _span_context("poll-1"), epoch=1)
    context_scope.enter_scope(owner, "wfc-1", lambda: _span_context("poll-2"), epoch=1)

    assert context_scope.depth(owner) == 1

    context_scope.exit_scope(owner, "wfc-1")
    assert otel_context.get_current() is before
    assert context_scope.depth(owner) == 0


def test_reentry_guard_keeps_enclosing_scopes():
    """Re-entering a nested key must not disturb the scope it is nested in."""
    owner = _Owner()
    context_scope.enter_scope(owner, "ctx", lambda: _span_context("ctx"), epoch=1)
    enclosing = otel_context.get_current()

    context_scope.enter_scope(owner, "inner", lambda: _span_context("inner-1"), epoch=1)
    context_scope.enter_scope(owner, "inner", lambda: _span_context("inner-2"), epoch=1)

    assert context_scope.depth(owner) == 2
    context_scope.exit_scope(owner, "inner")
    assert otel_context.get_current() is enclosing

    context_scope.exit_scope(owner, "ctx")


def test_ownership_is_read_from_the_context_not_the_thread():
    """A propagated context reports its owner on a thread with no tokens.

    Thread-local bookkeeping cannot answer "is the current span mine?" on a thread
    that received a context rather than attaching it, which is why ownership is
    recorded in the context.
    """
    owner = _Owner()
    context_scope.enter_scope(owner, "op-1", lambda: _span_context("op-1"))
    propagated = otel_context.get_current()
    assert context_scope.owns_current(owner) is True

    observed: dict[str, object] = {}

    def receive() -> None:
        # No scope was entered on this thread, so it holds no tokens.
        observed["depth"] = context_scope.depth(owner)
        observed["owns_before"] = context_scope.owns_current(owner)
        token = otel_context.attach(propagated)
        try:
            observed["owns_after"] = context_scope.owns_current(owner)
        finally:
            otel_context.detach(token)

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(receive).result()

    assert observed["depth"] == 0
    assert observed["owns_before"] is False
    assert observed["owns_after"] is True

    context_scope.exit_scope(owner, "op-1")
    assert context_scope.owns_current(owner) is False


def test_an_unowned_context_is_not_claimed():
    """A context this plugin never attached must not be reported as its own."""
    owner = _Owner()
    token = otel_context.attach(_span_context("ambient"))
    try:
        assert context_scope.owns_current(owner) is False
    finally:
        otel_context.detach(token)


def test_two_owners_each_recognise_their_own_scope():
    """Ownership accumulates, so the innermost scope does not mask the outer one."""
    first, second = _Owner(), _Owner()
    context_scope.enter_scope(first, "op-1", lambda: _span_context("first"))
    context_scope.enter_scope(second, "op-1", lambda: _span_context("second"))

    assert context_scope.owns_current(first) is True
    assert context_scope.owns_current(second) is True

    context_scope.exit_scope(first, "op-1")
    assert context_scope.owns_current(first) is False
    assert context_scope.owns_current(second) is False


def test_enter_discards_scopes_from_a_previous_epoch():
    """A scope a suspended operation left behind must not outlive its invocation.

    The SDK re-raises ``SuspendExecution`` without calling
    ``on_user_function_end``, so the next invocation on a reused thread finds a
    stale scope; the epoch check drops it before attaching.
    """
    owner = _Owner()
    before = otel_context.get_current()

    context_scope.enter_scope(
        owner, "suspended", lambda: _span_context("suspended"), epoch=1
    )
    assert context_scope.depth(owner) == 1

    context_scope.enter_scope(owner, "next", lambda: _span_context("next"), epoch=2)
    assert context_scope.depth(owner) == 1

    context_scope.exit_scope(owner, "next")
    assert otel_context.get_current() is before
    assert context_scope.depth(owner) == 0


def test_unwind_detaches_every_scope_for_one_owner():
    owner = _Owner()
    before = otel_context.get_current()

    context_scope.enter_scope(owner, "a", lambda: _span_context("a"))
    context_scope.enter_scope(owner, "b", lambda: _span_context("b"))

    context_scope.unwind(owner)

    assert otel_context.get_current() is before
    assert context_scope.depth(owner) == 0


def test_unwind_is_a_noop_for_an_owner_with_no_scopes():
    before = otel_context.get_current()

    context_scope.unwind(_Owner())

    assert otel_context.get_current() is before


def test_scopes_are_confined_to_the_thread_that_attached_them():
    """A worker thread's scopes must not appear on, or be poppable from, another.

    Tokens are only resettable in the ``contextvars.Context`` that created them,
    so the stack is per thread.
    """
    owner = _Owner()
    before = otel_context.get_current()
    observed: dict[str, object] = {}

    def worker() -> None:
        context_scope.enter_scope(owner, "worker", lambda: _span_context("worker"))
        observed["worker_depth"] = context_scope.depth(owner)
        observed["worker_span_valid"] = (
            trace.get_current_span().get_span_context().is_valid
        )
        context_scope.exit_scope(owner, "worker")
        observed["worker_depth_after"] = context_scope.depth(owner)

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(worker).result()

    assert observed["worker_depth"] == 1
    assert observed["worker_span_valid"] is True
    assert observed["worker_depth_after"] == 0
    # The calling thread never saw the worker's scope.
    assert context_scope.depth(owner) == 0
    assert otel_context.get_current() is before


# ---------------------------------------------------------------------------
# Plugin lifecycle paths
# ---------------------------------------------------------------------------
def _execution_plugin() -> tuple[ExecutionOtelPlugin, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return (
        ExecutionOtelPlugin(
            OtelPluginConfig(
                provider_source=ProviderSource.EXPLICIT,
                tracer_provider=provider,
                context_extractor=lambda _: Context(),
                enrich_logger=False,
            )
        ),
        exporter,
    )


def _invocation_plugin() -> tuple[InvocationOtelPlugin, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return (
        InvocationOtelPlugin(
            OtelPluginConfig(
                provider_source=ProviderSource.EXPLICIT,
                tracer_provider=provider,
                context_extractor=lambda _: Context(),
                enrich_logger=False,
            )
        ),
        exporter,
    )


def _invocation_start() -> InvocationStartInfo:
    return InvocationStartInfo(
        request_id="request-1",
        execution_arn=EXECUTION_ARN,
        execution_start_time=START_TIME,
        is_first_invocation=True,
    )


def _invocation_end(
    status: InvocationStatus = InvocationStatus.SUCCEEDED,
) -> InvocationEndInfo:
    return InvocationEndInfo(
        request_id="request-1",
        execution_arn=EXECUTION_ARN,
        execution_start_time=START_TIME,
        is_first_invocation=True,
        status=status,
        error=None,
    )


def _step_start(operation_id: str) -> UserFunctionStartInfo:
    return UserFunctionStartInfo(
        operation_id=operation_id,
        operation_type=OperationType.STEP,
        sub_type=OperationSubType.STEP,
        name=operation_id,
        parent_id=None,
        start_time=START_TIME,
        is_replayed=False,
        status=OperationStatus.STARTED,
        is_replay_children=False,
        attempt=1,
    )


def _step_end(operation_id: str) -> UserFunctionEndInfo:
    return UserFunctionEndInfo(
        operation_id=operation_id,
        operation_type=OperationType.STEP,
        sub_type=OperationSubType.STEP,
        name=operation_id,
        parent_id=None,
        start_time=START_TIME,
        end_time=END_TIME,
        is_replayed=False,
        status=OperationStatus.SUCCEEDED,
        is_replay_children=False,
        attempt=1,
        outcome=UserFunctionOutcome.SUCCEEDED,
        error=None,
    )


def _context_start(operation_id: str, parent_id: str | None) -> UserFunctionStartInfo:
    """Start info for a child-context (including virtual/FLAT branch) body."""
    return UserFunctionStartInfo(
        operation_id=operation_id,
        operation_type=OperationType.CONTEXT,
        sub_type=None,
        name=operation_id,
        parent_id=parent_id,
        start_time=START_TIME,
        is_replayed=False,
        status=OperationStatus.STARTED,
        is_replay_children=False,
        attempt=None,
    )


def _context_end(operation_id: str, parent_id: str | None) -> UserFunctionEndInfo:
    """End info for a child-context body."""
    return UserFunctionEndInfo(
        operation_id=operation_id,
        operation_type=OperationType.CONTEXT,
        sub_type=None,
        name=operation_id,
        parent_id=parent_id,
        start_time=START_TIME,
        end_time=END_TIME,
        is_replayed=False,
        status=OperationStatus.SUCCEEDED,
        is_replay_children=False,
        attempt=None,
        outcome=UserFunctionOutcome.SUCCEEDED,
        error=None,
    )


@pytest.mark.parametrize("factory", [_execution_plugin, _invocation_plugin])
def test_invocation_end_unwinds_a_suspended_operation_scope(factory):
    """A step that suspends never gets its end hook; invocation end cleans up.

    ``wrap_user_function`` re-raises ``SuspendExecution`` without calling
    ``on_user_function_end``, so the scope is still attached when the invocation
    winds down.
    """
    plugin, _ = factory()
    before = otel_context.get_current()

    plugin.on_invocation_start(_invocation_start())
    plugin.on_user_function_start(_step_start("step-suspends"))
    assert context_scope.depth(plugin) == 1

    plugin.on_invocation_end(_invocation_end(InvocationStatus.PENDING))

    assert context_scope.depth(plugin) == 0
    assert otel_context.get_current() is before


@pytest.mark.parametrize("factory", [_execution_plugin, _invocation_plugin])
def test_user_function_hooks_on_a_worker_thread_leave_the_caller_alone(factory):
    """Verify hooks running on the user-code thread do not touch the caller.

    User code runs on a worker the SDK owns, and ``ThreadPoolExecutor`` does not
    copy contextvars, so the plugin's scope must stay on that thread.
    """
    plugin, _ = factory()
    plugin.on_invocation_start(_invocation_start())
    before = otel_context.get_current()
    observed: dict[str, object] = {}

    def run_step() -> None:
        plugin.on_user_function_start(_step_start("step-1"))
        observed["inside"] = trace.get_current_span().get_span_context().is_valid
        plugin.on_user_function_end(_step_end("step-1"))
        observed["after"] = context_scope.depth(plugin)

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(run_step).result()

    assert observed["inside"] is True
    assert observed["after"] == 0
    assert otel_context.get_current() is before
    assert trace.get_current_span().get_span_context().is_valid is False

    plugin.on_invocation_end(_invocation_end())


def test_the_new_context_is_built_after_stale_scopes_are_dropped():
    """The attached context must not inherit a scope that is being detached.

    The context is normally derived from what is current, so building it before
    cleanup would copy baggage or suppression values out of the stale scope, and
    detaching afterwards cannot remove them from an already-built Context.
    """
    owner = _Owner()
    before = otel_context.get_current()

    # A stale scope under the same key, carrying baggage.
    context_scope.enter_scope(
        owner,
        "op-1",
        lambda: baggage.set_baggage(
            "poll", "first", context=otel_context.get_current()
        ),
        epoch=1,
    )
    assert baggage.get_baggage("poll") == "first"

    # Re-entering builds its context from whatever is current at that moment,
    # which must already be the pre-stale context.
    observed: dict[str, object] = {}

    def build() -> Context:
        observed["seen_during_build"] = baggage.get_baggage("poll")
        return otel_context.get_current()

    context_scope.enter_scope(owner, "op-1", build, epoch=1)

    assert observed["seen_during_build"] is None
    assert baggage.get_baggage("poll") is None

    context_scope.exit_scope(owner, "op-1")
    assert otel_context.get_current() is before


@pytest.mark.parametrize("factory", [_execution_plugin, _invocation_plugin])
def test_flat_branch_scope_survives_its_inner_operations(factory):
    """A FLAT map/parallel branch scope must stay attached across inner steps.

    A virtual (FLAT) branch reports its inner operations' parent as the
    grandparent -- None for a top-level branch -- while the branch's own context
    scope is still running (see DurableContext.is_virtual). Physical nesting is
    therefore not derivable from parent_id, and treating an inner step as
    root-level must not detach the live branch scope: work between two inner steps
    would fall out of the durable trace.
    """
    plugin, _ = factory()
    plugin.on_invocation_start(_invocation_start())
    observed: dict[str, object] = {}

    def run_branch() -> None:
        # The virtual branch body: a CONTEXT operation at the top level.
        plugin.on_user_function_start(
            _context_start("flat-branch", parent_id=None),
        )
        branch_span_id = trace.get_current_span().get_span_context().span_id

        # Inner steps of a FLAT branch report parent_id=None, not the branch.
        for index in range(2):
            step_id = f"flat-branch-step-{index}"
            plugin.on_user_function_start(_step_start(step_id))
            plugin.on_user_function_end(_step_end(step_id))
            # Between inner steps the branch scope is still current.
            observed[f"between-{index}"] = (
                trace.get_current_span().get_span_context().span_id
            )

        observed["branch"] = branch_span_id
        plugin.on_user_function_end(
            _context_end("flat-branch", parent_id=None),
        )
        observed["after_branch_depth"] = context_scope.depth(plugin)

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(run_branch).result()

    assert observed["between-0"] == observed["branch"]
    assert observed["between-1"] == observed["branch"]
    assert observed["after_branch_depth"] == 0

    plugin.on_invocation_end(_invocation_end())


@pytest.mark.parametrize("factory", [_execution_plugin, _invocation_plugin])
def test_propagated_operation_context_still_correlates_to_its_span(factory):
    """Logs on a thread that received an operation's context keep that span.

    User code may hand the operation's context to another thread. That thread has
    the attempt span current but holds no scope tokens, so a thread-local depth
    check would drop to the invocation span and lose precision.
    """
    plugin, _ = factory()
    plugin.on_invocation_start(_invocation_start())
    observed: dict[str, object] = {}

    def run_step() -> None:
        plugin.on_user_function_start(_step_start("step-1"))
        propagated = otel_context.get_current()
        attempt_span_id = trace.get_current_span().get_span_context().span_id

        def worker() -> None:
            token = otel_context.attach(propagated)
            try:
                resolved = plugin.get_current_span_context()
                observed["propagated"] = resolved.span_id if resolved else None
            finally:
                otel_context.detach(token)

        # A thread the user handed the operation's context to.
        with ThreadPoolExecutor(max_workers=1) as inner:
            inner.submit(worker).result()

        observed["attempt"] = attempt_span_id
        plugin.on_user_function_end(_step_end("step-1"))

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(run_step).result()

    assert observed["propagated"] == observed["attempt"]

    plugin.on_invocation_end(_invocation_end())


@pytest.mark.parametrize("factory", [_execution_plugin, _invocation_plugin])
def test_a_nested_scope_on_a_propagated_context_builds_on_it(factory):
    """A scope entered where a context was propagated must not discard it.

    The receiving thread holds no tokens, so treating the scope as outermost would
    rebase onto the extracted context and drop the propagated baggage.
    """
    plugin, _ = factory()
    plugin._context_extractor = lambda _info: Context()
    plugin.on_invocation_start(_invocation_start())
    observed: dict[str, object] = {}

    def run_step() -> None:
        plugin.on_user_function_start(_step_start("outer"))
        # Baggage added by user code inside the operation.
        token = otel_context.attach(
            baggage.set_baggage("tenant", "acme", context=otel_context.get_current())
        )
        propagated = otel_context.get_current()
        otel_context.detach(token)

        def worker() -> None:
            handed = otel_context.attach(propagated)
            try:
                plugin.on_user_function_start(_step_start("inner"))
                observed["baggage"] = baggage.get_baggage("tenant")
                plugin.on_user_function_end(_step_end("inner"))
            finally:
                otel_context.detach(handed)

        with ThreadPoolExecutor(max_workers=1) as inner:
            inner.submit(worker).result()

        plugin.on_user_function_end(_step_end("outer"))

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(run_step).result()

    assert observed["baggage"] == "acme"

    plugin.on_invocation_end(_invocation_end())


@pytest.mark.parametrize("factory", [_execution_plugin, _invocation_plugin])
def test_an_empty_extracted_context_isolates_the_operation(factory):
    """An extractor returning an empty Context must not inherit ambient values.

    Context subclasses dict, so an empty one is falsy; treating it as "no context"
    would silently invert the extractor's intent and leak the worker's ambient
    baggage into the operation.
    """
    plugin, _ = factory()
    plugin._context_extractor = lambda _info: Context()
    plugin.on_invocation_start(_invocation_start())
    observed: dict[str, object] = {}

    def run_step() -> None:
        # Something ambient on the worker, as auto-instrumentation might leave.
        token = otel_context.attach(
            baggage.set_baggage("ambient", "leaked", context=otel_context.get_current())
        )
        try:
            plugin.on_user_function_start(_step_start("step-1"))
            observed["inside"] = baggage.get_baggage("ambient")
            plugin.on_user_function_end(_step_end("step-1"))
        finally:
            otel_context.detach(token)

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(run_step).result()

    assert observed["inside"] is None

    plugin.on_invocation_end(_invocation_end())


@pytest.mark.parametrize("factory", [_execution_plugin, _invocation_plugin])
def test_extracted_context_values_reach_user_code_on_a_worker_thread(factory):
    """Values from the context extractor must be current inside user code.

    The worker running user code starts with an empty context, so the outermost
    durable scope has to be layered onto the extracted context -- otherwise
    baggage and any other non-span values the extractor supplied are dropped and
    downstream instrumentation inside the step cannot propagate them.
    """
    plugin, _ = factory()
    # An extractor that supplies baggage, as a propagator-based one would.
    plugin._context_extractor = lambda _info: baggage.set_baggage(
        "tenant", "acme", context=Context()
    )
    plugin.on_invocation_start(_invocation_start())
    observed: dict[str, object] = {}

    def run_step() -> None:
        plugin.on_user_function_start(_step_start("step-1"))
        observed["inside"] = baggage.get_baggage("tenant")
        # A nested scope keeps it too, since it layers onto the current context.
        plugin.on_user_function_start(_step_start("step-2"))
        observed["nested"] = baggage.get_baggage("tenant")
        plugin.on_user_function_end(_step_end("step-2"))
        plugin.on_user_function_end(_step_end("step-1"))

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(run_step).result()

    assert observed["inside"] == "acme"
    assert observed["nested"] == "acme"

    plugin.on_invocation_end(_invocation_end())


@pytest.mark.parametrize("factory", [_execution_plugin, _invocation_plugin])
def test_suspend_then_reenter_then_end_leaves_no_residue(factory):
    """A suspended operation re-entered in the same invocation stays balanced.

    The first start has no matching end (the SDK re-raises SuspendExecution), so
    the re-entry must replace that scope rather than stack on it.
    """
    plugin, _ = factory()
    before = otel_context.get_current()
    plugin.on_invocation_start(_invocation_start())
    observed: dict[str, object] = {}

    def run_polls() -> None:
        # Poll 1 suspends: start fires, end never does.
        plugin.on_user_function_start(_step_start("wfc-1"))
        # Poll 2 re-enters the same operation and completes.
        plugin.on_user_function_start(_step_start("wfc-1"))
        observed["depth_after_reentry"] = context_scope.depth(plugin)
        plugin.on_user_function_end(_step_end("wfc-1"))
        observed["depth_after_end"] = context_scope.depth(plugin)

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(run_polls).result()

    assert observed["depth_after_reentry"] == 1
    assert observed["depth_after_end"] == 0
    assert otel_context.get_current() is before

    plugin.on_invocation_end(_invocation_end())


def test_two_plugins_on_one_thread_unwind_in_lifo_order():
    """Both plugins ship as entry points and can be enabled together.

    Hooks dispatch in registration order, so the second plugin's scope is
    detached while the first plugin's end hook runs. The shared stack keeps the
    underlying ContextVar LIFO instead of reviving the first plugin's scope.
    """
    first, _ = _execution_plugin()
    second, _ = _invocation_plugin()
    before = otel_context.get_current()

    for plugin in (first, second):
        plugin.on_invocation_start(_invocation_start())
    for plugin in (first, second):
        plugin.on_user_function_start(_step_start("step-1"))
    for plugin in (first, second):
        plugin.on_user_function_end(_step_end("step-1"))

    assert context_scope.depth() == 0
    assert otel_context.get_current() is before

    for plugin in (first, second):
        plugin.on_invocation_end(_invocation_end())
