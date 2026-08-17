"""Balanced ``opentelemetry.context`` attach/detach bookkeeping for the plugins.

The OpenTelemetry Context specification requires every ``context.attach()`` to
have a corresponding ``context.detach(token)``. Detaching is only possible with
the token that ``attach`` returned, so the token has to survive from the hook
that attached to the hook that pops it -- the plugin hooks are separate calls,
so the idiomatic ``with tracer.start_as_current_span(...)`` form is unavailable.

Two properties of the runtime shape the design:

* **Tokens are thread-confined.** The plugin hooks run on several threads: the
  invocation hooks on the Lambda handler thread, the user-function hooks on the
  ``dex-handler`` worker that runs user code, and on a branch worker for each
  ``map``/``parallel`` branch. ``ContextVar.reset()`` only accepts a token
  created in the same ``contextvars.Context``, so each thread keeps its own
  stack and only ever detaches its own tokens.
* **Detach order matters.** Unlike OpenTelemetry Java's ``Scope.close()`` --
  which ignores a close that does not represent the current context --
  ``ContextVar.reset()`` unconditionally writes back the token's captured value.
  Detaching out of order therefore *revives* a stale context instead of failing
  safe. The stack is module level rather than per plugin instance so that two
  plugins attaching on the same thread (both ship as separate entry points and
  can be enabled together) still unwind in true LIFO order.

Scopes are keyed by ``(owner, key)`` so a plugin instance can pop the exact
scope it pushed, while :func:`exit_scope` still unwinds anything stacked above
it. Nothing here raises: a plugin must never break an execution over
observability bookkeeping.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable


if TYPE_CHECKING:
    from contextvars import Token

    from opentelemetry.context import Context


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _Entry:
    """One attached scope: who pushed it, under what key, and its token."""

    owner_id: int
    key: str
    epoch: int
    token: Token[Context]


class _ThreadState(threading.local):
    """Per-thread LIFO stack of attached scopes."""

    def __init__(self) -> None:
        self.entries: list[_Entry] = []


_state = _ThreadState()

# Records which plugin instances established the current OTel context. Unlike the
# token stack, this lives *in* the context, so it survives propagation: code that
# carries an operation's context to another thread -- asyncio.to_thread,
# contextvars.copy_context, an instrumented executor -- takes the marker with it,
# and a plugin can still tell that the current span is one of its own. Thread-local
# bookkeeping cannot answer that, because the receiving thread has no tokens.
_OWNER_KEY = "aws-durable-execution-otel-scope-owners"


def _owner_ids(context: Context | None = None) -> tuple[int, ...]:
    """Return the plugin ids that established ``context`` (default: current)."""
    from opentelemetry import context as otel_context

    owners = otel_context.get_value(_OWNER_KEY, context=context)
    if isinstance(owners, tuple):
        return owners
    return ()


def owns_current(owner: Any) -> bool:
    """True if ``owner`` established the context that is current on this thread.

    Answers "is the current span one of mine?" -- the question both plugins need
    before trusting ``trace.get_current_span()`` over their own span registry. A
    context this plugin never attached (the ambient Lambda span an ADOT layer makes
    current on the handler thread) has no marker and correctly reports False.

    Ids accumulate, so two plugins tracking the same operation each recognise their
    own scope rather than only the innermost one.
    """
    return id(owner) in _owner_ids()


def _detach(entry: _Entry) -> None:
    """Detach one entry, swallowing any failure."""
    from opentelemetry import context as otel_context

    try:
        otel_context.detach(entry.token)
    except Exception:  # noqa: BLE001 - observability must not break execution
        logger.debug("Failed to detach OTel context scope %s", entry.key, exc_info=True)


def enter_scope(
    owner: Any,
    key: str,
    context_factory: Callable[[], Context],
    epoch: int = 0,
) -> None:
    """Attach a context on this thread and remember how to restore it.

    Scopes left over from an earlier ``epoch``, or from an earlier entry under the
    same ``key``, are unwound first: the SDK re-raises ``SuspendExecution`` without
    calling ``on_user_function_end``, so a suspended operation leaves its scope
    attached.

    ``context_factory`` is called *after* that cleanup, not before. The context to
    attach is normally derived from what is current, so building it first would
    copy values from a scope that is about to be detached -- baggage, suppression
    flags -- and detaching afterwards cannot remove them from a context that has
    already been constructed.

    Args:
        owner: The plugin instance pushing the scope.
        key: Registry key for the scope, unique per owner (operation or attempt).
        context_factory: Builds the context to attach, called after cleanup.
        epoch: The owner's invocation counter; scopes from older epochs are
            discarded before the new scope is pushed.
    """
    from opentelemetry import context as otel_context

    owner_id = id(owner)
    _discard_stale(owner_id, epoch)
    _discard_reentered(owner_id, key)
    try:
        context = context_factory()
        # Stamp ownership into the context itself so it travels with any
        # propagation of it (see _OWNER_KEY).
        context = otel_context.set_value(
            _OWNER_KEY, (*_owner_ids(context), owner_id), context=context
        )
        token = otel_context.attach(context)
    except Exception:  # noqa: BLE001
        logger.debug("Failed to attach OTel context scope %s", key, exc_info=True)
        return
    _state.entries.append(_Entry(owner_id=owner_id, key=key, epoch=epoch, token=token))


def exit_scope(owner: Any, key: str) -> None:
    """Detach the scope ``owner`` pushed under ``key``, restoring what preceded it.

    Scopes stacked above the target are detached first so the underlying
    ``ContextVar`` is always reset in LIFO order. A key this thread never pushed
    is a no-op -- the scope belongs to another thread (or was already unwound),
    and detaching someone else's token would corrupt the context.
    """
    owner_id = id(owner)
    index = _find_last(owner_id, key)
    if index is None:
        return
    for entry in reversed(_state.entries[index:]):
        _detach(entry)
    del _state.entries[index:]


def unwind(owner: Any) -> None:
    """Detach every scope ``owner`` still holds on this thread, newest first.

    Called at invocation end so the handler thread is left exactly as the plugin
    found it. Scopes this owner pushed on *other* threads cannot be detached from
    here; those threads are created per invocation and their context dies with
    them.
    """
    owner_id = id(owner)
    index = _find_first(owner_id)
    if index is None:
        return
    for entry in reversed(_state.entries[index:]):
        _detach(entry)
    del _state.entries[index:]


def depth(owner: Any | None = None) -> int:
    """Return the number of scopes attached on this thread (for tests)."""
    if owner is None:
        return len(_state.entries)
    owner_id = id(owner)
    return sum(1 for entry in _state.entries if entry.owner_id == owner_id)


def _discard_reentered(owner_id: int, key: str) -> None:
    """Unwind a scope this owner already holds under ``key`` on this thread.

    The epoch check only catches a *previous invocation's* leftovers. The same
    operation key can also be entered twice inside one invocation, when a
    suspended operation is re-entered after its branch is resubmitted, and its
    first scope is still attached because the suspending path had no end hook to
    pop it. Without this, the second enter would stack on the first and the
    eventual end hook -- which pops one scope -- would leave the original
    attached.

    A scope abandoned by a *different* operation on this thread cannot be
    detected here. Physical nesting is not derivable from the hook payloads:
    ``parent_id`` is checkpoint hierarchy, and a FLAT map/parallel branch
    deliberately reports its inner operations' parent as the grandparent (see
    ``DurableContext.is_virtual``), so a live branch scope would be
    indistinguishable from an abandoned sibling. Closing that gap needs the SDK
    to report the end of a suspended user function, which it does not do today.
    """
    index = next(
        (
            position
            for position, entry in enumerate(_state.entries)
            if entry.owner_id == owner_id and entry.key == key
        ),
        None,
    )
    if index is None:
        return
    for entry in reversed(_state.entries[index:]):
        _detach(entry)
    del _state.entries[index:]


def _discard_stale(owner_id: int, epoch: int) -> None:
    """Unwind this owner's scopes left over from a previous epoch."""
    index = next(
        (
            position
            for position, entry in enumerate(_state.entries)
            if entry.owner_id == owner_id and entry.epoch != epoch
        ),
        None,
    )
    if index is None:
        return
    for entry in reversed(_state.entries[index:]):
        _detach(entry)
    del _state.entries[index:]


def _find_last(owner_id: int, key: str) -> int | None:
    """Index of this owner's most recent scope for ``key``, if any."""
    for position in range(len(_state.entries) - 1, -1, -1):
        entry = _state.entries[position]
        if entry.owner_id == owner_id and entry.key == key:
            return position
    return None


def _find_first(owner_id: int) -> int | None:
    """Index of this owner's oldest scope, if any."""
    for position, entry in enumerate(_state.entries):
        if entry.owner_id == owner_id:
            return position
    return None
