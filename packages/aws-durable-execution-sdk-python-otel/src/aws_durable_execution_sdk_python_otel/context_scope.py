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
from typing import TYPE_CHECKING, Any


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
    context: Context,
    epoch: int = 0,
    parent_key: str | None = None,
) -> None:
    """Attach ``context`` on this thread and remember how to restore it.

    Scopes this owner holds that the new one does not nest inside are unwound
    first, as are any left over from an earlier ``epoch``. Both cover the paths
    where a paired pop never runs: the SDK re-raises ``SuspendExecution`` without
    calling ``on_user_function_end``, so a suspended operation leaves its scope
    attached, and the worker that ran it goes on to other work.

    Args:
        owner: The plugin instance pushing the scope.
        key: Registry key for the scope, unique per owner (operation or attempt).
        context: The context to attach.
        epoch: The owner's invocation counter; scopes from older epochs are
            discarded before the new scope is pushed.
        parent_key: Scope key of the enclosing operation, or None for a
            root-level one. Used to tell legitimate nesting from a stale scope.
    """
    from opentelemetry import context as otel_context

    owner_id = id(owner)
    _discard_stale(owner_id, epoch)
    _unwind_non_ancestors(owner_id, parent_key)
    try:
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


def _unwind_non_ancestors(owner_id: int, parent_key: str | None) -> None:
    """Drop scopes this owner holds on this thread that the new scope is not inside.

    A scope may stay attached only while the operation it belongs to is still
    running on this thread, and the only place that can be checked is here: the
    suspending path has no end hook, so a suspended operation leaves its scope
    behind. Two cases produce one:

    * A branch-pool worker has no branch affinity. If branch A suspends and its
      worker next runs branch B, A's scope has the same epoch and a different key,
      so neither the epoch nor a same-key check clears it. B would nest inside A
      and, on exit, detach back into it.
    * The same operation can be re-entered when its branch is resubmitted.

    The new scope nests inside its parent operation, so anything above that parent
    is stale. When the parent is absent -- a root-level operation, or a parent that
    never ran on this thread -- nothing this owner holds here can enclose it.

    Detaching necessarily discards entries stacked above the cut, including other
    owners', because the underlying ``ContextVar`` can only be reset in order. The
    normal nesting path is a no-op, so that does not disturb a second plugin
    tracking the same operations.
    """
    positions = [
        position
        for position, entry in enumerate(_state.entries)
        if entry.owner_id == owner_id
    ]
    if not positions:
        return
    if parent_key is not None and _state.entries[positions[-1]].key == parent_key:
        # Normal nesting: the innermost scope we hold is the new scope's parent.
        return
    cut = positions[0]
    if parent_key is not None:
        for position in reversed(positions):
            if _state.entries[position].key == parent_key:
                cut = position + 1
                break
    for entry in reversed(_state.entries[cut:]):
        _detach(entry)
    del _state.entries[cut:]


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
