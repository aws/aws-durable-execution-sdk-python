"""Bound caller waiting for provider I/O without moving plugin lifecycle hooks."""

from __future__ import annotations

import contextvars
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field


logger = logging.getLogger(__name__)

# Match the OTel provider's existing default budget. Some processors ignore
# that cooperative budget, so the SDK bounds its own wait independently.
_FLUSH_TIMEOUT_SECONDS = 30.0
_lock = threading.Condition()


@dataclass
class _Attempt:
    done: threading.Event = field(default_factory=threading.Event)
    result: object = None
    error: BaseException | None = None
    timed_out: bool = False


_active: _Attempt | None = None


def _run(flush: Callable[[], object], attempt: _Attempt) -> None:
    global _active  # noqa: PLW0603
    try:
        try:
            attempt.result = flush()
        except BaseException as error:
            # Fatal exceptions completed within the wait are re-raised on the
            # lifecycle owner. A late error cannot be thrown into a returned call.
            attempt.error = error
        with _lock:
            late = attempt.timed_out
            if not late and _active is attempt:
                _active = None
                _lock.notify_all()
            attempt.done.set()
        if late and attempt.error is not None:
            logger.error(
                "Provider flush failed after the caller stopped waiting",
                exc_info=(
                    type(attempt.error),
                    attempt.error,
                    attempt.error.__traceback__,
                ),
            )
    finally:
        # Retain the slot through actual completion, even after timeout. There
        # is no queue and no new worker per invocation for a stuck exporter.
        with _lock:
            if _active is attempt:
                _active = None
                _lock.notify_all()


def flush_provider(flush: Callable[[], object]) -> bool:
    """Wait for provider I/O, with one outstanding attempt process-wide.

    Start/End and ContextVar-token cleanup stay on their existing owner. Only
    the supplied provider method runs elsewhere, using the post-cleanup Context
    snapshot. Admission and completion share one absolute deadline. Expired
    requests return False without queueing more work; active I/O is never cancelled.
    This does not bound synchronous span processors' earlier ``on_end`` calls,
    application-owned provider shutdown, or exporter transport I/O itself.
    """
    global _active  # noqa: PLW0603
    deadline = time.monotonic() + _FLUSH_TIMEOUT_SECONDS
    with _lock:
        while _active is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            _lock.wait(remaining)
        if _active is None and time.monotonic() < deadline:
            attempt = _Attempt()
            _active = attempt
        else:
            attempt = None
    if attempt is None:
        logger.warning("Provider flush admission exceeded the caller wait budget")
        return False

    owner_context = contextvars.copy_context()
    try:
        threading.Thread(
            target=owner_context.run,
            args=(_run, flush, attempt),
            name="durable-otel-flush",
            daemon=True,
        ).start()
    except BaseException:
        with _lock:
            if _active is attempt:
                _active = None
                _lock.notify_all()
        raise

    completed = attempt.done.wait(max(0.0, deadline - time.monotonic()))
    if not completed:
        with _lock:
            completed = attempt.done.is_set()
            if not completed:
                attempt.timed_out = True
        if not completed:
            logger.warning("Provider flush exceeded the caller wait budget")
            return False
    if attempt.error is not None:
        raise attempt.error
    if attempt.result is False:
        logger.warning("Provider reported an incomplete flush")
        return False
    return True
