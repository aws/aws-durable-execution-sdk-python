"""Per-execution handler-invocation state."""

from __future__ import annotations

from enum import Enum


class InvocationState(Enum):
    """Tracks whether a handler is currently running for one execution.

    Held in memory per execution and never persisted: a persisted
    ``INVOKING`` state could be read back after a crash as "a handler is
    running" when none is, stranding the execution behind the gate.

    Transitions:

    * ``PRE_INVOKE`` -> ``INVOKING`` when a handler call is dispatched.
    * ``INVOKING`` -> ``PRE_INVOKE`` when a handler returns ``PENDING``
      or fails; a retry re-enters ``PRE_INVOKE`` -> ``INVOKING``.
    * ``INVOKING`` -> ``COMPLETED`` when the execution reaches a terminal
      status (handler returned ``SUCCEEDED`` / ``FAILED``, a stop was
      requested, or the invocation timed out).
    """

    PRE_INVOKE = "PRE_INVOKE"
    INVOKING = "INVOKING"
    COMPLETED = "COMPLETED"
