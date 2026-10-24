"""One monotonic time budget for cooperative invocation interruption."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.exceptions import InvocationDeadlineExceeded

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.threading import CompletionEvent
    from aws_durable_execution_sdk_python.types import LambdaContext

# Stop admitting work before the platform deadline to leave time for cooperative
# cleanup. This is not a bound on arbitrary user code, clients, or plugin hooks.
_CLEANUP_MARGIN_SECONDS = 0.1


@dataclass(frozen=True)
class InvocationDeadline:
    """Internal invocation-local deadline; None keeps standalone state unbounded."""

    expires_at: float | None = None

    @classmethod
    def from_lambda_context(cls, context: LambdaContext) -> InvocationDeadline:
        # Capture once: wall-clock adjustments cannot extend this invocation.
        started_at = time.monotonic()
        remaining_seconds = context.get_remaining_time_in_millis() / 1000
        return cls(started_at + max(0.0, remaining_seconds - _CLEANUP_MARGIN_SECONDS))

    def remaining(self, timeout: float | None = None) -> float | None:
        """Clamp an existing wait to the same budget, without resetting it."""
        if self.expires_at is None:
            return timeout
        remaining = max(0.0, self.expires_at - time.monotonic())
        return remaining if timeout is None else min(timeout, remaining)

    def check(self) -> None:
        if self.remaining() == 0:
            raise InvocationDeadlineExceeded("Lambda invocation time budget exhausted")

    def wait(self, event: CompletionEvent, timeout: float | None = None) -> bool:
        """Do not turn an acknowledged checkpoint into an error after its wait."""
        completed = event.wait(self.remaining(timeout))
        if not completed:
            self.check()
        return completed
