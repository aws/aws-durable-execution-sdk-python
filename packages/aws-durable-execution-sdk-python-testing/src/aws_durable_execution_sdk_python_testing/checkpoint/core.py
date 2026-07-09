"""The shared checkpoint write-transaction core.

Both checkpoint entry points need to detect an idempotent replay before
doing real work. This class holds that shared logic so neither caller
duplicates it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.execution import (
        CheckpointIdempotencyRecord,
        Execution,
    )


class CheckpointCore:
    """Shared checkpoint logic used by both the web and in-process paths."""

    @staticmethod
    def match_cached(
        execution: Execution,
        checkpoint_token: str,
        client_token: str | None,
    ) -> CheckpointIdempotencyRecord | None:
        """Return the cached idempotency record when the incoming
        ``(client_token, checkpoint_token)`` matches the last checkpoint.

        Returns ``None`` when there is no match. A missing or empty
        ``client_token`` cannot replay because idempotency requires an
        explicit client identifier.
        """
        if not client_token:
            return None
        cached: CheckpointIdempotencyRecord | None = execution.last_checkpoint
        if cached is None:
            return None
        if (
            cached.client_token != client_token
            or cached.inbound_checkpoint_token != checkpoint_token
        ):
            return None
        return cached
