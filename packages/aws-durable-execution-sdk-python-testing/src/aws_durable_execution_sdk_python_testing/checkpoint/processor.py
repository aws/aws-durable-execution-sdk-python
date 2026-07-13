"""In-process checkpoint flow.

Orchestrates the full checkpoint request for in-process callers
(``InMemoryServiceClient``). Mirrors the flow inside
``Executor.checkpoint_execution`` used by the HTTP path, so both
entry points share identical delta + watermark semantics.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    StateOutput,
)

from aws_durable_execution_sdk_python_testing.checkpoint.core import CheckpointCore
from aws_durable_execution_sdk_python_testing.checkpoint.transformer import (
    CheckpointRequestDispatcher,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.observer import apply_effects
from aws_durable_execution_sdk_python_testing.token import CheckpointToken


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.lambda_service import OperationUpdate

    from aws_durable_execution_sdk_python_testing.execution import Execution
    from aws_durable_execution_sdk_python_testing.observer import ExecutionObserver
    from aws_durable_execution_sdk_python_testing.scheduler import Scheduler
    from aws_durable_execution_sdk_python_testing.stores.base import ExecutionStore


# Canonical invocation-page byte budget, shared with the Executor.
DEFAULT_MAX_INVOCATION_PAGE_BYTES = 5 * 1024 * 1024


class CheckpointProcessor:
    """In-process checkpoint flow used by ``InMemoryServiceClient``.

    Uses the same :class:`CheckpointRequestDispatcher` + paginator
    primitives as ``Executor.checkpoint_execution`` so observable
    behaviour is identical across the two entry points.
    """

    def __init__(
        self,
        store: ExecutionStore,
        scheduler: Scheduler,  # noqa: ARG002 — kept for backward-compatible signature
    ):
        self._store = store
        self._observers: list[ExecutionObserver] = []
        self._dispatcher = CheckpointRequestDispatcher()

    def add_execution_observer(self, observer: ExecutionObserver) -> None:
        """Add observer for execution events."""
        self._observers.append(observer)

    def process_checkpoint(
        self,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        """Apply ``updates`` and return the delta since the handler's
        last observation.

        Advances ``token_sequence`` exactly once; bumps
        ``seq_counter`` once per accepted update via
        ``touch_operation``; advances ``handler_seen_seq`` only for
        operations actually returned in the response.
        """
        token = CheckpointToken.from_str(checkpoint_token)
        execution: Execution = self._store.load(token.execution_arn)

        # Idempotency first, before the token-sequence check.
        cached = _maybe_replay_cached(execution, checkpoint_token, client_token)
        if cached is not None:
            return cached

        if (
            execution.is_complete
            or token.token_sequence != execution.token_sequence
            or token.invocation_id != execution.current_invocation_id
        ):
            msg = "Invalid checkpoint token"
            raise InvalidParameterValueException(msg)

        result = CheckpointCore.apply(
            execution,
            checkpoint_token,
            updates,
            client_token,
            self._dispatcher,
        )

        self._store.update(execution)

        for observer in self._observers:
            apply_effects(result.effects, observer)

        return CheckpointOutput(
            checkpoint_token=result.checkpoint_token,
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=result.operations,
                next_marker=None,
            ),
        )

    def get_execution_state(
        self,
        checkpoint_token: str,
        next_marker: str,  # noqa: ARG002
        max_items: int = 1000,  # noqa: ARG002
    ) -> StateOutput:
        """Get current execution state.

        Returns the full navigable operation list with a null marker.
        Marker round-tripping and the invocation-state gate are
        enforced on the HTTP path in :class:`Executor`.
        """
        token = CheckpointToken.from_str(checkpoint_token)
        execution = self._store.load(token.execution_arn)
        return StateOutput(
            operations=execution.get_navigable_operations(),
            next_marker=None,
        )


def _maybe_replay_cached(
    execution: Execution,
    checkpoint_token: str,
    client_token: str | None,
) -> CheckpointOutput | None:
    """Replay the cached response when the incoming
    ``(client_token, checkpoint_token)`` matches the last checkpoint.

    Returns ``None`` when there is no matching cached record.
    """
    cached = CheckpointCore.match_cached(execution, checkpoint_token, client_token)
    if cached is None:
        return None
    return CheckpointOutput(
        checkpoint_token=cached.outbound_checkpoint_token,
        new_execution_state=CheckpointUpdatedExecutionState(
            operations=list(cached.operations),
            next_marker=cached.next_marker,
        ),
    )
