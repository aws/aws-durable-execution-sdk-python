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
from aws_durable_execution_sdk_python_testing.checkpoint.validators.checkpoint import (
    CheckpointValidator,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.execution import (
    CheckpointIdempotencyRecord,
    OperationPaginatorState,
)
from aws_durable_execution_sdk_python_testing.observer import ExecutionNotifier
from aws_durable_execution_sdk_python_testing.token import CheckpointToken


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.lambda_service import OperationUpdate

    from aws_durable_execution_sdk_python_testing.execution import Execution
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
        self._notifier = ExecutionNotifier()
        self._dispatcher = CheckpointRequestDispatcher()

    def add_execution_observer(self, observer) -> None:
        """Add observer for execution events."""
        self._notifier.add_observer(observer)

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

        if updates:
            CheckpointValidator.validate_input(updates, execution)
            self._dispatcher.apply_updates(
                execution=execution,
                updates=updates,
                client_token=client_token,
                notifier=self._notifier,
                touch=execution.touch_operation,
            )

        new_token_sequence = execution.advance_token_sequence()

        paginator = OperationPaginatorState.pin(execution)
        # The checkpoint response returns the full unseen delta in a single
        # response. Advance handler_seen_seq to cover every returned op so
        # the next delta carries only operations touched after this response.
        response_ops = paginator.unseen_operations()
        if response_ops:
            highest_delivered_seq = max(
                execution.operation_last_touched_seq[op.operation_id]
                for op in response_ops
            )
            paginator.advance_handler_seen(highest_delivered_seq)

        new_token = CheckpointToken(
            execution_arn=execution.durable_execution_arn,
            token_sequence=new_token_sequence,
            invocation_id=execution.current_invocation_id,
        ).to_str()

        execution.last_checkpoint = CheckpointIdempotencyRecord(
            client_token=client_token or "",
            inbound_checkpoint_token=checkpoint_token,
            outbound_checkpoint_token=new_token,
            operations=list(response_ops),
            next_marker=None,
        )

        self._store.update(execution)

        return CheckpointOutput(
            checkpoint_token=new_token,
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=response_ops,
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
    """Replay cached CheckpointOutput when the incoming call matches
    the last checkpoint. Returns ``None`` when there is no match.
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
