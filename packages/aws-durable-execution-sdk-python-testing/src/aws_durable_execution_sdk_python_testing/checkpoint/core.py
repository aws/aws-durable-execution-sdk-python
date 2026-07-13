"""The shared checkpoint write-transaction.

Both checkpoint entry points apply a batch of updates and compute the
delta of operations the handler has not yet seen. :class:`CheckpointCore`
holds that common logic so the two callers differ only in their gate,
locking, and response type.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple

from aws_durable_execution_sdk_python_testing.checkpoint.validators.checkpoint import (
    CheckpointValidator,
)
from aws_durable_execution_sdk_python_testing.execution import (
    CheckpointIdempotencyRecord,
    OperationPaginatorState,
)
from aws_durable_execution_sdk_python_testing.token import CheckpointToken

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.lambda_service import (
        Operation,
        OperationUpdate,
    )

    from aws_durable_execution_sdk_python_testing.checkpoint.effects import (
        CheckpointEffect,
    )
    from aws_durable_execution_sdk_python_testing.checkpoint.transformer import (
        CheckpointRequestDispatcher,
    )
    from aws_durable_execution_sdk_python_testing.execution import Execution


class CheckpointResult(NamedTuple):
    """Outcome of applying a checkpoint: the new token, the operations to
    return to the handler this round, and the lifecycle effects raised."""

    checkpoint_token: str
    operations: list[Operation]
    effects: list[CheckpointEffect]


class CheckpointCore:
    """The checkpoint write-transaction shared by both entry points."""

    @staticmethod
    def match_cached(
        execution: Execution,
        checkpoint_token: str,
        client_token: str | None,
    ) -> CheckpointIdempotencyRecord | None:
        """Return the cached idempotency record when the incoming
        ``(client_token, checkpoint_token)`` matches the last checkpoint,
        else ``None``.

        A missing ``client_token`` cannot replay: idempotency requires an
        explicit client identifier. The caller builds its own response
        type from the returned record.
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

    @staticmethod
    def apply(
        execution: Execution,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
        dispatcher: CheckpointRequestDispatcher,
    ) -> CheckpointResult:
        """Apply ``updates`` to ``execution`` and compute the response delta.

        Advances ``token_sequence`` exactly once, returns the full set of
        operations the handler has not yet seen, advances
        ``handler_seen_seq`` to cover them, and records the
        idempotency entry for a byte-identical replay of a retried call.
        The caller is responsible for the invocation gate, locking,
        persistence, and applying the returned effects.
        """
        effects: list[CheckpointEffect] = []
        if updates:
            CheckpointValidator.validate_input(updates, execution)
            effects = dispatcher.apply_updates(
                execution=execution,
                updates=updates,
                client_token=client_token,
                touch=execution.touch_operation,
            )

        new_token_sequence: int = execution.advance_token_sequence()

        paginator: OperationPaginatorState = OperationPaginatorState.pin(execution)
        # The checkpoint response returns the full unseen delta in a single
        # response. Advance handler_seen_seq to cover every returned op so
        # the next delta carries only operations touched after this response.
        response_ops: list[Operation] = paginator.unseen_operations()
        if response_ops:
            highest_delivered_seq: int = max(
                execution.operation_last_touched_seq[op.operation_id]
                for op in response_ops
            )
            paginator.advance_handler_seen(highest_delivered_seq)

        new_token: str = CheckpointToken(
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

        return CheckpointResult(new_token, response_ops, effects)
