"""Checkpoint request dispatcher.

Routes each ``OperationUpdate`` in a checkpoint to its type-specific
processor (step, wait, callback, context, execution) and applies the
result to the execution.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, ClassVar

from aws_durable_execution_sdk_python.lambda_service import (
    OperationType,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processors.callback import (
    CallbackProcessor,
)
from aws_durable_execution_sdk_python_testing.checkpoint.processors.context import (
    ContextProcessor,
)
from aws_durable_execution_sdk_python_testing.checkpoint.processors.execution import (
    ExecutionProcessor,
)
from aws_durable_execution_sdk_python_testing.checkpoint.processors.step import (
    StepProcessor,
)
from aws_durable_execution_sdk_python_testing.checkpoint.processors.wait import (
    WaitProcessor,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)
from aws_durable_execution_sdk_python_testing.observer import ExecutionNotifier


if TYPE_CHECKING:
    from collections.abc import Callable, MutableMapping

    from aws_durable_execution_sdk_python.lambda_service import (
        OperationUpdate,
    )

    from aws_durable_execution_sdk_python_testing.checkpoint.effects import (
        CheckpointEffect,
    )
    from aws_durable_execution_sdk_python_testing.checkpoint.processors.base import (
        OperationProcessor,
    )
    from aws_durable_execution_sdk_python_testing.execution import Execution


class CheckpointRequestDispatcher:
    """Apply a batch of ``OperationUpdate``\\ s to an :class:`Execution`.

    Dispatches each update to the per-type processor (step, wait,
    callback, context, execution), upserts the resulting operation
    into ``execution.operations``, records per-op payload size in the
    ``execution.operation_size_bytes`` sidecar dict (``Operation`` is
    immutable, so size is tracked out-of-band), and calls the supplied
    ``touch`` callback once per accepted update.
    """

    _DEFAULT_PROCESSORS: ClassVar[dict[OperationType, OperationProcessor]] = {
        OperationType.STEP: StepProcessor(),
        OperationType.WAIT: WaitProcessor(),
        OperationType.CONTEXT: ContextProcessor(),
        OperationType.CALLBACK: CallbackProcessor(),
        OperationType.EXECUTION: ExecutionProcessor(),
    }

    def __init__(
        self,
        processors: MutableMapping[OperationType, OperationProcessor] | None = None,
    ):
        self.processors = processors if processors else self._DEFAULT_PROCESSORS

    def apply_updates(
        self,
        execution: Execution,
        updates: list[OperationUpdate],
        client_token: str | None,  # noqa: ARG002 — reserved for future idempotency diagnostics
        touch: Callable[[str], None],
    ) -> list[CheckpointEffect]:
        """Apply ``updates`` to ``execution`` in place.

        Callers are responsible for running
        :class:`CheckpointValidator.validate_input` before calling this
        method. The dispatcher does not re-validate — it assumes each
        update is well-formed so that per-type processors can focus on
        state transitions.

        Each accepted update:

        * is dispatched to the per-type processor via
          ``execution.operations`` upsert semantics (existing op with
          matching ``operation_id`` is replaced in place; new op is
          appended).
        * records a payload size estimate in
          ``execution.operation_size_bytes[op_id]`` for later paging.
        * triggers ``touch(op_id)`` exactly once, which (in the
          production caller) bumps :attr:`Execution.seq_counter` and
          sets ``operation_last_touched_seq[op_id]``.

        Returns the lifecycle effects raised by the updates (completion,
        failure, callback creation) for the caller to apply once the
        write is done. Response construction is the responsibility of the
        checkpoint orchestrator.
        """
        collector = ExecutionNotifier()
        op_map = {op.operation_id: op for op in execution.operations}

        for update in updates:
            processor = self.processors.get(update.operation_type)
            if processor is None:
                msg = f"Checkpoint for {update.operation_type} is not implemented yet."
                raise InvalidParameterValueException(msg)

            current_op = op_map.get(update.operation_id)
            updated_op = processor.process(
                update=update,
                current_op=current_op,
                notifier=collector,
                execution_arn=execution.durable_execution_arn,
            )
            if updated_op is None:
                continue

            if update.operation_id in op_map:
                for i, op in enumerate(execution.operations):  # pragma: no branch
                    if op.operation_id == update.operation_id:
                        execution.operations[i] = updated_op
                        break
            else:
                execution.operations.append(updated_op)

            op_map[update.operation_id] = updated_op
            execution.operation_size_bytes[update.operation_id] = (
                _estimate_payload_size(update)
            )
            touch(update.operation_id)

        execution.updates.extend(updates)
        execution.update_timestamps.extend(datetime.now(UTC) for _ in updates)

        return collector.effects


def _estimate_payload_size(update: OperationUpdate) -> int:
    """Estimate the on-wire size of an ``OperationUpdate``'s payload.

    Approximate — paging decisions need this to be consistent and
    reproducible, not exact. Sums JSON-ish lengths of ``payload`` and
    ``error`` fields when present.
    """
    size = 0
    payload = update.payload
    if payload is not None:
        size += _byte_length(payload)
    error = update.error
    if error is not None:
        try:
            size += len(json.dumps(error.to_dict()))
        except (AttributeError, TypeError):  # pragma: no cover — defensive
            size += len(str(error))
    return size


def _byte_length(payload: object) -> int:
    if isinstance(payload, bytes):
        return len(payload)
    if isinstance(payload, str):
        return len(payload.encode())
    return len(str(payload))
