"""Operation transformer for converting OperationUpdates to Operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationType,
    OperationUpdate,
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


if TYPE_CHECKING:
    from collections.abc import MutableMapping

    from aws_durable_execution_sdk_python_testing.checkpoint.processors.base import (
        OperationProcessor,
    )

from typing import ClassVar


class OperationTransformer:
    """Transforms OperationUpdates to Operations while maintaining order and triggering scheduler actions."""

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

    def process_updates(
        self,
        updates: list[OperationUpdate],
        current_operations: list[Operation],
        notifier,
        execution_arn: str,
    ) -> tuple[list[Operation], list[OperationUpdate]]:
        """Transform updates maintaining operation order and return (operations, updates)."""
        op_map = {op.operation_id: op for op in current_operations}

        # Start with copy of current operations list
        result_operations = current_operations.copy()

        for update in updates:
            processor = self.processors.get(update.operation_type)
            if processor:
                current_op = op_map.get(update.operation_id)
                updated_op = processor.process(
                    update=update,
                    current_op=current_op,
                    notifier=notifier,
                    execution_arn=execution_arn,
                )

                if updated_op is not None:
                    if update.operation_id in op_map:
                        # Update existing operation in-place
                        for i, op in enumerate(result_operations):  # pragma: no branch
                            # no branch coverage because result_operation empty not reachable here
                            if op.operation_id == update.operation_id:
                                result_operations[i] = updated_op
                                break
                    else:
                        # Append new operation to end
                        result_operations.append(updated_op)

                    # Update map for future lookups
                    op_map[update.operation_id] = updated_op
            else:
                msg: str = (
                    f"Checkpoint for {update.operation_type} is not implemented yet."
                )
                raise InvalidParameterValueException(msg)

        return result_operations, updates
