"""Operation identifier types for durable executions."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from aws_durable_execution_sdk_python.exceptions import (
    NonDeterministicExecutionError,
)
from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationType,
    OperationSubType,
)


@dataclass(frozen=True)
class OperationIdNamespace:
    """The operation-id namespace of one context.

    Maps a logical step position to its deterministic operation id.
    Pure: the same position always yields the same id, so ids can be
    derived concurrently and ahead of execution without mutating
    context state.
    """

    prefix: str | None = None

    def create_id_for_step(self, step: int) -> str:
        step_id: str = f"{self.prefix}-{step}" if self.prefix else str(step)
        return hashlib.blake2b(step_id.encode()).hexdigest()[:64]


@dataclass(frozen=True)
class OperationIdentifier:
    """Container for operation id, parent id, and name."""

    operation_id: str
    sub_type: OperationSubType
    parent_id: str | None = None
    name: str | None = None

    @property
    def type(self) -> OperationType:
        return OperationType.from_sub_type(self.sub_type)

    def validate_checkpoint(self, checkpoint: Operation | None) -> None:
        """Ensure replay history belongs to this operation before it is consumed."""
        if not isinstance(checkpoint, Operation):
            return

        expected_name = self.name or None
        checkpoint_name = checkpoint.name or None
        expected_parent_id = self.parent_id or None
        checkpoint_parent_id = checkpoint.parent_id or None
        mismatches: list[str] = []

        if checkpoint.operation_type is not self.type:
            mismatches.append(
                f"type checkpoint={checkpoint.operation_type.value!r} current={self.type.value!r}"
            )
        if checkpoint.sub_type is not self.sub_type:
            checkpoint_sub_type = (
                checkpoint.sub_type.value if checkpoint.sub_type is not None else None
            )
            mismatches.append(
                f"subtype checkpoint={checkpoint_sub_type!r} current={self.sub_type.value!r}"
            )
        if checkpoint_name != expected_name:
            mismatches.append(
                f"name checkpoint={checkpoint_name!r} current={expected_name!r}"
            )
        if checkpoint_parent_id != expected_parent_id:
            mismatches.append(
                f"parent_id checkpoint={checkpoint_parent_id!r} current={expected_parent_id!r}"
            )

        if mismatches:
            mismatch_details = ", ".join(mismatches)
            msg = (
                "Non-deterministic operation identity at "
                f"id={self.operation_id!r}: {mismatch_details}"
            )
            raise NonDeterministicExecutionError(msg, step_id=self.operation_id)
