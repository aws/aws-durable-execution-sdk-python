"""Operation identifier types for durable executions."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from aws_durable_execution_sdk_python.lambda_service import (
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
