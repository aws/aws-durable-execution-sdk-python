"""Operation identifier types for durable executions."""

from __future__ import annotations

from dataclasses import dataclass

from aws_durable_execution_sdk_python.lambda_service import (
    OperationType,
    OperationSubType,
)


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
