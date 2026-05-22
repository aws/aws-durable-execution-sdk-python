"""Operation identifier types for durable executions."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OperationIdentifier:
    """Container for operation id, parent id, and name."""

    operation_id: str
    parent_id: str | None = None
    name: str | None = None
