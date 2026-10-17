"""Service views of retained operation state; raw execution storage stays complete."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationStatus,
    OperationType,
)


_CLOSED_CONTEXT = (OperationStatus.SUCCEEDED, OperationStatus.FAILED)


def prune_replay_children(operations: Sequence[Operation]) -> list[Operation]:
    """Omit a descendant when any known completed context prunes its children."""
    by_id = {operation.operation_id: operation for operation in operations}
    visible = []
    for operation in operations:
        parent_id = operation.parent_id
        visited = {operation.operation_id}
        hidden = False
        while parent_id is not None and parent_id not in visited:
            parent = by_id.get(parent_id)
            if parent is None:
                break
            if (
                parent.operation_type is OperationType.CONTEXT
                and parent.status in _CLOSED_CONTEXT
                and not (
                    parent.context_details
                    and parent.context_details.replay_children is True
                )
            ):
                hidden = True
                break
            visited.add(parent_id)
            parent_id = parent.parent_id
        if not hidden:
            visible.append(operation)
    return visible


def operation_state_to_dict(
    operation: Operation, *, json_timestamps: bool = False
) -> dict[str, Any]:
    """Project service ReplayChildren presence without changing Core's model API."""
    data = dict(operation.to_json_dict() if json_timestamps else operation.to_dict())
    if (
        operation.operation_type is OperationType.CONTEXT
        and operation.context_details is not None
    ):
        details = dict(data.get("ContextDetails", {}))
        if operation.status is OperationStatus.STARTED:
            details.pop("ReplayChildren", None)
        elif operation.status in _CLOSED_CONTEXT:
            details["ReplayChildren"] = (
                operation.context_details.replay_children is True
            )
        data["ContextDetails"] = details
    return data
