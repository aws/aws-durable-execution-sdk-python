"""Visible completed context metadata for terminal-update parent references."""

from collections.abc import Mapping

from aws_durable_execution_sdk_python.plugin import (
    InvocationStartInfo,
    OperationInfo,
    OperationStatus,
    OperationType,
)


def terminal_updates(
    operations: Mapping[str, OperationInfo],
) -> dict[str, OperationInfo]:
    """Retain visible terminal updates until their operation hook or invocation End."""
    return {
        operation_id: operation
        for operation_id, operation in operations.items()
        if operation.status
        in (
            OperationStatus.SUCCEEDED,
            OperationStatus.FAILED,
            OperationStatus.TIMED_OUT,
            OperationStatus.STOPPED,
            OperationStatus.CANCELLED,
        )
    }


def completed_input_parents(info: InvocationStartInfo) -> list[OperationInfo]:
    """Known terminal contexts that may parent input or later checkpoint updates."""
    return [
        operation
        for operation in info.operations.values()
        if operation.operation_type is OperationType.CONTEXT
        and operation.status
        in (
            OperationStatus.SUCCEEDED,
            OperationStatus.FAILED,
            OperationStatus.TIMED_OUT,
            OperationStatus.STOPPED,
            OperationStatus.CANCELLED,
        )
    ]
