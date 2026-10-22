"""Invocation-owned completion metadata and bounded finalization."""

from collections.abc import Callable, Mapping

from aws_durable_execution_sdk_python.plugin import (
    InvocationStartInfo,
    OperationInfo,
    OperationStatus,
    OperationType,
)


class InvocationFinalization:
    """Defer failures until invocation cleanup, with one ordinary export retry.

    Operation claims and failures use the plugin's registry lock. Finalization
    actions and error reporting run on the invocation owner. Control exceptions
    are held only to finish cleanup; they are never retried or swallowed.
    """

    def __init__(self) -> None:
        self._attempts: dict[str, int] = {}
        self._stopped_operations: set[str] = set()
        self._retryable_operations: set[str] = set()
        self._errors: list[BaseException] = []

    def claim_operation(self, operation_id: str) -> bool:
        attempts = self._attempts.get(operation_id, 0)
        if attempts >= 2 or operation_id in self._stopped_operations:
            return False
        self._attempts[operation_id] = attempts + 1
        self._retryable_operations.discard(operation_id)
        return True

    def operation_failed(self, operation_id: str, error: BaseException) -> None:
        self._errors.append(error)
        if not isinstance(error, Exception):
            self._stopped_operations.add(operation_id)
        elif self._attempts[operation_id] < 2:
            self._retryable_operations.add(operation_id)

    def can_retry(self, operation_id: str) -> bool:
        return operation_id in self._retryable_operations

    def run(self, action: Callable[[], None]) -> None:
        try:
            action()
        except BaseException as error:
            self._errors.append(error)

    def raise_errors(self) -> None:
        if len(self._errors) == 1:
            raise self._errors[0]
        if self._errors:
            raise BaseExceptionGroup(
                "OpenTelemetry invocation finalization failed", self._errors
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
