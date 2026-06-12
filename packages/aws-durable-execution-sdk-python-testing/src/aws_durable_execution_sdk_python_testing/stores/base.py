"""Base classes and protocols for execution stores."""

from __future__ import annotations

from datetime import UTC
from enum import Enum
from typing import TYPE_CHECKING, Protocol


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.lambda_service import Operation

    from aws_durable_execution_sdk_python_testing.execution import Execution


class StoreType(Enum):
    """Supported execution store types."""

    MEMORY = "memory"
    FILESYSTEM = "filesystem"
    SQLITE = "sqlite"


class ExecutionStore(Protocol):
    """Protocol for execution storage implementations."""

    # ignore cover because coverage doesn't understand elipses
    def save(self, execution: Execution) -> None: ...  # pragma: no cover
    def load(self, execution_arn: str) -> Execution: ...  # pragma: no cover
    def update(self, execution: Execution) -> None: ...  # pragma: no cover
    def query(
        self,
        function_name: str | None = None,
        execution_name: str | None = None,
        status_filter: str | None = None,
        started_after: str | None = None,
        started_before: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        reverse_order: bool = False,  # noqa: FBT001, FBT002
    ) -> tuple[list[Execution], str | None]: ...  # pragma: no cover
    def list_all(
        self,
    ) -> list[Execution]: ...  # pragma: no cover  # Keep for backward compatibility


class BaseExecutionStore(ExecutionStore):
    """Base implementation for execution stores with shared query logic."""

    @staticmethod
    def process_query(
        executions: list[Execution],
        function_name: str | None = None,
        execution_name: str | None = None,
        status_filter: str | None = None,
        started_after: str | None = None,
        started_before: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        reverse_order: bool = False,  # noqa: FBT001, FBT002
    ) -> tuple[list[Execution], str | None]:
        """Apply filtering, sorting, and pagination to executions."""
        # Apply filters
        filtered: list[Execution] = []
        for execution in executions:
            if function_name and execution.start_input.function_name != function_name:
                continue
            if (
                execution_name
                and execution.start_input.execution_name != execution_name
            ):
                continue

            # Status filtering
            if status_filter and execution.current_status().value != status_filter:
                continue

            # Time filtering
            if started_after or started_before:
                try:
                    operation: Operation = execution.get_operation_execution_started()
                    if operation.start_timestamp:
                        timestamp: float = (
                            operation.start_timestamp.timestamp()
                            if hasattr(operation.start_timestamp, "timestamp")
                            else operation.start_timestamp.replace(
                                tzinfo=UTC
                            ).timestamp()
                        )
                        if started_after and timestamp < float(started_after):
                            continue
                        if started_before and timestamp > float(started_before):
                            continue
                except (ValueError, AttributeError):
                    continue

            filtered.append(execution)

        # Sort by start timestamp
        def get_sort_key(exe: Execution):
            try:
                op: Operation = exe.get_operation_execution_started()
                if op.start_timestamp:
                    return (
                        op.start_timestamp.timestamp()
                        if hasattr(op.start_timestamp, "timestamp")
                        else op.start_timestamp.replace(tzinfo=UTC).timestamp()
                    )
            except Exception:  # noqa: BLE001, S110
                pass
            return 0

        filtered.sort(key=get_sort_key, reverse=reverse_order)

        # Apply pagination
        if limit is not None and limit > 0:
            end_idx: int = offset + limit
            paginated: list[Execution] = filtered[offset:end_idx]
            has_more: bool = end_idx < len(filtered)
            next_marker: str | None = str(end_idx) if has_more else None
            return paginated, next_marker
        return filtered[offset:], None

    def query(
        self,
        function_name: str | None = None,
        execution_name: str | None = None,
        status_filter: str | None = None,
        started_after: str | None = None,
        started_before: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        reverse_order: bool = False,  # noqa: FBT001, FBT002
    ) -> tuple[list[Execution], str | None]:
        """Apply filtering, sorting, and pagination to executions."""
        executions: list[Execution] = self.list_all()
        return self.process_query(
            executions,
            function_name=function_name,
            execution_name=execution_name,
            status_filter=status_filter,
            started_after=started_after,
            started_before=started_before,
            limit=limit,
            offset=offset,
            reverse_order=reverse_order,
        )
