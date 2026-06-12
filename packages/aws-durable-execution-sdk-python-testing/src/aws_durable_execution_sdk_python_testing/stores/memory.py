"""In-memory execution store implementation."""

from __future__ import annotations

from threading import Lock
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python_testing.stores.base import (
    BaseExecutionStore,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.execution import Execution


class InMemoryExecutionStore(BaseExecutionStore):
    """Dict-based storage for testing."""

    def __init__(self) -> None:
        self._store: dict[str, Execution] = {}
        self._lock: Lock = Lock()

    def save(self, execution: Execution) -> None:
        with self._lock:
            self._store[execution.durable_execution_arn] = execution

    def load(self, execution_arn: str) -> Execution:
        with self._lock:
            return self._store[execution_arn]

    def update(self, execution: Execution) -> None:
        with self._lock:
            self._store[execution.durable_execution_arn] = execution

    def list_all(self) -> list[Execution]:
        with self._lock:
            return list(self._store.values())
