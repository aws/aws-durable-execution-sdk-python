"""The registry of per-execution workers."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python_testing.worker.execution_worker import (
    ExecutionWorker,
)

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python_testing.scheduler import Scheduler
    from aws_durable_execution_sdk_python_testing.stores.base import ExecutionStore


class ExecutionRegistry:
    """Owns one :class:`ExecutionWorker` per execution ARN.

    Creates a worker the first time an execution is acted on, hands the
    same worker out for every later operation on that execution, and
    drops it once the execution completes. Lookups and mutations are
    guarded so concurrent callers always resolve to the same worker for
    a given ARN.
    """

    def __init__(self, store: ExecutionStore, scheduler: Scheduler) -> None:
        self._store = store
        self._scheduler = scheduler
        self._workers: dict[str, ExecutionWorker] = {}
        self._lock = threading.Lock()

    def get_or_create(self, execution_arn: str) -> ExecutionWorker:
        """Return the worker for ``execution_arn``, creating it if absent."""
        with self._lock:
            worker: ExecutionWorker | None = self._workers.get(execution_arn)
            if worker is None:
                worker = ExecutionWorker.create(
                    execution_arn, self._store, self._scheduler, self
                )
                self._workers[execution_arn] = worker
            return worker

    def get(self, execution_arn: str) -> ExecutionWorker | None:
        """Return the worker for ``execution_arn`` if one exists."""
        with self._lock:
            return self._workers.get(execution_arn)

    def remove(self, execution_arn: str) -> None:
        """Drop the worker for ``execution_arn`` if present."""
        with self._lock:
            self._workers.pop(execution_arn, None)

    def active_count(self) -> int:
        """Return the number of live workers."""
        with self._lock:
            return len(self._workers)
