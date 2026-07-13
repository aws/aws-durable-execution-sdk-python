"""The registry of per-execution workers."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, TypeVar

from aws_durable_execution_sdk_python_testing.worker.execution_worker import (
    ExecutionWorker,
)

if TYPE_CHECKING:
    from concurrent.futures import Future

    from aws_durable_execution_sdk_python_testing.scheduler import Scheduler
    from aws_durable_execution_sdk_python_testing.stores.base import ExecutionStore
    from aws_durable_execution_sdk_python_testing.worker.task import ExecutionTask

T = TypeVar("T")


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

    # Submitting can lose a race with a worker tearing its own lane
    # down (it stops the lane, then leaves the registry), so a
    # handed-out worker may reject the submit. Evicting the dead worker
    # and retrying resolves to a fresh lane, which is never stopped, so
    # a couple of passes always suffice; the bound only guards against a
    # pathological storm of completions.
    _MAX_SUBMIT_ATTEMPTS: int = 5

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

    def submit(self, execution_arn: str, task: ExecutionTask[T]) -> Future[T]:
        """Submit ``task`` to ``execution_arn``'s worker and return its
        future.

        Resolves the race where the worker tears its own lane down
        between hand-out and submit: if the handed-out worker rejects
        the submit, evict it and retry on a fresh worker. The task is
        enqueued at most once, so a retry never double-runs it.

        Raises:
            RuntimeError: If a live lane could not be obtained within
                ``_MAX_SUBMIT_ATTEMPTS`` (not expected in practice).
        """
        last_error: RuntimeError | None = None
        for _ in range(self._MAX_SUBMIT_ATTEMPTS):
            worker: ExecutionWorker = self.get_or_create(execution_arn)
            try:
                return worker.submit(task)
            except RuntimeError as err:
                last_error = err
                self._evict_if_current(execution_arn, worker)
        msg: str = "could not obtain a live worker lane"
        raise RuntimeError(msg) from last_error

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

    def shutdown(self) -> None:
        """Stop every worker's lane and drop all workers.

        Called when the owning runner shuts down so per-execution lane
        threads do not outlive it: an execution that never reached a
        terminal status still has a live lane, which this stops.
        """
        with self._lock:
            workers: list[ExecutionWorker] = list(self._workers.values())
            self._workers.clear()
        for worker in workers:
            worker.stop()

    def _evict_if_current(self, execution_arn: str, worker: ExecutionWorker) -> None:
        """Drop ``worker`` only if it is still the mapped worker, so a
        replacement created by another thread is not clobbered."""
        with self._lock:
            if self._workers.get(execution_arn) is worker:
                del self._workers[execution_arn]
