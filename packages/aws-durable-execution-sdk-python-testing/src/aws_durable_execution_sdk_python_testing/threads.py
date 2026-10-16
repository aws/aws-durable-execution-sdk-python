"""A bounded pool of daemon threads for blocking calls.

The runner runs two kinds of blocking call off the scheduler loop: a
handler invocation, and the synchronous Invoke of a plain chained-invoke
target. Each blocks until the endpoint answers or the read timeout
expires. Python cannot interrupt a thread blocked in a socket read. So
the runner cannot stop such a call. It can stop the call from holding
the process.

``concurrent.futures.ThreadPoolExecutor`` does not allow that. Its
workers are not daemon threads, and the interpreter joins them at exit.
So a runner closed while one Invoke is blocked keeps the process alive
until that read timeout. This pool has the same shape (a worker cap, a
FIFO queue, lazily started workers) and differs in two ways. Its workers
are daemon threads, which the interpreter does not wait for. Its
``shutdown`` drops queued work, refuses new work, and never joins.
"""

from __future__ import annotations

import os
import queue
import threading
from concurrent.futures import Executor, Future
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


def default_max_workers() -> int:
    """The worker cap ``ThreadPoolExecutor`` uses when none is given."""
    return min(32, (os.cpu_count() or 1) + 4)


class _WorkItem:
    def __init__(
        self,
        future: Future[Any],
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        self.future = future
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def run(self) -> None:
        if not self.future.set_running_or_notify_cancel():
            return
        try:
            result = self.fn(*self.args, **self.kwargs)
        except BaseException as exc:  # noqa: BLE001 — the future carries the error
            self.future.set_exception(exc)
        else:
            self.future.set_result(result)


class _Stop:
    """Sentinel that makes an idle worker return."""


class DaemonThreadPool(Executor):
    """Run callables on at most ``max_workers`` daemon threads, in FIFO order.

    A worker is started when work is submitted and no worker is idle,
    until the cap is reached. Beyond the cap, work waits in the queue in
    submission order, as in ``ThreadPoolExecutor``.

    ``shutdown`` marks the pool closed, cancels every queued future, and
    tells idle workers to return. A worker busy in a blocking call is
    left alone: it finishes when its call returns, and its result lands
    on a future nobody reads. ``wait`` is accepted for API compatibility
    and ignored, because joining a blocked worker would wait for the read
    timeout this pool exists to avoid. Workers are daemon threads, so
    process exit does not wait for them either.
    """

    def __init__(
        self, max_workers: int | None = None, thread_name_prefix: str = ""
    ) -> None:
        if max_workers is None:
            max_workers = default_max_workers()
        if max_workers <= 0:
            msg = "max_workers must be greater than 0"
            raise ValueError(msg)
        self._max_workers = max_workers
        self._thread_name_prefix = thread_name_prefix or "daemon-pool"
        self._queue: queue.SimpleQueue[_WorkItem | _Stop] = queue.SimpleQueue()
        self._idle = threading.Semaphore(0)
        self._threads: list[threading.Thread] = []
        self._closed = False
        self._lock = threading.Lock()

    @property
    def max_workers(self) -> int:
        return self._max_workers

    def submit(
        self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any
    ) -> Future[Any]:
        with self._lock:
            if self._closed:
                msg = "cannot schedule new futures after shutdown"
                raise RuntimeError(msg)
            future: Future[Any] = Future()
            self._queue.put(_WorkItem(future, fn, args, kwargs))
            self._adjust_thread_count()
            return future

    def _adjust_thread_count(self) -> None:
        # An idle worker will take the item; do not start another.
        if self._idle.acquire(blocking=False):
            return
        if len(self._threads) >= self._max_workers:
            return
        thread = threading.Thread(
            target=self._worker,
            name=f"{self._thread_name_prefix}_{len(self._threads)}",
            daemon=True,
        )
        self._threads.append(thread)
        thread.start()

    def _worker(self) -> None:
        while True:
            item: _WorkItem | _Stop = self._queue.get()
            if isinstance(item, _Stop):
                return
            item.run()
            del item
            self._idle.release()

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:  # noqa: ARG002, FBT001, FBT002
        """Close the pool without waiting for busy workers.

        Queued work is cancelled whatever ``cancel_futures`` says: work
        that has not started must not start after shutdown. ``wait`` is
        ignored, see the class docstring.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            drained: list[_WorkItem] = []
            while True:
                try:
                    item = self._queue.get_nowait()
                except queue.Empty:
                    break
                if isinstance(item, _WorkItem):
                    drained.append(item)
            for _ in self._threads:
                self._queue.put(_Stop())
        for item in drained:
            item.future.cancel()
