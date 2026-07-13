"""A serial task lane.

One daemon consumer thread drains a FIFO queue, running submitted
callables one at a time. :meth:`SerialTaskLane.submit` returns a
:class:`concurrent.futures.Future` the caller may block on (such as a
handler or HTTP thread) or ignore (such as a timer callback). Routing
every operation for one execution through a single lane makes them run
in submit order with no overlap, which keeps that execution's state
consistent when several callers act concurrently. Independent lanes run
in parallel.
"""

from __future__ import annotations

import logging
import queue
import threading
from concurrent.futures import Future
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable

logger: logging.Logger = logging.getLogger(__name__)

T = TypeVar("T")


class _Stop:
    """Sentinel enqueued by :meth:`SerialTaskLane.stop` to end the loop."""


class SerialTaskLane:
    """A single-consumer FIFO lane that runs callables serially.

    Tasks submitted from any number of producer threads run one at a
    time on a dedicated daemon thread, in the order they were enqueued.
    An exception raised by one task is captured on its future and does
    not stop the lane.

    Build instances with :meth:`create`, which starts the consumer
    thread; the constructor only initializes fields.
    """

    def __init__(self, name: str | None = None) -> None:
        self._queue: queue.Queue[tuple[Callable[[], object], Future] | _Stop] = (
            queue.Queue()
        )
        self._stopped: bool = False
        self._stop_lock: threading.Lock = threading.Lock()
        self._thread: threading.Thread = threading.Thread(
            target=self._run,
            name=name or f"serial-task-lane-{id(self):x}",
            daemon=True,
        )

    @classmethod
    def create(cls, name: str | None = None) -> SerialTaskLane:
        """Build a lane and start its consumer thread."""
        lane: SerialTaskLane = cls(name)
        lane._thread.start()  # noqa: SLF001 — own private member
        return lane

    def submit(self, fn: Callable[[], T]) -> Future[T]:
        """Enqueue ``fn`` to run on the lane and return its future.

        Raises:
            RuntimeError: If the lane has been stopped.
        """
        future: Future[T] = Future()
        with self._stop_lock:
            if self._stopped:
                msg: str = "SerialTaskLane is stopped"
                raise RuntimeError(msg)
            self._queue.put((fn, future))
        return future

    def _run(self) -> None:
        while True:
            item: tuple[Callable[[], object], Future] | _Stop = self._queue.get()
            try:
                if isinstance(item, _Stop):
                    return
                fn, future = item
                if not future.set_running_or_notify_cancel():
                    continue
                try:
                    result: object = fn()
                except BaseException as exc:  # noqa: BLE001 — relayed to the future
                    future.set_exception(exc)
                else:
                    future.set_result(result)
            finally:
                # Drop references before blocking on the next get so a
                # task's inputs/outputs are not pinned between tasks.
                del item
                self._queue.task_done()

    def stop(self, *, wait: bool = True) -> None:
        """Stop the lane after draining queued tasks.

        Tasks already enqueued run to completion before the lane exits.
        Submitting after ``stop`` raises ``RuntimeError``.

        Args:
            wait: Join the consumer thread before returning.
        """
        with self._stop_lock:
            if self._stopped:
                return
            self._stopped = True
            self._queue.put(_Stop())
        if wait:
            self._thread.join()

    def is_running(self) -> bool:
        """Return True while the consumer thread is alive."""
        return self._thread.is_alive()
