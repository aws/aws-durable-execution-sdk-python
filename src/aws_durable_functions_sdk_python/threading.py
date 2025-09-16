"""Concurrency and locking."""

from __future__ import annotations

from collections import deque
from threading import Event, Lock
from typing import TYPE_CHECKING

from aws_durable_functions_sdk_python.exceptions import OrderedLockError

if TYPE_CHECKING:
    from typing import Self


class OrderedLock:
    """Lock that guarantees callers acquire in the invocation order.

    Locks acquire in first-in,first-out (FIFO) order.

    This class is necessary because in a standard Lock the order of pending calls
    acquiring the lock is not necessarily guaranteed by the thread scheduler.

    For example, assume calls to acquire the lock in order A -> B -> C.
    A blocks with B and C pending. When A releases, the thread scheduler could favour
    C rather than B next, which is out of order.

    This OrderedLock instead will guarantee that the order in which callers will
    acquire the lock is the order of invocation. In the case of example, this means
    that the order of lock acquire would always be A -> B -> C.

    Once an error occurs in a lock, this instance of the lock is broken and no subsequent lock attempts
    can succeed, because if any subsequent locks acquire it would violate the order guarantee.

    If a lock fails to acquire, OrderedLock will raise the causing exception to the caller.
    If there are any other blocked callers waiting in queue, those callers will receive a
    OrderedLockError, which contains the original causing exception too.

    You can use OrderedLock as a context manager.
    """

    def __init__(self) -> None:
        """Initialize ordered lock."""
        self._lock: Lock = Lock()
        self._waiters: deque[Event] = deque()
        self._is_broken: bool = False
        self._exception: Exception | None = None

    def acquire(self) -> bool:
        """Acquire lock.

        Returns: True if acquired successfully

        Raises:
            OrderedLockError: When a preceding caller could not release its lock because it errored.
        """
        with self._lock:
            if self._is_broken:
                # don't grow queue if already broken
                msg = "Cannot acquire lock in guaranteed order because a previous lock exited with an exception."
                raise OrderedLockError(msg, self._exception)

            event = Event()
            self._waiters.append(event)

            if len(self._waiters) == 1:
                # first waiter, nothing else in queue so no need to wait
                event.set()

        # block until it's our turn to proceed
        event.wait()

        # this is the only thread progressing and holding the lock, so doesn't need to be under lock
        if self._is_broken:
            msg = "Cannot acquire lock in guaranteed order because a previous lock exited with an exception."
            raise OrderedLockError(msg, self._exception)

        return True

    def release(self) -> None:
        """Release lock. This makes the lock available for the next queued up waiter."""
        with self._lock:
            if not self._waiters:
                msg = "You have to acquire a lock before you can release it."
                raise OrderedLockError(msg)
            # remove the current lock from the queue, since it's done
            self._waiters.popleft()
            if self._waiters and not self._is_broken:
                # let the next-in-line waiter proceed
                self._waiters[0].set()

    def reset(self) -> None:
        """Reset the lock.

        This assumes all waiters have cleared.

        Raises: OrderedLockError when there still are pending waiters.
        """
        with self._lock:
            if self._waiters:
                msg = (
                    "Cannot reset lock because there are callers waiting for the lock."
                )
                raise OrderedLockError(msg)
            self._is_broken = False
            self._exception = None

    def is_broken(self) -> bool:
        """Return True if the lock is broken."""
        with self._lock:
            return self._is_broken

    # region Context Manager
    def __enter__(self) -> Self:
        """Acquire lock."""
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the context manager by releasing the current lock."""
        if exc_type is not None:
            # can't allow any subsequent locks to succeed, because that would break order guarantee
            with self._lock:
                self._is_broken = True
                self._exception = exc_val
                # break the queue and let all waiters know
                for waiter in self._waiters:
                    waiter.set()

        self.release()

    # endregion Context Manager


class OrderedCounter:
    """Thread-safe counter that guarantees callers get the next increment in the invocation order.

    The counter starts at 0.
    """

    def __init__(self) -> None:
        self._lock: OrderedLock = OrderedLock()
        self._counter: int = 0

    def increment(self) -> int:
        """Increment the counter by 1."""
        with self._lock:
            self._counter += 1
            return self._counter

    def decrement(self) -> int:
        """Decrement the counter by 1."""
        with self._lock:
            self._counter -= 1
            return self._counter

    def get_current(self) -> int:
        """Return the current value of the counter."""
        with self._lock:
            return self._counter
