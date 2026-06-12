"""A Scheduler that can run awaitables or standard sync callables on a schedule once or repeatedly."""

from __future__ import annotations

import asyncio
import itertools
import logging
import threading
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Callable
    from concurrent.futures import Future

logger = logging.getLogger(__name__)


class Event:
    """An event created by Scheduler that will block on wait until it's set."""

    def __init__(self, scheduler: Scheduler, asyncio_event: asyncio.Event) -> None:
        self._scheduler: Scheduler = scheduler
        self._asyncio_event: asyncio.Event = asyncio_event
        self._exception: Exception | None = None

    def set(self):
        """Set the event with this to unblock wait."""
        self._scheduler.set_event(self._asyncio_event)

    def set_exception(self, exception: Exception):
        """Set exception and unblock waiters."""
        self._exception = exception
        self._scheduler.set_event(self._asyncio_event)

    def wait(self, timeout: float | None = None, *, clear_on_set: bool = True) -> bool:
        """Wait until the event is set.

        Args:
            timeout (int | float | None): Wait for event to set until this timeout.
            clear_on_set (bool): Remove the event from the Scheduler on completion.
                                 Use this if you won't re-use the event.

        Returns:
            True when set. False if the event timed out without being set.

        Raises:
            Exception: If an exception was stored via set_exception().
        """
        result = self._scheduler.wait_for_event(self._asyncio_event, timeout)
        if clear_on_set:
            self._scheduler.remove_event(self._asyncio_event)
        if result and self._exception:
            raise self._exception
        return result

    def remove(self):
        """Remove the event from the Scheduler. Do this to avoid build-up of many events in the scheduler."""
        self._scheduler.remove_event(self._asyncio_event)


class Scheduler:
    """A Scheduler to run callables later, repeatedly or raise events."""

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        self._ready_event: threading.Event = threading.Event()
        self._thread: threading.Thread = threading.Thread(
            target=self._start_loop, daemon=True
        )
        self._running: bool = False
        self._events: set[asyncio.Event] = set()

    # region context manager
    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    # endregion context manager

    # region event loop
    def start(self):
        """Start the scheduler. Not thread-safe."""
        if self._running:
            return

        self._running = True

        self._thread.start()
        # Wait for inside of loop to notify it's ready (meaning _start_loop has completed)
        self._ready_event.wait()

    def stop(self):
        """Stop the scheduler, releasing resources. Not thread-safe."""
        if not self._running:
            return

        self._running = False
        self._loop.call_soon_threadsafe(self._cleanup_and_stop)
        self._thread.join()

    def is_started(self) -> bool:
        """Return True if the scheduler is started."""
        return self._running

    def event_count(self) -> int:
        """Return the number of events in the scheduler."""
        return len(self._events)

    def task_count(self) -> int:
        """Return the number of tasks in the scheduler."""
        if not self._running:
            return 0
        return len(asyncio.all_tasks(self._loop))

    def _cleanup_and_stop(self):
        """Cancel all tasks and clear all events. Stop the event-loop."""
        # Cancel all tasks
        for task in asyncio.all_tasks(self._loop):
            task.cancel()

        # Clear events (don't set them)
        self._events.clear()

        self._loop.stop()

    def _start_loop(self):
        """Initialize the event-loop. The ready event notifies that the loop is started."""
        asyncio.set_event_loop(self._loop)
        # signal that loop is ready from within the loop
        self._loop.call_soon(self._ready_event.set)
        # block indefinitely - call_soon with the read_event will run soon as the loop starts
        self._loop.run_forever()

    # endregion event loop
    # region Tasks
    def call_later(
        self,
        func: Callable[[], Any],
        delay: float = 0,
        count: int | None = 1,
        completion_event: Event | None = None,
    ) -> Future[Any]:
        """Call func after the delay.

        If func is async it runs inside a thread-safe coroutine. If func is sync it runs in its own
        threadpool, so it won't block the event loop.

        Args:
            func (Callable[[], Any]): The function to call later. This can be an async or a standard
                                      sync function.
            delay (float | int): Delay in seconds before calling func.
            count (int | None): Number of times to call func. Default is 1 (call once).
                               Use None for infinite repeats.
            completion_event (Event | None): Event to notify on exception.

        Returns: Future that completes when the scheduled work is done.
        """
        # infinite counter if count = None, else it maxes out at count
        loop_iter: itertools.count[int] | range = (
            itertools.count() if count is None else range(count)
        )

        async def delayed_func() -> Any:
            try:
                for _ in loop_iter:
                    await asyncio.sleep(delay)

                    try:
                        if asyncio.iscoroutinefunction(func):
                            result = await func()
                        else:
                            result = await asyncio.to_thread(func)
                        return result  # noqa: TRY300
                    except Exception as err:
                        if completion_event:
                            completion_event.set_exception(err)
                        else:
                            msg: str = "error in scheduled task"
                            logger.exception(msg)
                        raise
            except asyncio.CancelledError:  # noqa: TRY302
                # might want to handle more things here
                raise

        future: Future[Any] = asyncio.run_coroutine_threadsafe(
            delayed_func(), self._loop
        )
        return future

    # endregion Tasks

    # region Events

    def create_event(self) -> Event:
        """Create an event controlled by the Scheduler to signal between threads and coroutines."""
        # create event inside the Scheduler event-loop
        future: Future[asyncio.Event] = asyncio.run_coroutine_threadsafe(
            self._create_event(), self._loop
        )

        # Add timeout to prevent surprising "hangs" if for whatever reason event fails to create.
        # result with block. Do NOT call anything in _create_event that calls back into scheduler
        # methods because it could create a circular depdendency which will deadlock.
        event = future.result(timeout=5.0)
        return Event(self, event)

    def wait_for_event(
        self, event: asyncio.Event, timeout: float | None = None
    ) -> bool:
        """Run event's wait inside the Scheduler event-loop."""
        if event not in self._events:
            return False

        future: Future[bool] = asyncio.run_coroutine_threadsafe(
            asyncio.wait_for(event.wait(), timeout), self._loop
        )

        try:
            return future.result()
        except TimeoutError:
            return False

    def set_event(self, event: asyncio.Event):
        """Set event inside the Scheduler event-loop."""
        if event in self._events:
            self._loop.call_soon_threadsafe(event.set)

    def remove_event(self, event: asyncio.Event):
        """Remove event from Scheduler in the Scheduler event-loop."""

        def _remove():
            self._events.discard(event)

        self._loop.call_soon_threadsafe(_remove)

    async def _create_event(self) -> asyncio.Event:
        """Create event and add it to the scheduler events list."""
        event = asyncio.Event()
        self._events.add(event)
        return event

    # endregion Events
