from __future__ import annotations

import asyncio
import inspect
import queue
import threading
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar, cast


T = TypeVar("T")


def resolve_awaitable(value: T | Awaitable[T]) -> T:
    if inspect.isawaitable(value):
        return run_awaitable(cast(Awaitable[T], value))
    return value


def invoke_callable(func: Callable[..., T | Awaitable[T]], *args, **kwargs) -> T:
    return resolve_awaitable(func(*args, **kwargs))


def run_awaitable(awaitable: Awaitable[T]) -> T:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(awaitable)

    return _run_awaitable_in_thread(awaitable)


def _run_awaitable_in_thread(awaitable: Awaitable[T]) -> T:
    result_queue: queue.Queue[tuple[bool, T | BaseException]] = queue.Queue(maxsize=1)

    def runner() -> None:
        try:
            result_queue.put((True, asyncio.run(awaitable)))
        except BaseException as exc:  # noqa: BLE001
            result_queue.put((False, exc))

    thread = threading.Thread(target=runner, name="dex-async-user-code", daemon=True)
    thread.start()
    success, payload = result_queue.get()
    thread.join()

    if success:
        return cast(T, payload)
    raise cast(BaseException, payload)
