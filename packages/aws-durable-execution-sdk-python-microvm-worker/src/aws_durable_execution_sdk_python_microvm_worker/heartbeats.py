# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Heartbeats for one job.

A job with a heartbeat timeout fails when no heartbeat reaches the service
within that timeout. So the worker sends heartbeats while the job runs, and
while it reports the job's outcome.
"""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Callable

from aws_durable_execution_sdk_python_microvm_worker.callback_reporter import (
    CallbackReporter,
    CancelScope,
    error_code,
    is_permanent_error,
    is_terminal_callback_error,
)
from aws_durable_execution_sdk_python_microvm_worker.logger import (
    MicrovmWorkerLogger,
    describe,
)


MAX_HEARTBEAT_INTERVAL_SECONDS = 15 * 60.0
"""The longest heartbeat interval, 15 minutes."""

MAX_QUICK_HEARTBEAT_RETRIES = 2
"""Failed heartbeats in a row that are retried after a short delay."""

_MIN_JITTER_SECONDS = 1.0
_MAX_JITTER_SECONDS = 2.0

# How long stop() waits for the heartbeat thread. A cancelled call returns at
# once, so the thread normally ends within milliseconds. The bound keeps a
# logger that blocks from holding the job forever.
_STOP_JOIN_SECONDS = 5.0


def heartbeat_interval(
    heartbeat_timeout_seconds: float, override_seconds: float | None
) -> float:
    """Return the heartbeat interval for a job, in seconds.

    The default is a third of the heartbeat timeout, at most 15 minutes. An
    explicit interval replaces the default. Either way the interval is at
    most a third of the heartbeat timeout. So a large explicit interval
    cannot leave the job without heartbeats.
    """
    third = max(0.001, heartbeat_timeout_seconds / 3)
    return min(
        MAX_HEARTBEAT_INTERVAL_SECONDS
        if override_seconds is None
        else override_seconds,
        third,
    )


def jitter_source(callback_id: str) -> Callable[[], float]:
    """Return a source of numbers in [0, 1) that differs per job.

    The n-th call returns the first 32 bits of SHA-256 of
    ``<callback_id>:<n>``, scaled to [0, 1).

    Why not the ``random`` module:

    1. Lambda snapshots the running worker process when it builds the image.
    2. ``random`` keeps its generator state in that process's memory.
    3. So every MicroVM restored from the snapshot starts with the same
       generator state, and the same calls return the same values.
    4. So MicroVMs that start together, for example from a ``map``, would
       get the same heartbeat delays, and would retry together.
    5. Each job has its own callback ID. So a hash of the ID differs per job,
       whatever the generator state is after a restore.
    """
    counter = 0
    lock = threading.Lock()

    def next_value() -> float:
        nonlocal counter
        with lock:
            n = counter
            counter += 1
        digest = hashlib.sha256(f"{callback_id}:{n}".encode()).digest()
        return int.from_bytes(digest[:4], "big") / 2**32

    return next_value


def heartbeat_delay(interval: float, random: Callable[[], float]) -> float:
    """Return the delay before the next heartbeat, in seconds.

    Many MicroVMs can start at the same moment, for example from a ``map``.
    With a fixed interval, their heartbeats would reach the service at the
    same moments. So each delay is the interval minus 1 to 2 seconds.

    The jitter is subtracted, not added. So a heartbeat never arrives later
    than the interval. The jitter is capped at half the interval. So a short
    interval stays positive.
    """
    jitter = min(
        _MIN_JITTER_SECONDS + random() * (_MAX_JITTER_SECONDS - _MIN_JITTER_SECONDS),
        interval / 2,
    )
    return interval - jitter


def heartbeat_retry_delay(interval: float, random: Callable[[], float]) -> float:
    """Return the delay after a failed heartbeat: an eighth to a quarter of
    the interval. The jitter keeps MicroVMs that failed together from
    retrying together."""
    return max(0.001, interval / 4 - random() * interval / 8)


def heartbeat_call_timeout(interval: float) -> float:
    """Return how long one heartbeat call may take: half the interval.

    With interval I, the heartbeat timeout is at least 3I. The wait after a
    success is at most I minus a jitter j. The wait after one of the first
    two failures in a row is at most I/4. Each call, including the one that
    ends the gap, takes at most I/2. So the gap between two successful
    heartbeats is at most:

    - no failure: I - j + I/2, about 1.5I;
    - one failed or stalled call: I - j + I/2 + I/4 + I/2, about 2.25I;
    - two in a row: I - j + 2 (I/2 + I/4) + I/2 = 3I - j, under the
      heartbeat timeout by the jitter.

    A third failure in a row waits a full interval, and the job can then
    reach its heartbeat timeout.
    """
    return max(0.001, interval / 2)


class Heartbeats:
    """Sends a heartbeat now, and then about every interval, until stopped.

    A terminal error means the durable function no longer waits for this
    job. While the handler runs, the heartbeats call ``on_callback_gone``
    with the error, and stop. After the handler has settled, they only stop.

    Another rejection, such as ``AccessDeniedException`` for a missing
    ``lambda:SendDurableExecutionCallbackHeartbeat`` permission, is unlikely
    to clear soon, but it can. It is logged once as an error, and heartbeats
    continue. The same rejection is not logged again until a heartbeat
    succeeds. A transient error is logged as a warning.

    After any failure, the next heartbeat comes after a short delay, for at
    most two failures in a row, and on the normal schedule after that.

    A job without a heartbeat timeout gets no heartbeats. Then this object
    does nothing.

    Args:
        reporter: The job's reporter.
        callback_id: The job's callback ID. It seeds the jitter.
        heartbeat_timeout_seconds: The job's heartbeat timeout, or ``None``.
        on_callback_gone: Called once, from the heartbeat thread, when a
            heartbeat finds the callback gone while the handler runs.
        interval_override: An explicit interval in seconds.
        logger: The worker's logger.
    """

    def __init__(
        self,
        reporter: CallbackReporter,
        callback_id: str,
        heartbeat_timeout_seconds: float | None,
        on_callback_gone: Callable[[BaseException], None],
        interval_override: float | None,
        logger: MicrovmWorkerLogger,
    ) -> None:
        self._reporter = reporter
        self._on_callback_gone = on_callback_gone
        self._logger = logger
        self._stopped = threading.Event()
        # Set when the handler has settled. A terminal heartbeat error then
        # only stops the heartbeats. The durable function has probably
        # received the outcome already, so the job must not be cancelled.
        self._handler_settled = False
        # Ends a heartbeat in flight when the job ends. The job then does not
        # wait for a stalled call's timeout, which can be minutes.
        self._cancel = CancelScope()
        self._thread: threading.Thread | None = None
        if heartbeat_timeout_seconds is None:
            return
        self._interval = heartbeat_interval(
            heartbeat_timeout_seconds, interval_override
        )
        self._call_timeout = heartbeat_call_timeout(self._interval)
        self._random = jitter_source(callback_id)
        self._thread = threading.Thread(
            target=self._run, name=f"heartbeats-{callback_id[:16]}", daemon=True
        )
        self._thread.start()

    @property
    def interval(self) -> float | None:
        """The interval in seconds, or ``None`` when the job has no heartbeats."""
        return None if self._thread is None else self._interval

    def handler_settled(self) -> None:
        """Mark the handler as settled. ``on_callback_gone`` is not called after it."""
        self._handler_settled = True

    def stop(self) -> None:
        """Stop the heartbeats, cancel one in flight, and wait for the thread."""
        self._stopped.set()
        self._cancel.cancel()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(_STOP_JOIN_SECONDS)

    def _run(self) -> None:
        # Failed heartbeats since the last success, rejections included.
        failures = 0
        # The rejection codes logged as errors since the last success.
        logged_rejections: set[str] = set()
        while not self._stopped.is_set():
            failed = False
            try:
                self._reporter.heartbeat(self._call_timeout, self._cancel)
            except Exception as error:
                if self._stopped.is_set():
                    # The job has ended. A late failure, for example from a
                    # cancelled call, says nothing about the job.
                    return
                if is_terminal_callback_error(error):
                    self._stopped.set()
                    if not self._handler_settled:
                        try:
                            self._on_callback_gone(error)
                        except Exception as callback_error:  # noqa: BLE001
                            self._logger.error(
                                "the callback-gone handler raised",
                                {"error": describe(callback_error)},
                            )
                    elif error_code(error) != "InvalidParameterValueException":
                        # "Already complete" usually means that the completion
                        # landed while this heartbeat was in flight. Any other
                        # terminal answer is logged. The completion call meets
                        # it too, and reports it.
                        self._logger.info(
                            "the callback no longer accepts heartbeats",
                            {"error": describe(error)},
                        )
                    return
                failed = True
                if is_permanent_error(error):
                    name = error_code(error) or type(error).__name__
                    if name not in logged_rejections:
                        logged_rejections.add(name)
                        self._logger.error(
                            "the service rejected a heartbeat. Heartbeats "
                            "continue, and the same rejection is not logged "
                            "again until a heartbeat succeeds. A missing "
                            "permission, for example, needs "
                            "lambda:SendDurableExecutionCallbackHeartbeat in "
                            "the MicroVM's execution role.",
                            {
                                "error": describe(error),
                                "handlerSettled": self._handler_settled,
                            },
                        )
                else:
                    self._logger.warning("heartbeat failed", {"error": describe(error)})
            if not failed and logged_rejections:
                logged_rejections.clear()
                self._logger.info("heartbeats are accepted again", {})
            failures = failures + 1 if failed else 0
            delay = (
                heartbeat_retry_delay(self._interval, self._random)
                if 0 < failures <= MAX_QUICK_HEARTBEAT_RETRIES
                else heartbeat_delay(self._interval, self._random)
            )
            if self._stopped.wait(delay):
                return
