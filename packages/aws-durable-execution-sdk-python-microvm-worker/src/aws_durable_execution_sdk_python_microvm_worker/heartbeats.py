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
import math
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
    safe_logger,
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


def validate_heartbeat_interval(heartbeat_interval_seconds: float | None) -> None:
    """Check an explicit heartbeat interval.

    An interval of 0 would send heartbeats with no wait between them. An
    interval over 15 minutes is longer than the default ever is.

    Raises:
        ValueError: When the value is not a number above 0 and at most 900.
    """
    if heartbeat_interval_seconds is None:
        return
    value = heartbeat_interval_seconds
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not math.isfinite(value)
        or not 0 < value <= MAX_HEARTBEAT_INTERVAL_SECONDS
    ):
        msg = (
            "heartbeat_interval_seconds must be a number above 0 and at most "
            f"{MAX_HEARTBEAT_INTERVAL_SECONDS:g}. Got {value!r}."
        )
        raise ValueError(msg)


def heartbeat_interval(
    heartbeat_timeout_seconds: float, heartbeat_interval_seconds: float | None
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
        if heartbeat_interval_seconds is None
        else heartbeat_interval_seconds,
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


def heartbeat_delay(interval: float, jitter: Callable[[], float]) -> float:
    """Return the delay before the next heartbeat, in seconds.

    Many MicroVMs can start at the same moment, for example from a ``map``.
    With a fixed interval, their heartbeats would reach the service at the
    same moments. So each delay is the interval minus 1 to 2 seconds. The
    amount comes from ``jitter``, a source of numbers in [0, 1).

    The jitter is subtracted, not added. So a heartbeat never arrives later
    than the interval. The jitter is capped at half the interval. So a short
    interval stays positive.
    """
    amount = min(
        _MIN_JITTER_SECONDS + jitter() * (_MAX_JITTER_SECONDS - _MIN_JITTER_SECONDS),
        interval / 2,
    )
    return interval - amount


def heartbeat_retry_delay(interval: float, jitter: Callable[[], float]) -> float:
    """Return the delay after a failed heartbeat: an eighth to a quarter of
    the interval. The jitter keeps MicroVMs that failed together from
    retrying together."""
    return max(0.001, interval / 4 - jitter() * interval / 8)


def heartbeat_call_timeout(interval: float) -> float:
    """Return how long one heartbeat call may take: a third of the interval.

    The service starts the heartbeat timeout again when it receives a
    heartbeat. It can receive a call at any moment during the call. So the
    worst gap runs from a call that the service received at its start to a
    call that the service received at its end.

    Let I be the interval, c the call timeout, and j the jitter. The
    heartbeat timeout is at least 3I, because the interval is at most a
    third of it. The worst gap between two heartbeats that the service
    receives is:

    1. The last successful call: received at its start, so it adds up to c.
    2. The wait after it: I - j.
    3. Each failed or stalled call: up to c, then a wait of up to I/4.
    4. The next successful call: received at its end, so it adds up to c.

    With c = I/3, the gap is at most:

    - no failure: 2c + I - j, about 1.67I;
    - one failure: 3c + 1.25I - j, about 2.25I;
    - two failures in a row: 4c + 1.5I - j, about 2.83I. This stays under
      the 3I timeout by at least I/6 plus the jitter.

    A call timeout of I/2 would give 3.5I - j for two failures, which can
    exceed the timeout. A third failure in a row waits a full interval, and
    the job can then reach its heartbeat timeout.

    The bounds assume that the thread timers fire on time.
    """
    return max(0.001, interval / 3)


class Heartbeats:
    """Sends a heartbeat now, and then about every interval, until stopped.

    Create it with :meth:`start`.

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
    """

    def __init__(
        self,
        *,
        reporter: CallbackReporter,
        on_callback_gone: Callable[[BaseException], None],
        interval: float | None,
        logger: MicrovmWorkerLogger,
    ) -> None:
        self._reporter = reporter
        self._on_callback_gone = on_callback_gone
        self._interval = interval
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

    @classmethod
    def start(
        cls,
        reporter: CallbackReporter,
        *,
        heartbeat_timeout_seconds: float | None,
        on_callback_gone: Callable[[BaseException], None],
        heartbeat_interval_seconds: float | None = None,
        logger: MicrovmWorkerLogger | None = None,
    ) -> Heartbeats:
        """Start the heartbeats of one job.

        A job without a heartbeat timeout gets no heartbeats. The returned
        object then does nothing.

        Args:
            reporter: The job's reporter. Its callback ID seeds the jitter.
            heartbeat_timeout_seconds: The job's heartbeat timeout, or ``None``.
            on_callback_gone: Called once, from the heartbeat thread, when a
                heartbeat finds the callback gone while the handler runs.
            heartbeat_interval_seconds: An explicit interval. It is cut to a
                third of the heartbeat timeout.
            logger: The worker's logger. Defaults to the package's standard
                library logger.

        Raises:
            ValueError: When ``heartbeat_interval_seconds`` is not above 0 and
                at most 900.
        """
        validate_heartbeat_interval(heartbeat_interval_seconds)
        heartbeats = cls(
            reporter=reporter,
            on_callback_gone=on_callback_gone,
            interval=(
                None
                if heartbeat_timeout_seconds is None
                else heartbeat_interval(
                    heartbeat_timeout_seconds, heartbeat_interval_seconds
                )
            ),
            logger=safe_logger(logger),
        )
        heartbeats._start_thread()  # noqa: SLF001
        return heartbeats

    @property
    def interval(self) -> float | None:
        """The interval in seconds, or ``None`` when the job has no heartbeats."""
        return self._interval

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

    def _start_thread(self) -> None:
        if self._interval is None:
            return
        callback_id = self._reporter.callback_id
        self._thread = threading.Thread(
            target=self._run,
            args=(self._interval, jitter_source(callback_id)),
            name=f"heartbeats-{callback_id[:16]}",
            daemon=True,
        )
        self._thread.start()

    def _run(self, interval: float, jitter: Callable[[], float]) -> None:
        call_timeout = heartbeat_call_timeout(interval)
        # Failed heartbeats since the last success, rejections included.
        failures = 0
        # The rejection codes logged as errors since the last success.
        logged_rejections: set[str] = set()
        while not self._stopped.is_set():
            failed = False
            try:
                self._reporter.heartbeat(call_timeout, self._cancel)
            except Exception as error:
                if self._stopped.is_set():
                    # The job has ended. A late failure, for example from a
                    # cancelled call, says nothing about the job.
                    return
                if is_terminal_callback_error(error):
                    self._callback_gone(error)
                    return
                failed = True
                self._log_failure(error, logged_rejections)
            if not failed and logged_rejections:
                logged_rejections.clear()
                self._logger.info("heartbeats are accepted again")
            failures = failures + 1 if failed else 0
            delay = (
                heartbeat_retry_delay(interval, jitter)
                if 0 < failures <= MAX_QUICK_HEARTBEAT_RETRIES
                else heartbeat_delay(interval, jitter)
            )
            if self._stopped.wait(delay):
                return

    def _callback_gone(self, error: BaseException) -> None:
        self._stopped.set()
        if not self._handler_settled:
            try:
                self._on_callback_gone(error)
            except Exception as callback_error:  # noqa: BLE001
                self._logger.error(
                    "the callback-gone handler raised",
                    extra={"error": describe(callback_error)},
                )
        elif error_code(error) != "InvalidParameterValueException":
            # "Already complete" usually means that the completion landed
            # while this heartbeat was in flight. Any other terminal answer is
            # logged. The completion call meets it too, and reports it.
            self._logger.info(
                "the callback no longer accepts heartbeats",
                extra={"error": describe(error)},
            )

    def _log_failure(self, error: BaseException, logged_rejections: set[str]) -> None:
        if not is_permanent_error(error):
            self._logger.warning("heartbeat failed", extra={"error": describe(error)})
            return
        name = error_code(error) or type(error).__name__
        if name in logged_rejections:
            return
        logged_rejections.add(name)
        self._logger.error(
            "the service rejected a heartbeat. Heartbeats continue, and the "
            "same rejection is not logged again until a heartbeat succeeds. A "
            "missing permission, for example, needs "
            "lambda:SendDurableExecutionCallbackHeartbeat in the MicroVM's "
            "execution role.",
            extra={"error": describe(error), "handlerSettled": self._handler_settled},
        )
