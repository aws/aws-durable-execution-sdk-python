# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the heartbeat schedule."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

import pytest

from aws_durable_execution_sdk_python_microvm_worker.callback_reporter import (
    CallbackReporter,
)
from aws_durable_execution_sdk_python_microvm_worker.heartbeats import (
    MAX_HEARTBEAT_INTERVAL_SECONDS,
    MAX_QUICK_HEARTBEAT_RETRIES,
    Heartbeats,
    heartbeat_call_timeout,
    heartbeat_delay,
    heartbeat_interval,
    heartbeat_jitter,
    heartbeat_retry_delay,
)

# A short interval keeps each test fast. The call timeout is a third of it.
INTERVAL = 0.06


def wait_until(condition: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            msg = "condition not met in time"
            raise AssertionError(msg)
        time.sleep(0.005)


def start(
    client: Any, logger: Any, gone: list[BaseException] | None = None, **kwargs: Any
) -> Heartbeats:
    reporter = CallbackReporter("cb-1", client)
    return Heartbeats.start(
        reporter,
        heartbeat_timeout_seconds=kwargs.pop("heartbeat_timeout_seconds", 60),
        on_callback_gone=kwargs.pop(
            "on_callback_gone", (gone.append if gone is not None else lambda _e: None)
        ),
        heartbeat_interval_seconds=kwargs.pop("heartbeat_interval_seconds", INTERVAL),
        logger=logger,
    )


# region schedule


def test_interval_is_a_third_of_the_timeout_and_at_most_15_minutes():
    assert heartbeat_interval(30, None) == 10
    assert heartbeat_interval(3 * 3600, None) == MAX_HEARTBEAT_INTERVAL_SECONDS
    # An explicit interval cannot exceed a third of the timeout.
    assert heartbeat_interval(30, 20) == 10
    assert heartbeat_interval(30, 2) == 2
    assert heartbeat_interval(float("inf"), None) == MAX_HEARTBEAT_INTERVAL_SECONDS


def test_delay_subtracts_one_to_two_seconds():
    assert heartbeat_delay(10, 0.0) == 9
    assert heartbeat_delay(10, 0.999) == pytest.approx(8.001)
    # The jitter is at most half the interval.
    assert heartbeat_delay(1, 0.5) == 0.5


def test_retry_delay_is_an_eighth_to_a_quarter_of_the_interval():
    assert heartbeat_retry_delay(8, 0.0) == 2
    assert heartbeat_retry_delay(8, 0.999) == pytest.approx(1.001)


def test_call_timeout_is_a_third_of_the_interval():
    assert heartbeat_call_timeout(9) == 3


@pytest.mark.parametrize(
    "interval", [0.3, 1, 2, 4, 10, 60, MAX_HEARTBEAT_INTERVAL_SECONDS]
)
def test_two_failures_in_a_row_stay_within_the_heartbeat_timeout(interval):
    """The worst gap between two heartbeats that the service receives.

    The service times the gap from when it received the last good call. That
    can be at the call's start, so the call's whole duration counts. The next
    good call can be received at its end. See heartbeat_call_timeout.
    """
    call = heartbeat_call_timeout(interval)
    smallest_jitter = min(1.0, interval / 2)
    longest_retry_wait = heartbeat_retry_delay(interval, 0.0)
    longest_wait = heartbeat_delay(interval, 0.0)
    assert longest_wait == interval - smallest_jitter
    gap = (
        call
        + longest_wait
        + MAX_QUICK_HEARTBEAT_RETRIES * (call + longest_retry_wait)
        + call
    )
    # The heartbeat timeout is at least three intervals.
    assert gap < 3 * interval


def test_jitter_is_deterministic_per_callback_and_differs_between_callbacks():
    values = [heartbeat_jitter("cb-1", n) for n in range(5)]
    assert values == [heartbeat_jitter("cb-1", n) for n in range(5)]
    assert values != [heartbeat_jitter("cb-2", n) for n in range(5)]
    assert all(0 <= value < 1 for value in values)
    assert len(set(values)) == 5


def test_jitter_matches_the_javascript_worker():
    """The JavaScript worker's jitterSource("cb-1") returns these 3 values.

    They come from running worker.ts's jitterSource. So the two workers
    schedule the same heartbeats for the same job.
    """
    assert [heartbeat_jitter("cb-1", n) for n in range(3)] == [
        0.8399852730799466,
        0.5069360784254968,
        0.24054260458797216,
    ]


# endregion schedule


# region running heartbeats


def test_no_heartbeat_timeout_sends_nothing(fake_client, logger):
    client = fake_client()
    heartbeats = start(client, logger, heartbeat_timeout_seconds=None)
    assert heartbeats.interval is None
    time.sleep(0.05)
    heartbeats.stop()
    assert client.calls == []


def test_heartbeats_start_at_once_and_repeat_until_stopped(fake_client, logger):
    client = fake_client()
    heartbeats = start(client, logger)
    wait_until(lambda: len(client.calls) >= 3)
    heartbeats.stop()
    count = len(client.calls)
    time.sleep(INTERVAL * 3)
    assert len(client.calls) == count
    assert set(client.names()) == {"heartbeat"}


def test_terminal_error_while_the_handler_runs_reports_the_callback_gone(
    fake_client, make_error, logger
):
    gone: list[BaseException] = []
    client = fake_client(make_error("CallbackTimeoutException"))
    heartbeats = start(client, logger, gone)
    wait_until(lambda: len(gone) == 1)
    time.sleep(INTERVAL * 3)
    heartbeats.stop()
    assert client.names() == ["heartbeat"]
    assert "CallbackTimeoutException" in str(gone[0])


@pytest.mark.parametrize(
    ("code", "logged"),
    [
        # A closed callback: the completion landed while the heartbeat was in
        # flight. Expected, so not logged.
        ("CallbackTimeoutException", False),
        # An invalid callback ID is unexpected after a settled handler.
        ("InvalidParameterValueException", True),
    ],
)
def test_terminal_error_after_the_handler_settled_only_stops(
    fake_client, make_error, logger, code, logged
):
    gone: list[BaseException] = []
    release = threading.Event()
    # The first heartbeat waits until the handler has settled.
    client = fake_client(lambda: release.wait(5), make_error(code))
    heartbeats = start(client, logger, gone)
    wait_until(lambda: len(client.calls) == 1)
    assert heartbeats.handler_settled() is True
    release.set()
    wait_until(lambda: len(client.calls) == 2)
    time.sleep(INTERVAL * 3)
    heartbeats.stop()
    assert gone == []
    assert len(client.calls) == 2
    assert (
        "the callback no longer accepts heartbeats" in logger.messages("info")
    ) is logged


def test_handler_settled_after_the_callback_gone_returns_false(
    fake_client, make_error, logger
):
    """The heartbeat thread decided first, so the caller must not report.

    on_callback_gone is held open here, so handler_settled() arrives after
    the decision is made. This checks that the decision is recorded before
    on_callback_gone runs. The next test checks the lock itself.
    """
    entered = threading.Event()
    release = threading.Event()
    gone: list[BaseException] = []

    def on_gone(error: BaseException) -> None:
        entered.set()
        release.wait(5)
        gone.append(error)

    client = fake_client(make_error("CallbackTimeoutException"))
    heartbeats = start(client, logger, on_callback_gone=on_gone)
    assert entered.wait(5)
    assert heartbeats.handler_settled() is False
    release.set()
    wait_until(lambda: len(gone) == 1)
    heartbeats.stop()


def test_handler_settled_waits_for_a_decision_in_progress(
    fake_client, make_error, logger
):
    """The heartbeat thread reads "not settled", then pauses before it decides.

    This is the interleaving the review found. handler_settled() must wait
    for the decision and return False. Without the lock, it returns True,
    and on_callback_gone still runs. The subclass pauses the heartbeat
    thread inside the read of the private _handler_settled attribute.
    """
    read_not_settled = threading.Event()

    class PausingHeartbeats(Heartbeats):
        @property
        def _handler_settled(self) -> bool:
            value: bool = self.__dict__["_settled"]
            if threading.current_thread() is self._thread and not value:
                read_not_settled.set()
                time.sleep(0.2)
            return value

        @_handler_settled.setter
        def _handler_settled(self, value: bool) -> None:
            self.__dict__["_settled"] = value

    gone: list[BaseException] = []
    client = fake_client(make_error("CallbackTimeoutException"))
    heartbeats = PausingHeartbeats.start(
        CallbackReporter("cb-1", client),
        heartbeat_timeout_seconds=60,
        on_callback_gone=gone.append,
        heartbeat_interval_seconds=INTERVAL,
        logger=logger,
    )
    assert read_not_settled.wait(5)
    assert heartbeats.handler_settled() is False
    wait_until(lambda: len(gone) == 1)
    heartbeats.stop()


def test_on_callback_gone_never_runs_after_handler_settled_returns_true(
    fake_client, make_error, logger
):
    """Many races between the two threads. A True return always wins.

    A failure here needs a thread switch inside the few instructions between
    the read and the decision. So this test rarely catches a missing lock.
    The test above pauses inside that window instead.
    """
    for _ in range(50):
        gone: list[BaseException] = []
        client = fake_client(make_error("CallbackTimeoutException"))
        heartbeats = start(client, logger, gone)
        settled = heartbeats.handler_settled()
        if settled:
            time.sleep(0.01)
            heartbeats.stop()
            assert gone == []
        else:
            wait_until(lambda: len(gone) == 1)
            heartbeats.stop()


def test_rejection_is_logged_once_and_heartbeats_continue(
    fake_client, make_error, logger
):
    denied = make_error("AccessDeniedException", 403)
    client = fake_client(denied, denied, denied)
    heartbeats = start(client, logger)
    wait_until(lambda: len(client.calls) >= 5)
    heartbeats.stop()
    errors = logger.messages("error")
    assert len(errors) == 1
    assert all(extra["callbackId"] == "cb-1" for _, _, extra in logger.lines)
    assert "lambda:SendDurableExecutionCallbackHeartbeat" in errors[0]
    assert "heartbeats are accepted again" in logger.messages("info")


def test_transient_failure_is_a_warning(fake_client, make_error, logger):
    client = fake_client(make_error("ServiceException", 500))
    heartbeats = start(client, logger)
    wait_until(lambda: len(client.calls) >= 2)
    heartbeats.stop()
    assert logger.messages("warning") == ["heartbeat failed"]
    assert logger.messages("error") == []
    # Each line names its job, because one thread runs per job.
    assert all(extra["callbackId"] == "cb-1" for _, _, extra in logger.lines)


def test_stalled_call_times_out_and_the_next_heartbeat_follows(fake_client, logger):
    release = threading.Event()
    client = fake_client(lambda: release.wait(5))
    heartbeats = start(client, logger)
    wait_until(lambda: len(client.calls) >= 2)
    heartbeats.stop()
    release.set()
    assert logger.messages("warning")[0] == "heartbeat failed"


def test_stop_cancels_a_call_in_flight(fake_client, logger):
    release = threading.Event()
    client = fake_client(lambda: release.wait(5))
    # A long interval gives a long call timeout. Only the cancel can end it.
    heartbeats = start(client, logger, heartbeat_interval_seconds=20)
    wait_until(lambda: len(client.calls) == 1)
    started = time.monotonic()
    heartbeats.stop()
    assert time.monotonic() - started < 1
    release.set()
    assert logger.messages("warning") == []


def test_a_raising_callback_gone_handler_is_logged(fake_client, make_error, logger):
    def raise_error(_error: BaseException) -> None:
        msg = "handler bug"
        raise RuntimeError(msg)

    client = fake_client(make_error("CallbackTimeoutException"))
    heartbeats = start(client, logger, on_callback_gone=raise_error)
    wait_until(lambda: "the callback-gone handler raised" in logger.messages("error"))
    heartbeats.stop()


def test_stop_from_the_heartbeat_thread_does_not_join_itself(
    fake_client, make_error, logger
):
    holder: list[Heartbeats] = []
    stopped = threading.Event()

    def stop_from_callback(_error: BaseException) -> None:
        holder[0].stop()
        stopped.set()

    client = fake_client(
        lambda: wait_until(lambda: bool(holder)), make_error("CallbackTimeoutException")
    )
    holder.append(start(client, logger, on_callback_gone=stop_from_callback))
    assert stopped.wait(5)


# endregion running heartbeats


# region validation


@pytest.mark.parametrize(
    "value", [0, -1, 900.5, float("nan"), float("inf"), True, "10", 10**400]
)
def test_start_rejects_an_invalid_interval(fake_client, logger, value):
    client = fake_client()
    with pytest.raises(ValueError, match="heartbeat_interval_seconds"):
        start(client, logger, heartbeat_interval_seconds=value)
    time.sleep(0.02)
    assert client.calls == []


def test_start_accepts_the_largest_interval(fake_client, logger):
    heartbeats = start(fake_client(), logger, heartbeat_interval_seconds=900)
    # The interval is cut to a third of the 60-second heartbeat timeout.
    assert heartbeats.interval == 20
    heartbeats.stop()


def test_start_without_an_interval_uses_a_third_of_the_timeout(fake_client, logger):
    heartbeats = start(fake_client(), logger, heartbeat_interval_seconds=None)
    assert heartbeats.interval == 20
    heartbeats.stop()


def test_the_constructor_starts_no_thread(fake_client, logger):
    client = fake_client()
    reporter = CallbackReporter("cb-1", client)
    Heartbeats(
        reporter=reporter,
        on_callback_gone=lambda _e: None,
        interval=INTERVAL,
        logger=logger,
    )
    time.sleep(INTERVAL * 2)
    assert client.calls == []


# endregion validation
