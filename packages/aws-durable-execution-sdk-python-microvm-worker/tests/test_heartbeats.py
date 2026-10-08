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
    Heartbeats,
    heartbeat_call_timeout,
    heartbeat_delay,
    heartbeat_interval,
    heartbeat_retry_delay,
    jitter_source,
)

# A short interval keeps each test fast. The call timeout is half of it.
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
    reporter = CallbackReporter("cb-1", "us-east-1", client=client)
    return Heartbeats(
        reporter,
        "cb-1",
        kwargs.pop("heartbeat_timeout_seconds", 60),
        kwargs.pop(
            "on_callback_gone", (gone.append if gone is not None else lambda _e: None)
        ),
        kwargs.pop("interval_override", INTERVAL),
        logger,
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
    assert heartbeat_delay(10, lambda: 0.0) == 9
    assert heartbeat_delay(10, lambda: 0.999) == pytest.approx(8.001)
    # The jitter is at most half the interval.
    assert heartbeat_delay(1, lambda: 0.5) == 0.5


def test_retry_delay_is_an_eighth_to_a_quarter_of_the_interval():
    assert heartbeat_retry_delay(8, lambda: 0.0) == 2
    assert heartbeat_retry_delay(8, lambda: 0.999) == pytest.approx(1.001)


def test_call_timeout_is_half_the_interval():
    assert heartbeat_call_timeout(10) == 5


def test_jitter_is_deterministic_per_callback_and_differs_between_callbacks():
    first = jitter_source("cb-1")
    again = jitter_source("cb-1")
    other = jitter_source("cb-2")
    values = [first() for _ in range(5)]
    assert values == [again() for _ in range(5)]
    assert values != [other() for _ in range(5)]
    assert all(0 <= value < 1 for value in values)
    assert len(set(values)) == 5


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
    [("InvalidParameterValueException", False), ("CallbackTimeoutException", True)],
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
    heartbeats.handler_settled()
    release.set()
    wait_until(lambda: len(client.calls) == 2)
    time.sleep(INTERVAL * 3)
    heartbeats.stop()
    assert gone == []
    assert len(client.calls) == 2
    assert (
        "the callback no longer accepts heartbeats" in logger.messages("info")
    ) is logged


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
    assert "lambda:SendDurableExecutionCallbackHeartbeat" in errors[0]
    assert "heartbeats are accepted again" in logger.messages("info")


def test_transient_failure_is_a_warning(fake_client, make_error, logger):
    client = fake_client(make_error("ServiceException", 500))
    heartbeats = start(client, logger)
    wait_until(lambda: len(client.calls) >= 2)
    heartbeats.stop()
    assert logger.messages("warning") == ["heartbeat failed"]
    assert logger.messages("error") == []


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
    heartbeats = start(client, logger, interval_override=20)
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
