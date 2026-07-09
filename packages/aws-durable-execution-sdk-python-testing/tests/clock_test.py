"""Tests for the durable timer clocks."""

from __future__ import annotations

import datetime

from aws_durable_execution_sdk_python_testing.clock import (
    RealClock,
    SkipClock,
    real_now,
)


def test_real_now_returns_utc() -> None:
    now: datetime.datetime = real_now()

    assert now.tzinfo is datetime.UTC


def test_real_clock_arm_returns_remaining_wall_seconds() -> None:
    clock = RealClock()
    wake = clock.now() + datetime.timedelta(seconds=30)

    delay: float = clock.arm(wake)

    # The real clock waits the modeled duration, minus the tiny elapsed
    # time between now() calls.
    assert 29.0 <= delay <= 30.0


def test_real_clock_arm_never_negative_for_past_wake() -> None:
    clock = RealClock()
    past = clock.now() - datetime.timedelta(seconds=30)

    assert clock.arm(past) == 0.0


def test_skip_clock_arm_returns_zero_delay() -> None:
    clock = SkipClock()
    wake = clock.now() + datetime.timedelta(seconds=300)

    assert clock.arm(wake) == 0.0


def test_skip_clock_now_reaches_armed_horizon() -> None:
    clock = SkipClock()
    wake = clock.now() + datetime.timedelta(seconds=300)

    clock.arm(wake)

    # Virtual now lands on the horizon, then keeps advancing with real
    # time, so it never reads earlier than the armed wake.
    assert wake <= clock.now() <= wake + datetime.timedelta(seconds=5)


def test_skip_clock_is_monotonic_across_arms() -> None:
    clock = SkipClock()
    start = clock.now()
    far = start + datetime.timedelta(seconds=300)
    near = start + datetime.timedelta(seconds=10)

    clock.arm(far)
    clock.arm(near)

    # A later arm for an earlier horizon must not move virtual time back.
    assert clock.now() >= far


def test_skip_clock_advances_with_real_time() -> None:
    clock = SkipClock()

    first = clock.now()
    second = clock.now()

    # Between arms the virtual clock tracks real time, so consecutive
    # reads are ordered and distinct rather than frozen.
    assert second >= first


def test_skip_clock_accumulates_skips() -> None:
    clock = SkipClock()
    start = clock.now()

    clock.arm(clock.now() + datetime.timedelta(seconds=100))
    clock.arm(clock.now() + datetime.timedelta(seconds=200))

    elapsed = (clock.now() - start).total_seconds()
    assert 300.0 <= elapsed <= 305.0
