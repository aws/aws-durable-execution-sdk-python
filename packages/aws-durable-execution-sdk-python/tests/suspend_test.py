import datetime

import pytest

from aws_durable_execution_sdk_python.exceptions import (
    SuspendExecution,
    TimedSuspendExecution,
)
from aws_durable_execution_sdk_python.suspend import (
    suspend_with_optional_resume_delay,
    suspend_with_optional_resume_timestamp,
)


def test_suspend_optional_timestamp_with_none():
    with pytest.raises(
        SuspendExecution,
        match="No timestamp provided. Suspending without retry timestamp.",
    ):
        suspend_with_optional_resume_timestamp(
            "test",
            None,
        )


def test_suspend_optional_timestamp_with_past():
    with pytest.raises(SuspendExecution, match="Invalid timestamp"):
        suspend_with_optional_resume_timestamp(
            "test",
            datetime.datetime.now(tz=datetime.UTC) - datetime.timedelta(seconds=1),
        )


def test_suspend_optional_timestamp_with_future():
    with pytest.raises(TimedSuspendExecution, match="test"):
        suspend_with_optional_resume_timestamp(
            "test",
            datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(seconds=1),
        )


def test_suspend_optional_timeout_with_none():
    with pytest.raises(SuspendExecution, match="suspending without retry timestamp"):
        suspend_with_optional_resume_delay(
            "test",
            None,
        )


def test_suspend_optional_timeout_with_negative():
    with pytest.raises(
        SuspendExecution, match="Invalid delay_seconds -1, suspending with delay 0"
    ):
        suspend_with_optional_resume_delay(
            "test",
            -1,
        )


def test_suspend_optional_timeout_with_positive():
    with pytest.raises(TimedSuspendExecution, match="test"):
        suspend_with_optional_resume_delay(
            "test",
            1,
        )
