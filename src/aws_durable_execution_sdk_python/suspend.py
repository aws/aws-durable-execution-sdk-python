import datetime

from aws_durable_execution_sdk_python.exceptions import (
    SuspendExecution,
    TimedSuspendExecution,
)


def suspend_with_optional_timestamp(
    msg: str, datetime_timestamp: datetime.datetime | None = None
) -> None:
    """Suspend execution with optional timestamp.

    Args:
        msg: Descriptive message for the suspension
        timestamp: Timestamp to suspend until, or None/0 for indefinite

    Raises:
        TimedSuspendExecution: When timestamp is in the future
        SuspendExecution: When timestamp is None or in the past
    """
    # TODO: confirm with backend about the behaviour of 0 time suspend
    if datetime_timestamp and datetime_timestamp > datetime.datetime.now(
        tz=datetime.UTC
    ):
        raise TimedSuspendExecution.from_datetime(msg, datetime_timestamp)
    msg = f"Invalid timestamp {datetime_timestamp}, suspending without retry timestamp, original operation: [{msg}]"
    raise SuspendExecution(msg)


def suspend_with_optional_timeout(msg: str, timeout_seconds: int | None = None) -> None:
    """Suspend execution with optional timeout.

    Args:
        msg: Descriptive message for the suspension
        timeout_seconds: Duration to suspend in seconds, or None/0 for indefinite

    Raises:
        TimedSuspendExecution: When timeout_seconds > 0
        SuspendExecution: When timeout_seconds is None or <= 0
    """
    # TODO: confirm with backend about the behaviour of 0 time suspend
    if timeout_seconds and timeout_seconds > 0:
        raise TimedSuspendExecution.from_delay(msg, timeout_seconds)
    msg = f"Invalid timeout seconds {timeout_seconds}, suspending without retry timestamp, original operation: [{msg}]"
    raise SuspendExecution(msg)
