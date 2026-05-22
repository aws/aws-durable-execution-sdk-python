"""
The model expects >= 1 seconds when receiving an OperationUpdate via api calls
    (see StepOptions, WaitOptions, etc)
We don't force a minimum delay_seconds OR time to timestamp here because:

1. suspension can be reached from multiple handlers,
2. suspension can be reached from multiple "contexts", e.g. top level wait, or a child retry
3. we use TimedSuspendExecution as an optimization mechanism within concurrent executions (map/parallel)
    to know when to retry without being told so.

As such, it is up to the caller to ensure consistency wrt what dataplane sees and what happens within a
function.

Behaviour:
- When we suspend without a target delay / timestamp THEN we suspend indefinitely.
- When `delay_seconds` or timestamp exist, THEN we suspend for `max(delay_seconds, 0)` or suspend for "now".
- When suspension happens within a child branch and the branch suspends for `0` seconds, THEN execution will
resume immediately as it will be inserted at the top of the queue
- When suspension happens within a child branch and the branch suspends for > 0 seconds, THEN execution will
resume as soon as `delay_seconds` have passed
- When suspension happens at the top level, then the Lambda will terminate and Dataplane is responsible
    for resuming

"""

import datetime
from typing import NoReturn

from aws_durable_execution_sdk_python.exceptions import (
    SuspendExecution,
    TimedSuspendExecution,
)


def suspend_with_optional_resume_timestamp(
    msg: str, datetime_timestamp: datetime.datetime | None = None
) -> NoReturn:
    """Suspend execution with optional timestamp.

    Args:
        msg: Descriptive message for the suspension
        timestamp: Timestamp to suspend until, or None for indefinite

    Raises:
        TimedSuspendExecution: When timestamp is in the future or now()
        SuspendExecution: When timestamp is None or in the past
    """

    if datetime_timestamp is None:
        msg = f"No timestamp provided. Suspending without retry timestamp. Original operation: [{msg}]"
        raise SuspendExecution(msg)

    if datetime_timestamp < datetime.datetime.now(tz=datetime.UTC):
        msg = f"Invalid timestamp {datetime_timestamp}, suspending with immediate retry, original operation: [{msg}]"
        raise TimedSuspendExecution.from_datetime(
            msg, datetime.datetime.now(tz=datetime.UTC)
        )

    raise TimedSuspendExecution.from_datetime(msg, datetime_timestamp)


def suspend_with_optional_resume_delay(
    msg: str, delay_seconds: int | None = None
) -> NoReturn:
    """Suspend execution with optional delay.

    Args:
        msg: Descriptive message for the suspension
        delay_seconds: Duration to suspend in seconds, or None for indefinite

    Raises:
        TimedSuspendExecution: When delay_seconds when delay_seconds is not None
        SuspendExecution: When delay_seconds is None
    """

    if delay_seconds is None:
        msg = f"No delay_seconds provided, suspending without retry timestamp, original operation: [{msg}]"
        raise SuspendExecution(msg)

    if delay_seconds < 0:
        msg = f"Invalid delay_seconds {delay_seconds}, suspending with delay 0, original operation: [{msg}]"
        raise TimedSuspendExecution.from_delay(msg, 0)

    raise TimedSuspendExecution.from_delay(msg, delay_seconds)
