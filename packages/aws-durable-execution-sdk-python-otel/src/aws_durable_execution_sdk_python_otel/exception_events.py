"""Record serialized errors without reconstructing application exceptions.

Emit the event directly from the checkpointed fields instead of handing OTel a
carrier exception. Resolving the original class is unnecessary and could run
application imports or constructors during replay, and a placeholder carrier is
worse than none: ``Span.record_exception`` derives ``exception.type``,
``exception.message``, and ``exception.stacktrace`` from the carrier and only
then merges the explicit attributes, so a field the checkpoint never stored
would be reported as an invented value rather than left out.
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.lambda_service import ErrorObject


def error_object_exception_attributes(error: ErrorObject) -> dict[str, Any]:
    """Return event attributes holding exactly the checkpointed fields.

    An absent field is omitted so consumers can tell missing metadata apart
    from a supplied empty value.
    """
    attributes: dict[str, Any] = {}
    if error.type is not None:
        attributes["exception.type"] = error.type
    if error.message is not None:
        attributes["exception.message"] = error.message
    if error.stack_trace:
        attributes["exception.stacktrace"] = "\n".join(error.stack_trace)
    return attributes


def record_error_object_exception(
    span: Any, error: ErrorObject | None, *, timestamp: int
) -> None:
    """Record a span exception event that preserves the checkpointed error.

    A failed outcome with no error object, or with neither a type nor a message,
    has no diagnostic event: the OTel exception conventions require whichever of
    the two the other lacks, so an event carrying neither would be malformed,
    and inventing one of them is what this module exists to avoid. The span's
    error status and ``durable.operation.status`` still record the failure.

    Terminal callers supply the normalized span-end timestamp so delayed
    delivery does not place an event outside the span.
    """
    if error is None or (error.type is None and error.message is None):
        return
    span.add_event(
        "exception",
        attributes=error_object_exception_attributes(error),
        timestamp=timestamp,
    )
