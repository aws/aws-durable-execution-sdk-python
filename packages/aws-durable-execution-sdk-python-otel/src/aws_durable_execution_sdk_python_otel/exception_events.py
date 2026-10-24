"""Record serialized errors without reconstructing application exceptions.

Use a plain exception as the event carrier and explicitly supply the original
checkpointed type, message, and stack trace. Resolving the original class is
unnecessary and could run application imports or constructors during replay.
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.lambda_service import ErrorObject


_DEFAULT_MESSAGE = "Unknown error"


def exception_from_error_object(error: ErrorObject) -> Exception:
    """Create a plain event carrier; attributes preserve the original details."""
    message: str = error.message if error.message is not None else ""
    if not error.type:
        message = message or _DEFAULT_MESSAGE
    return Exception(message)


def error_object_exception_attributes(error: ErrorObject) -> dict[str, Any]:
    """Return explicit event attributes for a checkpointed error.

    OTel derives exception attributes from the supplied exception. The plain
    carrier has neither the original runtime type nor its traceback, so the
    checkpointed values are supplied explicitly.
    """
    attributes: dict[str, Any] = {}
    if error.type is not None:
        attributes["exception.type"] = error.type
    if error.message is not None:
        attributes["exception.message"] = error.message
    if error.stack_trace:
        attributes["exception.stacktrace"] = "\n".join(error.stack_trace)
    return attributes


def record_error_object_exception(span: Any, error: ErrorObject | None) -> None:
    """Record a span exception that preserves the SDK's original error type.

    ``error`` may be ``None`` for a failed outcome that carried no metadata; a
    generic exception is recorded in that case.
    """
    if error is None:
        span.record_exception(Exception(_DEFAULT_MESSAGE))
        return
    span.record_exception(
        exception_from_error_object(error),
        attributes=error_object_exception_attributes(error),
    )
