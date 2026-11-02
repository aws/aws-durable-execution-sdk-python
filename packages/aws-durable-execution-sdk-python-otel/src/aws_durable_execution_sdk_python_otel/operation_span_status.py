"""OpenTelemetry operation status mapping shared by both views."""

from aws_durable_execution_sdk_python.lambda_service import OperationStatus
from aws_durable_execution_sdk_python.plugin import OperationEndInfo
from opentelemetry.trace import Span, StatusCode


_FAILURE_STATUSES = frozenset(
    {OperationStatus.FAILED, OperationStatus.TIMED_OUT, OperationStatus.STOPPED}
)


def set_operation_span_status(span: Span, info: OperationEndInfo) -> None:
    """Apply the durable outcome, with supplied error details taking precedence."""
    if info.error:
        span.set_status(StatusCode.ERROR, info.error.message or "")
    elif info.status in _FAILURE_STATUSES:
        span.set_status(StatusCode.ERROR)
    elif info.status is OperationStatus.SUCCEEDED:
        span.set_status(StatusCode.OK)
