"""OpenTelemetry timestamp conversion shared by span producers."""

from datetime import datetime


def to_otel_timestamp(value: datetime) -> int:
    """Convert a datetime to nanoseconds using the SDK's existing precision."""
    return int(value.timestamp() * 1_000_000_000)
