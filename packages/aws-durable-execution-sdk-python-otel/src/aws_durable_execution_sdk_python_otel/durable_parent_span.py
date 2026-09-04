"""OpenTelemetry parent span used while a durable span is not recording."""

from __future__ import annotations

import datetime
import threading
from collections.abc import Mapping
from typing import Any

from opentelemetry.trace import (
    Span,
    SpanContext,
    SpanKind,
    Status,
    StatusCode,
)


class DurableParentSpan(Span):
    """A non-recording parent compatible with SDK span processors.

    ``NonRecordingSpan`` is the standard OpenTelemetry representation for a
    span context without a recording span. Some vendor span processors,
    including the ADOT processor used by Lambda, nevertheless inspect SDK-only
    fields such as ``kind`` and ``attributes`` on every parent. This class
    carries the same immutable context while exposing those read-only fields
    with safe defaults.

    The span also remembers the earliest and latest timestamps observed for
    descendants. Durable operation spans are materialized only when the
    operation completes, so this lets the eventual recording span enclose
    attempts that started before that materialization point.
    """

    def __init__(
        self,
        span_context: SpanContext,
        *,
        start_time: datetime.datetime | None = None,
    ) -> None:
        self._span_context = span_context
        self.kind = SpanKind.INTERNAL
        self.attributes: Mapping[str, Any] = {}
        self.name = ""
        self.status = Status(StatusCode.UNSET)
        self._earliest_start_time = start_time
        self._latest_end_time: datetime.datetime | None = None
        self._timestamp_lock = threading.Lock()

    def get_span_context(self) -> SpanContext:
        return self._span_context

    def is_recording(self) -> bool:
        return False

    def end(self, end_time: int | None = None) -> None:
        return

    def set_attributes(self, attributes: Mapping[str, Any]) -> None:
        return

    def set_attribute(self, key: str, value: Any) -> None:
        return

    def add_event(
        self,
        name: str,
        attributes: Mapping[str, Any] | None = None,
        timestamp: int | None = None,
    ) -> None:
        return

    def add_link(
        self,
        context: SpanContext,
        attributes: Mapping[str, Any] | None = None,
    ) -> None:
        return

    def update_name(self, name: str) -> None:
        return

    def set_status(
        self,
        status: Status | StatusCode,
        description: str | None = None,
    ) -> None:
        return

    def record_exception(
        self,
        exception: BaseException,
        attributes: Mapping[str, Any] | None = None,
        timestamp: int | None = None,
        escaped: bool = False,
    ) -> None:
        return

    def note_start_time(self, timestamp: datetime.datetime | None) -> None:
        """Include a descendant or operation start timestamp."""
        if timestamp is None:
            return
        with self._timestamp_lock:
            if (
                self._earliest_start_time is None
                or timestamp < self._earliest_start_time
            ):
                self._earliest_start_time = timestamp

    def note_end_time(self, timestamp: datetime.datetime | None) -> None:
        """Include a descendant or operation end timestamp."""
        if timestamp is None:
            return
        with self._timestamp_lock:
            if self._latest_end_time is None or timestamp > self._latest_end_time:
                self._latest_end_time = timestamp

    def normalized_start_time(
        self, timestamp: datetime.datetime | None
    ) -> datetime.datetime | None:
        """Return a start that encloses all observed descendants."""
        with self._timestamp_lock:
            if timestamp is None:
                return self._earliest_start_time
            if self._earliest_start_time is None:
                return timestamp
            return min(timestamp, self._earliest_start_time)

    def normalized_end_time(
        self,
        timestamp: datetime.datetime | None,
        *,
        start_time: datetime.datetime | None = None,
    ) -> datetime.datetime | None:
        """Return an end that encloses all observed descendants."""
        with self._timestamp_lock:
            if timestamp is None:
                normalized = self._latest_end_time
            elif self._latest_end_time is None:
                normalized = timestamp
            else:
                normalized = max(timestamp, self._latest_end_time)
        if (
            start_time is not None
            and normalized is not None
            and normalized <= start_time
        ):
            return start_time + datetime.timedelta(microseconds=1)
        return normalized


def ensure_end_after_start(
    start_time: datetime.datetime | None,
    end_time: datetime.datetime | None,
) -> datetime.datetime | None:
    """Prevent a completed span from ending at or before its start."""
    if start_time is not None and end_time is not None and end_time <= start_time:
        return start_time + datetime.timedelta(microseconds=1)
    return end_time
