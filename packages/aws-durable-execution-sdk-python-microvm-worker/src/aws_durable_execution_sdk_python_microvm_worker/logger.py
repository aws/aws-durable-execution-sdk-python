# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The logger that the worker writes to."""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class MicrovmWorkerLogger(Protocol):
    """Receives the worker's log lines.

    Each method takes a message and an optional mapping of structured data.
    """

    def info(self, message: str, data: Mapping[str, Any] | None = None) -> None:
        """Log a line at INFO."""

    def warning(self, message: str, data: Mapping[str, Any] | None = None) -> None:
        """Log a line at WARNING."""

    def error(self, message: str, data: Mapping[str, Any] | None = None) -> None:
        """Log a line at ERROR."""


class JsonLogger:
    """Writes each line as one JSON object: INFO to stdout, others to stderr.

    A MicroVM's log configuration collects both streams. One object per line
    keeps each line one log event.
    """

    def info(self, message: str, data: Mapping[str, Any] | None = None) -> None:
        """Log a line at INFO."""
        _write(sys.stdout, "INFO", message, data)

    def warning(self, message: str, data: Mapping[str, Any] | None = None) -> None:
        """Log a line at WARNING."""
        _write(sys.stderr, "WARN", message, data)

    def error(self, message: str, data: Mapping[str, Any] | None = None) -> None:
        """Log a line at ERROR."""
        _write(sys.stderr, "ERROR", message, data)


def _write(
    stream: Any, level: str, message: str, data: Mapping[str, Any] | None
) -> None:
    # default=str keeps a value that JSON cannot encode, such as an
    # exception, as its text instead of failing the line.
    line = json.dumps({"level": level, "message": message, **(data or {})}, default=str)
    stream.write(line + "\n")
    stream.flush()


class SafeLogger:
    """Wraps a logger, and drops a line whose logger raises.

    The worker logs from job threads and from background threads. A logger
    that raises there would end the thread, and a job could then never
    report its outcome. A log line is not worth that. So every exception
    from the wrapped logger is dropped.
    """

    def __init__(self, inner: MicrovmWorkerLogger) -> None:
        self._inner = inner

    def info(self, message: str, data: Mapping[str, Any] | None = None) -> None:
        """Log a line at INFO."""
        try:
            self._inner.info(message, data)
        except Exception:  # noqa: BLE001, S110
            pass

    def warning(self, message: str, data: Mapping[str, Any] | None = None) -> None:
        """Log a line at WARNING."""
        try:
            self._inner.warning(message, data)
        except Exception:  # noqa: BLE001, S110
            pass

    def error(self, message: str, data: Mapping[str, Any] | None = None) -> None:
        """Log a line at ERROR."""
        try:
            self._inner.error(message, data)
        except Exception:  # noqa: BLE001, S110
            pass


def describe(value: object) -> object:
    """Describe an error for a log line, and never raise."""
    if not isinstance(value, BaseException):
        return value
    return {
        "name": _safe_text(lambda: type(value).__name__, "Exception") or "Exception",
        "message": _safe_text(lambda: str(value), "unknown error"),
    }


def _safe_text(read: Any, fallback: str) -> str:
    try:
        text = read()
    except Exception:  # noqa: BLE001
        return fallback
    return text if isinstance(text, str) else fallback
