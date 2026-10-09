# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The logger that the worker writes to."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Protocol

from aws_durable_execution_sdk_python_microvm_worker._util import safe_text


DEFAULT_LOGGER_NAME = "aws_durable_execution_sdk_python_microvm_worker"
"""The name of the standard library logger that the worker uses by default."""


class MicrovmWorkerLogger(Protocol):
    """Receives the worker's log lines.

    The signature is the one of :class:`logging.Logger`, and of the core
    SDK's ``LoggerInterface``. So a ``logging.Logger`` or a ``LoggerAdapter``
    works as it is. The worker passes its structured fields in ``extra``.
    """

    def info(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None: ...  # pragma: no cover

    def warning(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None: ...  # pragma: no cover

    def error(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None: ...  # pragma: no cover


def default_logger() -> MicrovmWorkerLogger:
    """Return the worker's standard library logger.

    The logger has no handler of its own. Configure the ``logging`` module in
    the image, for example with ``logging.basicConfig(level=logging.INFO)``,
    to see the worker's INFO lines.
    """
    return logging.getLogger(DEFAULT_LOGGER_NAME)


class _SafeLogger:
    """Wraps a logger, and drops a line whose logger raises.

    The worker logs from job threads and from background threads. A logger
    that raises there would end the thread, and a job could then never
    report its outcome. A log line is not worth that. So every exception
    from the wrapped logger is dropped.
    """

    def __init__(self, inner: MicrovmWorkerLogger) -> None:
        self._inner = inner

    def info(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        try:
            self._inner.info(msg, *args, extra=extra)
        except Exception:  # noqa: BLE001, S110
            pass

    def warning(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        try:
            self._inner.warning(msg, *args, extra=extra)
        except Exception:  # noqa: BLE001, S110
            pass

    def error(
        self, msg: object, *args: object, extra: Mapping[str, object] | None = None
    ) -> None:
        try:
            self._inner.error(msg, *args, extra=extra)
        except Exception:  # noqa: BLE001, S110
            pass


def safe_logger(inner: MicrovmWorkerLogger | None = None) -> MicrovmWorkerLogger:
    """Return a logger that drops a line whose logger raises.

    Args:
        inner: The logger to wrap. Defaults to :func:`default_logger`.
    """
    return _SafeLogger(inner if inner is not None else default_logger())


def describe(value: object) -> object:
    """Describe an error for a log line, and never raise."""
    if not isinstance(value, BaseException):
        return value
    return {
        "name": safe_text(lambda: type(value).__name__, "Exception") or "Exception",
        "message": safe_text(lambda: str(value), "unknown error"),
    }
