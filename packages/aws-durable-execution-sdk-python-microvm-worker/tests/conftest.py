# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Test doubles for the Lambda client."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

import pytest
from botocore.exceptions import ClientError


def client_error(code: str, status: int = 400, retries: int = 0) -> ClientError:
    """A botocore ClientError with a code, an HTTP status, and SDK retries."""
    response: Any = {
        "Error": {"Code": code, "Message": f"{code} message"},
        "ResponseMetadata": {"HTTPStatusCode": status, "RetryAttempts": retries},
    }
    return ClientError(response, "SendDurableExecutionCallbackSuccess")


Response = BaseException | Callable[[], Any] | None


class FakeLambdaClient:
    """Records callback API calls, and answers from a scripted list.

    Each answer is an exception to raise, a function to call, or ``None`` for
    success. After the list ends, every call succeeds. ``default`` replaces
    that success for one kind of call.
    """

    def __init__(self, *responses: Response, default: Response = None) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._responses = list(responses)
        self._default = default
        self._lock = threading.Lock()
        self.closed = False

    def names(self) -> list[str]:
        with self._lock:
            return [name for name, _ in self.calls]

    def _answer(self, name: str, kwargs: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self.calls.append((name, kwargs))
            response = self._responses.pop(0) if self._responses else self._default
        if isinstance(response, BaseException):
            raise response
        if callable(response):
            response()
        return {}

    def send_durable_execution_callback_success(self, **kwargs: Any) -> dict[str, Any]:
        return self._answer("success", kwargs)

    def send_durable_execution_callback_failure(self, **kwargs: Any) -> dict[str, Any]:
        return self._answer("failure", kwargs)

    def send_durable_execution_callback_heartbeat(
        self, **kwargs: Any
    ) -> dict[str, Any]:
        return self._answer("heartbeat", kwargs)

    def close(self) -> None:
        self.closed = True


class RecordingLogger:
    """Records the worker's log lines."""

    def __init__(self) -> None:
        self.lines: list[tuple[str, str, dict[str, Any]]] = []
        self._lock = threading.Lock()

    def _add(self, level: str, message: str, data: Any) -> None:
        with self._lock:
            self.lines.append((level, message, dict(data or {})))

    def info(self, msg: object, *args: object, extra: Any = None) -> None:
        self._add("info", str(msg), extra)

    def warning(self, msg: object, *args: object, extra: Any = None) -> None:
        self._add("warning", str(msg), extra)

    def error(self, msg: object, *args: object, extra: Any = None) -> None:
        self._add("error", str(msg), extra)

    def messages(self, level: str) -> list[str]:
        with self._lock:
            return [message for lvl, message, _ in self.lines if lvl == level]


# The fakes are fixtures, not imports. Several packages in this repository
# have a top-level ``tests`` package, so ``from tests.conftest import ...``
# could resolve to another package's tests.


@pytest.fixture
def fake_client() -> type[FakeLambdaClient]:
    return FakeLambdaClient


@pytest.fixture
def make_error() -> Callable[..., ClientError]:
    return client_error


@pytest.fixture
def logger() -> RecordingLogger:
    return RecordingLogger()
