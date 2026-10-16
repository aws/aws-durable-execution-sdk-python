# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the worker's own SuspendMicrovm call."""

from __future__ import annotations

import socket
import threading
import time
from typing import Any

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    NoCredentialsError,
)

from aws_durable_execution_sdk_python_microvm_worker.self_suspend import (
    SUSPEND_CALL_TIMEOUT_SECONDS,
    SuspendDeadlineError,
    SuspendOutcome,
    classify_suspend_error,
    create_microvms_client,
    suspend_microvm,
)


def suspend_error(code: str, status: int, retries: int = 0) -> ClientError:
    response: Any = {
        "Error": {"Code": code, "Message": f"{code} message"},
        "ResponseMetadata": {"HTTPStatusCode": status, "RetryAttempts": retries},
    }
    return ClientError(response, "SuspendMicrovm")


@pytest.mark.parametrize(
    "error",
    [
        suspend_error("AccessDeniedException", 403),
        suspend_error("ValidationException", 400),
        suspend_error("ResourceNotFoundException", 404),
        suspend_error("SomeServiceError", 403),
        # The rejection codes count whatever their status.
        suspend_error("AccessDeniedException", 500),
        NoCredentialsError(),
    ],
)
def test_classifies_a_rejection_on_the_first_attempt_as_rejected(
    error: BaseException,
) -> None:
    assert classify_suspend_error(error) is SuspendOutcome.REJECTED


@pytest.mark.parametrize(
    "error",
    [
        suspend_error("ThrottlingException", 429),
        # Some services answer a throttle with HTTP 400.
        suspend_error("ThrottlingException", 400),
        suspend_error("TooManyRequestsException", 429),
        suspend_error("SomeThrottle", 429),
        suspend_error("ExpiredTokenException", 403),
        suspend_error("SignatureDoesNotMatch", 403),
        suspend_error("RequestTimeTooSkewed", 403),
    ],
)
def test_classifies_a_first_attempt_that_can_succeed_later_as_refused_for_now(
    error: BaseException,
) -> None:
    assert classify_suspend_error(error) is SuspendOutcome.REFUSED_FOR_NOW


@pytest.mark.parametrize(
    "error",
    [
        # An earlier attempt may have suspended the MicroVM.
        suspend_error("ValidationException", 400, retries=1),
        suspend_error("AccessDeniedException", 403, retries=2),
        suspend_error("ThrottlingException", 429, retries=2),
        suspend_error("ConflictException", 409),
        suspend_error("ConflictException", 400),
        suspend_error("RequestTimeoutException", 408),
        suspend_error("InternalServerException", 500),
        TimeoutError("timed out"),
        SuspendDeadlineError("passed"),
        EndpointConnectionError(endpoint_url="https://example.com"),
        RuntimeError("boom"),
    ],
)
def test_classifies_any_other_error_as_an_unknown_outcome(
    error: BaseException,
) -> None:
    assert classify_suspend_error(error) is SuspendOutcome.UNKNOWN


def test_reads_a_client_error_without_metadata_as_an_unknown_outcome() -> None:
    error = ClientError({"Error": {"Code": "SomeError"}}, "SuspendMicrovm")  # type: ignore[typeddict-item]

    assert classify_suspend_error(error) is SuspendOutcome.UNKNOWN


class _Client:
    def __init__(self, answer: Any = None, delay: float = 0.0) -> None:
        self.calls: list[dict[str, Any]] = []
        self._answer = answer
        self._delay = delay
        self.release = threading.Event()

    def suspend_microvm(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self._delay:
            self.release.wait(self._delay)
        if isinstance(self._answer, BaseException):
            raise self._answer
        return {}


def test_calls_suspend_microvm_with_the_microvm_id() -> None:
    client: Any = _Client()

    suspend_microvm(client, "mvm-1", 5.0)

    assert client.calls == [{"microvmIdentifier": "mvm-1"}]


def test_raises_the_call_s_own_error() -> None:
    error = suspend_error("AccessDeniedException", 403)

    client: Any = _Client(error)

    with pytest.raises(ClientError) as raised:
        suspend_microvm(client, "mvm-1", 5.0)

    assert raised.value is error


def test_raises_the_call_s_own_timeout_error() -> None:
    client: Any = _Client(TimeoutError("read timed out"))

    with pytest.raises(TimeoutError, match="^read timed out$"):
        suspend_microvm(client, "mvm-1", 5.0)


def test_stops_waiting_for_a_stalled_call_after_the_timeout() -> None:
    client: Any = _Client(delay=10.0)
    started = time.monotonic()

    with pytest.raises(TimeoutError, match="longer than 0.05 seconds"):
        suspend_microvm(client, "mvm-1", 0.05)

    assert time.monotonic() - started < 2.0
    client.release.set()


def test_the_default_client_ends_a_read_at_the_call_timeout() -> None:
    client = create_microvms_client("us-west-2")
    try:
        config: Any = client.meta.config
        assert client.meta.region_name == "us-west-2"
        assert config.read_timeout == SUSPEND_CALL_TIMEOUT_SECONDS
        assert config.connect_timeout == 5
        assert config.retries == {"mode": "standard", "total_max_attempts": 3}
    finally:
        client.close()


class _SilentService:
    """A local HTTP service that accepts connections and never answers, and
    records when each attempt arrives."""

    def __init__(self) -> None:
        self.attempts: list[float] = []
        self._server = socket.create_server(("127.0.0.1", 0))
        self._server.settimeout(0.1)
        self._stop = threading.Event()
        self._connections: list[socket.socket] = []
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._server.getsockname()[1]}"

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                connection, _ = self._server.accept()
            except OSError:
                continue
            self.attempts.append(time.monotonic())
            self._connections.append(connection)

    def close(self) -> None:
        self._stop.set()
        self._thread.join(5)
        for connection in self._connections:
            connection.close()
        self._server.close()


def test_starts_no_attempt_after_the_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # botocore retries a read timeout. Without the deadline handler, it went
    # on retrying in the call's thread after the wait had ended.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    service = _SilentService()
    try:
        client = boto3.session.Session().client(
            "lambda-microvms",
            region_name="us-east-1",
            endpoint_url=service.url,
            config=Config(
                connect_timeout=1,
                read_timeout=0.3,
                retries={"mode": "standard", "total_max_attempts": 5},
            ),
        )
        started = time.monotonic()

        # The call ends with the wait's TimeoutError, or with
        # SuspendDeadlineError when botocore meets the handler first.
        with pytest.raises(TimeoutError):
            suspend_microvm(client, "mvm-1", 0.5)

        # The attempt in flight at the deadline ends at its read timeout,
        # and botocore then meets the deadline handler.
        time.sleep(2.0)
        assert service.attempts
        assert all(at - started < 0.5 for at in service.attempts)
        # The handler is gone once the call has ended.
        handlers = client.meta.events._emitter._unique_id_handlers  # type: ignore[attr-defined]  # noqa: SLF001
        assert not [key for key in handlers if "microvm-worker-deadline" in key]
        client.close()
    finally:
        service.close()


def test_skips_the_deadline_handler_for_a_client_without_events() -> None:
    client: Any = _Client()

    suspend_microvm(client, "mvm-1", 5.0)

    assert client.calls == [{"microvmIdentifier": "mvm-1"}]
