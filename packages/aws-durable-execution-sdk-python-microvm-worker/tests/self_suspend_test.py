# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the worker's own SuspendMicrovm call."""

from __future__ import annotations

import json
import socket
import threading
import time
from email.utils import formatdate
from typing import Any

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import boto3
import pytest
from botocore.awsrequest import AWSResponse, HTTPHeaders
from botocore.config import Config
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    EndpointConnectionError,
    NoCredentialsError,
)
from botocore.retries.standard import ExponentialBackoff

from aws_durable_execution_sdk_python_microvm_worker.self_suspend import (
    SUSPEND_CALL_TIMEOUT_SECONDS,
    SuspendDeadlineError,
    SuspendOutcome,
    SuspendResumedError,
    classify_suspend_error,
    create_microvms_client,
    suspend_microvm,
)


_BEFORE_SEND = "before-send.lambda-microvms.SuspendMicrovm"


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
        # A resume hook stopped a later attempt. An earlier attempt may have
        # suspended the MicroVM.
        SuspendResumedError("resumed"),
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


def test_returns_only_when_a_call_in_flight_at_the_deadline_has_ended() -> None:
    # botocore cannot cancel an attempt in flight. So the call returns only
    # when the attempt ends, and the worker refuses jobs until then.
    client: Any = _Client(delay=0.3)
    started = time.monotonic()

    suspend_microvm(client, "mvm-1", 0.05)

    assert time.monotonic() - started >= 0.29


def test_the_default_client_ends_a_read_at_the_call_timeout() -> None:
    client = create_microvms_client("us-west-2")
    try:
        config: Any = client.meta.config
        assert client.meta.region_name == "us-west-2"
        assert config.read_timeout == SUSPEND_CALL_TIMEOUT_SECONDS
        assert config.connect_timeout == 5
        # One attempt: a retry after a lost answer can run after the MicroVM
        # resumed, and suspend it with the next job inside.
        assert config.retries == {"mode": "standard", "total_max_attempts": 1}
    finally:
        client.close()


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def stream(self, **_: Any) -> Any:
        yield self._data


def _answer_every_attempt(
    client: Any, status: int, code: str, **headers: str
) -> list[int]:
    """Answer each attempt of the client with one error, instead of HTTP.

    botocore's retry handler still runs on each answer, because a
    ``before-send`` handler replaces only the send. Returns the attempt
    list, which grows by one per attempt.
    """
    attempts: list[int] = []
    answer_headers = HTTPHeaders()
    answer_headers["Content-Type"] = "application/json"
    answer_headers["x-amzn-ErrorType"] = code
    for name, value in headers.items():
        answer_headers[name] = value

    def send(**_: Any) -> AWSResponse:
        attempts.append(1)
        return AWSResponse(
            "https://lambda-microvms.us-east-1.amazonaws.com",
            status,
            answer_headers,
            _Body(json.dumps({"message": f"{code} message"}).encode()),
        )

    client.meta.events.register("before-send.lambda-microvms.SuspendMicrovm", send)
    return attempts


@pytest.mark.parametrize(
    ("status", "code", "headers", "outcome"),
    [
        # The service did nothing. The worker calls again after the next
        # idle time.
        (429, "ThrottlingException", {}, SuspendOutcome.REFUSED_FOR_NOW),
        # A server error does not show whether the service acted.
        (500, "InternalServerException", {}, SuspendOutcome.UNKNOWN),
        (503, "ServiceUnavailableException", {}, SuspendOutcome.UNKNOWN),
        # botocore does not correct the clock from the server's Date header.
        (
            403,
            "InvalidSignatureException",
            {"Date": formatdate(time.time() + 600, usegmt=True)},
            SuspendOutcome.REFUSED_FOR_NOW,
        ),
        (403, "InvalidSignatureException", {}, SuspendOutcome.REFUSED_FOR_NOW),
        (403, "ExpiredTokenException", {}, SuspendOutcome.REFUSED_FOR_NOW),
    ],
    ids=[
        "throttle",
        "server-error",
        "unavailable",
        "signature-clock-offset",
        "signature-mismatch",
        "expired-token",
    ],
)
def test_the_default_client_makes_one_attempt_for_an_error_answer(
    credentials: None,
    status: int,
    code: str,
    headers: dict[str, str],
    outcome: SuspendOutcome,
) -> None:
    client = create_microvms_client("us-east-1")
    try:
        sent = _answer_every_attempt(client, status, code, **headers)

        with pytest.raises(ClientError) as raised:
            suspend_microvm(client, "mvm-1", 5.0)

        assert len(sent) == 1
        metadata = raised.value.response["ResponseMetadata"]
        assert metadata["RetryAttempts"] == 0
        assert classify_suspend_error(raised.value) is outcome
    finally:
        client.close()


def test_the_default_client_makes_one_attempt_after_a_connection_error(
    credentials: None,
) -> None:
    # botocore retries a closed connection in its standard mode. The default
    # client makes no second attempt, and the outcome is unknown.
    client = create_microvms_client("us-east-1")
    try:
        sent: list[int] = []

        def close(**_: Any) -> None:
            sent.append(1)
            raise ConnectionClosedError(endpoint_url="https://example.com")

        client.meta.events.register(_BEFORE_SEND, close)

        with pytest.raises(ConnectionClosedError) as raised:
            suspend_microvm(client, "mvm-1", 5.0)

        assert sent == [1]
        assert classify_suspend_error(raised.value) is SuspendOutcome.UNKNOWN
    finally:
        client.close()


def test_a_retrying_client_retries_a_throttle_and_its_error_is_unknown(
    credentials: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A caller's client can retry. Its error then reports the retries, and an
    # earlier attempt may have reached the service.
    monkeypatch.setattr(ExponentialBackoff, "delay_amount", lambda self, context: 0)
    client = _retrying_client()
    try:
        sent = _answer_every_attempt(client, 429, "ThrottlingException")

        with pytest.raises(ClientError) as raised:
            suspend_microvm(client, "mvm-1", 5.0)

        assert len(sent) == 3
        assert raised.value.response["ResponseMetadata"]["RetryAttempts"] == 2
        assert classify_suspend_error(raised.value) is SuspendOutcome.UNKNOWN
    finally:
        client.close()


def _retrying_client() -> Any:
    """A caller's client that makes 3 attempts."""
    return boto3.session.Session().client(
        "lambda-microvms",
        region_name="us-east-1",
        config=Config(retries={"mode": "standard", "total_max_attempts": 3}),
    )


def test_starts_no_attempt_after_the_microvm_resumed(
    credentials: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # 1. The first attempt reaches the service, and the service suspends the
    #    MicroVM.
    # 2. The freeze cuts the connection, so the answer is lost.
    # 3. The resume hook arrives, and the worker accepts jobs again.
    # 4. botocore then retries the closed connection. That attempt would
    #    suspend the MicroVM with a new job inside.
    # So the handler stops the retry once resumed() is True.
    monkeypatch.setattr(ExponentialBackoff, "delay_amount", lambda self, context: 0)
    client = _retrying_client()
    resumes = 0
    try:
        sent: list[int] = []

        def land_then_lose_the_answer(**_: Any) -> None:
            nonlocal resumes
            sent.append(1)
            resumes += 1
            raise ConnectionClosedError(endpoint_url="https://example.com")

        client.meta.events.register(_BEFORE_SEND, land_then_lose_the_answer)

        with pytest.raises(SuspendResumedError):
            suspend_microvm(client, "mvm-1", 5.0, resumed=lambda: resumes > 0)

        assert sent == [1]
        assert _deadline_handlers(client) == []
    finally:
        client.close()


def test_starts_no_first_attempt_when_the_microvm_resumed_before_the_call() -> None:
    events = _RecordingEvents()
    client: Any = _EmittingClient(events)

    with pytest.raises(SuspendResumedError):
        suspend_microvm(client, "mvm-1", 5.0, resumed=lambda: True)

    assert client.sent == 0
    assert events.handlers == {}


def test_starts_the_attempt_while_the_microvm_has_not_resumed() -> None:
    events = _RecordingEvents()
    client: Any = _EmittingClient(events)
    checks: list[int] = []

    def resumed() -> bool:
        checks.append(1)
        return False

    suspend_microvm(client, "mvm-1", 5.0, resumed=resumed)

    assert client.sent == 1
    assert checks == [1]
    assert events.handlers == {}


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


def _microvms_client(url: str, read_timeout: float = 5.0) -> Any:
    return boto3.session.Session().client(
        "lambda-microvms",
        region_name="us-east-1",
        endpoint_url=url,
        config=Config(
            connect_timeout=1,
            read_timeout=read_timeout,
            retries={"mode": "standard", "total_max_attempts": 5},
        ),
    )


def _deadline_handlers(client: Any) -> list[str]:
    handlers = client.meta.events._emitter._unique_id_handlers  # noqa: SLF001
    return [key for key in handlers if "microvm-worker-deadline" in key]


@pytest.fixture
def credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")


def test_starts_no_attempt_after_the_deadline(credentials: None) -> None:
    # botocore retries a read timeout. Without the deadline handler, it went
    # on retrying after the deadline.
    service = _SilentService()
    try:
        client = _microvms_client(service.url, read_timeout=0.3)
        # Runs after the deadline handler, so it records only the attempts
        # that the handler let through.
        sent: list[float] = []
        client.meta.events.register(
            "before-send.lambda-microvms.SuspendMicrovm",
            lambda **_: sent.append(time.monotonic()),
        )
        started = time.monotonic()

        # The attempt in flight at the deadline ends at its read timeout.
        # botocore then waits its backoff, and meets the deadline handler.
        with pytest.raises(SuspendDeadlineError):
            suspend_microvm(client, "mvm-1", 0.5)

        assert service.attempts
        assert sent
        # The handler and this recorder run back to back. The margin covers
        # a thread switch between them on a loaded machine.
        assert all(at - started < 0.55 for at in sent)
        assert _deadline_handlers(client) == []
        client.close()
    finally:
        service.close()


class _AnsweringService:
    """A local HTTP service that answers every request with ``200 {}``."""

    def __init__(self) -> None:
        self.requests = 0
        service = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                service.requests += 1
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, *args: Any) -> None:
                return

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_address[1]}"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


def test_a_caller_client_keeps_no_expired_handler_after_a_failed_call(
    credentials: None,
) -> None:
    # A client from create_microvms_client can serve every suspend. An
    # expired handler left on it would fail every later call before it
    # sends.
    service = _AnsweringService()
    try:
        client = _microvms_client(service.url)

        with pytest.raises(SuspendDeadlineError):
            suspend_microvm(client, "mvm-1", 0)
        assert _deadline_handlers(client) == []
        assert service.requests == 0

        suspend_microvm(client, "mvm-1", 5.0)
        assert service.requests == 1
        assert _deadline_handlers(client) == []
        client.close()
    finally:
        service.close()


def test_skips_the_deadline_handler_for_a_client_without_events() -> None:
    client: Any = _Client()

    suspend_microvm(client, "mvm-1", 5.0)

    assert client.calls == [{"microvmIdentifier": "mvm-1"}]


class _RecordingEvents:
    """Records deadline handlers, and keeps one per ID, as botocore does."""

    def __init__(self) -> None:
        self.handlers: dict[str, Any] = {}
        self.ids: list[str] = []
        self._lock = threading.Lock()

    def register_first(self, event: str, handler: Any, unique_id: str) -> None:
        with self._lock:
            self.ids.append(unique_id)
            self.handlers.setdefault(unique_id, handler)

    def unregister(self, event: str, unique_id: str) -> None:
        with self._lock:
            self.handlers.pop(unique_id, None)


class _EmittingClient:
    """A client that runs the registered handlers before its one attempt,
    as botocore emits ``before-send``."""

    def __init__(self, events: _RecordingEvents) -> None:
        self.meta = type("Meta", (), {"events": events})()
        self.sent = 0

    def suspend_microvm(self, **kwargs: Any) -> dict[str, Any]:
        for handler in list(self.meta.events.handlers.values()):
            handler(request=None)
        self.sent += 1
        return {}


class _HeldClient:
    """A client whose calls wait until two of them run at once."""

    def __init__(self, events: _RecordingEvents) -> None:
        self.meta = type("Meta", (), {"events": events})()
        self.both_running = threading.Barrier(2, timeout=5)
        self.release = threading.Event()

    def suspend_microvm(self, **kwargs: Any) -> dict[str, Any]:
        self.both_running.wait()
        self.release.wait(5)
        return {}


def test_overlapping_calls_on_one_client_each_keep_their_handler() -> None:
    # botocore ignores a second registration with an ID it already has. So
    # each call needs its own ID, or the first call to end would remove the
    # other call's deadline.
    events = _RecordingEvents()
    client: Any = _HeldClient(events)
    threads = [
        threading.Thread(target=suspend_microvm, args=(client, "mvm-1", 5.0))
        for _ in range(2)
    ]
    for thread in threads:
        thread.start()
    assert eventually_true(
        lambda: client.both_running.n_waiting == 0 and len(events.ids) == 2
    )

    assert len(set(events.ids)) == 2
    assert len(events.handlers) == 2
    client.release.set()
    for thread in threads:
        thread.join(5)
    assert events.handlers == {}


def eventually_true(condition: Any) -> bool:
    deadline = time.monotonic() + 5
    while not condition():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.005)
    return True
