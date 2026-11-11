# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Test doubles for the Lambda MicroVMs and the bedrock-agentcore clients."""

from __future__ import annotations

import json
import socket
import threading
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, ClassVar

import pytest
from botocore.exceptions import ClientError


def client_error(code: str, status: int = 400) -> ClientError:
    """A botocore ClientError with a code and an HTTP status."""
    response: Any = {
        "Error": {"Code": code, "Message": f"{code} message"},
        "ResponseMetadata": {"HTTPStatusCode": status},
    }
    return ClientError(response, "RunMicrovm")


Answer = BaseException | Callable[[dict[str, Any]], Any] | None


class FakeMicrovmsClient:
    """Records Lambda MicroVMs API calls, and answers from scripted lists.

    ``launches``, ``tokens``, ``terminates``, and ``suspends`` hold the
    answers of ``run_microvm``, ``create_microvm_auth_token``,
    ``terminate_microvm``, and ``suspend_microvm``, in order. Each answer is
    an exception to raise, a function that receives the request, or ``None``
    for success. After a list ends, every call succeeds.

    ``states`` holds the answers of ``get_microvm``: a state such as
    ``"SUSPENDED"``, or an exception to raise. After the list ends, the state
    is ``RUNNING``.

    The SDK never calls ``ResumeMicrovm``. So ``resume_microvm`` records the
    call and raises ``AssertionError``, and the autouse fixture
    ``no_resume_microvm`` fails any test whose client recorded such a call.

    ``run_microvm`` and ``get_microvm`` return ``endpoint``. The Nth auth
    token is ``{"x-test-token": "token-N"}``.
    """

    instances: ClassVar[list[FakeMicrovmsClient]] = []
    """Every client that a test created, for :func:`no_resume_microvm`."""

    def __init__(
        self,
        *,
        launches: list[Answer] | None = None,
        tokens: list[Answer] | None = None,
        terminates: list[Answer] | None = None,
        suspends: list[Answer] | None = None,
        states: list[str | BaseException] | None = None,
        endpoint: str | None = "mvm-1.microvm.example",
    ) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.endpoint = endpoint
        FakeMicrovmsClient.instances.append(self)
        self._answers: dict[str, list[Answer]] = {
            "run_microvm": list(launches or []),
            "create_microvm_auth_token": list(tokens or []),
            "terminate_microvm": list(terminates or []),
            "suspend_microvm": list(suspends or []),
        }
        self._states = list(states or [])
        self._lock = threading.Lock()

    def requests(self, name: str) -> list[dict[str, Any]]:
        with self._lock:
            return [request for call, request in self.calls if call == name]

    def _answer(self, name: str, request: dict[str, Any]) -> Any:
        with self._lock:
            self.calls.append((name, request))
            answers = self._answers[name]
            answer = answers.pop(0) if answers else None
        if isinstance(answer, BaseException):
            raise answer
        if callable(answer):
            return answer(request)
        return None

    def run_microvm(self, **request: Any) -> dict[str, Any]:
        self._answer("run_microvm", request)
        response: dict[str, Any] = {"microvmId": "mvm-1", "state": "PENDING"}
        if self.endpoint is not None:
            response["endpoint"] = self.endpoint
        return response

    def create_microvm_auth_token(self, **request: Any) -> dict[str, Any]:
        self._answer("create_microvm_auth_token", request)
        count = len(self.requests("create_microvm_auth_token"))
        return {"authToken": {"x-test-token": f"token-{count}"}}

    def terminate_microvm(self, **request: Any) -> dict[str, Any]:
        self._answer("terminate_microvm", request)
        return {}

    def suspend_microvm(self, **request: Any) -> dict[str, Any]:
        self._answer("suspend_microvm", request)
        return {}

    def resume_microvm(self, **request: Any) -> dict[str, Any]:
        with self._lock:
            self.calls.append(("resume_microvm", request))
        msg = "the SDK must never call ResumeMicrovm"
        raise AssertionError(msg)

    def get_microvm(self, **request: Any) -> dict[str, Any]:
        with self._lock:
            self.calls.append(("get_microvm", request))
            if self._states:
                state = self._states.pop(0)
            else:
                state = "RUNNING"
        if isinstance(state, BaseException):
            raise state
        response: dict[str, Any] = {"microvmId": "mvm-1", "state": state}
        if self.endpoint is not None:
            response["endpoint"] = self.endpoint
        return response


class FakeStream:
    """The streaming body of an ``InvokeAgentRuntime`` answer."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self.closed = False

    def read(self, amt: int | None = None) -> bytes:
        data, self._data = (
            (self._data, b"") if amt is None else (self._data[:amt], self._data[amt:])
        )
        return data

    def close(self) -> None:
        self.closed = True


class FakeAgentcoreClient:
    """Records ``bedrock-agentcore`` API calls, and answers from scripted lists.

    ``invokes`` and ``stops`` hold the answers of ``invoke_agent_runtime``
    and ``stop_runtime_session``, in order. Each answer is an exception to
    raise, a function that receives the request and returns the answer body,
    bytes for the answer body, or ``None`` for ``{"accepted":true}``. After a
    list ends, every call succeeds.
    """

    def __init__(
        self,
        *,
        invokes: list[Any] | None = None,
        stops: list[Any] | None = None,
    ) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.streams: list[FakeStream] = []
        self._answers: dict[str, list[Any]] = {
            "invoke_agent_runtime": list(invokes or []),
            "stop_runtime_session": list(stops or []),
        }
        self._lock = threading.Lock()

    def requests(self, name: str) -> list[dict[str, Any]]:
        with self._lock:
            return [request for call, request in self.calls if call == name]

    def _answer(self, name: str, request: dict[str, Any]) -> Any:
        with self._lock:
            self.calls.append((name, request))
            answers = self._answers[name]
            answer = answers.pop(0) if answers else None
        if isinstance(answer, BaseException):
            raise answer
        if callable(answer):
            return answer(request)
        return answer

    def invoke_agent_runtime(self, **request: Any) -> dict[str, Any]:
        body = self._answer("invoke_agent_runtime", request)
        stream = FakeStream(b'{"accepted":true}' if body is None else body)
        with self._lock:
            self.streams.append(stream)
        return {"response": stream, "statusCode": 200}

    def stop_runtime_session(self, **request: Any) -> dict[str, Any]:
        self._answer("stop_runtime_session", request)
        return {"statusCode": 200}


@pytest.fixture
def fake_agentcore_client() -> type[FakeAgentcoreClient]:
    return FakeAgentcoreClient


# The fakes are fixtures, not imports. Several packages in this repository
# have a top-level ``tests`` package, so ``from tests.conftest import ...``
# could resolve to another package's tests.


@pytest.fixture(autouse=True)
def aws_region(monkeypatch: pytest.MonkeyPatch) -> None:
    # The local runner's execution ARN is not an ARN. So the operation takes
    # the Region from AWS_REGION, as it does outside Lambda.
    monkeypatch.setenv("AWS_REGION", "us-west-2")


@pytest.fixture(autouse=True)
def no_resume_microvm() -> Iterator[None]:
    """Fail a test whose fake client received a ``resume_microvm`` call.

    A call raises ``AssertionError`` in the fake. Code that catches every
    error, such as a worker thread, could hide that error. So the fixture
    also checks the recorded calls after the test.
    """
    FakeMicrovmsClient.instances.clear()
    yield
    resumed = [
        client
        for client in FakeMicrovmsClient.instances
        if client.requests("resume_microvm")
    ]
    FakeMicrovmsClient.instances.clear()
    assert not resumed, "the SDK called ResumeMicrovm"


@pytest.fixture
def fake_client() -> type[FakeMicrovmsClient]:
    return FakeMicrovmsClient


@pytest.fixture
def make_error() -> Callable[..., ClientError]:
    return client_error


@pytest.fixture
def connect_failed() -> Status:
    """The answer of the endpoint of a suspended MicroVM."""
    return CONNECT_FAILED


Status = int | tuple[int, dict[str, str]] | None

CONNECT_FAILED: Status = (502, {"x-aws-proxy-error": "MICROVM_CONNECT_FAILED"})
"""The answer of the endpoint of a suspending or suspended MicroVM."""


class FakeEndpoint:
    """A local HTTP server that stands in for a MicroVM endpoint.

    It records each request, and answers from ``statuses`` in order. After
    the list ends, it answers 202. A status of ``None`` closes the connection
    without an answer. A ``(status, headers)`` pair adds the headers to the
    answer. The 202 after the list ends gets ``accept_headers``. A redirect
    status gets ``Location: <location>``, ``/moved`` by default. A
    ``location`` of ``None`` sends no ``Location``.
    """

    def __init__(
        self,
        statuses: list[Status] | None = None,
        location: str | None = "/moved",
        accept_headers: dict[str, str] | None = None,
    ) -> None:
        self.requests: list[dict[str, Any]] = []
        self._statuses = list(statuses or [])
        self._lock = threading.Lock()
        endpoint = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 - the http.server name
                # A followed 301, 302, or 303 arrives as a GET.
                with endpoint._lock:
                    endpoint.requests.append(
                        {
                            "method": "GET",
                            "path": self.path,
                            "headers": {k.lower(): v for k, v in self.headers.items()},
                            "body": None,
                        }
                    )
                self.send_response(200)
                self.send_header("content-length", "0")
                self.end_headers()

            def do_POST(self) -> None:  # noqa: N802 - the http.server name
                length = int(self.headers.get("content-length", "0"))
                body = self.rfile.read(length)
                with endpoint._lock:
                    endpoint.requests.append(
                        {
                            "path": self.path,
                            "headers": {k.lower(): v for k, v in self.headers.items()},
                            "body": json.loads(body),
                        }
                    )
                    answer = (
                        endpoint._statuses.pop(0)
                        if endpoint._statuses
                        else (202, dict(accept_headers or {}))
                    )
                if answer is None:
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    return
                status, headers = answer if isinstance(answer, tuple) else (answer, {})
                self.send_response(status)
                for name, value in headers.items():
                    self.send_header(name, value)
                if 300 <= status < 400 and location is not None:  # noqa: PLR2004
                    self.send_header("location", location)
                self.send_header("content-length", "0")
                self.end_headers()

            def log_message(self, *args: Any) -> None:
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_address[1]}"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


@pytest.fixture
def fake_endpoint() -> Iterator[Callable[..., FakeEndpoint]]:
    endpoints: list[FakeEndpoint] = []

    def _create(
        statuses: list[Status] | None = None,
        location: str | None = "/moved",
        accept_headers: dict[str, str] | None = None,
    ) -> FakeEndpoint:
        endpoint = FakeEndpoint(statuses, location, accept_headers)
        endpoints.append(endpoint)
        return endpoint

    yield _create
    for endpoint in endpoints:
        endpoint.close()
