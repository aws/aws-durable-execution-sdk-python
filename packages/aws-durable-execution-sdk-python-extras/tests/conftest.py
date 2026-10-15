# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Test doubles for the Lambda MicroVMs client."""

from __future__ import annotations

import json
import socket
import threading
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

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

    ``launches``, ``tokens``, and ``terminates`` hold the answers of
    ``run_microvm``, ``create_microvm_auth_token``, and ``terminate_microvm``,
    in order. Each answer is an exception to raise, a function that receives
    the request, or ``None`` for success. After a list ends, every call
    succeeds.

    ``run_microvm`` returns ``endpoint``. The Nth auth token is
    ``{"x-test-token": "token-N"}``.
    """

    def __init__(
        self,
        *,
        launches: list[Answer] | None = None,
        tokens: list[Answer] | None = None,
        terminates: list[Answer] | None = None,
        endpoint: str | None = "mvm-1.microvm.example",
    ) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.endpoint = endpoint
        self._answers: dict[str, list[Answer]] = {
            "run_microvm": list(launches or []),
            "create_microvm_auth_token": list(tokens or []),
            "terminate_microvm": list(terminates or []),
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


# The fakes are fixtures, not imports. Several packages in this repository
# have a top-level ``tests`` package, so ``from tests.conftest import ...``
# could resolve to another package's tests.


@pytest.fixture(autouse=True)
def aws_region(monkeypatch: pytest.MonkeyPatch) -> None:
    # The local runner's execution ARN is not an ARN. So the operation takes
    # the Region from AWS_REGION, as it does outside Lambda.
    monkeypatch.setenv("AWS_REGION", "us-west-2")


@pytest.fixture
def fake_client() -> type[FakeMicrovmsClient]:
    return FakeMicrovmsClient


@pytest.fixture
def make_error() -> Callable[..., ClientError]:
    return client_error


class FakeEndpoint:
    """A local HTTP server that stands in for a MicroVM endpoint.

    It records each request, and answers from ``statuses`` in order. After
    the list ends, it answers 202. A status of ``None`` closes the connection
    without an answer. A redirect status gets ``Location: <location>``,
    ``/moved`` by default.
    """

    def __init__(
        self, statuses: list[int | None] | None = None, location: str = "/moved"
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
                    status = endpoint._statuses.pop(0) if endpoint._statuses else 202
                if status is None:
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    return
                self.send_response(status)
                if 300 <= status < 400:  # noqa: PLR2004
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
        statuses: list[int | None] | None = None, location: str = "/moved"
    ) -> FakeEndpoint:
        endpoint = FakeEndpoint(statuses, location)
        endpoints.append(endpoint)
        return endpoint

    yield _create
    for endpoint in endpoints:
        endpoint.close()
