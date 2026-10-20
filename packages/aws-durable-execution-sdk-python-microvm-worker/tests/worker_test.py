# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the HTTP listener and the worker server."""

from __future__ import annotations

import contextlib
import http.client
import inspect
import io
import json
import socket
import struct
import threading
import time
from collections.abc import Callable, Generator, Iterator
from http.server import ThreadingHTTPServer
from typing import Any

import pytest

from aws_durable_execution_sdk_python_microvm_worker import (
    HOOK_PATH_PREFIX,
    MICROVM_JOB_PATH,
    MicrovmJobContext,
    MicrovmWorker,
    MicrovmWorkerListener,
    start_microvm_worker,
)
from aws_durable_execution_sdk_python_microvm_worker import worker as worker_module


RUN = f"{HOOK_PATH_PREFIX}run"
WAIT = 10.0


def run_payload(*, with_job: bool = True, **job: Any) -> dict[str, Any]:
    """A run hook payload with one job, whose fields ``job`` replaces."""
    payload: dict[str, Any] = {"version": 1, "region": "us-east-1"}
    if with_job:
        payload["job"] = {"callbackId": "cb-1", "input": {"repo": "org/app"}, **job}
    return payload


def run_body(payload: dict[str, Any] | None, microvm_id: str = "mvm-1") -> Any:
    body: dict[str, Any] = {"microvmId": microvm_id}
    if payload is not None:
        body["runHookPayload"] = json.dumps(payload)
    return body


def job_body(**fields: Any) -> dict[str, Any]:
    return {
        "version": 1,
        "region": "us-east-1",
        "callbackId": "cb-1",
        "input": {"repo": "org/app"},
        **fields,
    }


def request(
    worker: MicrovmWorker,
    path: str,
    body: Any = None,
    *,
    method: str = "POST",
    raw: Any = None,
    headers: dict[str, str] | None = None,
    chunked: bool = False,
) -> tuple[int, Any]:
    """Send one request, and return the status and the parsed answer."""
    connection = http.client.HTTPConnection("127.0.0.1", worker.port, timeout=WAIT)
    try:
        data = raw if raw is not None else json.dumps(body).encode()
        connection.request(
            method,
            path,
            body=data,
            headers={"Content-Type": "application/json", **(headers or {})},
            encode_chunked=chunked,
        )
        response = connection.getresponse()
        text = response.read()
    finally:
        connection.close()
    return response.status, json.loads(text) if text else None


def raw_request(worker: MicrovmWorker, data: bytes) -> bytes:
    """Send bytes, close the sending side, and return the raw answer."""
    with socket.create_connection(("127.0.0.1", worker.port), timeout=WAIT) as sock:
        sock.sendall(data)
        sock.shutdown(socket.SHUT_WR)
        chunks = []
        while chunk := sock.recv(65536):
            chunks.append(chunk)
    return b"".join(chunks)


def eventually(condition: Callable[[], bool]) -> bool:
    """Wait until ``condition`` holds.

    The worker writes some log lines after it has sent the answer. So a test
    that has the answer can still be ahead of the line.
    """
    deadline = time.monotonic() + WAIT
    while not condition():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.005)
    return True


def completions(client: Any) -> list[tuple[str, dict[str, Any]]]:
    return [(name, kwargs) for name, kwargs in client.calls if name != "heartbeat"]


class Held:
    """A handler that runs until the test releases it."""

    def __init__(self, result: Any = "ok") -> None:
        self.started = threading.Event()
        self.release = threading.Event()
        self.contexts: list[MicrovmJobContext] = []
        self.inputs: list[Any] = []
        self.result = result

    def __call__(self, job_input: Any, context: MicrovmJobContext) -> Any:
        self.inputs.append(job_input)
        self.contexts.append(context)
        self.started.set()
        assert self.release.wait(WAIT)
        return self.result


@pytest.fixture
def start(
    fake_client: Any, logger: Any
) -> Iterator[Callable[..., tuple[MicrovmWorker, Any]]]:
    """Start a worker on a free port with a fake client, and close it after."""
    workers: list[MicrovmWorker] = []

    def _start(
        handler: Any = None, *, client: Any = None, **options: Any
    ) -> tuple[MicrovmWorker, Any]:
        client = client if client is not None else fake_client()
        options.setdefault("create_client", lambda region: client)
        options.setdefault("logger", logger)
        worker = start_microvm_worker(handler=handler, port=0, **options)
        workers.append(worker)
        return worker, client

    yield _start
    for worker in workers:
        worker.idle(WAIT)
        worker.close()


def test_answers_the_run_hook_before_the_job_ends_then_reports_the_result(
    start: Any,
) -> None:
    handler = Held({"passed": True})
    worker, client = start(handler)

    assert request(worker, RUN, run_body(run_payload())) == (200, {})
    assert handler.started.wait(WAIT)
    assert completions(client) == []
    handler.release.set()
    assert worker.idle(WAIT)

    assert handler.inputs == [{"repo": "org/app"}]
    context = handler.contexts[0]
    assert (context.callback_id, context.microvm_id, context.region) == (
        "cb-1",
        "mvm-1",
        "us-east-1",
    )
    assert completions(client) == [
        ("success", {"CallbackId": "cb-1", "Result": b'{"passed":true}'})
    ]
    assert not client.closed


def test_sends_heartbeats_while_the_job_runs_and_none_after(start: Any) -> None:
    handler = Held()
    worker, client = start(handler, heartbeat_interval_seconds=0.02)

    request(worker, RUN, run_body(run_payload(heartbeatTimeoutSeconds=30)))
    assert handler.started.wait(WAIT)
    deadline = time.monotonic() + WAIT
    while client.names().count("heartbeat") < 3 and time.monotonic() < deadline:
        time.sleep(0.01)
    handler.release.set()
    assert worker.idle(WAIT)
    beats = client.names().count("heartbeat")
    time.sleep(0.1)

    assert beats >= 3
    assert client.names().count("heartbeat") == beats
    # Heartbeats continue while the outcome is reported. So a heartbeat can
    # be recorded after the success call, and only the completions are
    # compared.
    assert [name for name, _ in completions(client)] == ["success"]


def test_sends_no_heartbeats_without_a_heartbeat_timeout(start: Any) -> None:
    worker, client = start(lambda job_input, context: "ok")

    request(worker, RUN, run_body(run_payload()))
    assert worker.idle(WAIT)

    assert client.names() == ["success"]


def test_sets_cancelled_and_reports_nothing_when_the_callback_is_gone(
    start: Any, fake_client: Any, make_error: Any, logger: Any
) -> None:
    client = fake_client(make_error("CallbackTimeoutException"))
    seen: list[bool] = []

    def handler(job_input: Any, context: MicrovmJobContext) -> str:
        seen.append(context.cancelled.wait(WAIT))
        return "late"

    worker, _ = start(handler, client=client, heartbeat_interval_seconds=0.01)

    request(worker, RUN, run_body(run_payload(heartbeatTimeoutSeconds=30)))
    assert worker.idle(WAIT)

    assert seen == [True]
    assert completions(client) == []
    assert "job outcome not reported: the callback is gone" in logger.messages(
        "warning"
    )


def test_reports_the_handler_error_as_a_failure(start: Any) -> None:
    def handler(job_input: Any, context: MicrovmJobContext) -> None:
        msg = "3 tests failed"
        raise RuntimeError(msg)

    worker, client = start(handler)

    request(worker, RUN, run_body(run_payload()))
    assert worker.idle(WAIT)

    assert completions(client) == [
        (
            "failure",
            {
                "CallbackId": "cb-1",
                "Error": {
                    "ErrorType": "RuntimeError",
                    "ErrorMessage": "3 tests failed",
                },
            },
        )
    ]


def test_reports_system_exit_from_the_handler_as_a_failure(start: Any) -> None:
    def handler(job_input: Any, context: MicrovmJobContext) -> None:
        raise SystemExit(3)

    worker, client = start(handler)

    request(worker, RUN, run_body(run_payload()))
    assert worker.idle(WAIT)

    assert completions(client)[0][1]["Error"]["ErrorType"] == "SystemExit"


@pytest.mark.parametrize(
    ("result", "error_type"),
    [
        ({1, 2}, "ResultSerializationError"),
        ("x" * (256 * 1024), "ResultTooLargeError"),
    ],
    # The default test ID would hold the 256 KB result.
    ids=["not-json", "over-256-kb"],
)
def test_reports_a_result_that_cannot_be_sent_as_a_failure(
    start: Any, logger: Any, result: Any, error_type: str
) -> None:
    worker, client = start(lambda job_input, context: result)

    request(worker, RUN, run_body(run_payload()))
    assert worker.idle(WAIT)

    [(name, kwargs)] = completions(client)
    assert name == "failure"
    assert kwargs["Error"]["ErrorType"] == error_type
    assert "job failed: its result cannot be sent" in logger.messages("info")


def test_logs_an_outcome_that_cannot_be_reported(
    start: Any, fake_client: Any, make_error: Any, logger: Any
) -> None:
    client = fake_client(make_error("AccessDeniedException", 403))
    worker, _ = start(lambda job_input, context: "ok", client=client)

    request(worker, RUN, run_body(run_payload()))
    assert worker.idle(WAIT)

    [line] = [line for line in logger.lines if line[0] == "error"]
    assert line[1] == "job outcome could not be reported"
    assert line[2]["terminal"] is False
    assert line[2]["callbackId"] == "cb-1"


def test_logs_a_job_whose_client_cannot_be_created_and_runs_later_jobs(
    start: Any, fake_client: Any, logger: Any
) -> None:
    client = fake_client()
    attempts: list[str] = []

    def create_client(region: str) -> Any:
        attempts.append(region)
        if len(attempts) == 1:
            msg = "no client"
            raise RuntimeError(msg)
        return client

    handler = Held()
    handler.release.set()
    worker, _ = start(handler, routes={"/build": handler}, create_client=create_client)

    request(worker, RUN, run_body(run_payload()))
    assert worker.idle(WAIT)
    assert request(worker, "/build", job_body(callbackId="cb-2"))[0] == 202
    assert worker.idle(WAIT)

    assert "could not create the Lambda client for the job" in logger.messages("error")
    assert [kwargs["CallbackId"] for _, kwargs in completions(client)] == ["cb-2"]


def test_reports_the_outcome_when_the_logger_raises(start: Any) -> None:
    class RaisingLogger:
        def info(self, *args: Any, **kwargs: Any) -> None:
            raise RuntimeError

        warning = error = info

    worker, client = start(lambda job_input, context: "ok", logger=RaisingLogger())

    request(worker, RUN, run_body(run_payload()))
    assert worker.idle(WAIT)

    assert client.names() == ["success"]


def test_logs_an_unexpected_job_error(
    start: Any, logger: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    worker, _ = start(lambda job_input, context: "ok")

    def broken(*args: Any) -> None:
        msg = "broken"
        raise RuntimeError(msg)

    monkeypatch.setattr(MicrovmWorkerListener, "_run_job_and_report", broken)
    request(worker, RUN, run_body(run_payload()))
    assert worker.idle(WAIT)

    assert "job failed inside the worker" in logger.messages("error")


def test_ignores_every_run_hook_after_the_first(start: Any, logger: Any) -> None:
    handler = Held()
    handler.release.set()
    worker, client = start(handler)

    request(worker, RUN, run_body(run_payload()))
    assert worker.idle(WAIT)
    later = run_body(run_payload(callbackId="cb-2"), microvm_id="mvm-2")
    assert request(worker, RUN, later) == (200, {})
    assert worker.idle(WAIT)

    assert [kwargs["CallbackId"] for _, kwargs in completions(client)] == ["cb-1"]
    assert eventually(lambda: "later run hook ignored" in logger.messages("warning"))


def test_answers_400_to_an_invalid_payload_and_fails_the_named_callback(
    start: Any, logger: Any
) -> None:
    handler = Held()
    worker, client = start(handler)

    status, body = request(worker, RUN, run_body({**run_payload(), "version": 2}))
    assert worker.idle(WAIT)

    assert status == 400
    assert "version 2 is not supported" in body["error"]
    [(name, kwargs)] = completions(client)
    assert name == "failure"
    assert kwargs["CallbackId"] == "cb-1"
    assert kwargs["Error"]["ErrorType"] == "InvalidRunHookPayloadError"
    assert not handler.started.is_set()


def test_answers_400_to_an_empty_microvm_id_and_fails_the_job(start: Any) -> None:
    worker, client = start(Held())

    status, _ = request(worker, RUN, run_body(run_payload(), microvm_id=""))
    assert worker.idle(WAIT)

    assert status == 400
    assert [name for name, _ in completions(client)] == ["failure"]


def test_fails_a_callback_with_the_environment_region_when_the_request_has_none(
    start: Any, fake_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = fake_client()
    regions: list[str] = []

    def create_client(region: str) -> Any:
        regions.append(region)
        return client

    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    worker, _ = start(routes={"/build": Held()}, create_client=create_client)

    body = job_body()
    del body["region"]
    assert request(worker, "/build", body)[0] == 400
    assert worker.idle(WAIT)

    assert regions == ["eu-west-1"]
    assert [name for name, _ in completions(client)] == ["failure"]


def test_fails_no_callback_without_any_region(
    start: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("AWS_REGION", raising=False)
    worker, client = start(routes={"/build": Held()})

    body = job_body()
    del body["region"]
    assert request(worker, "/build", body)[0] == 400
    assert worker.idle(WAIT)

    assert client.calls == []


def test_logs_a_failure_report_that_the_service_rejects(
    start: Any, fake_client: Any, make_error: Any, logger: Any
) -> None:
    client = fake_client(make_error("AccessDeniedException", 403))
    worker, _ = start(routes={"/build": Held()}, client=client)

    request(worker, "/build", job_body(version=2))
    assert worker.idle(WAIT)

    assert "could not fail the callback" in logger.messages("error")


def test_logs_a_failure_report_for_a_closed_callback_as_info(
    start: Any, fake_client: Any, make_error: Any, logger: Any
) -> None:
    client = fake_client(make_error("CallbackTimeoutException"))
    worker, _ = start(routes={"/build": Held()}, client=client)

    request(worker, "/build", job_body(version=2))
    assert worker.idle(WAIT)

    assert "the callback no longer accepts a failure" in logger.messages("info")


def test_logs_a_failure_report_whose_client_cannot_be_closed(
    start: Any, logger: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    worker, _ = start(routes={"/build": Held()})

    def broken_close(self: Any) -> None:
        raise RuntimeError

    monkeypatch.setattr(worker_module.CallbackReporter, "close", broken_close)
    request(worker, "/build", job_body(version=2))
    assert worker.idle(WAIT)

    assert "could not close the Lambda client" in logger.messages("error")


def test_creates_a_default_client_per_job_and_closes_it(
    start: Any, fake_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    created: list[Any] = []

    def create_client(region: str) -> Any:
        client = fake_client()
        created.append(client)
        return client

    monkeypatch.setattr(
        "aws_durable_execution_sdk_python_microvm_worker.callback_reporter._create_client",
        create_client,
    )
    worker = start_microvm_worker(handler=lambda job_input, context: "ok", port=0)
    try:
        request(worker, RUN, run_body(run_payload()))
        assert worker.idle(WAIT)
    finally:
        worker.close()

    assert len(created) == 1
    assert created[0].closed
    assert created[0].names() == ["success"]


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        (b"{not json", "request body is not valid JSON"),
        (b"NaN", "request body is not valid JSON"),
        (b"", "request body is not valid JSON"),
    ],
)
def test_answers_400_to_a_body_that_is_not_json(
    start: Any, raw: bytes, message: str
) -> None:
    worker, client = start(Held())

    assert request(worker, RUN, raw=raw) == (400, {"error": message})
    assert worker.idle(WAIT)
    assert client.calls == []


def test_answers_400_to_a_body_without_a_content_length(start: Any) -> None:
    worker, _ = start(Held())

    answer = raw_request(
        worker, f"POST {RUN} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode()
    )

    assert answer.startswith(b"HTTP/1.1 400")
    assert b"request body is not valid JSON" in answer


def test_answers_400_to_an_invalid_content_length(start: Any) -> None:
    worker, _ = start(Held())

    answer = raw_request(
        worker,
        f"POST {RUN} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1_0\r\n\r\n".encode(),
    )

    assert answer.startswith(b"HTTP/1.1 400")
    assert b"Content-Length is not valid" in answer


def test_answers_400_with_a_generic_message_when_the_body_ends_early(
    start: Any, logger: Any
) -> None:
    worker, _ = start(Held())

    answer = raw_request(
        worker,
        f"POST {RUN} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 50\r\n\r\n{{".encode(),
    )

    assert answer.startswith(b"HTTP/1.1 400")
    assert answer.endswith(b'{"error":"invalid request"}')
    assert "could not read the request" in logger.messages("error")


def test_answers_400_to_a_body_over_the_limit_without_reading_it(start: Any) -> None:
    worker, _ = start(Held())
    size = worker_module.MAX_HOOK_BODY_BYTES + 1

    answer = raw_request(
        worker,
        f"POST {RUN} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {size}\r\n\r\n".encode(),
    )

    assert answer.startswith(b"HTTP/1.1 400")
    assert b"request body exceeds 65536 bytes" in answer


def test_answers_400_to_a_content_length_with_too_many_digits(start: Any) -> None:
    worker, _ = start(Held())

    for expect in ("", "Expect: 100-continue\r\n"):
        answer = raw_request(
            worker,
            (
                f"POST {RUN} HTTP/1.1\r\nHost: localhost\r\n{expect}"
                f"Content-Length: {'9' * 5000}\r\n\r\n"
            ).encode(),
        )

        # With Expect, the length is not known to be over the limit. So the
        # worker sends 100 Continue, and then the 400 for the invalid header.
        assert b"HTTP/1.1 400" in answer, expect
        assert answer.endswith(b'{"error":"request Content-Length is not valid"}')


def test_answers_expect_100_continue_before_it_reads_the_body(start: Any) -> None:
    worker, client = start(routes={"/build": lambda job_input, context: "ok"})
    data = json.dumps(job_body()).encode()
    head = (
        "POST /build HTTP/1.1\r\nHost: localhost\r\nExpect: 100-continue\r\n"
        f"Content-Length: {len(data)}\r\n\r\n"
    )

    with socket.create_connection(("127.0.0.1", worker.port), timeout=WAIT) as sock:
        sock.sendall(head.encode())
        interim = sock.recv(65536)
        sock.sendall(data)
        answer = b""
        while chunk := sock.recv(65536):
            answer += chunk
    assert worker.idle(WAIT)

    assert interim.startswith(b"HTTP/1.1 100 Continue")
    assert answer.startswith(b"HTTP/1.1 202")
    assert b"Connection: close" in answer
    assert client.names() == ["success"]


def test_answers_400_to_a_body_over_the_limit_that_the_client_sends(
    start: Any,
) -> None:
    worker, client = start(routes={"/build": Held()})
    data = b"x" * (worker_module.MAX_JOB_BODY_BYTES + 1)

    status, body = request(worker, "/build", raw=data)

    assert (status, body) == (400, {"error": "request body exceeds 6291456 bytes"})
    assert client.calls == []


def test_answers_400_to_a_chunked_body_over_the_limit_that_the_client_sends(
    start: Any,
) -> None:
    worker, _ = start(Held())
    data = b"x" * (worker_module.MAX_HOOK_BODY_BYTES + 1)

    status, body = request(
        worker, RUN, raw=iter([data[:1000], data[1000:]]), chunked=True
    )

    assert (status, body) == (400, {"error": "request body exceeds 65536 bytes"})


@pytest.mark.parametrize(
    ("path", "limit"),
    [
        ("/build", worker_module.MAX_JOB_BODY_BYTES),
        (RUN, worker_module.MAX_HOOK_BODY_BYTES),
    ],
)
def test_answers_400_instead_of_100_continue_to_a_body_over_the_limit(
    start: Any, logger: Any, path: str, limit: int
) -> None:
    worker, _ = start(Held(), routes={"/build": Held()})
    head = (
        f"POST {path} HTTP/1.1\r\nHost: localhost\r\nExpect: 100-continue\r\n"
        f"Content-Length: {limit + 1}\r\n\r\n"
    )

    with socket.create_connection(("127.0.0.1", worker.port), timeout=WAIT) as sock:
        sock.sendall(head.encode())
        answer = b""
        while chunk := sock.recv(65536):
            answer += chunk

    assert answer.startswith(b"HTTP/1.1 400")
    assert b"100 Continue" not in answer
    assert answer.endswith(f'{{"error":"request body exceeds {limit} bytes"}}'.encode())
    assert "invalid request" in logger.messages("error")


@pytest.mark.parametrize(
    ("method", "path", "framing"),
    [
        ("POST", "/other", "Content-Length: 99999999"),
        ("POST", f"{HOOK_PATH_PREFIX}ready", "Content-Length: 99999999"),
        ("GET", "/build", "Content-Length: 99999999"),
        ("POST", "/build", "Transfer-Encoding: chunked"),
    ],
)
def test_answers_100_continue_when_the_declared_size_is_not_over_a_limit(
    start: Any, method: str, path: str, framing: str
) -> None:
    worker, _ = start(Held(), routes={"/build": Held()})
    head = (
        f"{method} {path} HTTP/1.1\r\nHost: localhost\r\nExpect: 100-continue\r\n"
        f"{framing}\r\n\r\n"
    )

    with socket.create_connection(("127.0.0.1", worker.port), timeout=WAIT) as sock:
        sock.sendall(head.encode())
        interim = sock.recv(65536)

    assert interim.startswith(b"HTTP/1.1 100 Continue")


def _send_after_the_answer(
    worker: MicrovmWorker, seconds: float
) -> tuple[bytes, BaseException | None]:
    """Get a 400 for an oversized body, then keep sending for ``seconds``.

    The worker shuts down its side right after the answer. So the client
    reads the answer and end of file at once. The worker keeps reading and
    dropping input until its time limit, and then closes the socket. So a
    write after the limit fails.

    Returns:
        The answer, and the error of the first failed write, or ``None``.
    """
    head = f"POST {RUN} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 999999\r\n\r\n"
    with socket.create_connection(("127.0.0.1", worker.port), timeout=WAIT) as sock:
        sock.sendall(head.encode() + b"x" * 100)
        answer = b""
        while chunk := sock.recv(65536):
            answer += chunk
        deadline = time.monotonic() + seconds
        try:
            while time.monotonic() < deadline:
                sock.sendall(b"x" * 4096)
                time.sleep(0.01)
        except ConnectionError as error:
            return answer, error
    return answer, None


def test_stops_discarding_a_rejected_body_after_the_time_limit(
    start: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(worker_module, "_DISCARD_SECONDS", 0.2)
    worker, _ = start(Held())

    answer, error = _send_after_the_answer(worker, 1.5)

    assert answer.startswith(b"HTTP/1.1 400")
    assert isinstance(error, ConnectionError)


def test_keeps_discarding_a_rejected_body_until_the_time_limit(start: Any) -> None:
    # The default limit is 10 seconds. So writes during the first second must
    # succeed. A limit that ended too early would fail them.
    worker, _ = start(Held())

    answer, error = _send_after_the_answer(worker, 1.0)

    assert answer.startswith(b"HTTP/1.1 400")
    assert error is None


class _EndlessConnection:
    """A socket whose peer never stops sending."""

    def __init__(self) -> None:
        self.received = 0
        self.shut = False

    def shutdown(self, how: int) -> None:
        self.shut = True

    def settimeout(self, timeout: float) -> None: ...

    def recv(self, size: int) -> bytes:
        self.received += size
        return b"x" * size


def _rejected_exchange(connection: _EndlessConnection, logger: Any) -> Any:
    request = type("Request", (), {"connection": connection})()
    exchange = worker_module._Exchange(request, logger)  # noqa: SLF001
    exchange.too_large(1)
    return exchange


def test_stops_discarding_a_rejected_body_after_the_byte_limit(
    logger: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(worker_module, "_MAX_DISCARD_BYTES", 200_000)
    connection = _EndlessConnection()

    _rejected_exchange(connection, logger).discard_unread_body()

    assert connection.shut
    assert 200_000 <= connection.received < 200_000 + 65536


def test_stops_discarding_a_rejected_body_when_the_client_is_gone(
    logger: Any,
) -> None:
    class GoneConnection(_EndlessConnection):
        def shutdown(self, how: int) -> None:
            raise ConnectionResetError

    connection = GoneConnection()

    _rejected_exchange(connection, logger).discard_unread_body()

    assert connection.received == 0


def test_stops_discarding_a_rejected_body_when_the_time_is_up(
    logger: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(worker_module, "_DISCARD_SECONDS", 0)
    connection = _EndlessConnection()

    _rejected_exchange(connection, logger).discard_unread_body()

    assert connection.received == 0


def test_accepts_a_chunked_body(start: Any) -> None:
    worker, client = start(lambda job_input, context: job_input)
    data = json.dumps(run_body(run_payload())).encode()

    status, _ = request(worker, RUN, raw=iter([data[:10], data[10:]]), chunked=True)
    assert worker.idle(WAIT)

    assert status == 200
    assert completions(client)[0][1]["Result"] == b'{"repo":"org/app"}'


def test_accepts_a_chunked_body_with_extensions_and_trailers(start: Any) -> None:
    worker, client = start(routes={"/build": lambda job_input, context: job_input})
    data = json.dumps(job_body()).encode()
    head = (
        "POST /build HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n"
    )
    chunked = (
        f"{len(data):x};name=value\r\n".encode()
        + data
        + b"\r\n0\r\nX-Trailer: 1\r\n\r\n"
    )

    answer = raw_request(worker, head.encode() + chunked)
    assert worker.idle(WAIT)

    assert answer.startswith(b"HTTP/1.1 202")
    assert completions(client)[0][1]["Result"] == b'{"repo":"org/app"}'


_FRAMING_TOO_LARGE = b"the chunked framing of the request body exceeds 65536 bytes"


@pytest.mark.parametrize(
    ("chunks", "expected"),
    [
        (b"zz\r\n{}\r\n0\r\n\r\n", b"malformed chunked encoding"),
        (b"-2\r\n{}\r\n0\r\n\r\n", b"malformed chunked encoding"),
        (b"0x2\r\n{}\r\n0\r\n\r\n", b"malformed chunked encoding"),
        (b"+2\r\n{}\r\n0\r\n\r\n", b"malformed chunked encoding"),
        (b"0_2\r\n{}\r\n0\r\n\r\n", b"malformed chunked encoding"),
        (b"2\r\n{}XX0\r\n\r\n", b"malformed chunked encoding"),
        (b"2" * 2000, b"malformed chunked encoding"),
        (b"2\r\n{}\r\n", b'{"error":"invalid request"}'),
        (b"20000\r\n", b"request body exceeds 65536 bytes"),
        (
            b"0\r\n" + b"X: y\r\n" * 20_000 + b"\r\n",
            _FRAMING_TOO_LARGE,
        ),
        (
            b"".join(b"1;" + b"e" * 1000 + b"\r\nx\r\n" for _ in range(70)),
            _FRAMING_TOO_LARGE,
        ),
    ],
    # The default test IDs would hold the raw bytes, one of them 2,000 long.
    ids=[
        "not-hex",
        "negative",
        "hex-prefix",
        "plus-sign",
        "underscore",
        "no-crlf-after-chunk",
        "size-line-too-long",
        "ends-early",
        "over-the-limit",
        "trailers-over-the-limit",
        "extensions-over-the-limit",
    ],
)
def test_answers_400_to_a_malformed_chunked_body(
    start: Any, chunks: bytes, expected: bytes
) -> None:
    worker, _ = start(Held())
    head = (
        f"POST {RUN} HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n"
    )

    answer = raw_request(worker, head.encode() + chunks)

    assert answer.startswith(b"HTTP/1.1 400")
    assert expected in answer


def test_answers_404_outside_the_hook_paths_and_routes(start: Any) -> None:
    worker, _ = start(Held(), routes={"/build": Held()})

    assert request(worker, "/other", {}) == (404, {"error": "not found"})
    assert request(worker, "/build", method="GET", raw=b"") == (
        404,
        {"error": "not found"},
    )
    assert request(worker, RUN, method="PUT", raw=b"") == (404, {"error": "not found"})
    for method in ["PATCH", "DELETE"]:
        assert request(worker, "/build", method=method, raw=b"")[0] == 404


def test_logs_a_failure_report_whose_client_cannot_be_created(
    start: Any, logger: Any
) -> None:
    def create_client(region: str) -> Any:
        msg = "no client"
        raise RuntimeError(msg)

    worker, _ = start(routes={"/build": Held()}, create_client=create_client)

    assert request(worker, "/build", job_body(version=2))[0] == 400
    assert worker.idle(WAIT)

    assert "could not fail the callback" in logger.messages("error")


@pytest.mark.parametrize("hook", ["ready", "resume", "suspend", "terminate", "other"])
def test_answers_200_to_every_other_lifecycle_hook(
    start: Any, logger: Any, hook: str
) -> None:
    worker, client = start(Held())

    assert request(worker, f"{HOOK_PATH_PREFIX}{hook}", {}) == (200, {})
    assert client.calls == []
    assert ("info", "lifecycle hook", {"hook": hook}) in logger.lines


def test_answers_a_run_hook_without_a_payload_with_200(start: Any, logger: Any) -> None:
    worker, client = start(Held())

    assert request(worker, RUN, run_body(None)) == (200, {})
    assert worker.idle(WAIT)

    assert client.calls == []
    assert eventually(
        lambda: "run hook without runHookPayload" in logger.messages("warning")
    )


def test_answers_a_run_hook_without_a_job_with_200(start: Any, logger: Any) -> None:
    worker, client = start(Held())

    assert request(worker, RUN, run_body(run_payload(with_job=False))) == (200, {})
    assert worker.idle(WAIT)

    assert client.calls == []
    assert logger.messages("warning") == []


def test_answers_a_job_request_with_202_and_reports_the_route_result(
    start: Any,
) -> None:
    worker, client = start(routes={"/build": lambda job_input, context: "built"})

    assert request(worker, "/build?x=1", job_body()) == (202, {})
    assert worker.idle(WAIT)

    assert completions(client) == [
        ("success", {"CallbackId": "cb-1", "Result": b'"built"'})
    ]


def test_runs_handler_for_a_job_on_the_default_job_path(start: Any) -> None:
    worker, client = start(lambda job_input, context: "default")

    assert request(worker, MICROVM_JOB_PATH, job_body()) == (202, {})
    assert worker.idle(WAIT)

    assert completions(client)[0][1]["Result"] == b'"default"'


def test_answers_404_on_the_default_job_path_without_a_handler(start: Any) -> None:
    worker, _ = start(routes={"/build": Held()})

    assert request(worker, MICROVM_JOB_PATH, job_body())[0] == 404


def test_fails_a_run_hook_job_when_the_worker_has_only_routes(start: Any) -> None:
    worker, client = start(routes={"/build": Held()})

    assert request(worker, RUN, run_body(run_payload())) == (200, {})
    assert worker.idle(WAIT)

    [(name, kwargs)] = completions(client)
    assert name == "failure"
    assert kwargs["Error"]["ErrorType"] == "InvalidRunHookPayloadError"
    assert "no handler for run hook jobs" in kwargs["Error"]["ErrorMessage"]


def test_answers_400_to_an_invalid_job_request_and_fails_the_named_callback(
    start: Any,
) -> None:
    worker, client = start(routes={"/build": Held()})

    status, body = request(worker, "/build", job_body(heartbeatTimeoutSeconds=0))
    assert worker.idle(WAIT)

    assert status == 400
    assert "heartbeatTimeoutSeconds" in body["error"]
    assert [name for name, _ in completions(client)] == ["failure"]


def test_takes_the_microvm_id_from_a_job_request_before_the_run_hook(
    start: Any,
) -> None:
    handler = Held()
    handler.release.set()
    worker, _ = start(handler, routes={"/build": handler})

    request(worker, "/build", job_body(microvmId="mvm-early"))
    assert worker.idle(WAIT)
    request(worker, "/build", job_body(callbackId="cb-2", microvmId="mvm-other"))
    assert worker.idle(WAIT)
    request(worker, RUN, run_body(run_payload(callbackId="cb-3"), microvm_id="mvm-1"))
    assert worker.idle(WAIT)
    request(worker, "/build", job_body(callbackId="cb-4", microvmId="mvm-later"))
    assert worker.idle(WAIT)

    assert [context.microvm_id for context in handler.contexts] == [
        "mvm-early",
        "mvm-early",
        "mvm-1",
        "mvm-1",
    ]


def test_uses_unknown_until_the_microvm_id_arrives(start: Any) -> None:
    handler = Held()
    handler.release.set()
    worker, _ = start(routes={"/build": handler})

    request(worker, "/build", job_body())
    assert worker.idle(WAIT)

    assert handler.contexts[0].microvm_id == "unknown"


def test_runs_a_job_once_when_its_request_arrives_twice(
    start: Any, logger: Any
) -> None:
    handler = Held()
    worker, client = start(routes={"/build": handler})

    assert request(worker, "/build", job_body())[0] == 202
    assert handler.started.wait(WAIT)
    assert request(worker, "/build", job_body())[0] == 202
    handler.release.set()
    assert worker.idle(WAIT)
    assert request(worker, "/build", job_body())[0] == 202
    assert worker.idle(WAIT)

    assert len(handler.inputs) == 1
    assert len(completions(client)) == 1
    assert eventually(
        lambda: logger.messages("warning").count("duplicate job ignored") == 2
    )


def test_accepts_a_burst_of_parallel_job_requests(start: Any) -> None:
    # On Linux, socketserver's default backlog of 5 resets some of these
    # connections. The worker listens with Node's backlog of 511.
    worker, _ = start(routes={"/build": lambda job_input, context: job_input})
    barrier = threading.Barrier(50)
    statuses: list[int] = []

    def deliver(k: int) -> None:
        barrier.wait(WAIT)
        statuses.append(request(worker, "/build", job_body(callbackId=f"cb-{k}"))[0])

    threads = [threading.Thread(target=deliver, args=(k,)) for k in range(50)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(WAIT)
    assert worker.idle(WAIT)

    assert statuses == [202] * 50


def test_names_no_python_version_in_the_server_header(start: Any) -> None:
    worker, _ = start(Held())

    answer = raw_request(
        worker,
        f"POST {HOOK_PATH_PREFIX}ready HTTP/1.1\r\nHost: localhost\r\n"
        "Content-Length: 2\r\n\r\n{}".encode(),
    )

    assert b"Server: aws-durable-execution-sdk-python-microvm-worker\r\n" in answer
    assert b"Python/" not in answer


def test_prints_no_traceback_for_a_client_that_resets_during_the_headers(
    start: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    worker, _ = start(Held())

    with socket.create_connection(("127.0.0.1", worker.port), timeout=WAIT) as sock:
        sock.sendall(b"POST /aws/lambda-microvms/runtime/v1/run HTTP/1.1\r\nHo")
        # The reset must arrive while the server waits for the rest of the
        # headers. A probe printed the traceback only then.
        time.sleep(0.1)
        # A zero linger time makes close() send a TCP reset.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    # The request thread handles the reset after the close.
    time.sleep(0.3)
    assert request(worker, f"{HOOK_PATH_PREFIX}ready", {})[0] == 200

    assert "Traceback" not in capsys.readouterr().err


def test_reports_other_server_errors_as_the_base_class_does(
    start: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    worker, _ = start(Held())
    server = worker._server  # noqa: SLF001

    try:
        msg = "unexpected"
        raise ValueError(msg)
    except ValueError:
        server.handle_error(None, ("127.0.0.1", 0))

    assert "ValueError: unexpected" in capsys.readouterr().err


class _IPv6OnlyByDefault(socket.socket):
    """Starts each new IPv6 socket IPv6-only.

    Windows and the BSDs do this, and so does Linux with
    ``net.ipv6.bindv6only=1``.
    """

    def __init__(
        self,
        family: int = -1,
        type: int = -1,  # noqa: A002
        proto: int = -1,
        fileno: int | None = None,
    ) -> None:
        super().__init__(family, type, proto, fileno)
        # accept() passes fileno for each connection. Only a new socket
        # starts with the system default.
        if fileno is None and self.family == socket.AF_INET6:
            self.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)


def test_listens_on_ipv4_when_ipv6_sockets_start_ipv6_only(
    start: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Linux starts an IPv6 socket dual-stack by default. So there the server
    # accepts IPv4 without its IPV6_V6ONLY call, and the test above passes
    # either way. Here every new IPv6 socket starts IPv6-only.
    if not socket.has_dualstack_ipv6():
        pytest.skip("this host has no dual-stack IPv6")
    monkeypatch.setattr(socket, "socket", _IPv6OnlyByDefault)
    worker, _ = start(Held())

    assert request(worker, f"{HOOK_PATH_PREFIX}ready", {})[0] == 200


def test_runs_a_job_once_when_parallel_requests_deliver_it(start: Any) -> None:
    handler = Held()
    handler.release.set()
    worker, client = start(routes={"/build": handler})
    barrier = threading.Barrier(8)

    def deliver() -> None:
        barrier.wait(WAIT)
        request(worker, "/build", job_body())

    threads = [threading.Thread(target=deliver) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(WAIT)
    assert worker.idle(WAIT)

    assert len(handler.inputs) == 1
    assert len(completions(client)) == 1


def test_ignores_a_run_hook_job_that_already_arrived_over_http(
    start: Any, logger: Any
) -> None:
    handler = Held()
    handler.release.set()
    worker, client = start(handler)

    request(worker, MICROVM_JOB_PATH, job_body())
    assert worker.idle(WAIT)
    assert request(worker, RUN, run_body(run_payload())) == (200, {})
    assert worker.idle(WAIT)

    assert len(handler.inputs) == 1
    assert eventually(lambda: "duplicate job ignored" in logger.messages("warning"))


def test_forgets_the_oldest_finished_job(
    start: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(worker_module, "MAX_FINISHED_JOBS", 2)
    handler = Held()
    handler.release.set()
    worker, _ = start(routes={"/build": handler})

    for callback_id in ["cb-1", "cb-2", "cb-3", "cb-2", "cb-1"]:
        request(worker, "/build", job_body(callbackId=callback_id))
        assert worker.idle(WAIT)

    # cb-1 was forgotten when cb-3 ended. cb-2 was still remembered.
    assert len(handler.inputs) == 4


def test_answers_500_to_an_unexpected_error(
    start: Any, logger: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    worker, _ = start(Held())

    def broken(self: Any, exchange: Any) -> None:
        raise RuntimeError

    monkeypatch.setattr(MicrovmWorkerListener, "_dispatch", broken)

    assert request(worker, RUN, {}) == (500, {"error": "internal error"})
    assert "request handling failed" in logger.messages("error")


def test_sends_no_second_answer_after_an_unexpected_error(
    start: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    worker, _ = start(Held())

    def broken(self: Any, exchange: Any) -> None:
        exchange.respond(202, {})
        raise RuntimeError

    monkeypatch.setattr(MicrovmWorkerListener, "_dispatch", broken)

    assert request(worker, RUN, {}) == (202, {})


def test_starts_the_job_when_the_answer_cannot_be_sent(
    logger: Any, fake_client: Any
) -> None:
    client = fake_client()
    handler = Held()
    handler.release.set()
    listener = MicrovmWorkerListener.create(
        routes={"/build": handler}, create_client=lambda region: client, logger=logger
    )

    class BrokenPipe:
        def write(self, data: bytes) -> None:
            raise BrokenPipeError

    data = json.dumps(job_body()).encode()

    class Request:
        command = "POST"
        path = "/build"
        headers = {"Content-Length": str(len(data))}
        rfile = io.BytesIO(data)
        wfile = BrokenPipe()

        def send_response(self, status: int) -> None: ...
        def send_header(self, name: str, value: str) -> None: ...
        def end_headers(self) -> None: ...

    listener.handle_request(Request())  # type: ignore[arg-type]
    assert listener.idle(WAIT)

    assert len(handler.inputs) == 1
    assert "could not send the response" in logger.messages("warning")


def test_forgets_a_job_whose_thread_cannot_start(
    start: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    handler = Held()
    handler.release.set()
    worker, _ = start(routes={"/build": handler})
    original = threading.Thread.start
    failures = [True]

    def flaky_start(self: threading.Thread) -> None:
        if self.name.startswith("microvm-job-") and failures:
            failures.pop()
            msg = "can't start new thread"
            raise RuntimeError(msg)
        original(self)

    monkeypatch.setattr(threading.Thread, "start", flaky_start)
    # The thread starts before the answer. So a thread that cannot start gets
    # HTTP 500, which the durable function retries, and the retry runs the job.
    assert request(worker, "/build", job_body())[0] == 500
    assert worker.idle(WAIT)
    assert request(worker, "/build", job_body())[0] == 202
    assert worker.idle(WAIT)

    assert len(handler.inputs) == 1


def test_fails_a_run_hook_job_whose_thread_cannot_start(
    start: Any, logger: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    worker, client = start(Held())
    original = threading.Thread.start

    def failing_start(self: threading.Thread) -> None:
        if self.name.startswith("microvm-job-"):
            msg = "can't start new thread"
            raise RuntimeError(msg)
        original(self)

    monkeypatch.setattr(threading.Thread, "start", failing_start)
    # Lambda sends the run hook once. So the worker answers 200, and fails
    # the callback, instead of leaving the job to its heartbeat timeout.
    assert request(worker, RUN, run_body(run_payload())) == (200, {})
    assert worker.idle(WAIT)

    [(name, kwargs)] = completions(client)
    assert name == "failure"
    assert kwargs["Error"] == {
        "ErrorType": "RuntimeError",
        "ErrorMessage": "can't start new thread",
    }
    assert "could not start the run hook job" in logger.messages("error")


def test_fails_a_job_whose_heartbeats_cannot_start(
    start: Any, logger: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    handler = Held()
    handler.release.set()
    worker, client = start(routes={"/build": handler})
    original = threading.Thread.start

    def failing_start(self: threading.Thread) -> None:
        if self.name.startswith("heartbeats-"):
            msg = "can't start new thread"
            raise RuntimeError(msg)
        original(self)

    monkeypatch.setattr(threading.Thread, "start", failing_start)
    # The job already has its 202, so no delivery follows. The worker fails
    # the callback, instead of leaving the job to its heartbeat timeout.
    assert request(worker, "/build", job_body(heartbeatTimeoutSeconds=60))[0] == 202
    assert worker.idle(WAIT)

    [(name, kwargs)] = completions(client)
    assert name == "failure"
    assert kwargs["Error"] == {
        "ErrorType": "RuntimeError",
        "ErrorMessage": "can't start new thread",
    }
    assert handler.inputs == []
    assert "could not start the job's heartbeats" in logger.messages("error")


def test_counts_no_report_whose_thread_cannot_start(
    start: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    worker, _ = start(routes={"/build": Held()})
    original = threading.Thread.start

    def failing_start(self: threading.Thread) -> None:
        if self.name.startswith("microvm-failure-"):
            msg = "can't start new thread"
            raise RuntimeError(msg)
        original(self)

    monkeypatch.setattr(threading.Thread, "start", failing_start)

    assert request(worker, "/build", job_body(version=2))[0] == 500
    assert worker.idle(WAIT)


def test_idle_returns_false_while_a_job_runs(start: Any) -> None:
    handler = Held()
    worker, _ = start(handler)

    request(worker, RUN, run_body(run_payload()))
    assert handler.started.wait(WAIT)

    assert worker.idle(0.05) is False
    handler.release.set()
    assert worker.idle(WAIT) is True


def test_close_ends_wait_and_can_run_twice(start: Any) -> None:
    worker, _ = start(Held())

    assert worker.wait(0.01) is False
    worker.close()
    worker.close()

    assert worker.wait(0) is True
    with pytest.raises(OSError):
        request(worker, RUN, {})


def test_raises_when_the_port_is_taken(start: Any) -> None:
    worker, _ = start(Held())

    with pytest.raises(OSError):
        start_microvm_worker(handler=Held(), port=worker.port)


def _has_ipv6_loopback() -> bool:
    """Whether this host can use ``::1``.

    A host can support IPv6 sockets and still have no ``::1`` address. A
    Docker container with IPv6 turned off is one example. A connection to
    ``::1`` then fails with ``EADDRNOTAVAIL``.
    """
    if not socket.has_dualstack_ipv6():
        return False
    try:
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as probe:
            probe.bind(("::1", 0))
    except OSError:
        return False
    return True


def test_listens_on_ipv6_and_ipv4(start: Any) -> None:
    if not _has_ipv6_loopback():
        pytest.skip("this host has no IPv6 loopback address")
    worker, _ = start(Held())

    connection = http.client.HTTPConnection("::1", worker.port, timeout=WAIT)
    try:
        connection.request("POST", f"{HOOK_PATH_PREFIX}ready", body=b"{}")
        assert connection.getresponse().status == 200
    finally:
        connection.close()
    assert request(worker, f"{HOOK_PATH_PREFIX}ready", {})[0] == 200


def test_listens_on_ipv4_without_dual_stack(
    fake_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(worker_module.socket, "has_dualstack_ipv6", lambda: False)
    worker = start_microvm_worker(handler=Held(), port=0)
    try:
        assert request(worker, f"{HOOK_PATH_PREFIX}ready", {})[0] == 200
    finally:
        worker.close()


def test_listener_serves_a_server_of_the_caller(logger: Any, fake_client: Any) -> None:
    client = fake_client()
    listener = MicrovmWorkerListener.create(
        handler=lambda job_input, context: "ok",
        create_client=lambda region: client,
        logger=logger,
    )
    server = ThreadingHTTPServer(("127.0.0.1", 0), listener.request_handler_class)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        connection = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=WAIT
        )
        connection.request("POST", RUN, body=json.dumps(run_body(run_payload())))
        assert connection.getresponse().status == 200
        connection.close()
        assert listener.idle(WAIT)
    finally:
        server.shutdown()
        server.server_close()

    assert listener.request_handler_class is listener.request_handler_class
    assert client.names() == ["success"]


async def _async_handler(job_input: Any, context: MicrovmJobContext) -> None:
    return None  # pragma: no cover


class _AsyncCallable:
    async def __call__(self, job_input: Any, context: MicrovmJobContext) -> None:
        return None  # pragma: no cover


class _Awaitable:
    def __await__(self) -> Generator[None, None, None]:
        yield None


def test_fails_a_job_whose_handler_returns_a_coroutine_and_closes_it(
    start: Any,
) -> None:
    coroutine = _async_handler(None, None)  # type: ignore[arg-type]
    worker, client = start(lambda job_input, context: coroutine)

    request(worker, RUN, run_body(run_payload()))
    assert worker.idle(WAIT)

    # A closed coroutine does not warn that it was never awaited.
    assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED
    [(name, kwargs)] = completions(client)
    assert name == "failure"
    assert kwargs["Error"]["ErrorType"] == "TypeError"
    assert "returned an awaitable" in kwargs["Error"]["ErrorMessage"]


def test_fails_a_job_whose_handler_returns_another_awaitable(start: Any) -> None:
    worker, client = start(lambda job_input, context: _Awaitable())

    request(worker, RUN, run_body(run_payload()))
    assert worker.idle(WAIT)

    assert completions(client)[0][1]["Error"]["ErrorType"] == "TypeError"


async def _ignores_generator_exit() -> None:
    try:
        await _Awaitable()
    finally:
        await _Awaitable()


def test_fails_a_job_whose_started_coroutine_cannot_be_closed(start: Any) -> None:
    # The coroutine has started and awaits in a finally block. So close()
    # raises RuntimeError: coroutine ignored GeneratorExit. The job must still
    # report its TypeError.
    coroutine = _ignores_generator_exit()
    coroutine.send(None)
    worker, client = start(lambda job_input, context: coroutine)

    request(worker, RUN, run_body(run_payload()))
    assert worker.idle(WAIT)

    [(name, kwargs)] = completions(client)
    assert name == "failure"
    assert kwargs["Error"]["ErrorType"] == "TypeError"
    # Run the finally block to its end, so the coroutine closes before the
    # garbage collector meets it.
    with contextlib.suppress(GeneratorExit, StopIteration):
        coroutine.send(None)


@pytest.mark.parametrize(
    ("options", "error", "message"),
    [
        ({}, TypeError, "needs a handler or routes"),
        ({"routes": {}}, TypeError, "needs a handler or routes"),
        ({"handler": "run"}, TypeError, "must be callable. Got str"),
        ({"handler": _async_handler}, TypeError, "not an async function"),
        ({"handler": _AsyncCallable()}, TypeError, "not an async function"),
        ({"routes": {"/build": _async_handler}}, TypeError, "not an async function"),
        ({"routes": {"build": Held()}}, ValueError, 'must start with "/"'),
        (
            {"routes": {f"{HOOK_PATH_PREFIX}run": Held()}},
            ValueError,
            "lifecycle hook prefix",
        ),
        ({"routes": {MICROVM_JOB_PATH: Held()}}, ValueError, "is reserved"),
        (
            {"handler": Held(), "heartbeat_interval_seconds": 0},
            ValueError,
            "heartbeat_interval_seconds",
        ),
    ],
)
def test_create_rejects_invalid_options(
    options: dict[str, Any], error: type[Exception], message: str
) -> None:
    with pytest.raises(error, match=message):
        MicrovmWorkerListener.create(**options)
