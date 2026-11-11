# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the AgentCore worker, through real HTTP requests on loopback."""

from __future__ import annotations

import http.client
import json
import threading
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from aws_durable_execution_sdk_python_compute_worker.agentcore import (
    AgentcoreJobContext,
    AgentcoreWorker,
    start_agentcore_worker,
)
from aws_durable_execution_sdk_python_compute_worker.agentcore import (
    worker as worker_module,
)
from aws_durable_execution_sdk_python_compute_worker.microvm.callback_reporter import (
    CallbackReporter,
)


WAIT = 10


def job_body(callback_id: str = "cb-1", **fields: Any) -> dict[str, Any]:
    return {
        "version": 1,
        "region": "us-west-2",
        "callbackId": callback_id,
        "input": {"n": 2},
        **fields,
    }


def request(
    worker: AgentcoreWorker,
    method: str,
    path: str,
    body: Any = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, Any]:
    connection = http.client.HTTPConnection("127.0.0.1", worker.port, timeout=WAIT)
    try:
        data = (
            None
            if body is None
            else (body if isinstance(body, bytes) else json.dumps(body).encode())
        )
        connection.request(method, path, body=data, headers=headers or {})
        response = connection.getresponse()
        return response.status, json.loads(response.read() or b"null")
    finally:
        connection.close()


@pytest.fixture
def start(
    fake_client: Any, logger: Any
) -> Iterator[Callable[..., tuple[AgentcoreWorker, Any, list[str]]]]:
    workers: list[AgentcoreWorker] = []

    def _start(
        handler: Callable[[Any, AgentcoreJobContext], Any], client: Any = None
    ) -> tuple[AgentcoreWorker, Any, list[str]]:
        client = client or fake_client()
        regions: list[str] = []

        def create_client(region: str) -> Any:
            regions.append(region)
            return client

        worker = start_agentcore_worker(
            handler=handler,
            logger=logger,
            create_client=create_client,
            host="127.0.0.1",
            port=0,
        )
        workers.append(worker)
        return worker, client, regions

    yield _start
    for worker in workers:
        worker.close()


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Make each read of the worker's clock return the next second, from
    100. The list holds the values that the worker read."""
    reads: list[int] = []

    def now() -> int:
        reads.append(100 + len(reads))
        return reads[-1]

    monkeypatch.setattr(worker_module, "_now", now)
    return reads


def ping(worker: AgentcoreWorker) -> Any:
    status, answer = request(worker, "GET", "/ping")
    assert status == 200
    return answer


def test_ping_keeps_the_start_time_while_the_status_stays(
    start: Any, clock: list[int]
) -> None:
    worker, _, _ = start(lambda job_input, context: None)

    first = ping(worker)
    second = ping(worker)

    assert first == {"status": "Healthy", "time_of_last_update": 100}
    assert second == first
    assert clock == [100]


def test_ping_changes_the_time_when_the_first_job_starts_and_the_last_ends(
    start: Any, clock: list[int]
) -> None:
    release = threading.Event()
    started = threading.Semaphore(0)

    def handler(job_input: Any, context: AgentcoreJobContext) -> Any:
        started.release()
        assert release.wait(WAIT)
        return "done"

    worker, client, _ = start(handler)
    assert request(worker, "POST", "/invocations", job_body("cb-1")) == (
        200,
        {"accepted": True},
    )
    assert started.acquire(timeout=WAIT)
    busy = ping(worker)
    # A second job while one runs does not change the status.
    request(worker, "POST", "/invocations", job_body("cb-2"))
    assert started.acquire(timeout=WAIT)
    still_busy = ping(worker)
    release.set()
    assert worker.idle(WAIT)
    healthy = ping(worker)

    assert busy == {"status": "HealthyBusy", "time_of_last_update": 101}
    assert still_busy == busy
    assert healthy == {"status": "Healthy", "time_of_last_update": 102}
    assert ping(worker) == healthy
    assert client.names() == ["success", "success"]


def test_reports_the_result_to_the_callback_of_the_job(start: Any) -> None:
    seen: list[AgentcoreJobContext] = []

    def handler(job_input: Any, context: AgentcoreJobContext) -> Any:
        seen.append(context)
        return {"sum": job_input["n"] + 1}

    worker, client, regions = start(handler)
    status, _ = request(
        worker,
        "POST",
        "/invocations",
        job_body(),
        {"X-Amzn-Bedrock-AgentCore-Runtime-Session-Id": "s" * 36},
    )

    assert status == 200
    assert worker.idle(WAIT)
    assert client.calls == [("success", {"CallbackId": "cb-1", "Result": b'{"sum":3}'})]
    assert regions == ["us-west-2"]
    [context] = seen
    assert context.callback_id == "cb-1"
    assert context.session_id == "s" * 36
    assert context.region == "us-west-2"


def test_reports_the_error_of_a_failed_job(start: Any) -> None:
    def handler(job_input: Any, context: AgentcoreJobContext) -> Any:
        msg = "3 tests failed"
        raise RuntimeError(msg)

    worker, client, _ = start(handler)
    request(worker, "POST", "/invocations", job_body())

    assert worker.idle(WAIT)
    assert client.calls == [
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


def test_reports_a_result_that_is_not_json_as_a_failure(start: Any) -> None:
    worker, client, _ = start(lambda job_input, context: {1, 2})
    request(worker, "POST", "/invocations", job_body())

    assert worker.idle(WAIT)
    [(name, call)] = client.calls
    assert name == "failure"
    assert call["Error"]["ErrorType"] == "ResultSerializationError"


def test_runs_a_repeated_callback_id_once(start: Any) -> None:
    runs: list[str] = []
    release = threading.Event()

    def handler(job_input: Any, context: AgentcoreJobContext) -> Any:
        runs.append(context.callback_id)
        assert release.wait(WAIT)
        return None

    worker, client, _ = start(handler)
    # The second request arrives while the job runs, the third after it ended.
    assert request(worker, "POST", "/invocations", job_body())[1] == {"accepted": True}
    assert request(worker, "POST", "/invocations", job_body())[1] == {"accepted": True}
    release.set()
    assert worker.idle(WAIT)
    assert request(worker, "POST", "/invocations", job_body())[1] == {"accepted": True}
    assert worker.idle(WAIT)

    assert runs == ["cb-1"]
    assert client.names() == ["success"]


def test_sends_heartbeats_for_a_job_with_a_heartbeat_timeout(
    fake_client: Any, logger: Any
) -> None:
    release = threading.Event()
    client = fake_client()

    def handler(job_input: Any, context: AgentcoreJobContext) -> Any:
        assert release.wait(WAIT)
        return None

    worker = start_agentcore_worker(
        handler=handler,
        logger=logger,
        create_client=lambda region: client,
        heartbeat_interval_seconds=0.05,
        host="127.0.0.1",
        port=0,
    )
    try:
        request(worker, "POST", "/invocations", job_body(heartbeatTimeoutSeconds=60))
        deadline = threading.Event()
        for _ in range(200):
            if client.names().count("heartbeat") >= 2:
                break
            deadline.wait(0.05)
        release.set()
        assert worker.idle(WAIT)
    finally:
        worker.close()

    names = client.names()
    assert names.count("heartbeat") >= 2
    # Heartbeats continue while the worker reports the outcome. So a
    # heartbeat can follow the success call.
    assert names.count("success") == 1


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        job_body(version=2),
        {"version": 1, "region": "us-west-2"},
        {"version": 1, "callbackId": "cb-1"},
        job_body(heartbeatTimeoutSeconds=0),
    ],
)
def test_rejects_a_job_request_that_does_not_match_the_contract(
    start: Any, body: Any
) -> None:
    runs: list[Any] = []
    worker, client, _ = start(lambda job_input, context: runs.append(job_input))

    status, answer = request(worker, "POST", "/invocations", body)

    assert status == 400
    assert "error" in answer
    assert worker.idle(WAIT)
    assert runs == []
    assert client.calls == []


def test_answers_404_for_another_path(start: Any) -> None:
    worker, _, _ = start(lambda job_input, context: None)

    assert request(worker, "GET", "/other")[0] == 404
    assert request(worker, "POST", "/ping", {})[0] == 404


def test_refuses_an_async_handler() -> None:
    async def handler(job_input: Any, context: AgentcoreJobContext) -> Any:
        return None

    with pytest.raises(TypeError, match="async"):
        start_agentcore_worker(handler=handler, host="127.0.0.1", port=0)  # type: ignore[arg-type]


def test_refuses_a_handler_that_is_not_callable() -> None:
    with pytest.raises(TypeError, match="callable"):
        start_agentcore_worker(handler="job", host="127.0.0.1", port=0)  # type: ignore[arg-type]


def test_wait_returns_after_close(start: Any) -> None:
    worker, _, _ = start(lambda job_input, context: None)

    assert not worker.wait(0)
    worker.close()
    worker.close()
    assert worker.wait(0)


def test_refuses_an_object_whose_call_is_async() -> None:
    class Handler:
        async def __call__(self, job_input: Any, context: AgentcoreJobContext) -> Any:
            return None

    with pytest.raises(TypeError, match="async"):
        start_agentcore_worker(handler=Handler(), host="127.0.0.1", port=0)  # type: ignore[arg-type]


def test_answers_500_and_forgets_a_job_whose_thread_did_not_start(
    start: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    runs: list[Any] = []
    worker, client, _ = start(lambda job_input, context: runs.append(job_input))
    original = threading.Thread.start

    def fail_job_threads(self: threading.Thread) -> None:
        if self.name.startswith("agentcore-job-"):
            msg = "can't start new thread"
            raise RuntimeError(msg)
        original(self)

    monkeypatch.setattr(threading.Thread, "start", fail_job_threads)
    assert request(worker, "POST", "/invocations", job_body())[0] == 500
    monkeypatch.setattr(threading.Thread, "start", original)

    # The retry of the same job runs it, because the first one never ran.
    assert request(worker, "POST", "/invocations", job_body())[0] == 200
    assert worker.idle(WAIT)
    assert runs == [{"n": 2}]
    assert client.names() == ["success"]


def test_forgets_the_oldest_finished_job_after_the_limit(
    start: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(worker_module, "MAX_FINISHED_JOBS", 1)
    runs: list[str] = []

    def handler(job_input: Any, context: AgentcoreJobContext) -> Any:
        runs.append(context.callback_id)

    worker, _, _ = start(handler)
    for callback_id in ("cb-1", "cb-2", "cb-1"):
        request(worker, "POST", "/invocations", job_body(callback_id))
        assert worker.idle(WAIT)

    assert runs == ["cb-1", "cb-2", "cb-1"]


def test_logs_a_job_whose_client_cannot_be_created(
    logger: Any,
) -> None:
    def create_client(region: str) -> Any:
        msg = "no credentials"
        raise RuntimeError(msg)

    worker = start_agentcore_worker(
        handler=lambda job_input, context: None,
        logger=logger,
        create_client=create_client,
        host="127.0.0.1",
        port=0,
    )
    try:
        assert request(worker, "POST", "/invocations", job_body())[0] == 200
        assert worker.idle(WAIT)
        assert ping(worker)["status"] == "Healthy"
    finally:
        worker.close()

    assert logger.messages("error") == ["job failed inside the worker"]


def test_creates_its_own_reporter_without_create_client(
    fake_client: Any, logger: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = fake_client()
    regions: list[str] = []

    def create(callback_id: str, region: str, *, logger: Any = None) -> Any:
        regions.append(region)
        return CallbackReporter(callback_id, client, logger=logger)

    monkeypatch.setattr(worker_module.CallbackReporter, "create", create)
    worker = start_agentcore_worker(
        handler=lambda job_input, context: "done",
        logger=logger,
        host="127.0.0.1",
        port=0,
    )
    try:
        request(worker, "POST", "/invocations", job_body())
        assert worker.idle(WAIT)
    finally:
        worker.close()

    assert regions == ["us-west-2"]
    assert client.names() == ["success"]


def test_does_not_report_a_job_whose_callback_is_gone(
    fake_client: Any, make_error: Any, logger: Any
) -> None:
    # The first heartbeat finds the callback closed. The worker then sets
    # context.cancelled, and does not report the handler's outcome.
    client = fake_client(make_error("CallbackTimeoutException"))

    def handler(job_input: Any, context: AgentcoreJobContext) -> Any:
        assert context.cancelled.wait(WAIT)
        return "late"

    worker = start_agentcore_worker(
        handler=handler,
        logger=logger,
        create_client=lambda region: client,
        heartbeat_interval_seconds=0.05,
        host="127.0.0.1",
        port=0,
    )
    try:
        request(worker, "POST", "/invocations", job_body(heartbeatTimeoutSeconds=60))
        assert worker.idle(WAIT)
    finally:
        worker.close()

    assert client.names() == ["heartbeat"]
    assert "job outcome not reported: the callback is gone" in logger.messages(
        "warning"
    )


def test_logs_an_outcome_that_could_not_be_reported(
    fake_client: Any, make_error: Any, logger: Any
) -> None:
    client = fake_client(make_error("AccessDeniedException", 403))
    worker = start_agentcore_worker(
        handler=lambda job_input, context: "done",
        logger=logger,
        create_client=lambda region: client,
        host="127.0.0.1",
        port=0,
    )
    try:
        request(worker, "POST", "/invocations", job_body())
        assert worker.idle(WAIT)
    finally:
        worker.close()

    assert client.names() == ["success"]
    assert logger.messages("error") == ["job outcome could not be reported"]
