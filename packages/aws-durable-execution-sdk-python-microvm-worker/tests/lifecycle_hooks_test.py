# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the suspend, resume, and terminate hooks, and for auto-suspend.

The requests go over HTTP to a real worker. The idle time and the refusal
after a suspend run on a fake clock, which each test advances.
"""

from __future__ import annotations

import functools
import http.client
import json
import socket
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Any

import pytest
from botocore.exceptions import ClientError, NoCredentialsError

from aws_durable_execution_sdk_python_microvm_worker import (
    HOOK_PATH_PREFIX,
    MicrovmJobContext,
    MicrovmTerminatedError,
    MicrovmWorker,
    MicrovmWorkerListener,
    start_microvm_worker,
)
from aws_durable_execution_sdk_python_microvm_worker import worker as worker_module


WAIT = 10.0
IDLE = 10.0
GRACE = 30.0
TERMINATED_MESSAGE = "the MicroVM was terminated before the job finished"

# The warning for a failed SuspendMicrovm call has one message per outcome.
# The messages, the outcome names, and the field names match the JavaScript
# worker's word for word.
REJECTED = (
    "could not suspend the idle MicroVM, because the service rejected the "
    "call. A missing permission needs lambda:SuspendMicrovm on the MicroVM "
    "image in its execution role."
)
REFUSED = (
    "could not suspend the idle MicroVM, because the service refused the call for now."
)
UNKNOWN = "the suspend call for the idle MicroVM had an unknown outcome."
WARNINGS = {"rejected": REJECTED, "refusedForNow": REFUSED, "unknown": UNKNOWN}
WARNING_FIELDS = [
    "microvmId",
    "error",
    "outcome",
    "suspendHookArrived",
    "terminating",
    "closed",
    "refusingJobs",
]


def assert_warning(
    message: str,
    data: dict[str, Any],
    outcome: str,
    *,
    refusing_jobs: bool,
    suspend_hook_arrived: bool = False,
    terminating: bool = False,
    closed: bool = False,
) -> None:
    """Assert that one warning for a failed SuspendMicrovm call has this
    outcome and these fields."""
    assert message == WARNINGS[outcome]
    assert list(data) == WARNING_FIELDS
    assert data["microvmId"] == "mvm-1"
    assert sorted(data["error"]) == ["message", "name"]
    assert {name: data[name] for name in WARNING_FIELDS[2:]} == {
        "outcome": outcome,
        "suspendHookArrived": suspend_hook_arrived,
        "terminating": terminating,
        "closed": closed,
        "refusingJobs": refusing_jobs,
    }


def assert_one_warning(
    logger: Any,
    outcome: str,
    *,
    refusing_jobs: bool,
    suspend_hook_arrived: bool = False,
    terminating: bool = False,
    closed: bool = False,
) -> dict[str, Any]:
    """Assert that the worker logged exactly one warning, for one failed
    SuspendMicrovm call, with this outcome and these fields. Returns the
    fields."""
    warnings = [(m, data) for level, m, data in logger.lines if level == "warning"]
    assert len(warnings) == 1
    message, data = warnings[0]
    assert_warning(
        message,
        data,
        outcome,
        refusing_jobs=refusing_jobs,
        suspend_hook_arrived=suspend_hook_arrived,
        terminating=terminating,
        closed=closed,
    )
    return data


LAMBDA_HEADERS = {"Host": "localhost:8080"}
FORWARDED_HEADERS = {
    "Host": "7ed0892b.lambda-microvm.us-east-1.on.aws",
    "x-amzn-requestid": "6f1c0e0a-0000-4000-8000-000000000000",
}


@functools.cache
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


def host_of(worker: MicrovmWorker) -> str:
    """The loopback address on which a test reaches ``worker``.

    A dual-stack worker binds ``[::]`` with ``SO_REUSEADDR``. On macOS, that
    bind succeeds on a port that another process holds on ``127.0.0.1``. A
    connection to ``127.0.0.1`` then reaches the other process, and the test
    fails with its answer. So the tests connect to ``::1`` when the worker
    is dual-stack. Linux refuses such a bind, so a MicroVM is not affected.
    """
    server = worker._server  # noqa: SLF001
    if server.address_family == socket.AF_INET6 and _has_ipv6_loopback():
        return "::1"
    return "127.0.0.1"


def reaches_worker(worker: MicrovmWorker) -> bool:
    """Whether a connection to ``host_of(worker)`` reaches this worker.

    The port that the kernel picks can be held by another process on one
    loopback address, ``127.0.0.1`` or ``::1``. The worker's ``[::]`` bind
    still succeeds on macOS, and a connection then reaches the other
    process. So a test checks the ``Server`` header, and starts the worker
    again on another port when it is not the worker's.
    """
    connection = http.client.HTTPConnection(host_of(worker), worker.port, timeout=5)
    try:
        connection.request("GET", "/__reachability_check__")
        response = connection.getresponse()
        response.read()
        server = response.getheader("Server") or ""
    except OSError:
        return False
    finally:
        connection.close()
    return server.startswith("aws-durable-execution-sdk-python-microvm-worker")


def start_reachable_worker(start: Callable[[], MicrovmWorker]) -> MicrovmWorker:
    """Start a worker with ``start`` until a test connection reaches it."""
    for _ in range(10):
        worker = start()
        if reaches_worker(worker):
            return worker
        worker.close()
    msg = "every port that the kernel picked was held by another process"
    raise RuntimeError(msg)


def eventually(condition: Callable[[], bool]) -> bool:
    deadline = time.monotonic() + WAIT
    while not condition():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.005)
    return True


def suspend_error(code: str, status: int, retries: int = 0) -> ClientError:
    response: Any = {
        "Error": {"Code": code, "Message": f"{code} message"},
        "ResponseMetadata": {"HTTPStatusCode": status, "RetryAttempts": retries},
    }
    return ClientError(response, "SuspendMicrovm")


class FakeTimers:
    """Replaces the listener's timers with a clock that the test advances.

    ``advance`` runs every due action in the test's thread, in time order.
    The clock counts whole milliseconds, so that sums of seconds compare
    exactly.
    """

    def __init__(self) -> None:
        self.now = 0
        self.failure: BaseException | None = None
        # Fire cancelled timers too. A threading.Timer whose action has
        # begun runs it although cancel() was called.
        self.fire_cancelled = False
        self._lock = threading.Lock()
        self._timers: list[_FakeTimer] = []

    def start(self, delay: float, action: Callable[[], None]) -> Callable[[], None]:
        if self.failure is not None:
            raise self.failure
        with self._lock:
            timer = _FakeTimer(due=self.now + round(delay * 1000), action=action)
            self._timers.append(timer)
            return timer.cancel

    def advance(self, seconds: float) -> None:
        target = self.now + round(seconds * 1000)
        while True:
            with self._lock:
                due = [
                    t
                    for t in self._timers
                    if (self.fire_cancelled or not t.cancelled) and t.due <= target
                ]
                if not due:
                    self.now = target
                    return
                timer = min(due, key=lambda t: t.due)
                self._timers.remove(timer)
                self.now = timer.due
            timer.action()


@dataclass
class _FakeTimer:
    due: int
    action: Callable[[], None]
    cancelled: bool = False

    def cancel(self) -> None:
        self.cancelled = True


class FakeMicrovmsClient:
    """Records SuspendMicrovm calls. ``hold`` keeps each call open until
    ``finish()``. ``failure`` answers every call with an error."""

    def __init__(self) -> None:
        self.suspended: list[str] = []
        self.failure: BaseException | None = None
        self.hold = False
        self.closed = False
        self._release = threading.Event()
        self._lock = threading.Lock()

    def suspend_microvm(self, *, microvmIdentifier: str) -> dict[str, Any]:  # noqa: N803
        with self._lock:
            self.suspended.append(microvmIdentifier)
        if self.hold:
            self._release.wait(WAIT)
        if self.failure is not None:
            raise self.failure
        return {}

    def finish(self) -> None:
        self._release.set()

    def close(self) -> None:
        self.closed = True


class Jobs:
    """A handler whose jobs each run until the test releases them."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._releases: dict[str, threading.Event] = {}
        self.contexts: dict[str, MicrovmJobContext] = {}
        self.order: list[str] = []

    def _release(self, callback_id: str) -> threading.Event:
        with self._lock:
            return self._releases.setdefault(callback_id, threading.Event())

    def __call__(self, job_input: Any, context: MicrovmJobContext) -> Any:
        with self._lock:
            self.order.append(context.callback_id)
            self.contexts[context.callback_id] = context
        self._release(context.callback_id).wait(WAIT)
        return "done"

    def started(self) -> list[str]:
        with self._lock:
            return list(self.order)

    def release(self, callback_id: str) -> None:
        self._release(callback_id).set()

    def release_all(self) -> None:
        with self._lock:
            releases = list(self._releases.values())
        for release in releases:
            release.set()


class HeldLambdaClient:
    """Records callback completions. While ``hold`` is set, each completion
    waits until ``finish()`` or ``finish_first()``."""

    def __init__(self, heartbeat_error: BaseException | None = None) -> None:
        self.heartbeat_error = heartbeat_error
        self.completion_error: BaseException | None = None
        self.hold = False
        self._lock = threading.Lock()
        self._changed = threading.Condition(self._lock)
        self._waiting: list[threading.Event] = []
        self.completions: list[dict[str, Any]] = []

    def send_durable_execution_callback_heartbeat(self, **kwargs: Any) -> Any:
        if self.heartbeat_error is not None:
            raise self.heartbeat_error
        return {}

    def _complete(self, entry: dict[str, Any]) -> Any:
        with self._changed:
            gate = None
            if self.hold:
                gate = threading.Event()
                self._waiting.append(gate)
                self._changed.notify_all()
        if gate is not None:
            gate.wait(WAIT)
        if self.completion_error is not None:
            raise self.completion_error
        with self._lock:
            self.completions.append(entry)
        return {}

    def send_durable_execution_callback_success(self, **kwargs: Any) -> Any:
        return self._complete({"kind": "success", "callbackId": kwargs["CallbackId"]})

    def send_durable_execution_callback_failure(self, **kwargs: Any) -> Any:
        error = kwargs["Error"]
        return self._complete(
            {
                "kind": "failure",
                "callbackId": kwargs["CallbackId"],
                "errorType": error["ErrorType"],
                "message": error["ErrorMessage"],
            }
        )

    def waiting(self) -> int:
        with self._lock:
            return len(self._waiting)

    def finish_first(self) -> None:
        with self._lock:
            gate = self._waiting.pop(0) if self._waiting else None
        if gate is not None:
            gate.set()

    def finish(self) -> None:
        with self._lock:
            self.hold = False
            gates, self._waiting = self._waiting, []
        for gate in gates:
            gate.set()

    def recorded(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self.completions)

    def close(self) -> None:
        return


def terminated_failure(callback_id: str) -> dict[str, Any]:
    return {
        "kind": "failure",
        "callbackId": callback_id,
        "errorType": "MicrovmTerminatedError",
        "message": TERMINATED_MESSAGE,
    }


@dataclass
class Harness:
    worker: MicrovmWorker
    lambda_client: HeldLambdaClient
    microvms: FakeMicrovmsClient
    timers: FakeTimers
    jobs: Jobs
    logger: Any
    _threads: list[threading.Thread] = field(default_factory=list)

    @property
    def listener(self) -> MicrovmWorkerListener:
        return self.worker._listener  # noqa: SLF001

    def post(
        self,
        path: str,
        body: Any,
        headers: dict[str, str] | None = None,
        timeout: float = WAIT,
    ) -> int:
        connection = http.client.HTTPConnection(
            host_of(self.worker), self.worker.port, timeout=timeout
        )
        try:
            connection.request(
                "POST",
                path,
                body=json.dumps(body).encode(),
                headers={"Content-Type": "application/json", **(headers or {})},
            )
            response = connection.getresponse()
            response.read()
        finally:
            connection.close()
        return response.status

    def run_hook(
        self,
        idle_seconds: float | None = None,
        microvm_id: str = "mvm-1",
        job: dict[str, Any] | None = None,
    ) -> int:
        payload: dict[str, Any] = {"version": 1, "region": "us-east-1"}
        if idle_seconds is not None:
            payload["autoSuspendIdleSeconds"] = idle_seconds
        if job is not None:
            payload["job"] = job
        return self.post(
            f"{HOOK_PATH_PREFIX}run",
            {"microvmId": microvm_id, "runHookPayload": json.dumps(payload)},
        )

    def hook(
        self,
        name: str,
        headers: dict[str, str] | None = None,
        timeout: float = WAIT,
    ) -> int:
        return self.post(f"{HOOK_PATH_PREFIX}{name}", {}, headers, timeout)

    def hook_in_background(
        self,
        name: str,
        headers: dict[str, str] | None = None,
        timeout: float = WAIT,
    ) -> Future[int]:
        future: Future[int] = Future()

        def run() -> None:
            try:
                future.set_result(self.hook(name, headers, timeout))
            except BaseException as error:  # noqa: BLE001
                future.set_exception(error)

        thread = threading.Thread(target=run, daemon=True)
        self._threads.append(thread)
        thread.start()
        return future

    def job(self, callback_id: str, **fields: Any) -> int:
        return self.post(
            "/job",
            {
                "version": 1,
                "region": "us-east-1",
                "callbackId": callback_id,
                "input": {},
                **fields,
            },
        )

    def finish_job(self, callback_id: str) -> None:
        """Release a job, and wait until it has ended."""
        self.jobs.release(callback_id)
        assert eventually(lambda: callback_id not in self.listener._jobs)  # noqa: SLF001
        # The job's thread removes the job and starts the idle time in one
        # locked block. The check above reads without the lock, so it can see
        # the removal first. Taking the lock waits for the rest of the block.
        # The listener writes a timer-failure log line after it releases the
        # lock. So that line can come after this method returns.
        with self.listener._lock:  # noqa: SLF001
            pass

    def settle(self) -> None:
        """Wait until no SuspendMicrovm call runs."""
        assert eventually(lambda: self.listener._suspend_calls == 0)  # noqa: SLF001
        # The call's thread lowers the count and starts the idle time in one
        # locked block. The check above reads without the lock, so it can see
        # the lower count first. Taking the lock waits for the rest of the
        # block. The listener writes a timer-failure log line after it
        # releases the lock. So that line can come after this method returns.
        with self.listener._lock:  # noqa: SLF001
            pass

    def advance(self, seconds: float) -> None:
        """Advance the clock, and let a suspend call that it started end."""
        self.timers.advance(seconds)
        if not self.microvms.hold:
            self.settle()

    def started(self, callback_id: str) -> bool:
        return callback_id in self.jobs.started()


@pytest.fixture
def timers(monkeypatch: pytest.MonkeyPatch) -> FakeTimers:
    fake = FakeTimers()
    monkeypatch.setattr(worker_module, "_start_timer", fake.start)
    return fake


@pytest.fixture
def start(logger: Any, timers: FakeTimers) -> Iterator[Callable[..., Harness]]:
    harnesses: list[Harness] = []

    def _start(
        *,
        lambda_client: HeldLambdaClient | None = None,
        create_microvms_client: Any = "fake",
        **options: Any,
    ) -> Harness:
        client = lambda_client or HeldLambdaClient()
        microvms = FakeMicrovmsClient()
        jobs = Jobs()
        if create_microvms_client == "fake":
            create_microvms_client = lambda region: microvms  # noqa: E731

        def create_client(region: str) -> Any:
            return client

        options.setdefault("logger", logger)
        worker = start_reachable_worker(
            lambda: start_microvm_worker(
                handler=jobs,
                routes={"/job": jobs},
                create_client=create_client,
                create_microvms_client=create_microvms_client,
                port=0,
                **options,
            )
        )
        harness = Harness(
            worker=worker,
            lambda_client=client,
            microvms=microvms,
            timers=timers,
            jobs=jobs,
            logger=options["logger"],
        )
        harnesses.append(harness)
        return harness

    yield _start
    for harness in harnesses:
        harness.listener.close()
        harness.jobs.release_all()
        harness.lambda_client.finish()
        harness.microvms.finish()
        timers.advance(GRACE * 10)
        harness.worker.idle(WAIT)
        harness.worker.close()
        for thread in harness._threads:  # noqa: SLF001
            thread.join(WAIT)


class SlowCall:
    """A request whose body arrives in two parts.

    The worker has the headers and half of the body when the constructor
    returns. ``finish()`` sends the rest, and returns the status.
    """

    def __init__(self, harness: Harness, path: str, body: Any) -> None:
        data = json.dumps(body).encode()
        half = len(data) // 2
        self._socket = socket.create_connection(
            (host_of(harness.worker), harness.worker.port), timeout=WAIT
        )
        head = (
            f"POST {path} HTTP/1.1\r\nHost: localhost:8080\r\n"
            f"Content-Type: application/json\r\nContent-Length: {len(data)}\r\n\r\n"
        ).encode()
        self._socket.sendall(head + data[:half])
        self._rest = data[half:]

    def finish(self) -> int:
        try:
            self._socket.sendall(self._rest)
            chunks = []
            while chunk := self._socket.recv(65536):
                chunks.append(chunk)
        finally:
            self._socket.close()
        status_line = b"".join(chunks).split(b"\r\n", 1)[0]
        return int(status_line.split()[1])

    def abandon(self) -> None:
        self._socket.close()


RUN_HOOK_WITH_JOB = {
    "microvmId": "mvm-1",
    "runHookPayload": json.dumps(
        {
            "version": 1,
            "region": "us-east-1",
            "job": {"callbackId": "cb-run", "input": {}},
        }
    ),
}


def slow_run_hook(harness: Harness) -> SlowCall:
    call = SlowCall(harness, f"{HOOK_PATH_PREFIX}run", RUN_HOOK_WITH_JOB)
    assert eventually(lambda: harness.listener._run_hooks == 1)  # noqa: SLF001
    return call


def answered_within(future: Future[int], seconds: float) -> bool:
    try:
        future.result(seconds)
    except TimeoutError:
        return False
    return True


# ---------------------------------------------------------------------------
# Auto-suspend when idle
# ---------------------------------------------------------------------------


def test_suspends_its_own_microvm_after_the_idle_time_when_no_job_arrives(
    start: Any,
) -> None:
    h = start()

    h.run_hook(IDLE)
    h.advance(IDLE - 0.001)
    assert h.microvms.suspended == []

    h.advance(0.001)
    assert h.microvms.suspended == ["mvm-1"]


def test_closes_the_default_client_after_each_suspend_and_never_a_provided_one(
    start: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    created: list[FakeMicrovmsClient] = []

    def create_default(region: str) -> FakeMicrovmsClient:
        client = FakeMicrovmsClient()
        created.append(client)
        return client

    monkeypatch.setattr(worker_module, "create_microvms_client", create_default)
    h = start(create_microvms_client=None)
    h.run_hook(IDLE)
    h.advance(IDLE)

    assert [client.suspended for client in created] == [["mvm-1"]]
    assert created[0].closed

    provided = start()
    provided.run_hook(IDLE)
    provided.advance(IDLE)

    assert provided.microvms.suspended == ["mvm-1"]
    assert not provided.microvms.closed


def test_never_suspends_when_the_run_hook_payload_does_not_ask_for_it(
    start: Any,
) -> None:
    h = start()

    h.run_hook()
    h.advance(IDLE * 10)

    assert h.microvms.suspended == []


def test_does_not_suspend_while_a_job_runs_and_suspends_one_idle_time_after_it(
    start: Any,
) -> None:
    h = start()

    h.run_hook(IDLE)
    assert h.job("cb-1") == 202
    h.advance(IDLE * 4)
    assert h.microvms.suspended == []

    h.finish_job("cb-1")
    h.advance(IDLE - 0.001)
    assert h.microvms.suspended == []
    h.advance(0.001)
    assert h.microvms.suspended == ["mvm-1"]


def test_suspends_with_the_run_hook_id_after_a_job_that_arrived_first_with_another(
    start: Any,
) -> None:
    h = start()

    assert h.job("cb-1", microvmId="mvm-from-job") == 202
    h.run_hook(IDLE)
    # The run hook arrived while the job runs, so the idle time waits.
    h.advance(IDLE * 2)
    assert h.microvms.suspended == []

    h.finish_job("cb-1")
    h.advance(IDLE)
    assert h.microvms.suspended == ["mvm-1"]


def test_keeps_the_first_run_hook_target_when_a_later_run_hook_has_another_id(
    start: Any,
) -> None:
    h = start()

    h.run_hook(IDLE)
    h.run_hook(microvm_id="mvm-2")
    h.advance(IDLE)

    assert h.microvms.suspended == ["mvm-1"]


def test_ignores_a_later_run_hook_that_asks_to_suspend_another_microvm(
    start: Any,
) -> None:
    # The endpoint forwards the hook path from any caller with an auth token.
    # SuspendMicrovm authorizes on the image, so the later hook must not
    # choose the suspend target.
    h = start()

    h.run_hook()
    assert h.run_hook(IDLE, microvm_id="mvm-2") == 200
    h.advance(IDLE * 2)

    assert h.microvms.suspended == []


def test_accepts_a_valid_run_hook_after_an_invalid_one(start: Any) -> None:
    h = start()

    assert h.post(f"{HOOK_PATH_PREFIX}run", {}) == 400
    h.run_hook(IDLE)
    h.advance(IDLE)

    assert h.microvms.suspended == ["mvm-1"]


def test_starts_the_idle_time_again_when_a_job_arrives_before_it_ends(
    start: Any,
) -> None:
    h = start()

    h.run_hook(IDLE)
    h.advance(IDLE / 2)
    h.job("cb-1")
    h.finish_job("cb-1")
    # The first idle time would have ended here.
    h.advance(IDLE / 2)
    assert h.microvms.suspended == []

    h.advance(IDLE / 2)
    assert h.microvms.suspended == ["mvm-1"]


def test_does_not_suspend_while_one_of_two_parallel_jobs_still_runs(
    start: Any,
) -> None:
    h = start()

    h.run_hook(IDLE)
    h.job("cb-1")
    h.job("cb-2")
    h.finish_job("cb-1")
    h.advance(IDLE * 4)
    assert h.microvms.suspended == []

    h.finish_job("cb-2")
    h.advance(IDLE)
    assert h.microvms.suspended == ["mvm-1"]


def test_starts_the_idle_time_after_a_run_hook_job(start: Any) -> None:
    h = start()

    h.run_hook(IDLE, job={"callbackId": "cb-run", "input": {}})
    assert eventually(lambda: h.started("cb-run"))
    h.advance(IDLE * 2)
    assert h.microvms.suspended == []

    h.finish_job("cb-run")
    h.advance(IDLE)
    assert h.microvms.suspended == ["mvm-1"]


def test_refuses_a_job_with_503_while_its_suspend_call_runs(start: Any) -> None:
    h = start()
    h.microvms.hold = True

    h.run_hook(IDLE)
    h.advance(IDLE)
    assert eventually(lambda: h.microvms.suspended == ["mvm-1"])

    assert h.job("cb-1") == 503
    assert h.jobs.started() == []
    h.microvms.finish()


def test_refuses_jobs_after_the_call_returned_until_the_resume_hook_arrives(
    start: Any,
) -> None:
    h = start()

    h.run_hook(IDLE)
    h.advance(IDLE)
    assert h.job("cb-1") == 503

    assert h.hook("resume") == 200
    assert h.job("cb-1") == 202
    assert eventually(lambda: h.jobs.started() == ["cb-1"])

    # The idle time starts again after the job.
    h.finish_job("cb-1")
    h.advance(IDLE)
    assert h.microvms.suspended == ["mvm-1", "mvm-1"]


def test_accepts_jobs_again_30_seconds_after_the_call_without_a_resume_hook(
    start: Any,
) -> None:
    h = start()

    h.run_hook(IDLE)
    h.advance(IDLE)
    h.advance(GRACE - 0.001)
    assert h.job("cb-1") == 503

    h.advance(0.001)
    assert h.job("cb-1") == 202
    assert eventually(lambda: h.jobs.started() == ["cb-1"])


def test_starts_the_idle_time_again_at_the_end_of_the_30_second_refusal(
    start: Any,
) -> None:
    h = start()

    h.run_hook(IDLE)
    h.advance(IDLE)
    h.advance(GRACE)
    assert h.microvms.suspended == ["mvm-1"]

    h.advance(IDLE)
    assert h.microvms.suspended == ["mvm-1", "mvm-1"]


def test_suspends_again_one_idle_time_after_a_resume_that_brings_no_job(
    start: Any,
) -> None:
    h = start()

    h.run_hook(IDLE)
    h.advance(IDLE)
    h.hook("resume")
    h.advance(IDLE - 0.001)
    assert h.microvms.suspended == ["mvm-1"]

    h.advance(0.001)
    assert h.microvms.suspended == ["mvm-1", "mvm-1"]


def test_accepts_jobs_at_once_when_the_resume_hook_arrives_during_the_call(
    start: Any,
) -> None:
    h = start()
    h.microvms.hold = True

    h.run_hook(IDLE)
    h.advance(IDLE)
    assert eventually(lambda: h.microvms.suspended == ["mvm-1"])
    h.hook("resume")
    assert h.job("cb-1") == 202

    # The call returns after the resume. It must not start a new refusal.
    h.microvms.finish()
    h.settle()
    assert h.job("cb-2") == 202
    assert eventually(lambda: sorted(h.jobs.started()) == ["cb-1", "cb-2"])


def test_logs_a_rejected_suspend_accepts_jobs_at_once_and_does_not_retry(
    start: Any, logger: Any
) -> None:
    h = start()
    h.microvms.failure = suspend_error("AccessDeniedException", 403)

    h.run_hook(IDLE)
    h.advance(IDLE * 6)

    assert h.microvms.suspended == ["mvm-1"]
    data = assert_one_warning(logger, "rejected", refusing_jobs=False)
    assert data["error"]["name"] == "ClientError"
    assert h.job("cb-1") == 202


def test_retries_the_suspend_one_idle_time_after_a_job_that_follows_a_rejection(
    start: Any,
) -> None:
    h = start()
    h.microvms.failure = suspend_error("AccessDeniedException", 403)

    h.run_hook(IDLE)
    h.advance(IDLE)
    h.job("cb-1")
    h.finish_job("cb-1")
    h.advance(IDLE)

    assert h.microvms.suspended == ["mvm-1", "mvm-1"]


@pytest.mark.parametrize(
    "failure",
    [
        TimeoutError("timed out"),
        suspend_error("ConflictException", 409),
        suspend_error("InternalServerException", 500),
        suspend_error("RequestTimeoutException", 408),
        # The client retried a 5xx. The first attempt may have suspended the
        # MicroVM, and the retry then fails validation.
        suspend_error("ValidationException", 400, retries=1),
        # botocore retries a throttle. An earlier attempt may have reached
        # the service.
        suspend_error("ThrottlingException", 429, retries=2),
        ConnectionResetError("reset"),
    ],
    ids=[
        "timeout",
        "conflict",
        "server-error",
        "request-timeout",
        "retried-rejection",
        "retried-throttle",
        "connection-error",
    ],
)
def test_keeps_refusing_jobs_after_an_unknown_outcome(
    start: Any, logger: Any, failure: BaseException
) -> None:
    h = start()
    h.microvms.failure = failure

    h.run_hook(IDLE)
    h.advance(IDLE)
    assert_one_warning(logger, "unknown", refusing_jobs=True)
    assert h.job("cb-1") == 503

    h.advance(GRACE)
    assert h.job("cb-1") == 202


@pytest.mark.parametrize(
    "failure",
    [NoCredentialsError(), suspend_error("ValidationException", 400)],
    ids=["missing-credentials", "first-attempt-rejection"],
)
def test_accepts_jobs_at_once_after_a_rejection(
    start: Any, failure: BaseException
) -> None:
    h = start()
    h.microvms.failure = failure

    h.run_hook(IDLE)
    h.advance(IDLE)

    assert h.job("cb-1") == 202


@pytest.mark.parametrize(
    "failure",
    [
        suspend_error("ThrottlingException", 429),
        suspend_error("ThrottlingException", 400),
        suspend_error("ExpiredTokenException", 403),
        suspend_error("SignatureDoesNotMatch", 403),
    ],
    ids=["throttle", "throttle-400", "expired-token", "signature-error"],
)
def test_accepts_jobs_at_once_and_retries_after_a_refusal_for_now(
    start: Any, logger: Any, failure: BaseException
) -> None:
    h = start()
    h.microvms.failure = failure

    h.run_hook(IDLE)
    h.advance(IDLE)
    assert_one_warning(logger, "refusedForNow", refusing_jobs=False)
    assert h.job("cb-1") == 202
    h.finish_job("cb-1")

    # The idle time started again when the call failed, and once more when
    # the job ended. So the next call comes one idle time after the job.
    h.advance(IDLE)
    assert h.microvms.suspended == ["mvm-1", "mvm-1"]


@pytest.mark.parametrize(
    ("idle", "wait"),
    [(1.0, GRACE), (IDLE, GRACE), (45.0, 45.0)],
    ids=["idle-1s", "idle-10s", "idle-45s"],
)
def test_retries_a_refusal_for_now_after_the_idle_time_or_30_seconds_if_longer(
    start: Any, logger: Any, idle: float, wait: float
) -> None:
    # A wrong secret key gives the same error on every call. Without the
    # floor, a 1-second idle time made one call per second.
    h = start()
    h.microvms.failure = suspend_error("InvalidSignatureException", 403)

    h.run_hook(idle)
    h.advance(idle)
    assert h.microvms.suspended == ["mvm-1"]
    assert h.job("cb-1") == 202
    h.finish_job("cb-1")
    # The job restarted the normal idle time. So the next call comes one
    # idle time after it, and the floor applies to the restart after it.
    h.advance(idle)
    assert h.microvms.suspended == ["mvm-1", "mvm-1"]

    h.advance(wait - 0.001)
    assert h.microvms.suspended == ["mvm-1", "mvm-1"]
    h.advance(0.001)
    assert h.microvms.suspended == ["mvm-1", "mvm-1", "mvm-1"]
    # One warning per call. The service refused each call for now. So after
    # each call, the worker accepts jobs again.
    warnings = [(m, data) for level, m, data in logger.lines if level == "warning"]
    assert len(warnings) == 3
    for message, data in warnings:
        assert_warning(message, data, "refusedForNow", refusing_jobs=False)


def test_makes_one_call_per_30_seconds_while_a_refusal_for_now_persists(
    start: Any,
) -> None:
    h = start()
    h.microvms.failure = suspend_error("ExpiredTokenException", 403)

    h.run_hook(1.0)
    h.advance(1.0)
    for _ in range(4):
        h.advance(GRACE)

    assert h.microvms.suspended == ["mvm-1"] * 5


def test_a_job_that_ends_after_a_refusal_for_now_restarts_the_normal_idle_time(
    start: Any,
) -> None:
    h = start()
    h.microvms.failure = suspend_error("InvalidSignatureException", 403)

    h.run_hook(1.0)
    h.advance(1.0)
    h.advance(5.0)
    assert h.job("cb-1") == 202
    h.finish_job("cb-1")
    h.advance(1.0 - 0.001)
    assert h.microvms.suspended == ["mvm-1"]

    h.advance(0.001)
    assert h.microvms.suspended == ["mvm-1", "mvm-1"]


def test_a_resume_hook_after_a_refusal_for_now_restarts_the_normal_idle_time(
    start: Any,
) -> None:
    h = start()
    h.microvms.failure = suspend_error("InvalidSignatureException", 403)

    h.run_hook(1.0)
    h.advance(1.0)
    h.advance(5.0)
    assert h.hook("resume") == 200
    h.advance(1.0 - 0.001)
    assert h.microvms.suspended == ["mvm-1"]

    h.advance(0.001)
    assert h.microvms.suspended == ["mvm-1", "mvm-1"]


def test_waits_30_seconds_after_a_refusal_for_now_that_follows_a_resume_hook(
    start: Any,
) -> None:
    # The resume hook asks for the idle time while the call runs. The call
    # then ends refused for now. That outcome is the later request, so the
    # floor applies.
    h = start()
    h.microvms.hold = True
    h.microvms.failure = suspend_error("InvalidSignatureException", 403)

    h.run_hook(1.0)
    h.advance(1.0)
    assert eventually(lambda: h.microvms.suspended == ["mvm-1"])
    assert h.hook("resume") == 200
    h.microvms.hold = False
    h.microvms.finish()
    h.settle()
    h.advance(GRACE - 0.001)
    assert h.microvms.suspended == ["mvm-1"]

    h.advance(0.001)
    assert h.microvms.suspended == ["mvm-1", "mvm-1"]


def test_uses_the_normal_idle_time_after_a_job_that_ran_across_the_refused_call(
    start: Any,
) -> None:
    # A resume hook during the call lets a job start. The call ends refused
    # for now while the job runs. The job ends after the call. So the job's
    # end is the latest restart, and the normal idle time applies.
    h = start()
    h.microvms.hold = True
    h.microvms.failure = suspend_error("InvalidSignatureException", 403)

    h.run_hook(1.0)
    h.advance(1.0)
    assert eventually(lambda: h.microvms.suspended == ["mvm-1"])
    assert h.hook("resume") == 200
    assert h.job("cb-1") == 202
    assert eventually(lambda: h.started("cb-1"))
    h.microvms.hold = False
    h.microvms.finish()
    h.settle()

    # No call while the job runs.
    h.advance(GRACE * 2)
    assert h.microvms.suspended == ["mvm-1"]

    h.finish_job("cb-1")
    h.advance(1.0 - 0.001)
    assert h.microvms.suspended == ["mvm-1"]
    h.advance(0.001)
    assert h.microvms.suspended == ["mvm-1", "mvm-1"]


def test_waits_30_seconds_after_a_refusal_for_now_that_follows_a_job_end(
    start: Any,
) -> None:
    # A job starts and ends during the call. The call then ends refused for
    # now. That outcome is the later request, so the floor applies.
    h = start()
    h.microvms.hold = True
    h.microvms.failure = suspend_error("InvalidSignatureException", 403)

    h.run_hook(1.0)
    h.advance(1.0)
    assert eventually(lambda: h.microvms.suspended == ["mvm-1"])
    assert h.hook("resume") == 200
    assert h.job("cb-1") == 202
    h.finish_job("cb-1")
    h.microvms.hold = False
    h.microvms.finish()
    h.settle()
    h.advance(GRACE - 0.001)
    assert h.microvms.suspended == ["mvm-1"]

    h.advance(0.001)
    assert h.microvms.suspended == ["mvm-1", "mvm-1"]


class _ClosingClient(FakeMicrovmsClient):
    """A default client whose ``close()`` waits until the test releases it.

    The call is still counted as running during ``close()``. So a request to
    start the idle time then waits for the call to end.
    """

    def __init__(self) -> None:
        super().__init__()
        self.closing = threading.Event()
        self.release_close = threading.Event()

    def close(self) -> None:
        self.closing.set()
        self.release_close.wait(WAIT)
        super().close()


def test_a_job_that_ends_before_a_refused_call_has_ended_uses_the_normal_idle_time(
    start: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = _ClosingClient()
    client.failure = suspend_error("InvalidSignatureException", 403)
    monkeypatch.setattr(worker_module, "create_microvms_client", lambda r: client)
    h = start(create_microvms_client=None)

    h.run_hook(1.0)
    h.timers.advance(1.0)
    # The refusal has ended. The call is still closing its client.
    assert client.closing.wait(WAIT)
    assert h.job("cb-1") == 202
    h.finish_job("cb-1")
    client.release_close.set()
    h.settle()

    # The job ended after the refusal. So the normal idle time applies.
    h.advance(1.0 - 0.001)
    assert client.suspended == ["mvm-1"]
    h.advance(0.001)
    assert client.suspended == ["mvm-1", "mvm-1"]


def test_keeps_a_hook_refusal_after_a_refusal_for_now_of_the_own_call(
    start: Any, logger: Any
) -> None:
    h = start()
    h.microvms.hold = True
    h.microvms.failure = suspend_error("ThrottlingException", 429)
    h.run_hook(IDLE)
    h.advance(IDLE)
    assert eventually(lambda: h.microvms.suspended == ["mvm-1"])

    h.hook("suspend")
    h.microvms.finish()
    h.settle()

    assert h.job("cb-1") == 503
    assert_one_warning(
        logger, "refusedForNow", suspend_hook_arrived=True, refusing_jobs=True
    )


def hold_failed_call(h: Harness, failure: BaseException) -> None:
    """Start the first SuspendMicrovm call, and keep it open until
    :func:`end_failed_call`."""
    h.microvms.hold = True
    h.microvms.failure = failure
    h.run_hook(IDLE)
    h.advance(IDLE)
    assert eventually(lambda: h.microvms.suspended == ["mvm-1"])


def end_failed_call(h: Harness) -> None:
    """End the held call with its failure. Every later call succeeds, so it
    logs no warning."""
    h.microvms.hold = False
    h.microvms.finish()
    h.settle()
    h.microvms.failure = None


@pytest.mark.parametrize(
    "hooks",
    [["terminate"], ["suspend", "terminate"], ["resume", "terminate"]],
    ids=["terminate", "suspend-then-terminate", "resume-then-terminate"],
)
@pytest.mark.parametrize(
    ("failure", "outcome"),
    [
        (suspend_error("AccessDeniedException", 403), "rejected"),
        (suspend_error("InvalidSignatureException", 403), "refusedForNow"),
        (suspend_error("InternalServerException", 500, retries=2), "unknown"),
    ],
    ids=["rejected", "refused-for-now", "unknown"],
)
def test_never_calls_again_after_a_terminate_hook_during_a_failed_call(
    start: Any, logger: Any, failure: BaseException, outcome: str, hooks: list[str]
) -> None:
    # A terminating listener refuses jobs for good, and starts no idle time.
    h = start()
    hold_failed_call(h, failure)
    for hook in hooks:
        assert h.hook(hook) == 200
    end_failed_call(h)

    assert h.job("cb-1") == 503
    assert h.hook("resume") == 200
    h.advance(GRACE * 4)
    assert h.job("cb-2") == 503
    assert h.microvms.suspended == ["mvm-1"]
    assert_one_warning(
        logger,
        outcome,
        suspend_hook_arrived="suspend" in hooks,
        terminating=True,
        refusing_jobs=True,
    )


@pytest.mark.parametrize(
    ("failure", "outcome"),
    [
        (suspend_error("InvalidSignatureException", 403), "refusedForNow"),
        (suspend_error("AccessDeniedException", 403), "rejected"),
    ],
    ids=["refused-for-now", "rejected"],
)
def test_never_calls_again_after_close_during_a_failed_call(
    start: Any, logger: Any, failure: BaseException, outcome: str
) -> None:
    h = start()
    hold_failed_call(h, failure)
    h.listener.close()
    end_failed_call(h)

    assert h.job("cb-1") == 202
    h.finish_job("cb-1")
    assert h.hook("resume") == 200
    h.advance(GRACE * 4)
    assert h.microvms.suspended == ["mvm-1"]
    assert_one_warning(logger, outcome, closed=True, refusing_jobs=False)


def test_never_calls_again_after_close_and_a_terminate_hook_during_a_refused_call(
    start: Any, logger: Any
) -> None:
    h = start()
    hold_failed_call(h, suspend_error("InvalidSignatureException", 403))
    h.listener.close()
    assert h.hook("terminate") == 200
    end_failed_call(h)

    assert h.job("cb-1") == 503
    h.advance(GRACE * 4)
    assert h.microvms.suspended == ["mvm-1"]
    assert_one_warning(
        logger, "refusedForNow", terminating=True, closed=True, refusing_jobs=True
    )


@pytest.mark.parametrize("end", ["resume", "grace"])
@pytest.mark.parametrize(
    ("failure", "outcome"),
    [
        (suspend_error("AccessDeniedException", 403), "rejected"),
        (suspend_error("InvalidSignatureException", 403), "refusedForNow"),
    ],
    ids=["rejected", "refused-for-now"],
)
def test_keeps_a_suspend_hook_refusal_in_effect_after_a_failed_call(
    start: Any, logger: Any, failure: BaseException, outcome: str, end: str
) -> None:
    # Two suspend hooks arrive during the call, 10 seconds apart. The failed
    # call does not end their refusal. The refusal ends on the resume hook,
    # or 30 seconds after the last suspend hook. The idle time then starts
    # without the 30-second floor.
    h = start()
    hold_failed_call(h, failure)
    assert h.hook("suspend") == 200
    h.advance(10)
    assert h.hook("suspend") == 200
    end_failed_call(h)

    assert h.job("cb-1") == 503
    if end == "resume":
        assert h.hook("resume") == 200
    else:
        h.advance(GRACE - 0.001)
        assert h.job("cb-2") == 503
        h.advance(0.001)
    h.advance(IDLE - 0.001)
    assert h.microvms.suspended == ["mvm-1"]
    h.advance(0.001)
    assert h.microvms.suspended == ["mvm-1", "mvm-1"]
    assert_one_warning(logger, outcome, suspend_hook_arrived=True, refusing_jobs=True)


@pytest.mark.parametrize("then", ["no-job", "job"])
@pytest.mark.parametrize("end", ["resume", "grace"])
def test_accepts_jobs_after_a_refusal_for_now_when_the_hook_refusal_ended(
    start: Any, logger: Any, end: str, then: str
) -> None:
    # A suspend hook arrives during the call. Its refusal ends before the
    # call ends: on a resume hook, or after 30 seconds, because botocore
    # cannot cancel an attempt in flight. The refused outcome then neither
    # ends a refusal nor asks for the 30-second floor.
    h = start()
    hold_failed_call(h, suspend_error("InvalidSignatureException", 403))
    assert h.hook("suspend") == 200
    if end == "resume":
        assert h.hook("resume") == 200
    else:
        h.advance(GRACE)
    end_failed_call(h)

    if then == "job":
        assert h.job("cb-1") == 202
        h.advance(GRACE)
        assert h.microvms.suspended == ["mvm-1"]
        h.finish_job("cb-1")
    h.advance(IDLE - 0.001)
    assert h.microvms.suspended == ["mvm-1"]
    h.advance(0.001)
    assert h.microvms.suspended == ["mvm-1", "mvm-1"]
    assert_one_warning(
        logger, "refusedForNow", suspend_hook_arrived=True, refusing_jobs=False
    )


@pytest.mark.parametrize("end", ["resume", "grace"])
def test_accepts_jobs_after_a_rejection_when_the_hook_refusal_ended(
    start: Any, logger: Any, end: str
) -> None:
    h = start()
    hold_failed_call(h, suspend_error("AccessDeniedException", 403))
    assert h.hook("suspend") == 200
    if end == "resume":
        assert h.hook("resume") == 200
    else:
        h.advance(GRACE)
    end_failed_call(h)

    assert h.job("cb-1") == 202
    h.finish_job("cb-1")
    h.advance(IDLE - 0.001)
    assert h.microvms.suspended == ["mvm-1"]
    h.advance(0.001)
    assert h.microvms.suspended == ["mvm-1", "mvm-1"]
    assert_one_warning(
        logger, "rejected", suspend_hook_arrived=True, refusing_jobs=False
    )


@pytest.mark.parametrize("then", ["job", "no-job"])
@pytest.mark.parametrize(
    ("during", "failure"),
    [
        # The own call suspended the MicroVM, and its answer was lost across
        # the freeze. The resume hook arrives, and the connection then drops.
        ("resume", ConnectionResetError("reset")),
        ("suspend-resume", suspend_error("InternalServerException", 500, 2)),
        # botocore cannot cancel an attempt in flight. A client from
        # create_microvms_client with 60-second timeouts can outlast the
        # hook's 30 seconds.
        ("suspend-grace", ConnectionResetError("reset")),
    ],
    ids=["resume", "suspend-resume", "suspend-grace"],
)
def test_accepts_jobs_after_an_unknown_outcome_when_the_refusal_ended_during_the_call(
    start: Any, logger: Any, during: str, failure: BaseException, then: str
) -> None:
    # The refusal of jobs ends during the call: on a resume hook, or 30
    # seconds after a suspend hook. An unknown outcome then starts no new
    # refusal. So the worker accepts jobs. It calls again one idle time after
    # the call and every job end.
    h = start()
    hold_failed_call(h, failure)
    if during == "resume":
        assert h.hook("resume") == 200
    elif during == "suspend-resume":
        assert h.hook("suspend") == 200
        assert h.hook("resume") == 200
    else:
        assert h.hook("suspend") == 200
        h.advance(GRACE - 0.001)
        assert h.job("cb-0") == 503
        h.advance(0.001)
        # The call still runs. So no second call started.
        assert h.microvms.suspended == ["mvm-1"]
        assert logger.messages("warning") == []
    end_failed_call(h)

    expected = {
        "suspend_hook_arrived": during != "resume",
        "refusing_jobs": False,
    }
    assert_one_warning(logger, "unknown", **expected)
    if then == "job":
        assert h.job("cb-1") == 202
        assert eventually(lambda: h.started("cb-1"))
        h.advance(GRACE)
        assert h.microvms.suspended == ["mvm-1"]
        h.finish_job("cb-1")
    h.advance(IDLE - 0.001)
    assert h.microvms.suspended == ["mvm-1"]
    h.advance(0.001)
    assert h.microvms.suspended == ["mvm-1", "mvm-1"]
    assert_one_warning(logger, "unknown", **expected)


def test_calls_again_one_idle_time_after_the_last_job_after_an_unknown_outcome(
    start: Any, logger: Any
) -> None:
    # A resume hook ends the refusal during the call. A job then starts
    # during the call. A running job cancels the idle time. So the next call
    # comes one idle time after the last job ends, not after the call.
    h = start()
    hold_failed_call(h, ConnectionResetError("reset"))
    assert h.hook("resume") == 200
    assert h.job("cb-1") == 202
    assert eventually(lambda: h.started("cb-1"))
    end_failed_call(h)

    assert_one_warning(logger, "unknown", refusing_jobs=False)
    assert h.job("cb-2") == 202
    h.advance(IDLE * 3)
    assert h.microvms.suspended == ["mvm-1"]
    h.finish_job("cb-1")
    h.advance(IDLE)
    assert h.microvms.suspended == ["mvm-1"]
    h.finish_job("cb-2")
    h.advance(IDLE - 0.001)
    assert h.microvms.suspended == ["mvm-1"]
    h.advance(0.001)
    assert h.microvms.suspended == ["mvm-1", "mvm-1"]
    assert_one_warning(logger, "unknown", refusing_jobs=False)


def test_makes_no_call_after_an_unknown_outcome_after_close_and_a_resume_hook(
    start: Any, logger: Any
) -> None:
    # A closed listener starts no idle time. So the resume hook ends the
    # refusal, and no call follows.
    h = start()
    hold_failed_call(h, suspend_error("InternalServerException", 500, 2))
    h.listener.close()
    assert h.hook("resume") == 200
    end_failed_call(h)

    assert_one_warning(logger, "unknown", closed=True, refusing_jobs=False)
    assert h.job("cb-1") == 202
    h.finish_job("cb-1")
    h.advance(IDLE + GRACE * 4)
    assert h.microvms.suspended == ["mvm-1"]


@pytest.mark.parametrize("during", ["no-hook", "suspend", "close"])
def test_keeps_refusing_jobs_after_an_unknown_outcome_while_the_refusal_lasts(
    start: Any, logger: Any, during: str
) -> None:
    # The refusal is still in effect when the call ends. So the worker
    # refuses jobs for 30 seconds after the call, or until the resume hook.
    h = start()
    hold_failed_call(h, suspend_error("InternalServerException", 500, 2))
    if during == "suspend":
        assert h.hook("suspend") == 200
    elif during == "close":
        h.listener.close()
    end_failed_call(h)

    assert_one_warning(
        logger,
        "unknown",
        suspend_hook_arrived=during == "suspend",
        closed=during == "close",
        refusing_jobs=True,
    )
    assert h.job("cb-1") == 503
    h.advance(GRACE - 0.001)
    assert h.job("cb-2") == 503
    h.advance(0.001)
    assert h.job("cb-3") == 202
    h.finish_job("cb-3")
    h.advance(IDLE)
    expected = ["mvm-1"] if during == "close" else ["mvm-1", "mvm-1"]
    assert h.microvms.suspended == expected


def test_reads_the_unknown_outcome_fields_in_the_block_that_decides_the_refusal(
    start: Any, logger: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Issue #820. A suspend hook arrives during the call. The call ends with
    # an unknown outcome. The hook's 30 seconds then end while the worker
    # closes its client. The worker formerly read the refusal state before
    # close(), and decided the refusal after close(). So its warning said
    # that it refuses jobs, and a job then got HTTP 202. The worker now reads
    # the fields in the block after close(), which decides the refusal.
    client = _ClosingClient()
    client.hold = True
    client.failure = ConnectionResetError("reset")
    monkeypatch.setattr(worker_module, "create_microvms_client", lambda r: client)
    h = start(create_microvms_client=None)
    h.run_hook(IDLE)
    h.timers.advance(IDLE)
    assert eventually(lambda: client.suspended == ["mvm-1"])
    assert h.hook("suspend") == 200
    h.timers.advance(GRACE - 0.001)

    client.hold = False
    client.finish()
    assert client.closing.wait(WAIT)
    # The hook's 30 seconds end during close().
    h.timers.advance(0.001)
    assert logger.messages("warning") == []
    assert h.job("cb-1") == 202
    client.release_close.set()
    h.settle()

    assert_one_warning(
        logger, "unknown", suspend_hook_arrived=True, refusing_jobs=False
    )
    assert h.job("cb-2") == 202
    h.finish_job("cb-1")
    h.finish_job("cb-2")


class _CallbackError(ClientError):
    """A copy of a ClientError whose ``__str__()`` first calls ``callback``.

    A client from create_microvms_client can raise any exception type. So
    the caller of the listener controls ``__str__()``.
    """

    def __init__(self, base: ClientError, callback: Callable[[], None]) -> None:
        super().__init__(base.response, base.operation_name)
        self._callback = callback

    def __str__(self) -> str:
        self._callback()
        return super().__str__()


# One failure for each outcome. A rejection and a refusal for now take the
# lock in the except block. An unknown outcome takes it after close().
_EACH_OUTCOME = pytest.mark.parametrize(
    ("failure", "outcome"),
    [
        (suspend_error("AccessDeniedException", 403), "rejected"),
        (suspend_error("InvalidSignatureException", 403), "refusedForNow"),
        (suspend_error("InternalServerException", 500, retries=2), "unknown"),
    ],
    ids=["rejected", "refused-for-now", "unknown"],
)


@_EACH_OUTCOME
def test_describes_the_call_error_before_it_takes_the_lock(
    start: Any, logger: Any, failure: ClientError, outcome: str
) -> None:
    # The listener's lock is not reentrant. The error's __str__() calls
    # close(), which takes the lock. If the listener described the error
    # under the lock, close() would wait for that lock forever. Every hook
    # and job request would then wait too.
    h = start()
    closed = threading.Event()

    def close() -> None:
        h.listener.close()
        closed.set()

    h.microvms.failure = _CallbackError(failure, close)
    h.run_hook(IDLE)
    h.timers.advance(IDLE)
    if not closed.wait(WAIT):
        # threading.Lock lets any thread release it. So the test releases
        # the lock that the suspend thread holds. close() then returns, and
        # the fixture can close the worker.
        h.listener._lock.release()  # noqa: SLF001
        closed.wait(WAIT)
        pytest.fail("close() in the error's __str__() waited for the lock")
    h.settle()

    # Only after an unknown outcome does the refusal continue.
    assert_one_warning(logger, outcome, closed=True, refusing_jobs=outcome == "unknown")


@_EACH_OUTCOME
def test_answers_a_hook_that_the_call_error_sends(
    start: Any, logger: Any, failure: ClientError, outcome: str
) -> None:
    # The error's __str__() sends a suspend hook. A request thread handles
    # the hook, and takes the lock. If the listener described the error
    # under the lock, the hook would wait until its HTTP timeout.
    h = start()
    statuses: list[int | BaseException] = []
    answered = threading.Event()

    def send_hook() -> None:
        try:
            statuses.append(h.hook("suspend"))
        except Exception as error:  # noqa: BLE001
            statuses.append(error)
        finally:
            answered.set()

    h.microvms.failure = _CallbackError(failure, send_hook)
    h.run_hook(IDLE)
    h.timers.advance(IDLE)
    # The hook's HTTP timeout is WAIT. So this wait ends in either case.
    assert answered.wait(2 * WAIT)
    assert statuses == [200]
    h.settle()

    # The hook arrived before the listener acted on the outcome. So its
    # refusal stays.
    assert_one_warning(logger, outcome, suspend_hook_arrived=True, refusing_jobs=True)
    assert h.job("cb-1") == 503


@pytest.mark.parametrize(
    ("idle", "waited"), [(1.0, GRACE), (45.0, 45.0)], ids=["idle-1s", "idle-45s"]
)
def test_logs_the_idle_time_and_the_wait_before_each_call(
    start: Any, logger: Any, idle: float, waited: float
) -> None:
    # After a refusal for now, the wait lasts at least 30 seconds. So it can
    # differ from the session's idle time.
    h = start()
    h.microvms.failure = suspend_error("InvalidSignatureException", 403)

    h.run_hook(idle)
    h.advance(idle)
    h.advance(waited)

    assert h.microvms.suspended == ["mvm-1", "mvm-1"]
    calls = [
        data
        for level, message, data in logger.lines
        if level == "info" and message == "suspending the idle MicroVM"
    ]
    assert calls == [
        {"microvmId": "mvm-1", "idleSeconds": idle, "waitedSeconds": idle},
        {"microvmId": "mvm-1", "idleSeconds": idle, "waitedSeconds": waited},
    ]


@pytest.mark.parametrize(
    ("restart", "waited"),
    [
        ("job-end", 1.0),
        ("resume", 1.0),
        ("unknown-outcome-grace", 1.0),
        ("resume-during-refused-call", GRACE),
    ],
    ids=["job-end", "resume", "unknown-outcome-grace", "resume-during-refused-call"],
)
def test_logs_the_wait_of_the_idle_time_that_started_the_call(
    start: Any, logger: Any, restart: str, waited: float
) -> None:
    # waitedSeconds is the delay of the idle timer that started the call. A
    # job end, a resume hook, and the end of the 30-second grace each start
    # the normal idle time. A resume hook during a refused call asks for the
    # idle time first. The refusal for now asks for it last, with the floor.
    # So that wait lasts 30 seconds.
    h = start()
    if restart == "unknown-outcome-grace":
        h.microvms.failure = suspend_error("InternalServerException", 500, retries=2)
    else:
        h.microvms.failure = suspend_error("InvalidSignatureException", 403)
    h.microvms.hold = restart == "resume-during-refused-call"

    h.run_hook(1.0)
    h.advance(1.0)
    if restart == "resume-during-refused-call":
        assert eventually(lambda: h.microvms.suspended == ["mvm-1"])
        assert h.hook("resume") == 200
        h.microvms.hold = False
        h.microvms.finish()
        h.settle()
    h.microvms.failure = None
    if restart == "job-end":
        assert h.job("cb-1") == 202
        h.finish_job("cb-1")
    elif restart == "resume":
        assert h.hook("resume") == 200
    elif restart == "unknown-outcome-grace":
        h.advance(GRACE)
    h.advance(waited - 0.001)
    assert h.microvms.suspended == ["mvm-1"]
    h.advance(0.001)

    assert h.microvms.suspended == ["mvm-1", "mvm-1"]
    waits = [
        (data["idleSeconds"], data["waitedSeconds"])
        for level, message, data in logger.lines
        if level == "info" and message == "suspending the idle MicroVM"
    ]
    assert waits == [(1.0, 1.0), (1.0, waited)]


def test_keeps_refusing_jobs_until_a_call_past_its_deadline_has_ended(
    start: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # botocore cannot cancel an attempt in flight, so the attempt can reach
    # the service after the deadline. The refusal and its 30 seconds start
    # only when the call has ended.
    monkeypatch.setattr(worker_module, "SUSPEND_CALL_TIMEOUT_SECONDS", 0.05)
    h = start()
    h.microvms.hold = True

    h.run_hook(IDLE)
    h.advance(IDLE)
    assert eventually(lambda: h.microvms.suspended == ["mvm-1"])
    # The deadline passes while the call is held.
    time.sleep(0.3)
    h.advance(GRACE * 3)
    assert h.job("cb-1") == 503
    assert not h.worker.idle(0.05)

    h.microvms.finish()
    h.settle()
    h.advance(GRACE - 0.001)
    assert h.job("cb-1") == 503
    h.advance(0.001)
    assert h.job("cb-1") == 202


def test_accepts_jobs_at_once_when_the_microvms_client_cannot_be_created(
    start: Any, logger: Any
) -> None:
    def broken(region: str) -> Any:
        raise RuntimeError("no client")

    h = start(create_microvms_client=broken)

    h.run_hook(IDLE)
    h.advance(IDLE)

    assert "could not create the Lambda MicroVMs client" in logger.messages("error")
    assert h.job("cb-1") == 202


def test_logs_a_default_client_that_cannot_be_closed(
    start: Any, logger: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = FakeMicrovmsClient()

    def close() -> None:
        raise RuntimeError("close failed")

    client.close = close  # type: ignore[method-assign]
    monkeypatch.setattr(worker_module, "create_microvms_client", lambda r: client)
    h = start(create_microvms_client=None)

    h.run_hook(IDLE)
    h.advance(IDLE)

    assert "could not close the Lambda MicroVMs client" in logger.messages("error")
    assert h.job("cb-1") == 503


def test_tries_the_call_again_one_idle_time_after_an_unknown_outcome_refusal(
    start: Any,
) -> None:
    h = start()
    h.microvms.failure = suspend_error("InternalServerException", 500, retries=2)

    h.run_hook(IDLE)
    h.advance(IDLE)
    h.advance(GRACE + IDLE - 0.001)
    assert h.microvms.suspended == ["mvm-1"]

    h.advance(0.001)
    assert h.microvms.suspended == ["mvm-1", "mvm-1"]


def test_starts_the_idle_time_on_a_resume_hook_that_no_suspend_preceded(
    start: Any,
) -> None:
    h = start()

    h.run_hook(IDLE)
    h.job("cb-1")
    h.finish_job("cb-1")
    h.advance(IDLE / 2)
    # An idle policy can resume the MicroVM. The hook starts the idle time
    # again from zero.
    h.hook("resume")
    h.advance(IDLE / 2)
    assert h.microvms.suspended == []
    h.advance(IDLE / 2)
    assert h.microvms.suspended == ["mvm-1"]


def test_never_suspends_after_close(start: Any) -> None:
    h = start()

    h.run_hook(IDLE)
    h.listener.close()
    h.advance(IDLE * 4)

    assert h.microvms.suspended == []


def test_does_not_start_the_idle_time_when_a_job_ends_after_close(
    start: Any,
) -> None:
    h = start()

    h.run_hook(IDLE)
    h.job("cb-1")
    h.listener.close()
    h.finish_job("cb-1")
    h.advance(IDLE * 4)

    assert h.microvms.suspended == []


def test_ends_a_refusal_30_seconds_after_a_call_that_returned_after_close(
    start: Any,
) -> None:
    h = start()
    h.microvms.hold = True

    h.run_hook(IDLE)
    h.advance(IDLE)
    assert eventually(lambda: h.microvms.suspended == ["mvm-1"])
    h.listener.close()
    h.microvms.finish()
    h.settle()
    assert h.job("cb-1") == 503

    h.advance(GRACE)
    assert h.job("cb-1") == 202
    # The closed listener does not suspend again.
    h.finish_job("cb-1")
    h.advance(IDLE * 4)
    assert h.microvms.suspended == ["mvm-1"]


def test_ends_a_refusal_30_seconds_after_the_call_when_close_follows_the_call(
    start: Any,
) -> None:
    h = start()

    h.run_hook(IDLE)
    h.advance(IDLE)
    # The call has returned, and its grace timer runs.
    h.listener.close()
    assert h.job("cb-1") == 503

    h.advance(GRACE)
    assert h.job("cb-1") == 202


def test_idle_waits_for_a_running_suspend_call(start: Any) -> None:
    h = start()
    h.microvms.hold = True

    h.run_hook(IDLE)
    h.advance(IDLE)

    assert not h.worker.idle(0.1)
    h.microvms.finish()
    assert h.worker.idle(WAIT)


# ---------------------------------------------------------------------------
# The terminate hook
# ---------------------------------------------------------------------------


def test_terminate_fails_each_running_job_and_sets_its_cancelled_event(
    start: Any,
) -> None:
    h = start()
    h.run_hook()
    assert h.job("cb-1") == 202
    assert h.job("cb-2") == 202
    assert eventually(lambda: len(h.jobs.started()) == 2)

    assert h.hook("terminate") == 200

    assert sorted(h.lambda_client.recorded(), key=lambda c: c["callbackId"]) == [
        terminated_failure("cb-1"),
        terminated_failure("cb-2"),
    ]
    assert h.jobs.contexts["cb-1"].cancelled.is_set()
    assert h.jobs.contexts["cb-2"].cancelled.is_set()


def test_terminate_reports_nothing_more_when_the_handler_returns_after_it(
    start: Any, logger: Any
) -> None:
    h = start()
    h.run_hook()
    h.job("cb-1")
    assert eventually(lambda: h.started("cb-1"))
    h.hook("terminate")

    h.finish_job("cb-1")

    assert h.lambda_client.recorded() == [terminated_failure("cb-1")]
    assert eventually(
        lambda: "job outcome not reported: the MicroVM is terminating"
        in logger.messages("info")
    )


def test_terminate_answers_after_the_reports_end(start: Any) -> None:
    h = start()
    h.run_hook()
    h.job("cb-1")
    assert eventually(lambda: h.started("cb-1"))
    h.lambda_client.hold = True

    answer = h.hook_in_background("terminate")
    assert eventually(lambda: h.lambda_client.waiting() == 1)
    assert not answered_within(answer, 0.2)

    h.lambda_client.finish()
    assert answer.result(WAIT) == 200
    assert h.lambda_client.recorded() == [terminated_failure("cb-1")]


def test_terminate_answers_after_the_budget_when_a_report_stalls(
    start: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(worker_module, "TERMINATE_REPORT_BUDGET_SECONDS", 0.3)
    h = start()
    h.run_hook()
    h.job("cb-1")
    assert eventually(lambda: h.started("cb-1"))
    h.lambda_client.hold = True

    started = time.monotonic()
    assert h.hook("terminate") == 200

    assert 0.25 <= time.monotonic() - started < WAIT
    assert h.lambda_client.recorded() == []


def test_terminate_waits_for_a_job_that_is_reporting_its_own_outcome(
    start: Any,
) -> None:
    h = start()
    h.run_hook()
    h.job("cb-1")
    assert eventually(lambda: h.started("cb-1"))
    h.lambda_client.hold = True
    h.jobs.release("cb-1")
    assert eventually(lambda: h.lambda_client.waiting() == 1)

    answer = h.hook_in_background("terminate")
    assert not answered_within(answer, 0.2)

    h.lambda_client.finish()
    assert answer.result(WAIT) == 200
    assert h.lambda_client.recorded() == [{"kind": "success", "callbackId": "cb-1"}]


def test_holds_a_second_concurrent_terminate_until_the_first_one_ends(
    start: Any,
) -> None:
    h = start()
    h.run_hook()
    h.job("cb-1")
    assert eventually(lambda: h.started("cb-1"))
    h.lambda_client.hold = True

    first = h.hook_in_background("terminate")
    assert eventually(lambda: h.lambda_client.waiting() == 1)
    second = h.hook_in_background("terminate")
    assert not answered_within(second, 0.2)
    assert not first.done()

    h.lambda_client.finish()
    assert first.result(WAIT) == 200
    assert second.result(WAIT) == 200
    assert h.lambda_client.recorded() == [terminated_failure("cb-1")]


def test_terminate_reports_nothing_for_a_job_whose_callback_a_heartbeat_found_gone(
    start: Any, make_error: Any
) -> None:
    h = start(
        lambda_client=HeldLambdaClient(
            heartbeat_error=make_error("CallbackTimeoutException")
        )
    )
    h.run_hook()
    h.job("cb-gone", heartbeatTimeoutSeconds=30)
    assert eventually(lambda: h.started("cb-gone"))
    assert eventually(lambda: h.jobs.contexts["cb-gone"].cancelled.is_set())

    assert h.hook("terminate") == 200
    h.finish_job("cb-gone")

    assert h.lambda_client.recorded() == []


def test_terminate_stops_the_heartbeats_of_a_job_before_it_fails_it(
    start: Any,
) -> None:
    h = start()
    h.run_hook()
    h.job("cb-1", heartbeatTimeoutSeconds=30)
    assert eventually(lambda: h.started("cb-1"))
    heartbeats = h.listener._active["cb-1"].heartbeats  # noqa: SLF001
    assert heartbeats is not None

    h.hook("terminate")

    assert heartbeats._stopped.is_set()  # noqa: SLF001
    assert h.lambda_client.recorded() == [terminated_failure("cb-1")]


def test_terminate_logs_a_failure_report_that_the_service_refuses_at_info(
    start: Any, logger: Any, make_error: Any
) -> None:
    h = start()
    h.run_hook()
    h.job("cb-1")
    assert eventually(lambda: h.started("cb-1"))
    h.lambda_client.completion_error = make_error("CallbackTimeoutException")

    h.hook("terminate")

    assert "the callback no longer accepts a failure" in logger.messages("info")
    assert "could not fail the callback" not in logger.messages("error")


def test_terminate_answers_at_once_when_no_job_runs(start: Any) -> None:
    h = start()
    h.run_hook()

    assert h.hook("terminate") == 200
    assert h.lambda_client.recorded() == []


def test_terminate_fails_a_job_once_when_the_hook_arrives_twice(start: Any) -> None:
    h = start()
    h.run_hook()
    h.job("cb-1")
    assert eventually(lambda: h.started("cb-1"))

    h.hook("terminate")
    h.hook("terminate")

    assert h.lambda_client.recorded() == [terminated_failure("cb-1")]


def test_refuses_jobs_with_503_after_the_terminate_hook(start: Any) -> None:
    h = start()
    h.run_hook()
    h.hook("terminate")

    assert h.job("cb-1") == 503
    assert not h.started("cb-1")


def test_never_suspends_after_the_terminate_hook(start: Any) -> None:
    h = start()
    h.run_hook(IDLE)
    h.hook("terminate")

    h.advance(IDLE * 6)

    assert h.microvms.suspended == []


def test_fails_a_job_whose_thread_had_not_run_when_the_terminate_hook_came(
    start: Any, logger: Any
) -> None:
    # The job is registered before its thread runs. A terminate hook in
    # between fails the callback, and the thread then does not run the
    # handler.
    entered = threading.Event()
    release = threading.Event()
    lambda_client = HeldLambdaClient()
    calls = 0
    lock = threading.Lock()

    def create_client(region: str) -> HeldLambdaClient:
        nonlocal calls
        with lock:
            calls += 1
            first = calls == 1
        if first:
            entered.set()
            release.wait(WAIT)
        return lambda_client

    h = start(lambda_client=lambda_client)
    h.listener._create_client = create_client  # noqa: SLF001
    h.run_hook()
    h.job("cb-1")
    assert entered.wait(WAIT)

    assert h.hook("terminate") == 200
    release.set()
    assert eventually(lambda: "cb-1" not in h.listener._jobs)  # noqa: SLF001

    assert not h.started("cb-1")
    assert h.lambda_client.recorded() == [terminated_failure("cb-1")]
    assert "job not started: the MicroVM is terminating" in logger.messages("info")


def test_does_not_start_a_job_whose_thread_ran_after_the_terminate_hook(
    start: Any, logger: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The terminate hook can mark the job between its thread's two checks.
    h = start()
    original = h.listener._reporter_for  # noqa: SLF001
    marked = threading.Event()

    def reporter_for(callback_id: str, region: str) -> Any:
        if callback_id == "cb-1" and not marked.is_set():
            marked.set()
            with h.listener._lock:  # noqa: SLF001
                h.listener._active[callback_id].terminated = True  # noqa: SLF001
        return original(callback_id, region)

    monkeypatch.setattr(h.listener, "_reporter_for", reporter_for)
    h.run_hook()
    h.job("cb-1")

    assert eventually(lambda: "cb-1" not in h.listener._jobs)  # noqa: SLF001
    assert not h.started("cb-1")
    assert "job not started: the MicroVM is terminating" in logger.messages("info")


@pytest.mark.parametrize(
    "headers",
    [
        FORWARDED_HEADERS,
        {**LAMBDA_HEADERS, "x-amzn-requestid": "r-1"},
        {"Host": ""},
    ],
    ids=["forwarded", "request-id", "empty-host"],
)
def test_ignores_a_terminate_hook_that_did_not_come_from_lambda(
    start: Any, logger: Any, headers: dict[str, str]
) -> None:
    h = start()
    h.run_hook()
    h.job("cb-1")
    assert eventually(lambda: h.started("cb-1"))

    assert h.hook("terminate", headers) == 200

    assert h.lambda_client.recorded() == []
    assert not h.jobs.contexts["cb-1"].cancelled.is_set()
    assert h.job("cb-2") == 202
    ignored = [
        line
        for line in logger.lines
        if line[1] == "lifecycle hook ignored: it did not come from Lambda"
    ]
    assert ignored[0][2]["hook"] == "terminate"
    h.finish_job("cb-1")
    assert {"kind": "success", "callbackId": "cb-1"} in h.lambda_client.recorded()


def test_ignores_a_terminate_hook_without_a_host_header(start: Any) -> None:
    h = start()
    h.run_hook()
    h.job("cb-1")
    assert eventually(lambda: h.started("cb-1"))

    connection = http.client.HTTPConnection(
        host_of(h.worker), h.worker.port, timeout=WAIT
    )
    try:
        connection.putrequest("POST", f"{HOOK_PATH_PREFIX}terminate", skip_host=True)
        connection.putheader("Content-Length", "2")
        connection.endheaders(b"{}")
        response = connection.getresponse()
        response.read()
    finally:
        connection.close()

    assert response.status == 200
    assert h.lambda_client.recorded() == []


@pytest.mark.parametrize(
    "host", ["127.0.0.1:8080", "localhost", "[::1]:8080", "LOCALHOST:8080"]
)
def test_acts_on_a_terminate_hook_with_a_local_host(start: Any, host: str) -> None:
    h = start()
    h.run_hook()
    h.job("cb-1")
    assert eventually(lambda: h.started("cb-1"))

    h.hook("terminate", {"Host": host})

    assert h.lambda_client.recorded() == [terminated_failure("cb-1")]


# ---------------------------------------------------------------------------
# A request whose body is still arriving
# ---------------------------------------------------------------------------


def test_fails_a_slow_run_hook_job_when_the_terminate_hook_came_first(
    start: Any,
) -> None:
    h = start()
    run = slow_run_hook(h)

    # Lambda can end the MicroVM as soon as the terminate hook answers. So
    # the failure must be reported before that answer.
    answer = h.hook_in_background("terminate")
    assert not answered_within(answer, 0.2)
    assert run.finish() == 200

    assert answer.result(WAIT) == 200
    assert h.lambda_client.recorded() == [terminated_failure("cb-run")]
    assert not h.started("cb-run")


def test_waits_for_a_run_hook_that_begins_while_the_terminate_hook_waits(
    start: Any,
) -> None:
    h = start()
    # An HTTP job can arrive before the run hook. Its report holds the
    # terminate answer open while the run hook begins.
    assert h.job("cb-http") == 202
    assert eventually(lambda: h.started("cb-http"))
    h.lambda_client.hold = True

    answer = h.hook_in_background("terminate")
    assert eventually(lambda: h.lambda_client.waiting() == 1)
    run = slow_run_hook(h)
    h.lambda_client.finish()
    assert not answered_within(answer, 0.2)
    assert run.finish() == 200

    assert answer.result(WAIT) == 200
    assert sorted(c["callbackId"] for c in h.lambda_client.recorded()) == [
        "cb-http",
        "cb-run",
    ]


INVALID_RUN_HOOK = {
    "microvmId": "mvm-1",
    "runHookPayload": json.dumps(
        {
            "version": 99,
            "region": "us-east-1",
            "job": {"callbackId": "cb-invalid", "input": {}},
        }
    ),
}
INVALID_JOB = {
    "version": 99,
    "region": "us-east-1",
    "callbackId": "cb-invalid",
    "input": {},
}


@pytest.mark.parametrize(
    ("path", "body"),
    [(f"{HOOK_PATH_PREFIX}run", INVALID_RUN_HOOK), ("/job", INVALID_JOB)],
    ids=["run-hook", "job-request"],
)
def test_an_invalid_request_failure_report_holds_the_terminate_answer(
    start: Any, path: str, body: Any
) -> None:
    h = start()
    if path == "/job":
        h.run_hook()
    h.lambda_client.hold = True
    assert h.post(path, body) == 400
    assert eventually(lambda: h.lambda_client.waiting() == 1)

    answer = h.hook_in_background("terminate")
    assert not answered_within(answer, 0.2)

    h.lambda_client.finish()
    assert answer.result(WAIT) == 200
    assert [c["callbackId"] for c in h.lambda_client.recorded()] == ["cb-invalid"]


def test_an_invalid_request_failure_report_holds_the_answer_for_the_budget_only(
    start: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(worker_module, "TERMINATE_REPORT_BUDGET_SECONDS", 0.3)
    h = start()
    h.lambda_client.hold = True
    assert h.post(f"{HOOK_PATH_PREFIX}run", INVALID_RUN_HOOK) == 400

    started = time.monotonic()
    assert h.hook("terminate") == 200
    assert time.monotonic() - started >= 0.25


def test_an_invalid_request_that_begins_while_the_terminate_hook_waits_holds_it(
    start: Any,
) -> None:
    h = start()
    h.run_hook()
    assert h.job("cb-http") == 202
    assert eventually(lambda: h.started("cb-http"))
    h.lambda_client.hold = True
    answer = h.hook_in_background("terminate")
    assert eventually(lambda: h.lambda_client.waiting() == 1)

    assert h.post("/job", INVALID_JOB) == 400
    assert eventually(lambda: h.lambda_client.waiting() == 2)
    # The running job's report ends. The invalid request's report still runs,
    # so the answer must wait.
    h.lambda_client.finish_first()
    assert eventually(lambda: len(h.lambda_client.recorded()) == 1)
    assert not answered_within(answer, 0.2)

    h.lambda_client.finish()
    assert answer.result(WAIT) == 200
    assert sorted(c["callbackId"] for c in h.lambda_client.recorded()) == [
        "cb-http",
        "cb-invalid",
    ]


def test_answers_the_terminate_hook_after_the_budget_when_a_run_hook_never_ends(
    start: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(worker_module, "TERMINATE_REPORT_BUDGET_SECONDS", 0.3)
    h = start()
    run = slow_run_hook(h)

    started = time.monotonic()
    assert h.hook("terminate") == 200
    assert time.monotonic() - started >= 0.25
    run.abandon()


def test_starts_a_slow_run_hook_job_after_the_resume_hook_when_a_suspend_came_first(
    start: Any,
) -> None:
    h = start()
    run = slow_run_hook(h)

    assert h.hook("suspend") == 200
    assert run.finish() == 200
    assert not h.started("cb-run")

    h.hook("resume")
    assert eventually(lambda: h.started("cb-run"))
    h.finish_job("cb-run")
    assert h.lambda_client.recorded() == [{"kind": "success", "callbackId": "cb-run"}]


def test_starts_a_deferred_job_30_seconds_after_the_suspend_hook_without_a_resume(
    start: Any,
) -> None:
    h = start()
    run = slow_run_hook(h)
    h.hook("suspend")
    assert run.finish() == 200

    h.advance(GRACE - 0.001)
    assert not h.started("cb-run")
    h.advance(0.001)
    assert eventually(lambda: h.started("cb-run"))


def test_fails_a_deferred_job_when_the_terminate_hook_follows_the_suspend_hook(
    start: Any,
) -> None:
    h = start()
    run = slow_run_hook(h)
    h.hook("suspend")
    assert run.finish() == 200

    h.hook("terminate")
    h.advance(GRACE)

    assert not h.started("cb-run")
    assert h.lambda_client.recorded() == [terminated_failure("cb-run")]


def test_refuses_a_slow_http_job_with_503_when_the_terminate_hook_came_first(
    start: Any,
) -> None:
    h = start()
    h.run_hook()
    request = SlowCall(
        h,
        "/job",
        {"version": 1, "region": "us-east-1", "callbackId": "cb-http", "input": {}},
    )

    h.hook("terminate")
    assert request.finish() == 503
    assert not h.started("cb-http")


# ---------------------------------------------------------------------------
# The suspend hook
# ---------------------------------------------------------------------------


def test_the_suspend_hook_refuses_jobs_with_503_until_the_resume_hook(
    start: Any,
) -> None:
    h = start()
    h.run_hook()

    assert h.hook("suspend") == 200
    assert h.job("cb-1") == 503

    h.hook("resume")
    assert h.job("cb-1") == 202


def test_accepts_jobs_again_30_seconds_after_the_suspend_hook_without_a_resume(
    start: Any,
) -> None:
    h = start()
    h.run_hook()
    h.hook("suspend")

    h.advance(GRACE - 0.001)
    assert h.job("cb-1") == 503
    h.advance(0.001)
    assert h.job("cb-1") == 202


def test_the_suspend_hook_leaves_running_jobs_running(start: Any) -> None:
    h = start()
    h.run_hook()
    h.job("cb-1")
    assert eventually(lambda: h.started("cb-1"))

    h.hook("suspend")
    h.finish_job("cb-1")

    assert not h.jobs.contexts["cb-1"].cancelled.is_set()
    assert h.lambda_client.recorded() == [{"kind": "success", "callbackId": "cb-1"}]


def test_keeps_refusing_jobs_after_a_forwarded_resume(start: Any) -> None:
    h = start()
    h.run_hook()
    h.hook("suspend")

    assert h.hook("resume", FORWARDED_HEADERS) == 200
    assert h.job("cb-1") == 503

    h.hook("resume")
    assert h.job("cb-1") == 202


def test_ignores_a_forwarded_suspend_hook(start: Any) -> None:
    h = start()
    h.run_hook()

    assert h.hook("suspend", FORWARDED_HEADERS) == 200
    assert h.job("cb-1") == 202


def test_the_suspend_hook_stops_the_idle_time_and_the_resume_hook_restarts_it(
    start: Any,
) -> None:
    h = start()
    h.run_hook(IDLE)
    h.advance(IDLE / 2)
    h.hook("suspend")

    h.advance(IDLE * 2)
    assert h.microvms.suspended == []

    h.hook("resume")
    h.advance(IDLE)
    assert h.microvms.suspended == ["mvm-1"]


def test_keeps_the_refusal_when_the_hook_arrives_during_a_rejected_own_call(
    start: Any,
) -> None:
    # The worker's own SuspendMicrovm call triggers the suspend hook. A
    # rejection of a retried or concurrent call must not end the refusal
    # that the hook started, because the MicroVM is suspending.
    h = start()
    h.microvms.hold = True
    h.microvms.failure = suspend_error("AccessDeniedException", 403)
    h.run_hook(IDLE)
    h.advance(IDLE)
    assert eventually(lambda: h.microvms.suspended == ["mvm-1"])

    h.hook("suspend")
    h.microvms.finish()
    h.settle()

    assert h.job("cb-1") == 503
    h.hook("resume")
    assert h.job("cb-1") == 202


def test_ends_the_refusal_at_once_when_a_rejected_own_call_had_no_hook(
    start: Any,
) -> None:
    h = start()
    h.microvms.failure = suspend_error("AccessDeniedException", 403)
    h.run_hook(IDLE)
    h.advance(IDLE)

    assert h.job("cb-1") == 202


def test_keeps_one_30_second_refusal_when_the_hook_follows_the_own_call(
    start: Any,
) -> None:
    h = start()
    h.run_hook(IDLE)
    h.advance(IDLE)
    assert h.microvms.suspended == ["mvm-1"]

    h.advance(5)
    h.hook("suspend")

    # The hook started a new 30-second refusal 5 seconds after the call's.
    h.advance(25)
    assert h.job("cb-1") == 503
    h.advance(5)
    assert h.job("cb-1") == 202


def test_a_terminated_error_names_its_cause() -> None:
    error = MicrovmTerminatedError()

    assert str(error) == TERMINATED_MESSAGE
    assert type(error).__name__ == "MicrovmTerminatedError"


# ---------------------------------------------------------------------------
# Threads and timers that cannot start
# ---------------------------------------------------------------------------


def failing_thread_start(monkeypatch: pytest.MonkeyPatch, prefix: str) -> None:
    """Make every thread whose name starts with ``prefix`` fail to start."""
    original = threading.Thread.start

    def start(self: threading.Thread) -> None:
        if self.name.startswith(prefix):
            msg = "can't start new thread"
            raise RuntimeError(msg)
        original(self)

    monkeypatch.setattr(threading.Thread, "start", start)


def test_logs_an_idle_time_that_cannot_start(
    start: Any, logger: Any, timers: FakeTimers
) -> None:
    h = start()
    timers.failure = RuntimeError("can't start new thread")

    assert h.run_hook(IDLE) == 200

    assert "could not start the idle time" in logger.messages("error")
    timers.failure = None
    h.advance(IDLE * 2)
    assert h.microvms.suspended == []


def test_accepts_jobs_at_once_when_the_suspend_thread_cannot_start(
    start: Any, logger: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = start()
    h.run_hook(IDLE)
    failing_thread_start(monkeypatch, "microvm-suspend")

    h.advance(IDLE)

    assert "could not start the suspend of the idle MicroVM" in logger.messages("error")
    assert h.microvms.suspended == []
    monkeypatch.undo()
    assert h.job("cb-1") == 202


def test_refuses_jobs_until_the_resume_hook_when_the_refusal_timer_cannot_start(
    start: Any, logger: Any, timers: FakeTimers
) -> None:
    h = start()
    h.run_hook()
    timers.failure = RuntimeError("can't start new thread")

    h.hook("suspend")

    assert "could not start the end of the refusal" in logger.messages("error")
    timers.failure = None
    h.advance(GRACE * 2)
    assert h.job("cb-1") == 503
    h.hook("resume")
    assert h.job("cb-1") == 202


class _CallingLogger:
    """Records the worker's log lines. The first error line calls ``on_error``.

    Only the first error line calls it. A hook that ``on_error`` sends can
    fail to start a timer too, and log a second error line.
    """

    def __init__(self) -> None:
        self.lines: list[tuple[str, str, dict[str, Any]]] = []
        self.on_error: Callable[[], None] = lambda: None
        self._called = False
        self._lock = threading.Lock()

    def _add(self, level: str, message: str, data: Any) -> bool:
        with self._lock:
            self.lines.append((level, message, dict(data or {})))
            first = level == "error" and not self._called
            if first:
                self._called = True
            return first

    def info(self, msg: object, *args: object, extra: Any = None) -> None:
        self._add("info", str(msg), extra)

    def warning(self, msg: object, *args: object, extra: Any = None) -> None:
        self._add("warning", str(msg), extra)

    def error(self, msg: object, *args: object, extra: Any = None) -> None:
        if self._add("error", str(msg), extra):
            self.on_error()

    def errors(self) -> list[tuple[str, dict[str, Any]]]:
        with self._lock:
            return [(m, data) for level, m, data in self.lines if level == "error"]


@dataclass
class _TimerCase:
    """A hook that starts one timer, after a run hook with ``idle_seconds``."""

    idle_seconds: float | None
    hook: str
    message: str


# The resume hook starts the idle time. The suspend hook starts the timer
# that ends the refusal.
_EACH_TIMER = pytest.mark.parametrize(
    "case",
    [
        _TimerCase(IDLE, "resume", "could not start the idle time"),
        _TimerCase(None, "suspend", "could not start the end of the refusal"),
    ],
    ids=["idle-time", "grace"],
)

_THREAD_ERROR = {"name": "RuntimeError", "message": "can't start new thread"}


@_EACH_TIMER
def test_logs_a_timer_failure_after_it_releases_the_lock(
    start: Any, timers: FakeTimers, case: _TimerCase
) -> None:
    # The listener's lock is not reentrant. The logger's error() calls
    # close(), which takes the lock. If the listener logged the failure
    # under the lock, close() would wait for that lock forever. Every hook
    # and job request would then wait too.
    logger = _CallingLogger()
    h = start(logger=logger)
    assert h.run_hook(case.idle_seconds) == 200
    closed = threading.Event()

    def close() -> None:
        h.listener.close()
        closed.set()

    logger.on_error = close
    timers.failure = RuntimeError("can't start new thread")
    hook = h.hook_in_background(case.hook)
    if not closed.wait(WAIT):
        # threading.Lock lets any thread release it. So the test releases
        # the lock that the hook's thread holds. close() then returns, and
        # the fixture can close the worker.
        h.listener._lock.release()  # noqa: SLF001
        closed.wait(WAIT)
        pytest.fail("close() in the logger's error() waited for the lock")
    assert hook.result(WAIT) == 200

    assert logger.errors() == [(case.message, {"error": _THREAD_ERROR})]


@_EACH_TIMER
def test_answers_a_hook_that_the_timer_failure_log_sends(
    start: Any, timers: FakeTimers, case: _TimerCase
) -> None:
    # The logger's error() sends a resume hook. A request thread handles the
    # hook, and takes the lock. If the listener logged the failure under the
    # lock, the resume hook would wait until its HTTP timeout.
    logger = _CallingLogger()
    h = start(logger=logger)
    assert h.run_hook(case.idle_seconds) == 200
    statuses: list[int | BaseException] = []

    def send_hook() -> None:
        try:
            statuses.append(h.hook("resume"))
        except Exception as error:  # noqa: BLE001
            statuses.append(error)

    logger.on_error = send_hook
    timers.failure = RuntimeError("can't start new thread")
    # The inner resume hook's HTTP timeout is WAIT. The outer hook's HTTP
    # timeout is 2 * WAIT. Suppose the inner hook waits for the lock. Then
    # the inner hook times out first, and the outer hook still gets its
    # answer. So the check of statuses reports the failure. Each wait below
    # ends within 3 * WAIT.
    hook = h.hook_in_background(case.hook, timeout=2 * WAIT)
    assert hook.result(3 * WAIT) == 200
    assert statuses == [200]

    assert logger.errors()[0] == (case.message, {"error": _THREAD_ERROR})


@pytest.mark.parametrize("method", ["_start_idle_time", "_start_grace"])
def test_a_timer_start_under_the_plain_lock_raises(start: Any, method: str) -> None:
    # Only _locked() logs a timer failure. A timer start under a plain
    # ``with self._lock:`` block would leave its failure for an unrelated
    # block to log. So the timer start refuses to run there.
    h = start()
    assert h.run_hook(IDLE) == 200
    with h.listener._lock:  # noqa: SLF001
        with pytest.raises(RuntimeError, match=r"needs the lock through _locked\(\)"):
            getattr(h.listener, method)()


def test_fails_a_deferred_run_hook_job_whose_thread_cannot_start(
    start: Any, logger: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = start()
    run = slow_run_hook(h)
    h.hook("suspend")
    assert run.finish() == 200
    failing_thread_start(monkeypatch, "microvm-job-")

    h.hook("resume")

    assert eventually(lambda: len(h.lambda_client.recorded()) == 1)
    assert h.lambda_client.recorded()[0]["callbackId"] == "cb-run"
    assert h.lambda_client.recorded()[0]["errorType"] == "RuntimeError"
    assert "could not start the run hook job" in logger.messages("error")
    assert not h.started("cb-run")


def test_a_stale_idle_timer_does_not_suspend(start: Any, timers: FakeTimers) -> None:
    h = start()
    timers.fire_cancelled = True

    h.run_hook(IDLE)
    h.job("cb-1")
    h.advance(IDLE)

    assert h.microvms.suspended == []


def test_a_stale_refusal_timer_does_not_end_a_later_refusal(
    start: Any, timers: FakeTimers
) -> None:
    h = start()
    timers.fire_cancelled = True
    h.run_hook()

    h.hook("suspend")
    h.advance(1)
    h.hook("resume")
    h.advance(9)
    h.hook("suspend")
    # The first refusal's timer fires 30 seconds after the first hook.
    h.advance(GRACE - 9)

    assert h.job("cb-1") == 503
    h.advance(9)
    assert h.job("cb-1") == 202


def test_terminate_waits_for_and_does_not_repeat_a_heartbeat_start_failure(
    start: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A job whose heartbeats cannot start fails its own callback. The
    # terminate hook waits for that report, and does not fail the job again.
    h = start()
    h.run_hook()
    h.lambda_client.hold = True
    failing_thread_start(monkeypatch, "heartbeats-")
    assert h.job("cb-1", heartbeatTimeoutSeconds=30) == 202
    assert eventually(lambda: h.lambda_client.waiting() == 1)
    monkeypatch.undo()

    answer = h.hook_in_background("terminate")
    assert not answered_within(answer, 0.2)

    h.lambda_client.finish()
    assert answer.result(WAIT) == 200
    recorded = h.lambda_client.recorded()
    assert [(c["callbackId"], c["errorType"]) for c in recorded] == [
        ("cb-1", "RuntimeError")
    ]
    assert not h.started("cb-1")


def test_answers_the_resume_hook_before_it_reports_a_deferred_job_that_cannot_start(
    start: Any, logger: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A deferred job whose thread cannot start fails its callback, which can
    # take minutes. Lambda allows the resume hook about 10 seconds.
    h = start()
    run = slow_run_hook(h)
    h.hook("suspend")
    assert run.finish() == 200
    h.lambda_client.hold = True
    failing_thread_start(monkeypatch, "microvm-job-")

    answer = h.hook_in_background("resume")

    assert answer.result(WAIT) == 200
    assert eventually(lambda: h.lambda_client.waiting() == 1)
    monkeypatch.undo()
    h.lambda_client.finish()
    assert eventually(lambda: len(h.lambda_client.recorded()) == 1)
    assert h.lambda_client.recorded()[0]["errorType"] == "RuntimeError"


def test_starts_no_second_suspend_call_while_one_runs(start: Any) -> None:
    # A resume hook during the call ends the refusal. A second call started
    # then would share the grace period with the first one, and that period
    # could end the refusal while the second call can still freeze the
    # MicroVM.
    h = start()
    h.microvms.hold = True
    h.run_hook(IDLE)
    h.advance(IDLE)
    assert eventually(lambda: h.microvms.suspended == ["mvm-1"])

    h.hook("resume")
    h.advance(IDLE * 3)
    assert h.microvms.suspended == ["mvm-1"]

    # The idle time starts when the call ends.
    h.microvms.finish()
    h.settle()
    h.advance(IDLE - 0.001)
    assert h.microvms.suspended == ["mvm-1"]
    h.advance(0.001)
    assert eventually(lambda: h.microvms.suspended == ["mvm-1", "mvm-1"])


def test_starts_the_idle_time_after_the_call_when_a_job_ended_during_it(
    start: Any,
) -> None:
    h = start()
    h.microvms.hold = True
    h.run_hook(IDLE)
    h.advance(IDLE)
    assert eventually(lambda: h.microvms.suspended == ["mvm-1"])
    h.hook("resume")
    assert h.job("cb-1") == 202
    h.finish_job("cb-1")
    h.advance(IDLE * 2)
    assert h.microvms.suspended == ["mvm-1"]

    h.microvms.finish()
    h.settle()
    h.advance(IDLE)
    assert eventually(lambda: h.microvms.suspended == ["mvm-1", "mvm-1"])


def test_retries_a_rejected_call_when_a_resume_hook_came_during_it(
    start: Any,
) -> None:
    # A resume hook starts the idle time. During the call, the idle time
    # waits for the call to end. The JS worker also retries in this case,
    # because its resume hook starts the idle time too.
    h = start()
    h.microvms.hold = True
    h.microvms.failure = suspend_error("AccessDeniedException", 403)
    h.run_hook(IDLE)
    h.advance(IDLE)
    assert eventually(lambda: h.microvms.suspended == ["mvm-1"])
    h.hook("resume")

    h.microvms.finish()
    h.settle()
    h.advance(IDLE)
    assert eventually(lambda: h.microvms.suspended == ["mvm-1", "mvm-1"])


def thread_start_that(
    monkeypatch: pytest.MonkeyPatch, prefix: str, before_failing: Callable[[], None]
) -> None:
    """Make the thread named ``prefix`` run ``before_failing``, then fail to
    start."""
    original = threading.Thread.start

    def start(self: threading.Thread) -> None:
        if self.name.startswith(prefix):
            before_failing()
            msg = "can't start new thread"
            raise RuntimeError(msg)
        original(self)

    monkeypatch.setattr(threading.Thread, "start", start)


def test_keeps_a_hook_refusal_when_the_suspend_thread_cannot_start(
    start: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = start()
    h.run_hook(IDLE)
    thread_start_that(monkeypatch, "microvm-suspend", lambda: h.hook("suspend"))

    h.advance(IDLE)
    monkeypatch.undo()

    assert h.job("cb-1") == 503
    h.hook("resume")
    assert h.job("cb-1") == 202


def test_starts_the_idle_time_after_a_resume_during_a_suspend_thread_that_failed(
    start: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = start()
    h.run_hook(IDLE)
    thread_start_that(monkeypatch, "microvm-suspend", lambda: h.hook("resume"))

    h.advance(IDLE)
    monkeypatch.undo()

    assert not h.listener._idle_after_call  # noqa: SLF001
    h.advance(IDLE)
    assert eventually(lambda: h.microvms.suspended == ["mvm-1"])


@pytest.mark.parametrize("then", ["client-fails", "call-rejected"])
def test_keeps_a_hook_refusal_that_arrives_while_the_client_is_created(
    start: Any, then: str
) -> None:
    holder: dict[str, Harness] = {}

    def create(region: str) -> Any:
        holder["h"].hook("suspend")
        if then == "client-fails":
            raise RuntimeError("no client")
        holder["h"].microvms.failure = suspend_error("AccessDeniedException", 403)
        return holder["h"].microvms

    h = start(create_microvms_client=create)
    holder["h"] = h
    h.run_hook(IDLE)

    h.advance(IDLE)

    assert h.job("cb-1") == 503
    h.hook("resume")
    assert h.job("cb-1") == 202
