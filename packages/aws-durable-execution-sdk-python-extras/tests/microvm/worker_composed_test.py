# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The microvm operation and the real MicroVM worker, end to end, in one process.

1. The durable function runs on the local test runner.
2. The fake ``RunMicrovm`` sends the ``run`` hook to a real worker on a free
   port, as Lambda does when the MicroVM starts. It returns the worker's
   address as the MicroVM's endpoint. So an HTTP delivery reaches the worker
   too.
3. The worker runs the job, and reports through a Lambda client double that
   forwards each callback call to the runner.

So the test covers the job document and the callback contract between the two
packages.
"""

from __future__ import annotations

import functools
import http.client
import json
import logging
import socket
import threading
import time
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest
from aws_durable_execution_sdk_python.config import Duration, MapConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python_microvm_worker import (
    HOOK_PATH_PREFIX,
    MicrovmJobContext,
    MicrovmWorker,
    start_microvm_worker,
)
from aws_durable_execution_sdk_python_microvm_worker import worker as worker_module
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner

from aws_durable_execution_sdk_python_extras.microvm import (
    MAX_JOB_INPUT_BYTES,
    MAX_RUN_HOOK_PAYLOAD_LENGTH,
    MicrovmConfig,
    MicrovmError,
    MicrovmInvokeOptions,
    MicrovmRequestConfig,
    MicrovmSession,
    MicrovmSessionConfig,
    microvm,
    microvm_session,
)


WAIT = 30


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


def url_of(worker: MicrovmWorker) -> str:
    """The worker's endpoint URL for the durable function."""
    host = host_of(worker)
    return (
        f"http://[{host}]:{worker.port}"
        if ":" in host
        else f"http://{host}:{worker.port}"
    )


class RunnerCallbacks:
    """A Lambda client for the worker that sends each callback call to the
    local test runner."""

    def __init__(self, runner: DurableFunctionTestRunner) -> None:
        self._runner = runner
        self.heartbeats = 0

    def send_durable_execution_callback_success(
        self, CallbackId: str, Result: bytes | None = None
    ) -> dict[str, Any]:
        self._runner.send_callback_success(CallbackId, Result)
        return {}

    def send_durable_execution_callback_failure(
        self, CallbackId: str, Error: dict[str, Any]
    ) -> dict[str, Any]:
        self._runner.send_callback_failure(
            CallbackId,
            ErrorObject(
                message=Error.get("ErrorMessage"),
                type=Error.get("ErrorType"),
                data=Error.get("ErrorData"),
                stack_trace=None,
            ),
        )
        return {}

    def send_durable_execution_callback_heartbeat(
        self, CallbackId: str
    ) -> dict[str, Any]:
        self.heartbeats += 1
        self._runner.send_callback_heartbeat(CallbackId)
        return {}


def run_hook_to(worker: MicrovmWorker) -> Callable[[dict[str, Any]], None]:
    """A RunMicrovm answer that sends the run hook to ``worker``."""

    def deliver(request: dict[str, Any]) -> None:
        connection = http.client.HTTPConnection(
            host_of(worker), worker.port, timeout=WAIT
        )
        try:
            body = {"microvmId": "mvm-1", "runHookPayload": request["runHookPayload"]}
            connection.request("POST", f"{HOOK_PATH_PREFIX}run", body=json.dumps(body))
            assert connection.getresponse().status == 200
        finally:
            connection.close()

    return deliver


@pytest.fixture
def composed(fake_client: Any) -> Iterator[Callable[..., Any]]:
    """Run a job through the operation and a real worker."""
    closers: list[Callable[[], None]] = []

    def _run(
        job: Callable[[Any, MicrovmJobContext], Any],
        job_input: Any,
        **fields: Any,
    ) -> tuple[Any, Any, RunnerCallbacks]:
        holder: dict[str, MicrovmConfig] = {}

        @durable_execution
        def handler(event: Any, context: DurableContext) -> Any:
            try:
                return {
                    "result": microvm(
                        context, "build", job_input=event, config=holder["config"]
                    )
                }
            except MicrovmError as error:
                return {"error": type(error).__name__, "message": error.message}

        runner = DurableFunctionTestRunner(handler=handler, poll_interval=0.05)
        closers.append(runner.close)
        callbacks = RunnerCallbacks(runner)
        routes = fields.pop("routes", None)
        heartbeat_interval = fields.pop("heartbeat_interval", None)
        worker = start_reachable_worker(
            lambda: start_microvm_worker(
                handler=job,
                routes=routes,
                # The double has the three callback methods that the worker calls.
                create_client=lambda region: callbacks,  # type: ignore[arg-type, return-value]
                heartbeat_interval_seconds=heartbeat_interval,
                port=0,
            )
        )
        closers.append(worker.close)
        client = fake_client(
            launches=[run_hook_to(worker)],
            endpoint=url_of(worker),
        )
        holder["config"] = MicrovmConfig(
            image_identifier="img",
            execution_role_arn="role",
            timeout=Duration.from_minutes(5),
            client=client,
            **fields,
        )
        arn = runner.run_async(input=json.dumps(job_input), execution_timeout=WAIT)
        result = runner.wait_for_result(arn, timeout=WAIT)
        assert worker.idle(WAIT)
        assert result.status is InvocationStatus.SUCCEEDED, result.error
        return json.loads(result.result or "null"), client, callbacks

    yield _run
    for close in reversed(closers):
        close()


def test_returns_the_result_of_the_job_in_the_worker(composed: Any) -> None:
    def job(job_input: Any, context: MicrovmJobContext) -> Any:
        return {"sum": sum(job_input["numbers"]), "microvm": context.microvm_id}

    outcome, client, _ = composed(job, {"numbers": [1, 2, 3]})

    assert outcome == {"result": {"sum": 6, "microvm": "mvm-1"}}
    assert client.requests("terminate_microvm") == [{"microvmIdentifier": "mvm-1"}]


def test_fails_with_the_error_type_that_the_worker_reports(composed: Any) -> None:
    def job(job_input: Any, context: MicrovmJobContext) -> Any:
        msg = "3 tests failed"
        raise RuntimeError(msg)

    outcome, client, _ = composed(job, {})

    assert outcome == {
        "error": "MicrovmJobFailedError",
        "message": 'MicroVM "build": the job failed (RuntimeError): 3 tests failed',
    }
    assert len(client.requests("terminate_microvm")) == 1


def test_sends_heartbeats_when_the_job_has_a_heartbeat_timeout(
    composed: Any,
) -> None:
    def job(job_input: Any, context: MicrovmJobContext) -> Any:
        context.cancelled.wait(0.3)
        return "done"

    outcome, _, callbacks = composed(
        job,
        {},
        heartbeat_timeout=Duration.from_seconds(30),
        heartbeat_interval=0.05,
    )

    assert outcome == {"result": "done"}
    assert callbacks.heartbeats >= 2


def test_runs_a_job_too_large_for_the_run_hook_over_http(composed: Any) -> None:
    def job(job_input: Any, context: MicrovmJobContext) -> Any:
        return {"length": len(job_input), "microvm": context.microvm_id}

    outcome, client, _ = composed(job, "日" * MAX_RUN_HOOK_PAYLOAD_LENGTH)

    assert outcome == {"result": {"length": 4096, "microvm": "mvm-1"}}
    assert len(client.requests("create_microvm_auth_token")) == 1
    assert client.requests("terminate_microvm") == [{"microvmIdentifier": "mvm-1"}]


def test_runs_a_job_input_at_the_size_limit_in_the_worker(composed: Any) -> None:
    # The job body adds the job fields to the input. The worker accepts the
    # body, so the fields fit in the 8 KiB that the limit leaves.
    def job(job_input: Any, context: MicrovmJobContext) -> Any:
        return len(job_input)

    # The JSON of the input is the limit, with the 2 quotes.
    outcome, _, _ = composed(
        job, "x" * (MAX_JOB_INPUT_BYTES - 2), heartbeat_timeout=Duration.from_hours(8)
    )

    assert outcome == {"result": MAX_JOB_INPUT_BYTES - 2}


def test_runs_a_job_on_a_route_of_the_worker(composed: Any) -> None:
    def job(job_input: Any, context: MicrovmJobContext) -> Any:
        return "handler"

    outcome, _, _ = composed(
        job,
        {"n": 1},
        request=MicrovmRequestConfig(path="/clone-build"),
        routes={"/clone-build": lambda job_input, context: {"route": job_input}},
    )

    assert outcome == {"result": {"route": {"n": 1}}}


def test_runs_two_session_jobs_in_one_worker(fake_client: Any, tmp_path: Any) -> None:
    """The second job reads what the first job wrote in the same MicroVM."""

    def job(job_input: Any, context: MicrovmJobContext) -> Any:
        state = tmp_path / "artifact.txt"
        if job_input == "build":
            state.write_text(f"built in {context.microvm_id}")
            return "built"
        return state.read_text()

    holder: dict[str, MicrovmSessionConfig] = {}

    def pipeline(vm: MicrovmSession, ctx: DurableContext) -> Any:
        options = MicrovmInvokeOptions(timeout=Duration.from_minutes(1))
        return [
            vm.invoke(ctx, "build", job_input="build", options=options),
            vm.invoke(ctx, "test", job_input="test", options=options),
        ]

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        return microvm_session(
            context, "pipeline", config=holder["config"], handler=pipeline
        )

    runner = DurableFunctionTestRunner(handler=handler, poll_interval=0.05)
    callbacks = RunnerCallbacks(runner)
    worker = start_reachable_worker(
        lambda: start_microvm_worker(
            handler=job,
            create_client=lambda region: callbacks,  # type: ignore[arg-type, return-value]
            port=0,
        )
    )
    try:
        client = fake_client(
            launches=[run_hook_to(worker)],
            endpoint=url_of(worker),
        )
        holder["config"] = MicrovmSessionConfig(
            image_identifier="img",
            execution_role_arn="role",
            timeout=Duration.from_minutes(5),
            client=client,
        )
        arn = runner.run_async(input="null", execution_timeout=WAIT)
        result = runner.wait_for_result(arn, timeout=WAIT)
        assert worker.idle(WAIT)
    finally:
        worker.close()
        runner.close()

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert json.loads(result.result or "null") == ["built", "built in mvm-1"]
    assert len(client.requests("run_microvm")) == 1
    assert len(client.requests("create_microvm_auth_token")) == 2
    assert client.requests("terminate_microvm") == [{"microvmIdentifier": "mvm-1"}]


def post_hook(worker: MicrovmWorker, hook: str) -> None:
    """Send a lifecycle hook as Lambda does, with a local Host."""
    connection = http.client.HTTPConnection(host_of(worker), worker.port, timeout=WAIT)
    try:
        connection.request(
            "POST",
            f"{HOOK_PATH_PREFIX}{hook}",
            body=b"{}",
            headers={"Host": "localhost:8080"},
        )
        assert connection.getresponse().status == 200
    finally:
        connection.close()


class RecordingLogger:
    """Records the worker's log messages and their fields."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.messages: list[str] = []
        self._extras: list[tuple[str, dict[str, Any]]] = []

    def _add(self, msg: object, *args: object, extra: Any = None) -> None:
        with self._lock:
            self.messages.append(str(msg))
            self._extras.append((str(msg), dict(extra or {})))

    info = warning = error = _add

    def count(self, message: str) -> int:
        with self._lock:
            return self.messages.count(message)

    def extras(self, message: str) -> list[dict[str, Any]]:
        """The fields of each line with this message."""
        with self._lock:
            return [data for m, data in self._extras if m == message]


@pytest.mark.parametrize("resume_hook", [True, False], ids=["resume-hook", "none"])
def test_resumes_a_worker_that_suspended_itself_between_two_session_jobs(
    fake_client: Any, monkeypatch: pytest.MonkeyPatch, resume_hook: bool
) -> None:
    """The worker suspends its idle MicroVM, and the next job resumes it.

    A stand-in for Lambda keeps the MicroVM's state. SuspendMicrovm from the
    worker sets it to ``SUSPENDED`` and sends the ``suspend`` hook.
    ResumeMicrovm from the session sets it to ``RUNNING``, and sends the
    ``resume`` hook when the image enables it. The worker's timers run 20
    times faster, so the session's 60-second idle time takes 3 seconds, and
    the 30-second refusal takes 1.5 seconds.

    Without the ``resume`` hook, the worker answers the second job with 503
    until its refusal ends. The request step delivers it again after each
    503, after 0.25 seconds at first, doubling up to 4 seconds. The scaled
    idle time must stay longer than the gap between two deliveries.
    Otherwise the worker suspends again before each delivery. A 10-second
    idle time scaled to 1 second did that. Unscaled, the shortest idle time,
    10 seconds, is longer than the longest gap.
    """
    original_timer = worker_module._start_timer  # noqa: SLF001
    monkeypatch.setattr(
        worker_module,
        "_start_timer",
        lambda delay, action: original_timer(delay / 20, action),
    )
    state = {"value": "RUNNING"}
    lock = threading.Lock()
    holder: dict[Any, Any] = {}

    class StatefulMicrovms(fake_client):  # type: ignore[misc, valid-type]
        def get_microvm(self, **request: Any) -> dict[str, Any]:
            with lock:
                current = state["value"]
            response = super().get_microvm(**request)
            return {**response, "state": current}

        def resume_microvm(self, **request: Any) -> dict[str, Any]:
            super().resume_microvm(**request)
            with lock:
                state["value"] = "RUNNING"
            if resume_hook:
                post_hook(holder["worker"], "resume")
            return {}

    class WorkerMicrovms:
        """The worker's lambda-microvms client."""

        def __init__(self) -> None:
            self.suspended: list[str] = []

        def suspend_microvm(self, *, microvmIdentifier: str) -> dict[str, Any]:  # noqa: N803
            self.suspended.append(microvmIdentifier)
            with lock:
                state["value"] = "SUSPENDED"
            # Lambda calls the suspend hook before it freezes the MicroVM.
            post_hook(holder["worker"], "suspend")
            return {}

    worker_microvms = WorkerMicrovms()

    def job(job_input: Any, context: MicrovmJobContext) -> Any:
        return f"{job_input} done"

    def wait_until_suspended(_: Any) -> str:
        deadline = time.monotonic() + WAIT
        while time.monotonic() < deadline:
            with lock:
                if state["value"] == "SUSPENDED":
                    return "suspended"
            time.sleep(0.01)
        return "still running"

    def pipeline(vm: MicrovmSession, ctx: DurableContext) -> Any:
        options = MicrovmInvokeOptions(timeout=Duration.from_minutes(1))
        first = vm.invoke(ctx, "build", job_input="build", options=options)
        between = ctx.step(wait_until_suspended, name="pause")
        second = vm.invoke(ctx, "test", job_input="test", options=options)
        return [first, between, second]

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        return microvm_session(
            context, "pipeline", config=holder["config"], handler=pipeline
        )

    runner = DurableFunctionTestRunner(handler=handler, poll_interval=0.05)
    callbacks = RunnerCallbacks(runner)
    logger = RecordingLogger()
    worker = start_reachable_worker(
        lambda: start_microvm_worker(
            handler=job,
            create_client=lambda region: callbacks,  # type: ignore[arg-type, return-value]
            create_microvms_client=lambda region: worker_microvms,  # type: ignore[arg-type, return-value]
            logger=logger,
            port=0,
        )
    )
    holder["worker"] = worker
    try:
        client = StatefulMicrovms(
            launches=[run_hook_to(worker)],
            endpoint=url_of(worker),
        )
        holder["config"] = MicrovmSessionConfig(
            image_identifier="img",
            execution_role_arn="role",
            timeout=Duration.from_minutes(5),
            auto_suspend_on_idle=True,
            auto_suspend_idle_time=Duration.from_seconds(60),
            client=client,
        )
        arn = runner.run_async(input="null", execution_timeout=WAIT)
        result = runner.wait_for_result(arn, timeout=WAIT)
        assert worker.idle(WAIT)
    finally:
        worker.close()
        runner.close()

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert json.loads(result.result or "null") == [
        "build done",
        "suspended",
        "test done",
    ]
    assert worker_microvms.suspended[0] == "mvm-1"
    assert client.requests("resume_microvm") == [{"microvmIdentifier": "mvm-1"}]
    assert client.requests("terminate_microvm") == [{"microvmIdentifier": "mvm-1"}]
    refusals = logger.count("job refused: the MicroVM is suspending")
    if resume_hook:
        assert refusals == 0
    else:
        assert refusals >= 1


# ---------------------------------------------------------------------------
# suspend_after with the real worker
# ---------------------------------------------------------------------------

IGNORED_WARNING = (
    "the worker in MicroVM mvm-1 does not support suspendAfter. The MicroVM "
    "keeps running after the job."
)


class SimulatedMicrovm:
    """A stand-in for Lambda that keeps the state of one MicroVM.

    1. The worker's SuspendMicrovm sends the ``suspend`` hook, and then sets
       the state to ``SUSPENDED``. Lambda calls the hook before the freeze.
    2. The session's ResumeMicrovm sends the ``resume`` hook, and then sets
       the state to ``RUNNING``. A probe saw the hook before ``RUNNING``.
    3. :attr:`endpoint` stands in for the MicroVM endpoint. It forwards each
       job to the worker while the MicroVM runs. While it is suspended, it
       answers HTTP 502 with ``x-aws-proxy-error: MICROVM_CONNECT_FAILED``,
       as the service does.

    ``old_worker`` removes ``suspendAfter`` from each forwarded job. The
    worker then behaves as a release that does not know the field.
    """

    def __init__(self, *, old_worker: bool = False) -> None:
        self._lock = threading.Lock()
        self._state = "RUNNING"
        self.worker: MicrovmWorker | None = None
        self.suspended: list[str] = []
        self.answers: list[int] = []
        self._old_worker = old_worker
        simulated = self

        class Endpoint(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 - the http.server name
                body = self.rfile.read(int(self.headers.get("content-length", "0")))
                if simulated.state() == "SUSPENDED":
                    simulated.record(502)
                    self.send_response(502)
                    self.send_header("x-aws-proxy-error", "MICROVM_CONNECT_FAILED")
                    self.send_header("content-length", "0")
                    self.end_headers()
                    return
                status, accepted, data = simulated.forward(self.path, body)
                simulated.record(status)
                self.send_response(status)
                if accepted:
                    self.send_header("X-Microvm-Suspend-After", "accepted")
                self.send_header("content-length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args: Any) -> None:
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Endpoint)
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self._server.server_address[1]}"

    def state(self) -> str:
        with self._lock:
            return self._state

    def record(self, status: int) -> None:
        with self._lock:
            self.answers.append(status)

    def set_state(self, state: str) -> None:
        with self._lock:
            self._state = state

    def forward(self, path: str, body: bytes) -> tuple[int, bool, bytes]:
        """Send the job to the worker. Return the status, whether the worker
        accepted ``suspendAfter``, and the body of the answer."""
        worker = self.worker
        assert worker is not None
        if self._old_worker:
            document = json.loads(body)
            document.pop("suspendAfter", None)
            body = json.dumps(document).encode()
        connection = http.client.HTTPConnection(
            host_of(worker), worker.port, timeout=WAIT
        )
        try:
            connection.request(
                "POST", path, body=body, headers={"content-type": "application/json"}
            )
            response = connection.getresponse()
            data = response.read()
            # The endpoint sends one header with a fixed name and a fixed
            # value. A value from the worker's answer never reaches
            # send_header. The session accepts only this value. So an answer
            # with another value reaches the session as an answer without
            # the header, which the session treats the same way.
            accepted = response.getheader("X-Microvm-Suspend-After") == "accepted"
            return response.status, accepted, data
        finally:
            connection.close()

    def wait_for(self, state: str) -> str:
        deadline = time.monotonic() + WAIT
        while time.monotonic() < deadline:
            if self.state() == state:
                return state
            time.sleep(0.01)
        return f"not {state}"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


Pipeline = Callable[[MicrovmSession, DurableContext], Any]


def suspend_after_session(
    fake_client: Any,
    make_pipeline: Callable[[SimulatedMicrovm], Pipeline],
    job: Callable[[Any, MicrovmJobContext], Any],
    *,
    old_worker: bool = False,
) -> tuple[Any, Any, SimulatedMicrovm, RecordingLogger]:
    """Run a pipeline in a session without auto-suspend, against a real
    worker behind a :class:`SimulatedMicrovm`.

    ``make_pipeline`` receives the simulated MicroVM, so the pipeline can
    wait for its state.
    """
    microvm = SimulatedMicrovm(old_worker=old_worker)
    pipeline = make_pipeline(microvm)
    holder: dict[str, Any] = {}

    class SessionMicrovms(fake_client):  # type: ignore[misc, valid-type]
        def get_microvm(self, **request: Any) -> dict[str, Any]:
            response = super().get_microvm(**request)
            return {**response, "state": microvm.state()}

        def resume_microvm(self, **request: Any) -> dict[str, Any]:
            super().resume_microvm(**request)
            post_hook(holder["worker"], "resume")
            microvm.set_state("RUNNING")
            return {}

    class WorkerMicrovms:
        """The worker's lambda-microvms client."""

        def suspend_microvm(self, *, microvmIdentifier: str) -> dict[str, Any]:  # noqa: N803
            microvm.suspended.append(microvmIdentifier)
            post_hook(holder["worker"], "suspend")
            microvm.set_state("SUSPENDED")
            return {}

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        return microvm_session(
            context, "pipeline", config=holder["config"], handler=pipeline
        )

    runner = DurableFunctionTestRunner(handler=handler, poll_interval=0.05)
    callbacks = RunnerCallbacks(runner)
    logger = RecordingLogger()
    worker = start_reachable_worker(
        lambda: start_microvm_worker(
            handler=job,
            create_client=lambda region: callbacks,  # type: ignore[arg-type, return-value]
            create_microvms_client=lambda region: WorkerMicrovms(),  # type: ignore[arg-type, return-value]
            logger=logger,
            port=0,
        )
    )
    holder["worker"] = worker
    microvm.worker = worker
    try:
        client = SessionMicrovms(
            launches=[run_hook_to(worker)], endpoint=microvm.endpoint
        )
        holder["config"] = MicrovmSessionConfig(
            image_identifier="img",
            execution_role_arn="role",
            timeout=Duration.from_minutes(5),
            client=client,
        )
        arn = runner.run_async(input="null", execution_timeout=WAIT)
        result = runner.wait_for_result(arn, timeout=WAIT)
        assert worker.idle(WAIT)
    finally:
        worker.close()
        runner.close()
        microvm.close()
    assert result.status is InvocationStatus.SUCCEEDED, result.error
    return json.loads(result.result or "null"), client, microvm, logger


def options(*, suspend_after: bool = False) -> MicrovmInvokeOptions:
    return MicrovmInvokeOptions(
        timeout=Duration.from_minutes(1), suspend_after=suspend_after
    )


def echo(job_input: Any, context: MicrovmJobContext) -> Any:
    return f"{job_input} done"


def test_suspends_after_a_job_and_the_next_job_resumes_the_microvm(
    fake_client: Any, caplog: pytest.LogCaptureFixture
) -> None:
    def make_pipeline(microvm: SimulatedMicrovm) -> Pipeline:
        def pipeline(vm: MicrovmSession, ctx: DurableContext) -> Any:
            first = vm.invoke(
                ctx, "build", job_input="build", options=options(suspend_after=True)
            )
            between = ctx.step(lambda _: microvm.wait_for("SUSPENDED"), name="pause")
            second = vm.invoke(ctx, "test", job_input="test", options=options())
            return [first, between, second]

        return pipeline

    with caplog.at_level(logging.WARNING):
        outcome, client, microvm, logger = suspend_after_session(
            fake_client, make_pipeline, echo
        )

    assert outcome == ["build done", "SUSPENDED", "test done"]
    # The worker acknowledged suspendAfter. So the session logs no warning.
    assert IGNORED_WARNING not in [record.getMessage() for record in caplog.records]
    assert microvm.suspended == ["mvm-1"]
    [line] = logger.extras("suspending the MicroVM after a job")
    assert list(line) == ["microvmId", "callbackId"]
    assert line["microvmId"] == "mvm-1"
    # The flag makes the second job check the state before delivery. So it
    # resumes the MicroVM before it sends the job, and gets no 502.
    assert client.requests("resume_microvm") == [{"microvmIdentifier": "mvm-1"}]
    assert 502 not in microvm.answers
    assert microvm.state() == "RUNNING"
    assert client.requests("terminate_microvm") == [{"microvmIdentifier": "mvm-1"}]


def test_suspends_after_a_map_with_one_suspend_after_item(fake_client: Any) -> None:
    def slow_echo(job_input: Any, context: MicrovmJobContext) -> Any:
        # The items run at once. So the worker suspends only when the last
        # of them ends. The sleep keeps each job running until every item
        # has delivered its job.
        time.sleep(1)
        return f"{job_input} done"

    def make_pipeline(microvm: SimulatedMicrovm) -> Pipeline:
        def pipeline(vm: MicrovmSession, ctx: DurableContext) -> Any:
            shards: list[Any] = ctx.map(
                ["a", "b", "c"],
                lambda item_context, item, index, items: vm.invoke(
                    item_context,
                    f"shard-{item}",
                    job_input=item,
                    options=options(suspend_after=item == "a"),
                ),
                name="shards",
                config=MapConfig(max_concurrency=3),
            ).get_results()
            between = ctx.step(lambda _: microvm.wait_for("SUSPENDED"), name="pause")
            after = vm.invoke(ctx, "after", job_input="after", options=options())
            return [shards, between, after]

        return pipeline

    outcome, client, microvm, logger = suspend_after_session(
        fake_client, make_pipeline, slow_echo
    )

    assert outcome == [["a done", "b done", "c done"], "SUSPENDED", "after done"]
    # One request from item a, and one suspend.
    assert microvm.suspended == ["mvm-1"]
    assert len(logger.extras("suspending the MicroVM after a job")) == 1
    # The job after the map resumed the MicroVM, with the flag or after a
    # 502, depending on which invocation ran item a.
    assert client.requests("resume_microvm") == [{"microvmIdentifier": "mvm-1"}]
    assert microvm.state() == "RUNNING"


def test_a_replay_that_lost_the_flag_resumes_through_the_check_after_a_502(
    fake_client: Any,
) -> None:
    # 1. The map item asks for a suspend after its job.
    # 2. A durable wait ends the invocation. The next invocation replays
    #    the completed map without running its item. So the flag is lost.
    # 3. The next job goes to the suspended MicroVM, and gets a 502
    #    MICROVM_CONNECT_FAILED. The request step checks the state, resumes
    #    the MicroVM, and sends the job again.
    def make_pipeline(microvm: SimulatedMicrovm) -> Pipeline:
        def pipeline(vm: MicrovmSession, ctx: DurableContext) -> Any:
            shards: list[Any] = ctx.map(
                ["a"],
                lambda item_context, item, index, items: vm.invoke(
                    item_context,
                    f"shard-{item}",
                    job_input=item,
                    options=options(suspend_after=True),
                ),
                name="shards",
            ).get_results()
            between = ctx.step(lambda _: microvm.wait_for("SUSPENDED"), name="pause")
            ctx.wait(Duration.from_seconds(1), name="wait")
            after = vm.invoke(ctx, "after", job_input="after", options=options())
            return [shards, between, after]

        return pipeline

    outcome, client, microvm, _ = suspend_after_session(
        fake_client, make_pipeline, echo
    )

    assert outcome == [["a done"], "SUSPENDED", "after done"]
    assert microvm.suspended == ["mvm-1"]
    # The job after the wait made no state check before delivery. Its first
    # request got the 502. The check after it found SUSPENDED, resumed the
    # MicroVM, and polled until RUNNING. The second request reached the
    # worker.
    assert microvm.answers[-2:] == [502, 202]
    assert client.requests("resume_microvm") == [{"microvmIdentifier": "mvm-1"}]
    assert microvm.state() == "RUNNING"


def test_an_old_worker_ignores_suspend_after_and_the_session_warns_once(
    fake_client: Any, caplog: pytest.LogCaptureFixture
) -> None:
    def make_pipeline(microvm: SimulatedMicrovm) -> Pipeline:
        def pipeline(vm: MicrovmSession, ctx: DurableContext) -> Any:
            return [
                vm.invoke(
                    ctx, name, job_input=name, options=options(suspend_after=True)
                )
                for name in ["build", "test"]
            ]

        return pipeline

    with caplog.at_level(logging.WARNING):
        outcome, client, microvm, logger = suspend_after_session(
            fake_client, make_pipeline, echo, old_worker=True
        )

    assert outcome == ["build done", "test done"]
    # The worker sent no acknowledgement and never suspended the MicroVM.
    assert microvm.suspended == []
    assert logger.extras("suspending the MicroVM after a job") == []
    warnings = [
        record
        for record in caplog.records
        if record.levelname == "WARNING" and record.getMessage() == IGNORED_WARNING
    ]
    assert len(warnings) == 1
    assert client.requests("resume_microvm") == []


class LosingProxy:
    """A stand-in for the MicroVM endpoint in front of a real worker.

    It forwards each job request to the worker, and returns the worker's
    status. Before it forwards a request, it runs the action for that
    request's number, if there is one. It can drop the worker's answer to a
    request, as a network fault does.
    """

    def __init__(
        self,
        worker: MicrovmWorker,
        *,
        lose: set[int],
        before: dict[int, Callable[[], None]],
    ) -> None:
        self.statuses: list[int | None] = []
        self._lock = threading.Lock()
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 - the http.server name
                body = self.rfile.read(int(self.headers.get("content-length", "0")))
                with proxy._lock:
                    number = len(proxy.statuses) + 1
                    proxy.statuses.append(None)
                action = before.get(number)
                if action is not None:
                    action()
                connection = http.client.HTTPConnection(
                    host_of(worker), worker.port, timeout=WAIT
                )
                try:
                    connection.request(
                        "POST",
                        self.path,
                        body=body,
                        headers={"content-type": "application/json"},
                    )
                    response = connection.getresponse()
                    response.read()
                    status = response.status
                finally:
                    connection.close()
                if number in lose:
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    return
                with proxy._lock:
                    proxy.statuses[number - 1] = status
                self.send_response(status)
                self.send_header("content-length", "0")
                self.end_headers()

            def log_message(self, *args: Any) -> None:
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}"
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


def test_resumes_the_microvm_when_a_resend_of_a_running_job_meets_a_suspend(
    fake_client: Any,
) -> None:
    """A resend of a running job during a suspend gets 503, not 202.

    1. The worker accepts the job, and its 202 is lost.
    2. The request step checks the state again, and sees ``RUNNING``.
    3. Lambda sends the ``suspend`` hook, and the MicroVM is ``SUSPENDED``.
    4. The resend reaches the worker during the suspend refusal.

    A 202 at step 4 would end the delivery. Nothing would resume the
    MicroVM, and the frozen job would never complete its callback. The 503
    makes the request step check the state again, see ``SUSPENDED``, and
    resume the MicroVM. The job here continues only after ResumeMicrovm, as
    a frozen job does.
    """
    state = {"value": "RUNNING"}
    lock = threading.Lock()
    resumed = threading.Event()
    holder: dict[Any, Any] = {}

    class StatefulMicrovms(fake_client):  # type: ignore[misc, valid-type]
        def get_microvm(self, **request: Any) -> dict[str, Any]:
            with lock:
                current = state["value"]
            response = super().get_microvm(**request)
            return {**response, "state": current}

        def resume_microvm(self, **request: Any) -> dict[str, Any]:
            super().resume_microvm(**request)
            with lock:
                state["value"] = "RUNNING"
            resumed.set()
            post_hook(holder["worker"], "resume")
            return {}

    def suspend() -> None:
        post_hook(holder["worker"], "suspend")
        with lock:
            state["value"] = "SUSPENDED"

    runs: list[str] = []

    def job(job_input: Any, context: MicrovmJobContext) -> Any:
        runs.append(job_input)
        # A frozen job continues only after the MicroVM resumes.
        return "done" if resumed.wait(10) else "still frozen"

    def pipeline(vm: MicrovmSession, ctx: DurableContext) -> Any:
        options = MicrovmInvokeOptions(timeout=Duration.from_minutes(1))
        return vm.invoke(ctx, "build", job_input="build", options=options)

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        return microvm_session(
            context, "pipeline", config=holder["config"], handler=pipeline
        )

    runner = DurableFunctionTestRunner(handler=handler, poll_interval=0.05)
    callbacks = RunnerCallbacks(runner)
    worker = start_reachable_worker(
        lambda: start_microvm_worker(
            handler=job,
            create_client=lambda region: callbacks,  # type: ignore[arg-type, return-value]
            port=0,
        )
    )
    holder["worker"] = worker
    proxy = LosingProxy(worker, lose={1}, before={2: suspend})
    try:
        client = StatefulMicrovms(launches=[run_hook_to(worker)], endpoint=proxy.url)
        holder["config"] = MicrovmSessionConfig(
            image_identifier="img",
            execution_role_arn="role",
            timeout=Duration.from_minutes(5),
            auto_suspend_on_idle=True,
            auto_suspend_idle_time=Duration.from_seconds(60),
            client=client,
        )
        arn = runner.run_async(input="null", execution_timeout=WAIT)
        result = runner.wait_for_result(arn, timeout=WAIT)
        assert worker.idle(WAIT)
    finally:
        proxy.close()
        worker.close()
        runner.close()

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    assert json.loads(result.result or "null") == "done"
    assert runs == ["build"]
    assert proxy.statuses == [None, 503, 202]
    assert client.requests("resume_microvm") == [{"microvmIdentifier": "mvm-1"}]
