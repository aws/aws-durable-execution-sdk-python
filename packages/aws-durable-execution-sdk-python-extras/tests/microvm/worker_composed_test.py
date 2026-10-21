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
import socket
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from aws_durable_execution_sdk_python.config import Duration
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
    """Records the worker's log messages."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.messages: list[str] = []

    def _add(self, msg: object, *args: object, extra: Any = None) -> None:
        with self._lock:
            self.messages.append(str(msg))

    info = warning = error = _add

    def count(self, message: str) -> int:
        with self._lock:
            return self.messages.count(message)


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
