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

import http.client
import json
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
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner

from aws_durable_execution_sdk_python_extras.microvm import (
    MAX_RUN_HOOK_PAYLOAD_LENGTH,
    MicrovmConfig,
    MicrovmError,
    MicrovmRequestConfig,
    microvm,
)


WAIT = 30


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
        connection = http.client.HTTPConnection("127.0.0.1", worker.port, timeout=WAIT)
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
        worker = start_microvm_worker(
            handler=job,
            routes=fields.pop("routes", None),
            # The double has the three callback methods that the worker calls.
            create_client=lambda region: callbacks,  # type: ignore[arg-type, return-value]
            heartbeat_interval_seconds=fields.pop("heartbeat_interval", None),
            port=0,
        )
        closers.append(worker.close)
        client = fake_client(
            launches=[run_hook_to(worker)],
            endpoint=f"http://127.0.0.1:{worker.port}",
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
