# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The agentcore operation and the real AgentCore worker, end to end, in one
process.

1. The durable function runs on the local test runner.
2. The fake ``InvokeAgentRuntime`` POSTs the payload to ``/invocations`` of a
   real worker on loopback, with the session ID header, as AgentCore does.
   It returns the worker's answer as the response stream.
3. The worker runs the job, and reports through a Lambda client double that
   forwards each callback call to the runner.

So the test covers the job document and the callback contract between the
two packages.
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
from aws_durable_execution_sdk_python_compute_worker.agentcore import (
    AgentcoreJobContext,
    AgentcoreWorker,
    start_agentcore_worker,
)
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner

from aws_durable_execution_sdk_python_extras.compute.agentcore import (
    AgentcoreConfig,
    AgentcoreError,
    agentcore,
)


WAIT = 30
ARN = "arn:aws:bedrock-agentcore:us-west-2:123456789012:runtime/reviewer-abc123"


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
                data=None,
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


def invoke_worker(worker: AgentcoreWorker) -> Callable[[dict[str, Any]], bytes]:
    """An InvokeAgentRuntime answer that POSTs the payload to ``worker``."""

    def deliver(request: dict[str, Any]) -> bytes:
        connection = http.client.HTTPConnection("127.0.0.1", worker.port, timeout=WAIT)
        try:
            connection.request(
                "POST",
                "/invocations",
                body=request["payload"],
                headers={
                    "Content-Type": request["contentType"],
                    "X-Amzn-Bedrock-AgentCore-Runtime-Session-Id": request[
                        "runtimeSessionId"
                    ],
                },
            )
            response = connection.getresponse()
            assert response.status == 200
            return response.read()
        finally:
            connection.close()

    return deliver


@pytest.fixture
def composed(fake_agentcore_client: Any) -> Iterator[Callable[..., Any]]:
    """Run a job through the operation and a real worker."""
    closers: list[Callable[[], None]] = []

    def _run(
        job: Callable[[Any, AgentcoreJobContext], Any],
        job_input: Any,
        *,
        deliveries: int = 1,
        **fields: Any,
    ) -> tuple[Any, Any, RunnerCallbacks]:
        holder: dict[str, AgentcoreConfig] = {}

        @durable_execution
        def handler(event: Any, context: DurableContext) -> Any:
            try:
                return {
                    "result": agentcore(
                        context, "review", job_input=event, config=holder["config"]
                    )
                }
            except AgentcoreError as error:
                return {"error": type(error).__name__, "message": error.message}

        runner = DurableFunctionTestRunner(handler=handler, poll_interval=0.05)
        closers.append(runner.close)
        callbacks = RunnerCallbacks(runner)
        worker = start_agentcore_worker(
            handler=job,
            # The double has the three callback methods that the worker calls.
            create_client=lambda region: callbacks,  # type: ignore[arg-type, return-value]
            heartbeat_interval_seconds=fields.pop("heartbeat_interval", None),
            host="127.0.0.1",
            port=0,
        )
        closers.append(worker.close)
        deliver = invoke_worker(worker)

        def deliver_times(request: dict[str, Any]) -> bytes:
            answers = [deliver(request) for _ in range(deliveries)]
            return answers[-1]

        client = fake_agentcore_client(invokes=[deliver_times])
        holder["config"] = AgentcoreConfig(
            agent_runtime_arn=ARN,
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
    def job(job_input: Any, context: AgentcoreJobContext) -> Any:
        return {"sum": sum(job_input["numbers"]), "session": context.session_id}

    result, client, _ = composed(job, {"numbers": [1, 2, 3]})

    [invoke] = client.requests("invoke_agent_runtime")
    assert result == {"result": {"sum": 6, "session": invoke["runtimeSessionId"]}}
    assert len(client.requests("stop_runtime_session")) == 1


def test_fails_with_the_error_type_that_the_worker_reports(composed: Any) -> None:
    def job(job_input: Any, context: AgentcoreJobContext) -> Any:
        msg = "lint failed"
        raise ValueError(msg)

    result, client, _ = composed(job, {})

    assert result == {
        "error": "AgentcoreJobFailedError",
        "message": 'AgentCore "review": the job failed (ValueError): lint failed',
    }
    assert len(client.requests("stop_runtime_session")) == 1


def test_runs_a_job_that_arrives_twice_once(composed: Any) -> None:
    runs: list[Any] = []

    def job(job_input: Any, context: AgentcoreJobContext) -> Any:
        runs.append(job_input)
        return "done"

    result, _, _ = composed(job, {"n": 1}, deliveries=2)

    assert result == {"result": "done"}
    assert runs == [{"n": 1}]


def test_sends_heartbeats_when_the_job_has_a_heartbeat_timeout(
    composed: Any,
) -> None:
    def job(job_input: Any, context: AgentcoreJobContext) -> Any:
        context.cancelled.wait(0.3)
        return "done"

    result, _, callbacks = composed(
        job,
        {},
        heartbeat_timeout=Duration.from_seconds(30),
        heartbeat_interval=0.05,
    )

    assert result == {"result": "done"}
    assert callbacks.heartbeats >= 1
