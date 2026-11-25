# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""agentcore_session and the real AgentCore worker, end to end, in one
process.

1. The durable function runs on the local test runner.
2. The fake ``InvokeAgentRuntime`` POSTs each payload to ``/invocations`` of
   one real worker on loopback, with the session ID header, as AgentCore
   does. It returns the worker's answer as the response stream.
3. The worker runs each job, and reports through a Lambda client double that
   forwards each callback call to the runner.

So the test covers a session of several jobs across the two packages.
"""

from __future__ import annotations

import http.client
import json
from collections.abc import Callable, Iterator
from contextlib import ExitStack
from pathlib import Path
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

from aws_durable_execution_sdk_python_extensions.compute.agentcore import (
    AgentcoreInvokeOptions,
    AgentcoreSession,
    AgentcoreSessionConfig,
    agentcore_session,
)


WAIT = 30
ARN = "arn:aws:bedrock-agentcore:us-west-2:123456789012:runtime/reviewer-abc123"


class RunnerCallbacks:
    """A Lambda client for the worker that sends each callback call to the
    local test runner."""

    def __init__(self, runner: DurableFunctionTestRunner) -> None:
        self._runner = runner

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
        self._runner.send_callback_heartbeat(CallbackId)
        return {}


def deliver_to(worker: AgentcoreWorker, request: dict[str, Any]) -> bytes:
    """POST an InvokeAgentRuntime request to ``worker``, as AgentCore does."""
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


@pytest.fixture
def closers() -> Iterator[list[Callable[[], None]]]:
    """Run each close at teardown in reverse order, even if one fails."""
    closers: list[Callable[[], None]] = []
    yield closers
    with ExitStack() as stack:
        for close in closers:
            stack.callback(close)


def test_runs_two_jobs_in_one_session_of_one_worker(
    fake_agentcore_client: Any, tmp_path: Path, closers: list[Callable[[], None]]
) -> None:
    def job(job_input: Any, context: AgentcoreJobContext) -> Any:
        # Each session gets its own directory, as in a session's storage.
        notes = tmp_path / str(context.session_id) / "notes.txt"
        if job_input["step"] == "write":
            notes.parent.mkdir(parents=True)
            notes.write_text("lint passed")
            return {"session": context.session_id}
        return {"session": context.session_id, "notes": notes.read_text()}

    def body(session: AgentcoreSession, context: DurableContext) -> Any:
        options = AgentcoreInvokeOptions(timeout=Duration.from_minutes(5))
        return [
            session.invoke(context, step, job_input={"step": step}, options=options)
            for step in ("write", "read")
        ]

    @durable_execution
    def handler(event: Any, context: DurableContext) -> Any:
        config = AgentcoreSessionConfig(agent_runtime_arn=ARN, client=client)
        return agentcore_session(context, "review", config=config, handler=body)

    runner = DurableFunctionTestRunner(handler=handler, poll_interval=0.05)
    closers.append(runner.close)
    callbacks = RunnerCallbacks(runner)
    worker = start_agentcore_worker(
        handler=job,
        # The double has the three callback methods that the worker calls.
        create_client=lambda region: callbacks,  # type: ignore[arg-type, return-value]
        host="127.0.0.1",
        port=0,
    )
    closers.append(worker.close)
    client = fake_agentcore_client(
        invokes=[lambda request: deliver_to(worker, request)] * 2
    )
    arn = runner.run_async(input="{}", execution_timeout=WAIT)
    result = runner.wait_for_result(arn, timeout=WAIT)
    assert worker.idle(WAIT)

    assert result.status is InvocationStatus.SUCCEEDED, result.error
    [stop] = client.requests("stop_runtime_session")
    session_id = stop["runtimeSessionId"]
    invokes = client.requests("invoke_agent_runtime")
    assert [invoke["runtimeSessionId"] for invoke in invokes] == [session_id] * 2
    assert json.loads(result.result or "null") == [
        {"session": session_id},
        {"session": session_id, "notes": "lint passed"},
    ]
