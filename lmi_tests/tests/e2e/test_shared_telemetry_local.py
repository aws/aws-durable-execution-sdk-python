# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Public SDK/plugin integration on the local durable service, without AWS."""

from dataclasses import replace
import time

import pytest

from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.execution import DurableExecutionInvocationInput
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner
from lmi_tests import telemetry
from lmi_tests.shared_telemetry import collect_telemetry
from lmi_tests.tests.unit.test_shared_telemetry import (
    HEADER,
    SharedTrace,
    attempt,
    observations,
    wait_quiescence,
)


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("export_mode", ["sync", "async-bounded-flush"])
def test_public_local_runner_replay_preserves_export_ownership(
    monkeypatch, view, export_mode
):
    monkeypatch.setenv("_X_AMZN_TRACE_ID", HEADER)
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    traces, bodies = [], []
    telemetry._RUNTIMES.clear()

    def business(event, context):
        value = context.step(
            lambda _: bodies.append(event["marker"]) or event["marker"], name="saved"
        )
        context.wait(Duration.from_seconds(1), name="pause")
        return value

    def entry(event, context):
        invocation = (
            event
            if hasattr(event, "durable_execution_arn")
            else DurableExecutionInvocationInput.from_json_dict(event)
        )
        # The local runtime context is a test double. Give each simulated native
        # request its own ID before entering the actual public SDK handler.
        context = replace(context, aws_request_id="local-" + str(len(traces)))
        trace = SharedTrace(
            "replay", view=view, mode=export_mode, profile="python-lifecycle"
        )
        trace.identity["request"] = context.aws_request_id
        trace.identity["execution"] = invocation.durable_execution_arn
        traces.append(trace)
        trace.emit("WRAPPER_ENTER")

        def call(plugins):
            try:
                return durable_execution(business, plugins=plugins)(event, context)
            finally:
                trace.sdk_completed_at = time.time()

        result = telemetry.invoke(trace, call)
        trace.emit(
            "WRAPPER_RETURN", time=trace.sdk_completed_at, status=result["Status"]
        )
        return result

    try:
        with DurableFunctionTestRunner(
            entry, poll_interval=0.01, skip_time=False
        ) as runner:
            execution = runner.run_async(
                input={"marker": "saved"}, execution_timeout=15
            )
            result = runner.wait_for_result(execution, timeout=15)
            history = runner.get_execution_history(
                execution, include_execution_data=True
            )
        assert result.status.value == "SUCCEEDED"
        assert bodies == ["saved"]
        assert len(traces) >= 2
        assert len(history.events) > 1
        for trace in traces:
            wait_quiescence(trace)
            assert (
                observations(trace, "otel_factory")[0]["pluginExecutionArn"]
                == execution
            )
        raw = [record for trace in traces for record in trace.events]
        data = collect_telemetry(
            raw,
            [attempt(trace) for trace in traces],
            view=view,
            export_mode=export_mode,
            expected_sampled=True,
            caller_headers={trace.identity["request"]: HEADER for trace in traces},
            observed_until_ms=time.time_ns() / 1_000_000,
        )
        assert data["views"][view]["collection"]["status"] == "complete"
        spans = data["views"][view]["spans"]
        assert sum(record["span"]["name"] == "Invocation" for record in spans) == len(
            traces
        )
        assert sum(record["span"]["name"] == "Workflow" for record in spans) == 1
        assert len({binding["providerId"] for binding in data["providerBindings"]}) == 1
    finally:
        for runtime in telemetry._RUNTIMES.values():
            runtime.provider.shutdown()
        telemetry._RUNTIMES.clear()
