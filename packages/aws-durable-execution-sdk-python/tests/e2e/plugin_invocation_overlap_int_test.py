"""Two local executions may overlap without sharing invocation-hook state."""

import contextvars
import json
import threading

from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python.plugin import DurableInstrumentationPlugin
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner


def test_shared_handler_reports_both_invocation_ends(monkeypatch):
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    ready = threading.Event()
    release = threading.Event()
    binding: contextvars.ContextVar[str | None] = contextvars.ContextVar(
        "integration-plugin-owner", default=None
    )
    tokens: contextvars.ContextVar[contextvars.Token[str | None] | None] = (
        contextvars.ContextVar("integration-plugin-token", default=None)
    )
    starts = []
    ends = []

    class SafePlugin(DurableInstrumentationPlugin):
        def on_invocation_start(self, info):
            tokens.set(binding.set(info.execution_arn))
            starts.append(info.execution_arn)

        def on_invocation_end(self, info):
            assert binding.get() == info.execution_arn
            token = tokens.get()
            assert token is not None
            binding.reset(token)
            ends.append(info.execution_arn)

    def body(event, _context):
        if event["label"] == "a":
            ready.set()
            assert release.wait(5)
        return event["label"]

    wrapped = durable_execution(body, plugins=[SafePlugin()])
    try:
        with (
            DurableFunctionTestRunner(handler=wrapped, poll_interval=0.01) as first,
            DurableFunctionTestRunner(handler=wrapped, poll_interval=0.01) as second,
        ):
            first_arn = first.run_async(input='{"label":"a"}')
            assert ready.wait(5)
            try:
                second_result = second.run(input='{"label":"b"}')
            finally:
                release.set()
            first_result = first.wait_for_result(first_arn, timeout=5)
        assert first_result.status.value == second_result.status.value == "SUCCEEDED"
        assert [json.loads(first_result.result), json.loads(second_result.result)] == [
            "a",
            "b",
        ]
        assert len(starts) == 2 and len(set(starts)) == 2
        assert ends == list(reversed(starts))
        assert binding.get() is None
    finally:
        release.set()
