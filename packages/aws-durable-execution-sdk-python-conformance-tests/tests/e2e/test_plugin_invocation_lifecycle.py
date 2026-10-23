"""Exercise lifecycle log ordering through the SDK and its local runner."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import pytest
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner


HANDLERS_DIR = Path(__file__).resolve().parents[2] / "handlers"
sys.path.insert(0, str(HANDLERS_DIR))

from plugin.plugin_invocation_lifecycle import LifecyclePluginFactory, greet, handler  # noqa: E402


def _lifecycle_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if getattr(record, "plugin", None) == "CONFPLUGIN"
        or record.getMessage().startswith("Greeting step running for:")
    ]


def test_lifecycle_and_greeting_share_structured_logging(
    caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    caplog.set_level(logging.INFO)
    with DurableFunctionTestRunner(handler=handler) as runner:
        result = runner.run(input=json.dumps("Ada"), execution_timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert json.loads(result.result) == "Hello, Ada!"
    start, greeting, end = _lifecycle_records(caplog)
    assert start.plugin == end.plugin == "CONFPLUGIN"
    assert start.hook == "invocation-start"
    assert start.first is True
    assert greeting.getMessage() == "Greeting step running for: Ada"
    assert end.hook == "invocation-end"
    assert end.status == "SUCCEEDED"
    assert start.durableExecutionArn
    assert start.durableExecutionArn == greeting.executionArn
    assert start.durableExecutionArn == end.durableExecutionArn
    assert start.name == greeting.name == end.name
    assert capsys.readouterr().out == ""


def test_replay_logs_hooks_without_repeating_the_greeting(
    caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    @durable_execution(plugins=[LifecyclePluginFactory()])
    def handler_with_wait(event: str, context: DurableContext) -> str:
        result = context.step(greet(event))
        context.wait(Duration.from_seconds(10))
        return result

    caplog.set_level(logging.INFO)
    with DurableFunctionTestRunner(handler=handler_with_wait) as runner:
        result = runner.run(input=json.dumps("Ada"), execution_timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED
    assert json.loads(result.result) == "Hello, Ada!"
    records = _lifecycle_records(caplog)
    assert [getattr(record, "hook", "greeting") for record in records] == [
        "invocation-start",
        "greeting",
        "invocation-end",
        "invocation-start",
        "invocation-end",
    ]
    assert records[0].first is True
    assert records[2].status == "PENDING"
    assert records[3].first is False
    assert records[4].status == "SUCCEEDED"
    plugin_arns = {
        record.durableExecutionArn for record in records if hasattr(record, "hook")
    }
    assert plugin_arns == {records[1].executionArn}
    assert capsys.readouterr().out == ""
