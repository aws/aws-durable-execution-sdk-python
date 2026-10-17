# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Exercise a real invocation retry through the public decorator and local runner."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest
from aws_durable_execution_sdk_python.plugin import InvocationEndInfo, InvocationStatus
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from opentelemetry import context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


SRC_DIR = Path(__file__).resolve().parents[1] / "src"


@pytest.mark.parametrize("plugin_type", [ExecutionOtelPlugin, InvocationOtelPlugin])
def test_invocation_retry_preserves_completed_step(
    monkeypatch: pytest.MonkeyPatch,
    plugin_type: type[ExecutionOtelPlugin] | type[InvocationOtelPlugin],
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = plugin_type(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    statuses: list[InvocationStatus] = []
    original_end = plugin.on_invocation_end

    def observe_end(info: InvocationEndInfo) -> None:
        statuses.append(info.status)
        original_end(info)

    monkeypatch.setattr(plugin, "on_invocation_end", observe_end)
    before_context = context.get_current()
    common_spec = importlib.util.spec_from_file_location(
        "common", SRC_DIR / "common.py"
    )
    assert common_spec is not None and common_spec.loader is not None
    common = importlib.util.module_from_spec(common_spec)
    monkeypatch.setitem(sys.modules, "common", common)
    common_spec.loader.exec_module(common)
    monkeypatch.setattr(common, "otel_plugin", lambda: plugin)
    spec = importlib.util.spec_from_file_location(
        "otel_24_invocation_retry_status",
        SRC_DIR / "otel_24_invocation_retry_status.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    calls: list[str] = []

    def track(factory: Any, label: str) -> Any:
        def make_step() -> Any:
            step = factory()

            def counted(step_context: Any) -> str:
                calls.append(label)
                return step(step_context)

            return counted

        return make_step

    monkeypatch.setattr(
        module,
        "before_invocation_retry",
        track(module.before_invocation_retry, "saved"),
    )
    try:
        with DurableFunctionTestRunner(handler=module.handler) as runner:
            result = runner.run(
                input=json.dumps({"scenario": "invocation-retry-status"}), timeout=15
            )
        assert result.status.value == "SUCCEEDED"
        assert result.result is not None
        assert json.loads(result.result) == "retry-complete"
        assert calls == ["saved"]
        assert statuses == [InvocationStatus.RETRY, InvocationStatus.SUCCEEDED]
        spans = exporter.get_finished_spans()
        invocations = [span for span in spans if span.name == "Invocation"]
        assert len(invocations) == 2
        assert [span.status.status_code.name for span in invocations] == ["UNSET", "OK"]
        workflows = [span for span in spans if span.name == "Workflow"]
        assert len(workflows) == 1
        assert workflows[0].status.status_code.name == "OK"
        assert context.get_current() == before_context
    finally:
        provider.shutdown()
