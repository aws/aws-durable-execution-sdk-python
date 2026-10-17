# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Exercise case 21 through the public decorator and local durable runner."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest
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
def test_completed_step_replay_uses_saved_result(
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
        "otel_21_completed_step_replay", SRC_DIR / "otel_21_completed_step_replay.py"
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

    monkeypatch.setattr(module, "before_wait", track(module.before_wait, "before"))
    monkeypatch.setattr(module, "after_wait", track(module.after_wait, "after"))
    try:
        with DurableFunctionTestRunner(handler=module.handler) as runner:
            result = runner.run(
                input=json.dumps({"scenario": "completed-step-replay"}), timeout=15
            )
        assert result.status.value == "SUCCEEDED"
        assert result.result is not None
        assert json.loads(result.result) == "before-after"
        assert calls == ["before", "after"]
        spans = exporter.get_finished_spans()
        invocations = [span for span in spans if span.name == "Invocation"]
        assert [
            span.attributes["durable.invocation.status"]
            for span in invocations
            if span.attributes is not None
        ] == ["PENDING", "SUCCEEDED"]
        if plugin_type is ExecutionOtelPlugin:
            # Count raw exports, including duplicates that reuse a span ID.
            for name in ("otel-before-wait", "otel-replay-wait", "otel-after-wait"):
                assert len([span for span in spans if span.name == name]) == 1
        assert context.get_current() == before_context
    finally:
        provider.shutdown()
