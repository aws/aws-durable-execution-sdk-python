# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Verify actual SDK callback parents across retries and external completion."""

from __future__ import annotations

import importlib.util
import json
import sys
import time
from collections import Counter
from pathlib import Path

import pytest
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from opentelemetry import context, trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
EXPECTED_PARENTS = {
    "retry-attempt-1": "otel-context-retry-step attempt 1",
    "retry-attempt-2": "otel-context-retry-step attempt 2",
    "condition-check-1": "otel-context-condition attempt 1",
    "condition-check-2": "otel-context-condition attempt 2",
    "callback-submitter": "otel-context-callback submitter attempt 1",
    "with-retry-body": "otel-context-with-retry",
    "with-retry-strategy": "otel-context-with-retry",
    "virtual-child": "otel-context-virtual",
}


@pytest.mark.parametrize("plugin_type", [ExecutionOtelPlugin, InvocationOtelPlugin])
def test_callback_probes_keep_their_actual_sdk_parent(
    monkeypatch: pytest.MonkeyPatch,
    plugin_type: type[ExecutionOtelPlugin] | type[InvocationOtelPlugin],
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: provider)
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
        "otel_23_callback_function_context",
        SRC_DIR / "otel_23_callback_function_context.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    try:
        with DurableFunctionTestRunner(handler=module.handler) as runner:
            arn = runner.run_async(
                input=json.dumps({"scenario": "callback-function-context"}), timeout=30
            )
            callback_id = runner.wait_for_callback(
                arn, name="otel-context-callback create callback id", timeout=10
            )
            # External delivery delay, outside the durable handler.
            time.sleep(1)
            runner.send_callback_success(callback_id, result=b"callback-complete")
            result = runner.wait_for_result(arn, timeout=15)
        assert result.status.value == "SUCCEEDED"
        assert result.result is not None
        assert json.loads(result.result) == "callback-context-complete"
        spans = exporter.get_finished_spans()
        probes = [span for span in spans if span.name.startswith("conformance.")]
        assert Counter(span.name for span in probes) == Counter(
            f"conformance.{label}" for label in EXPECTED_PARENTS
        )
        spans_by_id = {
            (span.context.trace_id, span.context.span_id): span
            for span in spans
            if span.context is not None
        }
        sdk_traces = {
            span.context.trace_id
            for span in spans
            if span.context is not None
            and span.attributes is not None
            and span.attributes.get("durable.execution.arn") == arn
        }
        for probe_span in probes:
            assert probe_span.context is not None
            assert probe_span.parent is not None
            assert probe_span.attributes is not None
            label = str(probe_span.attributes["conformance.callback"])
            assert "durable.execution.arn" not in probe_span.attributes
            assert probe_span.context.trace_id in sdk_traces
            parent = spans_by_id[
                (probe_span.context.trace_id, probe_span.parent.span_id)
            ]
            assert parent.name == EXPECTED_PARENTS[label]
            assert parent.attributes is not None
            assert parent.attributes["durable.execution.arn"] == arn
        assert context.get_current() == before_context
    finally:
        provider.shutdown()
