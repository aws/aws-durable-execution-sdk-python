"""A closing batch remains the parent of a concurrent virtual branch end."""

import json
import logging
import threading

import pytest
from aws_durable_execution_sdk_python import durable_execution
from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    MapConfig,
    NestingType,
    ParallelBranch,
    ParallelConfig,
)
from aws_durable_execution_sdk_python.plugin import DurableInstrumentationPlugin
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import ExecutionOtelPlugin, OtelPluginConfig


@pytest.mark.parametrize("operation", ["map", "parallel"])
@pytest.mark.parametrize("interleave", [False, True])
def test_closing_batch_keeps_parent_registration(
    operation, interleave, monkeypatch, caplog
):
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    gates = {}
    saved = []
    hook_ends = []
    invocation_ends = []

    class Observe(DurableInstrumentationPlugin):
        def on_invocation_start(self, info):
            gates.clear()
            gates.update(
                slow_ready=threading.Event(),
                parent_materializing=threading.Event(),
                slow_end=threading.Event(),
                ended=threading.Event(),
                parent_waited=False,
                arn=info.execution_arn,
            )

        def on_user_function_end(self, info):
            if info.name == "slow-branch":
                gates["slow_ready"].set()
                if interleave:
                    assert gates["parent_materializing"].wait(5)
            elif info.name == "fast-branch":
                assert gates["slow_ready"].wait(5)

        def on_operation_end(self, info):
            hook_ends.append((gates["arn"], info.name, info.operation_id))
            if info.name == "slow-branch":
                gates["slow_end"].set()

        def on_invocation_end(self, info):
            invocation_ends.append(info.status.value)
            gates["ended"].set()

    class OtelFactory:
        def create_plugin(self, info):
            plugin = ExecutionOtelPlugin(
                OtelPluginConfig(
                    tracer_provider=provider,
                    context_extractor=lambda _: None,
                    enrich_logger=False,
                )
            )
            original_start_span = plugin._start_span

            def gated_start_span(**kwargs):
                # Only pause scheduling. The real SDK delivers every hook, and the
                # original renderer/provider still creates and exports every span.
                if interleave and kwargs["name"] == "race-parent":
                    gates["parent_materializing"].set()
                    assert gates["slow_end"].wait(5)
                    gates["parent_waited"] = True
                return original_start_span(**kwargs)

            monkeypatch.setattr(plugin, "_start_span", gated_start_span)

            return plugin

    class ObserveFactory:
        def create_plugin(self, info):
            return Observe()

    def save(_step):
        saved.append(1)
        return "complete"

    def handler(_event, ctx):
        completion = (
            CompletionConfig.first_successful()
            if interleave
            else CompletionConfig.all_successful()
        )
        if operation == "map":
            ctx.map(
                [0, 1],
                lambda _ctx, value, _index, _items: value,
                name="race-parent",
                config=MapConfig(
                    max_concurrency=2,
                    nesting_type=NestingType.FLAT,
                    completion_config=completion,
                    item_namer=lambda _value, index: ("fast-branch", "slow-branch")[
                        index
                    ],
                ),
            )
        else:
            ctx.parallel(
                [
                    ParallelBranch(func=lambda _ctx: 0, name="fast-branch"),
                    ParallelBranch(func=lambda _ctx: 1, name="slow-branch"),
                ],
                name="race-parent",
                config=ParallelConfig(
                    max_concurrency=2,
                    nesting_type=NestingType.FLAT,
                    completion_config=completion,
                ),
            )
        return ctx.step(save, name="root-save")

    wrapped = durable_execution(handler, plugins=[OtelFactory(), ObserveFactory()])
    try:
        with (
            caplog.at_level(logging.ERROR),
            DurableFunctionTestRunner(
                handler=wrapped,
                skip_time=False,
                poll_interval=0.01,
                execution_timeout=20,
            ) as runner,
        ):
            for execution in range(2):
                arn = runner.run_async(input="{}")
                result = runner.wait_for_result(arn, timeout=15)
                assert gates["ended"].wait(5)
                spans = [
                    span
                    for span in exporter.get_finished_spans()
                    if span.attributes.get("durable.execution.arn") == arn
                ]
                rows = [
                    dict(
                        name=span.name,
                        operation=span.attributes.get("durable.operation.name"),
                        operation_id=span.attributes.get("durable.operation.id"),
                        operation_status=span.attributes.get(
                            "durable.operation.status"
                        ),
                        trace_id=format(span.context.trace_id, "032x"),
                        span_id=format(span.context.span_id, "016x"),
                        parent_id=format(span.parent.span_id, "016x")
                        if span.parent
                        else None,
                    )
                    for span in spans
                ]
                assert result.status.value == "SUCCEEDED"
                assert json.loads(result.result) == "complete"
                assert gates["slow_ready"].is_set() and gates["slow_end"].is_set()
                assert gates["parent_waited"] is interleave
                parents = [r for r in rows if r["operation"] == "race-parent"]
                slow = [r for r in rows if r["operation"] == "slow-branch"]
                fast = [r for r in rows if r["operation"] == "fast-branch"]
                assert len(parents) == len(fast) == len(slow) == 1, rows
                assert slow[0]["parent_id"] == parents[0]["span_id"]
                assert fast[0]["parent_id"] == parents[0]["span_id"]
                assert slow[0]["operation_status"] == "SUCCEEDED"
                assert len([r for r in hook_ends if r[:2] == (arn, "slow-branch")]) == 1
                assert saved == [1] * (execution + 1)
                assert invocation_ends == ["SUCCEEDED"] * (execution + 1)
        assert not [r.message for r in caplog.records if r.levelno >= logging.ERROR]
    finally:
        provider.shutdown()
