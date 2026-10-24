"""A real completion can precede its replayed sibling's parent span."""

from __future__ import annotations

import json
import threading
import time
from typing import Any

import pytest
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import (
    CallbackConfig,
    Duration,
    ParallelConfig,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
    OperationChangeInfo,
    OperationEndInfo,
    OperationStatus,
    OperationType,
    UserFunctionStartInfo,
)
from aws_durable_execution_sdk_python.serdes import JsonSerDes
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    InvocationOtelPlugin,
    InvocationOtelPluginFactory,
    OtelPluginConfig,
)


def _span_start(span: ReadableSpan) -> int:
    assert span.start_time is not None
    return span.start_time


def test_early_sibling_callback_exports_under_real_parent_before_user_observation(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    instances: list[InvocationOtelPlugin] = []

    class RecordingFactory(InvocationOtelPluginFactory):
        def create_plugin(self, info: InvocationStartInfo) -> InvocationOtelPlugin:
            instance = super().create_plugin(info)
            instances.append(instance)
            return instance

    plugin = RecordingFactory(
        OtelPluginConfig(
            tracer_provider=provider,
            enrich_logger=False,
            context_extractor=lambda _: None,
        )
    )
    second_start = threading.Event()
    both_completed = threading.Event()
    completion_observed = threading.Event()
    parent_entered = threading.Event()
    generations = [0]
    observed: list[tuple[int, int]] = []
    hook_order: list[str] = []

    class Gate(DurableInstrumentationPlugin):
        def on_invocation_start(self, info: InvocationStartInfo) -> None:
            generations[0] += 1
            if generations[0] == 2:
                assert not any(
                    operation.name == "target-1"
                    for operation in info.updated_operations.values()
                )
                second_start.set()
                assert both_completed.wait(10)

        def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
            if generations[0] == 2 and info.name == "parallel-branch-1":
                hook_order.append("parent-gated")
                assert completion_observed.wait(10)
                hook_order.append("parent-entered")
                parent_entered.set()

    class GateFactory:
        def create_plugin(
            self, info: InvocationStartInfo
        ) -> DurableInstrumentationPlugin:
            return Gate()

    class ObserveCompletion(DurableInstrumentationPlugin):
        def on_operation_change(self, info: OperationChangeInfo) -> None:
            if generations[0] != 2:
                return
            for operation in info.updated_operations.values():
                if (
                    operation.name == "target-1"
                    and operation.operation_type is OperationType.CALLBACK
                    and operation.status is OperationStatus.SUCCEEDED
                ):
                    # OTel has retained this checkpoint snapshot before the
                    # sibling registers its replay parent. The end hook waits
                    # for the sibling to read its result, so cannot release it.
                    assert not parent_entered.is_set()
                    assert (
                        len(
                            [
                                span
                                for span in exporter.get_finished_spans()
                                if span.name == "target-1"
                            ]
                        )
                        == 1
                    )
                    hook_order.append("early-completion-observed")
                    completion_observed.set()

        def on_operation_end(self, info: OperationEndInfo) -> None:
            if (
                generations[0] == 2
                and info.name == "target-1"
                and info.operation_type is OperationType.CALLBACK
            ):
                assert parent_entered.is_set()
                hook_order.append("terminal-hook-observed")

    class ObserveCompletionFactory:
        def create_plugin(
            self, info: InvocationStartInfo
        ) -> DurableInstrumentationPlugin:
            return ObserveCompletion()

    def branch(child: DurableContext, index: int) -> Any:
        value = child.create_callback(
            name=f"target-{index}", config=CallbackConfig(serdes=JsonSerDes())
        ).result()
        if index == 1:
            finished = [
                span
                for span in exporter.get_finished_spans()
                if span.name == "target-1"
            ]
            observed.append(
                (len(finished), trace.get_current_span().get_span_context().span_id)
            )
            with provider.get_tracer("customer").start_as_current_span(
                "after-target-1"
            ):
                pass
        return value

    def handler(_event: Any, durable: DurableContext) -> list[Any]:
        result = durable.parallel(
            [lambda child: branch(child, 0), lambda child: branch(child, 1)],
            name="parallel",
            config=ParallelConfig(max_concurrency=2),
        )
        durable.wait(Duration.from_seconds(1), name="later-one")
        durable.wait(Duration.from_seconds(1), name="later-two")
        return result.get_results()

    wrapped = durable_execution(
        handler, plugins=[GateFactory(), plugin, ObserveCompletionFactory()]
    )
    try:
        with DurableFunctionTestRunner(
            handler=wrapped, poll_interval=0.01, skip_time=False, execution_timeout=25
        ) as runner:
            arn = runner.run_async(input="{}")
            deadline = time.monotonic() + 10
            callbacks: dict[str, str] = {}
            while time.monotonic() < deadline:
                history = runner.get_execution_history(
                    arn, include_execution_data=True
                ).events
                callbacks = {}
                for event in history:
                    if event.event_type == "CallbackStarted":
                        assert event.name is not None
                        assert event.callback_started_details is not None
                        assert event.callback_started_details.callback_id is not None
                        callbacks[event.name] = (
                            event.callback_started_details.callback_id
                        )
                if len(callbacks) == 2 and any(
                    event.event_type == "InvocationCompleted" for event in history
                ):
                    break
                time.sleep(0.01)
            assert set(callbacks) == {"target-0", "target-1"}
            runner.send_callback_success(
                callbacks["target-0"], result=json.dumps("left").encode()
            )
            assert second_start.wait(10)
            # The second callback completes after the invocation input snapshot,
            # so another branch's checkpoint returns this genuine new completion.
            runner.send_callback_success(
                callbacks["target-1"], result=json.dumps("right").encode()
            )
            both_completed.set()
            result = runner.wait_for_result(arn, timeout=15)
            history = runner.get_execution_history(
                arn, include_execution_data=True
            ).events
        assert result.status.value == "SUCCEEDED"
        assert result.result is not None
        assert json.loads(result.result) == ["left", "right"]
        assert completion_observed.is_set() and parent_entered.is_set()
        assert (
            hook_order.index("early-completion-observed")
            < hook_order.index("parent-entered")
            < hook_order.index("terminal-hook-observed")
        )
        assert len(observed) == 1 and observed[0][0] == 2
        spans = exporter.get_finished_spans()
        # Keep every span: Invocation view exports a nonterminal segment in
        # invocation one and a distinct terminal continuation in invocation two.
        # Neither segment may stand in for the other or reappear on later replays.
        for index in (0, 1):
            segments = sorted(
                [span for span in spans if span.name == f"target-{index}"],
                key=_span_start,
            )
            assert len(segments) == 2
            initial, terminal = segments
            assert initial.attributes is not None and terminal.attributes is not None
            assert initial.attributes["durable.operation.status"] == "STARTED"
            assert terminal.attributes["durable.operation.status"] == "SUCCEEDED"
            assert initial.context.span_id != terminal.context.span_id
            parents = sorted(
                [span for span in spans if span.name == f"parallel-branch-{index}"],
                key=_span_start,
            )
            assert len(parents) == 2
            assert initial.parent is not None
            assert initial.parent.span_id == parents[0].context.span_id
            assert terminal.parent is not None
            assert terminal.parent.span_id == parents[1].context.span_id
            assert initial.end_time is not None and terminal.start_time is not None
            assert initial.end_time <= terminal.start_time
            if index == 1:
                assert terminal.parent.span_id == observed[0][1]
                (marker,) = [span for span in spans if span.name == "after-target-1"]
                assert terminal.end_time is not None and marker.start_time is not None
                assert terminal.end_time <= marker.start_time
        assert sum(event.event_type == "InvocationCompleted" for event in history) == 4
        assert not [record for record in caplog.records if record.exc_info]
        assert len(instances) == 4
        assert len({id(instance) for instance in instances}) == 4
        assert all(instance._context_tokens == {} for instance in instances)
    finally:
        both_completed.set()
        completion_observed.set()
        provider.shutdown()
