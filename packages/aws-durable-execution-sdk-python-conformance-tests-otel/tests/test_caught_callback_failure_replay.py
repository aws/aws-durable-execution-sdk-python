"""The real case25 handler catches only its intended failure across replay."""

import importlib.util
import json
import os
from pathlib import Path
import sys
import time

import pytest
from aws_durable_execution_sdk_python import DurableContext
from aws_durable_execution_sdk_python.exceptions import (
    CallbackExternalError,
    ChildContextError,
)
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


SRC = Path(__file__).resolve().parents[1] / "src"
FAILED = "otel-failed-callback"
OBSERVED = FAILED + "-observed"
BARRIERS = [FAILED + "-barrier-one", FAILED + "-barrier-two"]
EXPECTED_HISTORY = [
    "ExecutionStarted",
    "ContextStarted",
    "CallbackStarted",
    "StepStarted",
    "StepSucceeded",
    "InvocationCompleted",
    "CallbackFailed",
    "ContextFailed",
    "StepStarted",
    "StepSucceeded",
    "ContextStarted",
    "CallbackStarted",
    "StepStarted",
    "StepSucceeded",
    "InvocationCompleted",
    "CallbackSucceeded",
    "ContextSucceeded",
    "ContextStarted",
    "CallbackStarted",
    "StepStarted",
    "StepSucceeded",
    "InvocationCompleted",
    "CallbackSucceeded",
    "ContextSucceeded",
    "InvocationCompleted",
    "ExecutionSucceeded",
]


def _callback_after_suspend(runner, arn, name):
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        events = runner.get_execution_history(arn, include_execution_data=True).events
        starts = [
            e
            for e in events
            if e.event_type == "CallbackStarted"
            and e.name == name + " create callback id"
        ]
        ends = [
            e
            for e in events
            if starts
            and e.event_type == "InvocationCompleted"
            and e.event_id > starts[0].event_id
        ]
        if starts and ends:
            return starts[0].callback_started_details.callback_id, (
                starts[0].event_id,
                ends[0].event_id,
            )
        time.sleep(0.01)
    raise AssertionError(f"No creating invocation completion for {name}")


def _load(monkeypatch, plugin):
    spec = importlib.util.spec_from_file_location("common", SRC / "common.py")
    common = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "common", common)
    spec.loader.exec_module(common)
    monkeypatch.setattr(common, "otel_plugin", lambda: plugin)
    spec = importlib.util.spec_from_file_location(
        "otel_25_caught_callback_failure_replay",
        SRC / "otel_25_caught_callback_failure_replay.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "view,mode",
    [
        (ExecutionOtelPlugin, "normal"),
        (InvocationOtelPlugin, "normal"),
        (ExecutionOtelPlugin, "unexpected-success"),
        (ExecutionOtelPlugin, "barrier-failure"),
    ],
)
def test_case25_public_caught_failure_and_replay(monkeypatch, tmp_path, view, mode):
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    starts = []
    invocation_ends = []
    exported = []
    operations = []
    user_starts = []
    failures = []
    marker_calls = []

    class Exporter(InMemorySpanExporter):
        def export(self, spans):
            exported.extend((s, len(starts)) for s in spans)
            return super().export(spans)

    exporter = Exporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = view(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )

    def record(method, collection):
        original = getattr(plugin, method)

        def wrapped(info):
            collection.append((len(starts), info))
            return original(info)

        monkeypatch.setattr(plugin, method, wrapped)

    original_start = plugin.on_invocation_start

    def on_start(info):
        starts.append(info)
        return original_start(info)

    monkeypatch.setattr(plugin, "on_invocation_start", on_start)
    record("on_invocation_end", invocation_ends)
    record("on_operation_end", operations)
    record("on_user_function_start", user_starts)
    original_wait = DurableContext.wait_for_callback

    def wait(context, *args, **kwargs):
        try:
            return original_wait(context, *args, **kwargs)
        except (CallbackExternalError, ChildContextError) as error:
            if kwargs.get("name") == FAILED:
                failures.append(
                    {
                        "invocation": len(starts),
                        "class": type(error).__name__,
                        "error_type": error.error_type,
                        "message": error.message,
                    }
                )
            raise

    monkeypatch.setattr(DurableContext, "wait_for_callback", wait)
    module = _load(monkeypatch, plugin)
    original_marker = module.observe_failure

    def counted_marker():
        step = original_marker()

        def run(context):
            marker_calls.append(len(starts))
            return step(context)

        return run

    monkeypatch.setattr(module, "observe_failure", counted_marker)
    gates = []
    try:
        with DurableFunctionTestRunner(
            handler=module.handler,
            store=FileSystemExecutionStore.create(tmp_path / "store"),
            skip_time=False,
            poll_interval=0.01,
            execution_timeout=30,
        ) as runner:
            arn = runner.run_async(
                input=json.dumps({"scenario": "caught-callback-failure-replay"})
            )
            callback, gate = _callback_after_suspend(runner, arn, FAILED)
            gates.append(gate)
            if mode == "unexpected-success":
                runner.send_callback_success(callback, result=b'"unexpected"')
            else:
                runner.send_callback_failure(callback)
                for name, result in zip(BARRIERS, ("one", "two")):
                    callback, gate = _callback_after_suspend(runner, arn, name)
                    gates.append(gate)
                    if mode == "barrier-failure":
                        runner.send_callback_failure(
                            callback, error=ErrorObject.from_message("barrier failure")
                        )
                        break
                    runner.send_callback_success(
                        callback, result=json.dumps(result).encode()
                    )
            result = runner.wait_for_result(arn, timeout=10)
            history = runner.get_execution_history(
                arn, include_execution_data=True
            ).to_dict()
        span_rows = [
            {
                "name": s.name,
                "invocation": n,
                "trace_id": f"{s.context.trace_id:032x}",
                "span_id": f"{s.context.span_id:016x}",
                "parent_id": f"{s.parent.span_id:016x}" if s.parent else None,
                "status": s.status.status_code.name,
                "start_time": s.start_time,
                "end_time": s.end_time,
                "attributes": dict(s.attributes or {}),
                "events": [
                    {
                        "name": e.name,
                        "timestamp": e.timestamp,
                        "attributes": dict(e.attributes or {}),
                    }
                    for e in s.events
                ],
                "links": [
                    {
                        "trace_id": f"{link.context.trace_id:032x}",
                        "span_id": f"{link.context.span_id:016x}",
                        "attributes": dict(link.attributes or {}),
                    }
                    for link in s.links
                ],
            }
            for s, n in exported
        ]
        summary = {
            "view": view.__name__,
            "mode": mode,
            "execution_status": result.status.value,
            "result": result.result,
            "error": result.error.to_dict() if result.error else None,
            "invocation_statuses": [info.status.value for _, info in invocation_ends],
            "gates": gates,
            "caught_failures": failures,
            "marker_body_invocations": marker_calls,
            "operation_ends": [
                {
                    "invocation": n,
                    "id": i.operation_id,
                    "name": i.name,
                    "parent_id": i.parent_id,
                    "type": i.operation_type.value,
                    "status": i.status.value,
                    "error": i.error.to_dict() if i.error else None,
                }
                for n, i in operations
            ],
            "user_function_starts": [
                {
                    "invocation": n,
                    "id": i.operation_id,
                    "name": i.name,
                    "type": i.operation_type.value,
                }
                for n, i in user_starts
            ],
        }
        proof_dir = Path(os.environ.get("PYTHON_CASE25_PROOF_DIR", str(tmp_path)))
        proof_dir.mkdir(parents=True, exist_ok=True)
        prefix = f"python-case25-{view.__name__}-{mode}"
        for suffix, data in (
            ("history", history),
            ("spans", span_rows),
            ("summary", summary),
        ):
            (proof_dir / f"{prefix}-{suffix}.json").write_text(
                json.dumps(data, indent=2, default=str)
            )
        if mode != "normal":
            assert result.status.value == "FAILED"
            assert marker_calls == ([] if mode == "unexpected-success" else [2])
            assert not any(e.get("Name") == BARRIERS[1] for e in history["Events"])
            return
        assert result.status.value == "SUCCEEDED"
        assert json.loads(result.result) == "caught/one/two"
        assert summary["invocation_statuses"] == [
            "PENDING",
            "PENDING",
            "PENDING",
            "SUCCEEDED",
        ]
        assert [e["EventType"] for e in history["Events"]] == EXPECTED_HISTORY
        assert [e["EventId"] for e in history["Events"]] == list(range(1, 27))
        assert gates == [(3, 6), (12, 15), (19, 22)]
        assert marker_calls == [2]
        assert [f["invocation"] for f in failures] == [2, 3, 4]
        assert {f["class"] for f in failures} == {"CallbackExternalError"}
        assert len({f["error_type"] for f in failures}) == 1
        failed = history["Events"][6]
        assert failed["CallbackFailedDetails"]["Error"] == {
            "Payload": {},
            "Truncated": False,
        }
        target_id = failed["Id"]
        leaves = [
            s
            for s in span_rows
            if s["attributes"].get("durable.operation.id") == target_id
            and s["attributes"].get("durable.operation.status") == "FAILED"
        ]
        assert len(leaves) == 1
        leaf = leaves[0]
        assert (
            leaf["invocation"] == 2
            and leaf["status"] == "ERROR"
            and leaf["events"] == []
        )
        marker_spans = [
            s
            for s in span_rows
            if s["attributes"].get("durable.operation.name") == OBSERVED
        ]
        assert marker_spans and all(s["invocation"] == 2 for s in marker_spans)
        assert leaf["end_time"] <= min(s["start_time"] for s in marker_spans)
        workflow = [s for s in span_rows if s["name"] == "Workflow"]
        assert len(workflow) == 1 and workflow[0]["status"] == "OK"
        parent = next(s for s in span_rows if s["span_id"] == leaf["parent_id"])
        assert (
            parent["attributes"]["durable.operation.id"] == history["Events"][1]["Id"]
        )
        if view is InvocationOtelPlugin:
            invocation = next(
                s for s in span_rows if s["span_id"] == parent["parent_id"]
            )
            assert invocation["name"] == "Invocation" and invocation["invocation"] == 2
        submitter_id = history["Events"][3]["Id"]
        assert sum(i.operation_id == submitter_id for _, i in user_starts) == 1
    finally:
        provider.shutdown()
