"""Headers on real public invokes identify each calling operation's actual span."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any
from unittest.mock import Mock, patch

import pytest
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import (
    InvokeConfig,
    ParallelBranch,
    ParallelConfig,
)
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    Operation,
    OperationType,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.plugin import (
    OperationStartInfo,
    PropagationInput,
    PropagationMetadata,
    OperationType as HookOperationType,
)
from opentelemetry import context, trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanContext

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    ExtractedContext,
    OtelPluginConfig,
    Sampling,
)


ARN = (
    "arn:aws:lambda:us-west-2:123456789012:function:parent:1/durable-execution/test/id"
)
TRACE = 0x68E1BE000123456789ABCDEF01234567


@pytest.mark.parametrize("plugin_type", [ExecutionOtelPlugin, InvocationOtelPlugin])
@pytest.mark.parametrize("sampled", [False, True])
def test_parallel_invokes_carry_distinct_actual_operation_parents(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    plugin_type: type[ExecutionOtelPlugin] | type[InvocationOtelPlugin],
    sampled: bool,
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = plugin_type(
        OtelPluginConfig(
            tracer_provider=provider,
            enrich_logger=False,
            context_extractor=lambda _: ExtractedContext(
                TRACE, 42, Sampling.SAMPLED if sampled else Sampling.NOT_SAMPLED
            ),
        )
    )
    inputs: list[PropagationInput] = []
    actual_contexts: dict[str, SpanContext] = {}
    producer = plugin.provide_propagation_metadata
    start = plugin.on_operation_start

    def produce(info: PropagationInput) -> PropagationMetadata | None:
        inputs.append(info)
        return producer(info)

    def started(info: OperationStartInfo) -> None:
        start(info)
        if info.operation_type is HookOperationType.CHAINED_INVOKE:
            span = plugin._get_span(info.operation_id)
            assert span is not None
            actual_contexts[info.operation_id] = span.get_span_context()

    monkeypatch.setattr(plugin, "provide_propagation_metadata", produce)
    monkeypatch.setattr(plugin, "on_operation_start", started)
    updates_seen: list[OperationUpdate] = []
    operations: dict[str, Operation] = {}
    timestamp = datetime.now(UTC)

    def checkpoint(
        durable_execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        assert durable_execution_arn == ARN
        changed: list[Operation] = []
        for update in updates:
            updates_seen.append(update)
            assert update.sub_type is not None
            raw: dict[str, Any] = {
                "Id": update.operation_id,
                "Type": update.operation_type.value,
                "Name": update.name,
                "SubType": update.sub_type.value,
                "ParentId": update.parent_id,
                "StartTimestamp": timestamp,
                "Status": "STARTED" if update.action.value == "START" else "SUCCEEDED",
            }
            if update.operation_type is OperationType.CHAINED_INVOKE:
                assert update.chained_invoke_options is not None
                raw.update(
                    Status="SUCCEEDED",
                    EndTimestamp=timestamp,
                    ChainedInvokeDetails={
                        "Result": json.dumps(
                            update.chained_invoke_options.function_name
                        )
                    },
                )
            elif update.action.value == "SUCCEED":
                raw.update(
                    EndTimestamp=timestamp, ContextDetails={"Result": update.payload}
                )
            operations[update.operation_id] = Operation.from_dict(raw)
            changed.append(operations[update.operation_id])
        return CheckpointOutput(
            "next-token", CheckpointUpdatedExecutionState(operations=changed)
        )

    def branch_a(durable: DurableContext) -> str:
        return durable.invoke(
            "child-a:live",
            {"branch": "a"},
            name="invoke-a",
            config=InvokeConfig(tenant_id="tenant-a"),
        )

    def branch_b(durable: DurableContext) -> str:
        return durable.invoke(
            "child-b:live",
            {"branch": "b"},
            name="invoke-b",
            config=InvokeConfig(tenant_id="tenant-b"),
        )

    def user_handler(_event: Any, durable: DurableContext) -> list[str]:
        return durable.parallel(
            [
                ParallelBranch(branch_a, "branch-a"),
                ParallelBranch(branch_b, "branch-b"),
            ],
            name="invoke-group",
            config=ParallelConfig(max_concurrency=2),
        ).get_results()

    handler = durable_execution(user_handler, plugins=[plugin])
    invocation: dict[str, Any] = {
        "DurableExecutionArn": ARN,
        "CheckpointToken": "checkpoint-token",
        "InitialExecutionState": {
            "Operations": [
                {
                    "Id": "id",
                    "Type": "EXECUTION",
                    "Status": "STARTED",
                    "StartTimestamp": int(timestamp.timestamp() * 1000),
                    "ExecutionDetails": {"InputPayload": "{}"},
                }
            ],
            "NextMarker": "",
        },
    }
    lambda_context = Mock()
    lambda_context.aws_request_id = "request"
    lambda_context.client_context = None
    lambda_context.identity = None
    lambda_context._epoch_deadline_time_in_ms = 0
    lambda_context.invoked_function_arn = "parent:1"
    lambda_context.tenant_id = None
    service = Mock()
    service.checkpoint = checkpoint
    before = context.get_current()
    try:
        with provider.get_tracer("unrelated").start_as_current_span(
            "ambient"
        ) as ambient:
            with patch(
                "aws_durable_execution_sdk_python.execution.LambdaClient.initialize_client",
                return_value=service,
            ):
                output = handler(invocation, lambda_context)
                assert output["Status"] == "SUCCEEDED", output
                assert json.loads(output["Result"]) == ["child-a:live", "child-b:live"]
                invocation["InitialExecutionState"]["Operations"].extend(
                    op.to_json_dict() for op in operations.values()
                )
                assert handler(invocation, lambda_context) == output
            assert trace.get_current_span() is ambient
        assert len(inputs) == 2
        invokes = [
            u for u in updates_seen if u.operation_type is OperationType.CHAINED_INVOKE
        ]
        assert len(invokes) == 2 and len(actual_contexts) == 2
        assert len({item.parent_operation_id for item in inputs}) == 2
        headers = []
        for update in invokes:
            options = update.chained_invoke_options
            assert options is not None and options.x_amzn_trace_id is not None
            headers.append(options.x_amzn_trace_id)
            info = next(
                item for item in inputs if item.operation_id == update.operation_id
            )
            assert info == PropagationInput(
                ARN, update.operation_id, options.function_name, update.parent_id
            )
            suffix = "a" if options.function_name == "child-a:live" else "b"
            assert options.tenant_id == f"tenant-{suffix}"
            assert update.payload is not None
            assert json.loads(update.payload) == {"branch": suffix}
            fields = dict(
                part.split("=", 1) for part in options.x_amzn_trace_id.split(";")
            )
            actual = actual_contexts[update.operation_id]
            assert actual.trace_id == TRACE
            assert fields["Root"] == "1-68e1be00-0123456789abcdef01234567"
            assert int(fields["Parent"], 16) == actual.span_id
            assert actual.span_id != 42
            assert fields["Sampled"] == str(int(sampled))
        assert len(set(headers)) == 2
        emitted = [
            s
            for s in exporter.get_finished_spans()
            if s.name in ("invoke-a", "invoke-b")
        ]
        assert len(emitted) == (2 if sampled else 0)
        assert context.get_current() == before
        assert not [
            record for record in caplog.records if record.levelno >= logging.ERROR
        ]
    finally:
        provider.shutdown()
