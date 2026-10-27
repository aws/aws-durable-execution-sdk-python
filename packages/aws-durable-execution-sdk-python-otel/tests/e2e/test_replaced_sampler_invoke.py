"""Public invokes never persist a guessed sampling bit from a deferred span."""

import json
from datetime import UTC, datetime
from typing import Any
from unittest.mock import Mock, patch

import pytest
from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    Operation,
    OperationType,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.plugin import InvocationStartInfo
from opentelemetry import context
from opentelemetry.sdk.trace import Tracer as SdkTracer, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, ALWAYS_ON

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    ExecutionOtelPluginFactory,
    InvocationOtelPlugin,
    InvocationOtelPluginFactory,
    OtelPluginConfig,
)


@pytest.mark.parametrize(
    "factory_type", [ExecutionOtelPluginFactory, InvocationOtelPluginFactory]
)
@pytest.mark.parametrize("invoke_sampled", [False, True])
def test_replaced_sampler_carrier_and_public_replay(
    factory_type: type[ExecutionOtelPluginFactory] | type[InvocationOtelPluginFactory],
    invoke_sampled: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DURABLE_EXECUTION_PLUGINS", raising=False)
    provider = TracerProvider(sampler=ALWAYS_OFF if invoke_sampled else ALWAYS_ON)
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    factory = factory_type(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    instances: list[ExecutionOtelPlugin | InvocationOtelPlugin] = []
    create_plugin = factory.create_plugin

    def create(info: InvocationStartInfo) -> ExecutionOtelPlugin | InvocationOtelPlugin:
        plugin = create_plugin(info)
        instances.append(plugin)
        return plugin

    monkeypatch.setattr(factory, "create_plugin", create)
    updates_seen: list[OperationUpdate] = []
    saved: dict[str, Operation] = {}
    timestamp = datetime.now(UTC)
    arn = "arn:aws:lambda:us-west-2:123456789012:function:parent:1/durable-execution/test/id"

    def checkpoint(
        durable_execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        changed = []
        for update in updates:
            updates_seen.append(update)
            assert update.operation_type is OperationType.CHAINED_INVOKE
            assert update.sub_type is not None
            op = Operation.from_dict(
                dict(
                    Id=update.operation_id,
                    Type=update.operation_type.value,
                    SubType=update.sub_type,
                    Name=update.name,
                    Status="SUCCEEDED",
                    StartTimestamp=timestamp,
                    EndTimestamp=timestamp,
                    ChainedInvokeDetails={"Result": '"child-result"'},
                )
            )
            changed.append(op)
            saved[op.operation_id] = op
        return CheckpointOutput(
            "next-token", CheckpointUpdatedExecutionState(operations=changed)
        )

    def user_handler(_event: Any, durable: DurableContext) -> str:
        # Reconfigure the actual instrumentation tracer after Start, not a
        # guessed sampler call or a fabricated operation span/context.
        plugin = instances[-1]
        assert isinstance(plugin._tracer, SdkTracer)
        plugin._tracer.sampler = ALWAYS_ON if invoke_sampled else ALWAYS_OFF
        return durable.invoke("child:1", {"payload": 1}, name="invoke-target")

    handler = durable_execution(user_handler, plugins=[factory])

    invocation: dict[str, Any] = dict(
        DurableExecutionArn=arn,
        CheckpointToken="first",
        InitialExecutionState={
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
    )
    lambda_context = Mock(
        aws_request_id="request",
        client_context=None,
        identity=None,
        _epoch_deadline_time_in_ms=0,
        invoked_function_arn="parent:1",
        tenant_id=None,
    )
    service = Mock(checkpoint=checkpoint)
    before = context.get_current()
    try:
        with patch(
            "aws_durable_execution_sdk_python.execution.LambdaClient.initialize_client",
            return_value=service,
        ):
            result = handler(invocation, lambda_context)
            assert (
                result["Status"] == "SUCCEEDED"
                and json.loads(result["Result"]) == "child-result"
            )
            invocation["InitialExecutionState"]["Operations"].extend(
                op.to_json_dict() for op in saved.values()
            )
            assert handler(invocation, lambda_context) == result
        assert len(instances) == 2 and instances[0] is not instances[1]
        assert len(updates_seen) == 1
        options = updates_seen[0].chained_invoke_options
        assert options is not None
        # The real core collects metadata before its checkpoint START creates
        # the operation span, so neither view has an authoritative decision yet.
        assert options.x_amzn_trace_id is None
        emitted = [
            s for s in exporter.get_finished_spans() if s.name == "invoke-target"
        ]
        assert bool(emitted) is invoke_sampled
        assert context.get_current() == before
    finally:
        provider.shutdown()
