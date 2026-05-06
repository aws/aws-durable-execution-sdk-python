"""Demonstrates handler execution without any durable operations."""

from typing import Any

import opentelemetry.sdk.trace
import opentelemetry.trace
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from aws_durable_execution_sdk_python import StepContext
from aws_durable_execution_sdk_python.config import Duration, StepConfig, StepSemantics
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution

from aws_durable_execution_sdk_python_otel import DurableExecutionOtelPlugin


# if using DD layer: arn:aws:lambda:us-west-2:464622532012:layer:Datadog-Extension:96
# initialize a tracer provider:
# tracer_provider = opentelemetry.sdk.trace.TracerProvider()
# exporter = OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces")
# tracer_provider.add_span_processor(BatchSpanProcessor(span_exporter=exporter))
# opentelemetry.trace.set_tracer_provider(tracer_provider)

# use default provider
tracer_provider = trace.get_tracer_provider()
otel = DurableExecutionOtelPlugin(tracer_provider)


@durable_step
def add_numbers(_step_context: StepContext, a: int, b: int) -> int:
    return a + b


@durable_with_child_context
def add_numbers_in_child(child_context: DurableContext, a: int, b: int):
    result: int = child_context.step(
        add_numbers(a, b),
        name=f"step-{b}",
        config=StepConfig(step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY),
    )
    child_context.wait(
        Duration.from_seconds(1),
        name=f"wait-{b}",
    )
    return result


@durable_execution(plugins=[otel])
def handler(_event: Any, context: DurableContext) -> int:
    result = 0
    for i in range(3):
        result += context.run_in_child_context(
            add_numbers_in_child(6, i),
            name=f"context-{i}",
        )
    return context.step(
        add_numbers(result, 2),
        name="final-step",
        config=StepConfig(step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY),
    )
