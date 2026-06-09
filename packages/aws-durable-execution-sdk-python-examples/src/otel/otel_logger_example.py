"""Demonstrates OTel-enriched logging in a durable execution.

The DurableExecutionOtelPlugin wraps the execution logger (enrich_logger=True
by default) so every log line emitted through context.logger / step_context.logger
is automatically enriched with the active OpenTelemetry trace context
(otel.trace_id, otel.span_id, otel.trace_sampled). This lets logs correlate to
the spans the plugin emits without any user code changes.

Logs emitted:
- at the top level correlate to the invocation span
- inside a step correlate to that step's span
- inside a child context correlate to the child-context span
"""

from typing import Any

from aws_durable_execution_sdk_python_otel import DurableExecutionOtelPlugin
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

from aws_durable_execution_sdk_python import StepContext
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution


tracer_provider = TracerProvider()
trace.set_tracer_provider(tracer_provider)

# enrich_logger defaults to True, so the execution logger is wrapped with OTel
# trace context injection (otel.trace_id, otel.span_id, otel.trace_sampled).
otel = DurableExecutionOtelPlugin(tracer_provider)


@durable_step
def greet(step_context: StepContext, name: str) -> str:
    # Logged inside a step: enriched with this step's span_id.
    # Note: avoid reserved LogRecord keys (e.g. "name") in extra.
    step_context.logger.info("Greeting inside step", extra={"greeting_name": name})
    return f"hello {name}"


@durable_with_child_context
def greet_in_child(child_context: DurableContext, name: str) -> str:
    # Logged inside a child context: enriched with the child-context span_id.
    child_context.logger.info("Entering child context")
    result: str = child_context.step(greet(name), name="child-greet")
    child_context.logger.info("Leaving child context", extra={"result": result})
    return result


@durable_execution(plugins=[otel])
def handler(_event: Any, context: DurableContext) -> str:
    # Logged at the top level: enriched with the invocation span_id.
    context.logger.info("Workflow started")

    top: str = context.step(greet("world"), name="top-greet")
    nested: str = context.run_in_child_context(
        greet_in_child("nested"), name="child-context"
    )

    context.logger.info("Workflow completed", extra={"top": top, "nested": nested})
    return f"{top} | {nested}"
