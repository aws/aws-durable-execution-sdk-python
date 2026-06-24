"""Demonstrates OTel-enriched logging in a durable execution.

The DurableExecutionOtelPlugin installs a logging filter on the root logger
(enrich_logger=True by default) when the plugin is constructed. The filter
stamps the active OpenTelemetry trace context (otel_trace_id, otel_span_id,
otel_trace_sampled) onto every log record that flows through the root handler.
This includes logs emitted via context.logger / step_context.logger as well as
direct logging.getLogger() calls and third-party library logs, so logs
correlate to the spans the plugin emits without any user code changes.

Logs emitted:
- at the top level correlate to the invocation span
- inside a step correlate to that step's span
- inside a child context correlate to the child-context span
"""

from typing import Any

from aws_durable_execution_sdk_python_otel import DurableExecutionOtelPlugin

from aws_durable_execution_sdk_python import StepContext
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution


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


@durable_execution(plugins=[DurableExecutionOtelPlugin()])
def handler(_event: Any, context: DurableContext) -> str:
    # Logged at the top level: enriched with the invocation span_id.
    context.logger.info("Workflow started")

    top: str = context.step(greet("world"), name="top-greet")
    nested: str = context.run_in_child_context(
        greet_in_child("nested"), name="child-context"
    )

    context.logger.info("Workflow completed", extra={"top": top, "nested": nested})
    return f"{top} | {nested}"
