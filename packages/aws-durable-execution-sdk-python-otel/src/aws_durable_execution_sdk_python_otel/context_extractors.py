"""Context extractors for propagating trace context into durable executions."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Callable

from opentelemetry import context as otel_context, propagate


if TYPE_CHECKING:
    from opentelemetry.context import Context

    from aws_durable_execution_sdk_python.plugin import InvocationStartInfo

ContextExtractor = Callable[["InvocationStartInfo"], "Context"]


def xray_context_extractor(info: "InvocationStartInfo") -> "Context":
    """Read the X-Ray trace header from the _X_AMZN_TRACE_ID environment variable.

    The durable execution backend propagates the same Root trace ID to every
    invocation, so all invocations share one traceId.
    """
    trace_header = os.environ.get("_X_AMZN_TRACE_ID")
    if not trace_header:
        return otel_context.get_current()
    return propagate.extract(
        carrier={"X-Amzn-Trace-Id": trace_header},
        context=otel_context.get_current(),
    )


def w3c_client_context_extractor(info: "InvocationStartInfo") -> "Context":
    """Read W3C traceparent from context.clientContext.custom.traceparent.

    Requires the backend clientContext propagation to be enabled.
    This extractor is a placeholder for when backend propagation is supported.
    """
    return otel_context.get_current()
