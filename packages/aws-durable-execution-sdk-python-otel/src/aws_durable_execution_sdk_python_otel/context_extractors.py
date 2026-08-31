"""Context extractors for propagating trace context into durable executions."""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Callable

from opentelemetry import context as otel_context, propagate


if TYPE_CHECKING:
    from opentelemetry.context import Context

    from aws_durable_execution_sdk_python.plugin import InvocationStartInfo


class Sampling(Enum):
    """Sampling decision propagated by the durable execution backend."""

    SAMPLED = "sampled"
    NOT_SAMPLED = "not_sampled"
    UNDECIDED = "undecided"


@dataclass(frozen=True)
class ExtractedContext:
    """Trace context extracted from the durable execution backend.

    Attributes:
        trace_id: OTel 128-bit trace ID, or ``None`` when no valid trace ID was
            present.
        parent_span_id: OTel 64-bit parent span ID, or ``None`` when no valid
            parent was present.
        sampling: Explicit backend sampling decision, or ``UNDECIDED`` when
            the backend header did not include one.
    """

    trace_id: int | None
    parent_span_id: int | None
    sampling: Sampling = Sampling.UNDECIDED

    @property
    def has_valid_trace_id(self) -> bool:
        return self.trace_id is not None and 0 < self.trace_id < 2**128

    @property
    def has_valid_parent_span_id(self) -> bool:
        return self.parent_span_id is not None and 0 < self.parent_span_id < 2**64

    @property
    def has_complete_remote_parent(self) -> bool:
        return self.has_valid_trace_id and self.has_valid_parent_span_id


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
