"""Trace-context extractors for durable execution telemetry."""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Callable


if TYPE_CHECKING:
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


ContextExtractor = Callable[["InvocationStartInfo"], ExtractedContext | None]


def _ensure_extracted_context(extracted: object) -> ExtractedContext | None:
    """Validate a context extractor result."""
    if extracted is None or isinstance(extracted, ExtractedContext):
        return extracted
    msg = "context extractor must return ExtractedContext or None"
    raise TypeError(msg)


def _parse_xray_trace_id(root: str | None) -> int | None:
    if root is None:
        return None
    parts = root.split("-")
    if len(parts) != 3 or parts[0] != "1":
        return None
    trace_id_hex = f"{parts[1]}{parts[2]}"
    if len(trace_id_hex) != 32:
        return None
    try:
        trace_id = int(trace_id_hex, 16)
    except ValueError:
        return None
    return trace_id if 0 < trace_id < 2**128 else None


def _parse_span_id(span_id_hex: str | None) -> int | None:
    if span_id_hex is None or len(span_id_hex) != 16:
        return None
    try:
        span_id = int(span_id_hex, 16)
    except ValueError:
        return None
    return span_id if 0 < span_id < 2**64 else None


def _parse_sampling(value: str | None) -> Sampling:
    if value == "1":
        return Sampling.SAMPLED
    if value == "0":
        return Sampling.NOT_SAMPLED
    return Sampling.UNDECIDED


def xray_context_extractor(info: "InvocationStartInfo") -> ExtractedContext | None:
    """Read durable execution trace context from ``_X_AMZN_TRACE_ID``.

    The Lambda durable execution backend propagates an X-Ray style header. A
    valid ``Root`` anchors the execution trace; a valid ``Parent`` becomes the
    remote execution ancestor; and ``Sampled`` is preserved as the backend's
    explicit sampling decision.
    """
    trace_header = os.environ.get("_X_AMZN_TRACE_ID")
    if not trace_header:
        return None

    parts: dict[str, str] = {}
    for segment in trace_header.split(";"):
        key, separator, value = segment.partition("=")
        if separator:
            parts[key.strip()] = value.strip()

    trace_id = _parse_xray_trace_id(parts.get("Root"))
    parent_span_id = _parse_span_id(parts.get("Parent"))
    sampling = _parse_sampling(parts.get("Sampled"))
    if trace_id is None and parent_span_id is None and sampling is Sampling.UNDECIDED:
        return None
    return ExtractedContext(
        trace_id=trace_id,
        parent_span_id=parent_span_id,
        sampling=sampling,
    )


def w3c_client_context_extractor(
    info: "InvocationStartInfo",
) -> ExtractedContext | None:
    """Placeholder for future W3C traceparent propagation support."""
    return None
