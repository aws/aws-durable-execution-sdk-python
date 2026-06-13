"""Deterministic ID generator for OpenTelemetry spans in durable executions."""

from __future__ import annotations

import hashlib
import os
import re
from datetime import UTC, datetime

from opentelemetry.sdk.trace import IdGenerator, RandomIdGenerator


HASHED_ID_PATTERN = re.compile(r"^[0-9a-f]{16}$")


def _parse_xray_root_trace_id(trace_header: str | None) -> str | None:
    """Parse the Root trace ID from an X-Ray trace header string.

    The header format is:
      Root=1-<8 hex>-<24 hex>;Parent=<16 hex>;Sampled=0|1

    Returns the root value (e.g. "1-5759e988-bd862e3fe1be46a994272793")
    or None if the header is missing or malformed.
    """
    if not trace_header:
        return None
    match = re.search(r"Root=(1-[0-9a-fA-F]{8}-[0-9a-fA-F]{24})", trace_header)
    return match.group(1) if match else None


def _xray_trace_id_to_otel(xray_trace_id: str) -> int:
    """Convert an X-Ray trace ID to the W3C/OpenTelemetry 32-char hex format.

    X-Ray format: "1-<8hex>-<24hex>" (36 chars with prefix and dashes)
    OTel format:  "<8hex><24hex>" (32 lowercase hex chars)
    """
    otel_id = xray_trace_id.replace("1-", "", 1).replace("-", "").lower()
    return int(otel_id, 16)


def _to_otel_trace_id(execution_arn: str, start_timestamp: datetime | None) -> int:
    """Build an OTel-compatible trace ID (128 bits)

    First attempts to read the trace ID from the _X_AMZN_TRACE_ID environment
    variable that Lambda populates on each invocation. This ties the durable
    execution spans to the same trace that X-Ray is already tracking.

    Falls back to generating a deterministic trace ID from the execution ARN
    and timestamp when the environment variable is not set (e.g. in tests or
    non-Lambda environments).
    """
    env_trace_id = _parse_xray_root_trace_id(os.environ.get("_X_AMZN_TRACE_ID"))
    if env_trace_id:
        return _xray_trace_id_to_otel(env_trace_id)

    # Fallback: deterministic ID from execution ARN + timestamp
    time_part = format(int((start_timestamp or datetime.now(UTC)).timestamp()), "08x")
    hash_part = hashlib.blake2b(execution_arn.encode()).hexdigest()[:24]  # noqa: S324
    return int(f"{time_part}{hash_part}", 16)


def operation_id_to_span_id(operation_id: str) -> int:
    """Derive a deterministic span ID (64 bits) from an operation ID."""
    hashed_operation_id = hashlib.blake2b(operation_id.encode()).hexdigest()[:16]
    return int(hashed_operation_id, 16)


class DeterministicIdGenerator(RandomIdGenerator):
    """An ID generator that produces deterministic span IDs when a pending
    operation ID is set, and falls back to the provided generator otherwise.

    Trace IDs are deterministic when an execution ARN is set, ensuring all
    invocations of the same durable execution share a single trace. When no
    deterministic ID is available, generation is delegated to the fallback
    generator (the tracer provider's original ID generator by default).

    Trace IDs embed a real timestamp so they satisfy the X-Ray format
    requirement (first 8 hex chars = Unix epoch seconds).

    Args:
        fallback_id_generator: Generator used when no deterministic ID is
            available. Defaults to a new ``RandomIdGenerator``.
    """

    def __init__(self, fallback_id_generator: IdGenerator | None = None) -> None:
        self._next_span_id: int | None = None
        self._execution_trace_id: int | None = None
        self._fallback_id_generator = fallback_id_generator or RandomIdGenerator()

    def set_next_span_id(self, span_id: int | None) -> None:
        """Set the operation ID to use for the next span's ID.

        After one span is created, it resets to random.
        """
        self._next_span_id = span_id

    def set_trace_id(
        self, execution_arn: str, start_timestamp: datetime | None
    ) -> None:
        """Compute and cache the deterministic trace ID for this execution.

        Args:
            execution_arn: The durable execution ARN (used for the hash portion).
            start_timestamp: start time of invocation
        """
        self._execution_trace_id = _to_otel_trace_id(execution_arn, start_timestamp)

    def generate_trace_id(self) -> int:
        """Generate a 128-bit trace ID."""
        return (
            self._execution_trace_id or self._fallback_id_generator.generate_trace_id()
        )

    def generate_span_id(self) -> int:
        """Generate a 64-bit span ID."""
        span_id, self._next_span_id = self._next_span_id, None
        return span_id or self._fallback_id_generator.generate_span_id()
