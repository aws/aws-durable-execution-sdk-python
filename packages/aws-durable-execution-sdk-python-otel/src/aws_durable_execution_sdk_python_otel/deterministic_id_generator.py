"""Deterministic ID generator for OpenTelemetry spans in durable executions."""

from __future__ import annotations

import contextvars
import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from opentelemetry.sdk.trace import IdGenerator, RandomIdGenerator


if TYPE_CHECKING:
    from opentelemetry.sdk.trace import Tracer as SdkTracer


@dataclass(frozen=True)
class _IdOverride:
    trace_id: int | None
    span_id: int | None


def _to_otel_trace_id(execution_arn: str, start_timestamp: datetime | None) -> int:
    """Build a deterministic OTel-compatible execution trace ID (128 bits).

    The ID is independent of ambient Lambda or X-Ray trace context so the
    parentless Workflow span remains the only root of the durable execution
    trace. Invocation spans inherit ambient context separately.
    """
    time_part = format(int((start_timestamp or datetime.now(UTC)).timestamp()), "08x")
    hash_part = hashlib.blake2b(execution_arn.encode()).hexdigest()[:24]  # noqa: S324
    return int(f"{time_part}{hash_part}", 16)


def operation_id_to_span_id(durable_execution_arn: str, operation_id: str) -> int:
    """Derive a deterministic span ID (64 bits) from an execution and operation."""
    plain_value = f"{durable_execution_arn}:{operation_id}"
    hashed_operation_id = hashlib.blake2b(plain_value.encode()).hexdigest()[:16]
    return int(hashed_operation_id, 16)


def derive_workflow_span_id(durable_execution_arn: str) -> int:
    """Derive the deterministic Workflow root span ID (64 bits) from an execution ARN.

    Mirrors the JS ``deriveWorkflowSpanId``: hash ``"workflow:" + arn`` and take
    the first 16 hex characters. All invocations of the same durable execution
    therefore share one Workflow root span ID, allowing the Workflow span to be
    (re-)created and exported exactly once per execution regardless of how many
    Lambda invocations back it.

    The hash algorithm is blake2b to stay consistent with
    :func:`operation_id_to_span_id`; it does not need to match the JS SDK's
    SHA-256 byte-for-byte because span IDs only need to be stable within a single
    execution's trace, not across language runtimes.

    Args:
        durable_execution_arn: The durable execution ARN. Must be non-empty.

    Returns:
        A 64-bit integer span ID. If the derived value is all-zero (invalid per
        the OTel spec) it is bumped to ``1``.

    Raises:
        ValueError: If ``durable_execution_arn`` is empty. Only emptiness is
            validated; ARN format is not checked.
    """
    if not durable_execution_arn:
        raise ValueError("execution ARN is required to derive a workflow span ID")
    plain_value = f"workflow:{durable_execution_arn}"
    hashed = hashlib.blake2b(plain_value.encode()).hexdigest()[:16]
    span_id = int(hashed, 16)
    return span_id or 1


class DeterministicIdGenerator(RandomIdGenerator):
    """An ID generator with invocation-scoped deterministic ID overrides.

    Deterministic IDs are active only inside :meth:`use_ids`. All other
    generation is delegated to the fallback generator. The override is stored
    in a context variable so concurrent threads and async tasks cannot consume
    or overwrite each other's IDs.

    Trace IDs embed a real timestamp so they satisfy the X-Ray format
    requirement (first 8 hex chars = Unix epoch seconds).

    Args:
        fallback_id_generator: Generator used when no deterministic ID is
            available. Defaults to a new ``RandomIdGenerator``.
    """

    def __init__(self, fallback_id_generator: IdGenerator | None = None) -> None:
        self._fallback_id_generator = fallback_id_generator or RandomIdGenerator()
        self._id_override: contextvars.ContextVar[_IdOverride | None] = (
            contextvars.ContextVar("durable_execution_id_override", default=None)
        )

    @classmethod
    def install_on_tracer(cls, tracer: SdkTracer) -> DeterministicIdGenerator:
        """Return the tracer's deterministic generator, installing one if needed.

        Installing on the plugin's tracer keeps unrelated instrumentation scopes
        on the provider's original generator. Reusing an installed generator also
        supports SDK versions that cache tracers by instrumentation scope.
        """
        current_generator = tracer.id_generator
        if isinstance(current_generator, cls):
            return current_generator

        generator = cls(fallback_id_generator=current_generator)
        tracer.id_generator = generator
        return generator

    @contextmanager
    def use_ids(self, *, trace_id: int | None, span_id: int | None) -> Iterator[None]:
        """Temporarily override IDs generated in the current execution context."""
        token = self._id_override.set(_IdOverride(trace_id, span_id))
        try:
            yield
        finally:
            self._id_override.reset(token)

    def generate_trace_id(self) -> int:
        """Generate a 128-bit trace ID."""
        override = self._id_override.get()
        if override is not None and override.trace_id is not None:
            return override.trace_id
        return self._fallback_id_generator.generate_trace_id()

    def generate_span_id(self) -> int:
        """Generate a 64-bit span ID."""
        override = self._id_override.get()
        if override is not None and override.span_id is not None:
            span_id = override.span_id
            # Consume before returning so a re-entrant call in the same span
            # creation falls back instead of reusing the deterministic ID.
            self._id_override.set(_IdOverride(override.trace_id, None))
            return span_id
        return self._fallback_id_generator.generate_span_id()

    def is_trace_id_random(self) -> bool:
        """Report whether the current trace ID is randomly generated."""
        override = self._id_override.get()
        if override is not None and override.trace_id is not None:
            return False
        fallback_method = getattr(
            self._fallback_id_generator, "is_trace_id_random", None
        )
        return bool(fallback_method()) if fallback_method is not None else False
