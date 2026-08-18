"""Tests for deterministic OpenTelemetry ID generation."""

from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime

import pytest
from opentelemetry.sdk.trace import IdGenerator, RandomIdGenerator, TracerProvider

from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    DeterministicIdGenerator,
    _to_otel_trace_id,
    operation_id_to_span_id,
)


class _StubIdGenerator(IdGenerator):
    """An IdGenerator that returns fixed, identifiable IDs."""

    def __init__(
        self, trace_id: int, span_id: int, *, trace_id_is_random: bool = True
    ) -> None:
        self._trace_id = trace_id
        self._span_id = span_id
        self._trace_id_is_random = trace_id_is_random

    def generate_trace_id(self) -> int:
        return self._trace_id

    def generate_span_id(self) -> int:
        return self._span_id

    def is_trace_id_random(self) -> bool:
        return self._trace_id_is_random


def test_to_otel_trace_id_is_independent_of_xray_root_header(monkeypatch):
    """The Workflow trace must not collide with the ambient X-Ray trace."""
    monkeypatch.setenv(
        "_X_AMZN_TRACE_ID",
        "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1",
    )
    start_timestamp = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)

    assert _to_otel_trace_id("different-execution-arn", start_timestamp) == int(
        "65937d2517528419530c40ebaa7ddacf", 16
    )


def test_to_otel_trace_id_uses_timestamp_and_execution_arn():
    """Verify trace IDs are deterministic for the same execution."""
    execution_arn = "arn:aws:lambda:us-west-2:123456789012:function:workflow:$LATEST"
    start_timestamp = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)

    assert _to_otel_trace_id(execution_arn, start_timestamp) == int(
        "65937d253aa8c3f7ffe36c50d65b1a6d", 16
    )


def test_operation_id_to_span_id_returns_deterministic_64_bit_id():
    """Verify execution and operation IDs map to stable 64-bit span IDs."""
    execution_arn = "arn:aws:lambda:us-west-2:123456789012:function:workflow:$LATEST"

    assert operation_id_to_span_id(execution_arn, "my-operation") == int(
        "7495b798d83a363a", 16
    )


def test_use_ids_temporarily_overrides_trace_and_span_ids():
    """Verify deterministic IDs apply only within the explicit scope."""
    fallback = _StubIdGenerator(trace_id=int("a" * 32, 16), span_id=int("b" * 16, 16))
    generator = DeterministicIdGenerator(fallback_id_generator=fallback)
    deterministic_trace_id = int("1" * 32, 16)
    deterministic_span_id = int("2" * 16, 16)

    assert generator.generate_trace_id() == int("a" * 32, 16)
    assert generator.generate_span_id() == int("b" * 16, 16)

    with generator.use_ids(
        trace_id=deterministic_trace_id, span_id=deterministic_span_id
    ):
        assert generator.generate_trace_id() == deterministic_trace_id
        assert generator.generate_span_id() == deterministic_span_id

    assert generator.generate_trace_id() == int("a" * 32, 16)
    assert generator.generate_span_id() == int("b" * 16, 16)


def test_use_ids_allows_deterministic_trace_with_fallback_span():
    """Verify trace and span generation can be overridden independently."""
    fallback_span_id = int("b" * 16, 16)
    generator = DeterministicIdGenerator(
        fallback_id_generator=_StubIdGenerator(
            trace_id=int("a" * 32, 16), span_id=fallback_span_id
        )
    )
    deterministic_trace_id = int("1" * 32, 16)

    with generator.use_ids(trace_id=deterministic_trace_id, span_id=None):
        assert generator.generate_trace_id() == deterministic_trace_id
        assert generator.generate_span_id() == fallback_span_id


def test_use_ids_consumes_deterministic_span_id_once():
    """Verify re-entrant generation cannot reuse a deterministic span ID."""
    fallback_span_id = int("b" * 16, 16)
    generator = DeterministicIdGenerator(
        fallback_id_generator=_StubIdGenerator(
            trace_id=int("a" * 32, 16), span_id=fallback_span_id
        )
    )
    deterministic_trace_id = int("1" * 32, 16)
    deterministic_span_id = int("2" * 16, 16)

    with generator.use_ids(
        trace_id=deterministic_trace_id, span_id=deterministic_span_id
    ):
        assert generator.generate_span_id() == deterministic_span_id
        assert generator.generate_span_id() == fallback_span_id
        assert generator.generate_trace_id() == deterministic_trace_id


def test_use_ids_restores_fallback_after_exception():
    """Verify an interrupted span creation cannot leak deterministic IDs."""
    fallback_trace_id = int("a" * 32, 16)
    fallback_span_id = int("b" * 16, 16)
    generator = DeterministicIdGenerator(
        fallback_id_generator=_StubIdGenerator(
            trace_id=fallback_trace_id, span_id=fallback_span_id
        )
    )

    with pytest.raises(RuntimeError, match="span creation failed"):
        with generator.use_ids(trace_id=int("1" * 32, 16), span_id=int("2" * 16, 16)):
            raise RuntimeError("span creation failed")

    assert generator.generate_trace_id() == fallback_trace_id
    assert generator.generate_span_id() == fallback_span_id


def test_use_ids_restores_outer_nested_scope():
    """Verify nested scopes restore the preceding deterministic IDs."""
    generator = DeterministicIdGenerator()
    outer_trace_id = int("1" * 32, 16)
    outer_span_id = int("2" * 16, 16)

    with generator.use_ids(trace_id=outer_trace_id, span_id=outer_span_id):
        with generator.use_ids(trace_id=int("3" * 32, 16), span_id=int("4" * 16, 16)):
            assert generator.generate_trace_id() == int("3" * 32, 16)
        assert generator.generate_trace_id() == outer_trace_id
        assert generator.generate_span_id() == outer_span_id


def test_nested_scope_does_not_restore_consumed_outer_span_id():
    """Verify nested scope cleanup preserves prior one-shot consumption."""
    fallback_span_id = int("b" * 16, 16)
    generator = DeterministicIdGenerator(
        fallback_id_generator=_StubIdGenerator(
            trace_id=int("a" * 32, 16), span_id=fallback_span_id
        )
    )
    outer_span_id = int("2" * 16, 16)
    inner_span_id = int("4" * 16, 16)

    with generator.use_ids(trace_id=int("1" * 32, 16), span_id=outer_span_id):
        assert generator.generate_span_id() == outer_span_id
        with generator.use_ids(trace_id=int("3" * 32, 16), span_id=inner_span_id):
            assert generator.generate_span_id() == inner_span_id
        assert generator.generate_span_id() == fallback_span_id


def test_deterministic_id_generator_defaults_to_random_fallback():
    """Verify the fallback defaults to a RandomIdGenerator when none is given."""
    generator = DeterministicIdGenerator()

    assert isinstance(generator._fallback_id_generator, RandomIdGenerator)


def test_deterministic_id_generator_uses_provided_fallback_for_trace_id():
    """Verify the supplied fallback generator produces trace IDs when no
    execution trace ID is set."""
    fallback = _StubIdGenerator(trace_id=int("a" * 32, 16), span_id=int("b" * 16, 16))
    generator = DeterministicIdGenerator(fallback_id_generator=fallback)

    assert generator.generate_trace_id() == int("a" * 32, 16)


def test_deterministic_id_generator_uses_provided_fallback_for_span_id():
    """Verify the supplied fallback generator produces span IDs when no
    deterministic span ID is pending."""
    fallback = _StubIdGenerator(trace_id=int("a" * 32, 16), span_id=int("b" * 16, 16))
    generator = DeterministicIdGenerator(fallback_id_generator=fallback)

    assert generator.generate_span_id() == int("b" * 16, 16)


def test_install_on_tracer_does_not_replace_provider_generator():
    """Verify deterministic generation is isolated to one instrumentation scope."""
    provider = TracerProvider()
    provider_generator = provider.id_generator
    tracer = provider.get_tracer("durable-plugin")

    generator = DeterministicIdGenerator.install_on_tracer(tracer)

    assert tracer.id_generator is generator
    assert provider.id_generator is provider_generator
    assert provider.get_tracer("unrelated-library").id_generator is provider_generator


def test_install_on_tracer_reuses_existing_generator():
    """Verify cached tracers share one scoped generator rather than wrappers."""
    provider = TracerProvider()
    tracer = provider.get_tracer("durable-plugin")

    first = DeterministicIdGenerator.install_on_tracer(tracer)
    second = DeterministicIdGenerator.install_on_tracer(tracer)

    assert second is first


def test_is_trace_id_random_delegates_outside_override():
    """Verify deterministic trace IDs do not receive the random-ID trace flag."""
    generator = DeterministicIdGenerator(
        fallback_id_generator=_StubIdGenerator(
            trace_id=int("a" * 32, 16),
            span_id=int("b" * 16, 16),
            trace_id_is_random=True,
        )
    )

    assert generator.is_trace_id_random() is True
    with generator.use_ids(trace_id=int("1" * 32, 16), span_id=None):
        assert generator.is_trace_id_random() is False


def test_id_overrides_are_isolated_across_threads():
    """Verify concurrent threads cannot consume or overwrite each other's IDs."""
    random_span_id = int("f" * 16, 16)
    fallback = _StubIdGenerator(trace_id=int("a" * 32, 16), span_id=random_span_id)
    generator = DeterministicIdGenerator(fallback_id_generator=fallback)

    barrier = threading.Barrier(2)
    results: dict[str, tuple[int, int]] = {}

    def worker(name: str, trace_id: int, span_id: int) -> None:
        with generator.use_ids(trace_id=trace_id, span_id=span_id):
            barrier.wait()
            results[name] = (
                generator.generate_trace_id(),
                generator.generate_span_id(),
            )

    worker_a_trace_id = int("1" * 32, 16)
    worker_a_span_id = int("2" * 16, 16)
    worker_b_trace_id = int("3" * 32, 16)
    worker_b_span_id = int("3" * 16, 16)
    thread_a = threading.Thread(
        target=worker, args=("a", worker_a_trace_id, worker_a_span_id)
    )
    thread_b = threading.Thread(
        target=worker, args=("b", worker_b_trace_id, worker_b_span_id)
    )
    thread_a.start()
    thread_b.start()
    thread_a.join()
    thread_b.join()

    assert results["a"] == (worker_a_trace_id, worker_a_span_id)
    assert results["b"] == (worker_b_trace_id, worker_b_span_id)
    assert generator.generate_span_id() == random_span_id


def test_id_overrides_are_isolated_across_async_tasks():
    """Verify interleaved tasks retain their own trace and span IDs."""
    fallback_span_id = int("e" * 16, 16)
    fallback = _StubIdGenerator(trace_id=int("a" * 32, 16), span_id=fallback_span_id)
    generator = DeterministicIdGenerator(fallback_id_generator=fallback)

    task_a_trace_id = int("4" * 32, 16)
    task_a_span_id = int("4" * 16, 16)
    task_b_trace_id = int("5" * 32, 16)
    task_b_span_id = int("5" * 16, 16)

    async def task(trace_id: int, span_id: int) -> tuple[int, int]:
        with generator.use_ids(trace_id=trace_id, span_id=span_id):
            await asyncio.sleep(0)
            return generator.generate_trace_id(), generator.generate_span_id()

    async def main() -> tuple[tuple[int, int], tuple[int, int]]:
        return await asyncio.gather(
            task(task_a_trace_id, task_a_span_id),
            task(task_b_trace_id, task_b_span_id),
        )

    result_a, result_b = asyncio.run(main())

    assert result_a == (task_a_trace_id, task_a_span_id)
    assert result_b == (task_b_trace_id, task_b_span_id)
