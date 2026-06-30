"""Tests for deterministic OpenTelemetry ID generation."""

from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime

from opentelemetry.sdk.trace import IdGenerator, RandomIdGenerator

from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    DeterministicIdGenerator,
    _parse_xray_root_trace_id,
    _to_otel_trace_id,
    _xray_trace_id_to_otel,
    operation_id_to_span_id,
)


class _StubIdGenerator(IdGenerator):
    """An IdGenerator that returns fixed, identifiable IDs."""

    def __init__(self, trace_id: int, span_id: int) -> None:
        self._trace_id = trace_id
        self._span_id = span_id

    def generate_trace_id(self) -> int:
        return self._trace_id

    def generate_span_id(self) -> int:
        return self._span_id


def test_parse_xray_root_trace_id_returns_root_from_header():
    """Verify X-Ray Root trace ID parsing ignores other header fields."""
    trace_header = (
        "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"
    )

    assert (
        _parse_xray_root_trace_id(trace_header) == "1-5759e988-bd862e3fe1be46a994272793"
    )


def test_parse_xray_root_trace_id_returns_none_for_missing_or_malformed_header():
    """Verify absent or malformed X-Ray headers are ignored."""
    assert _parse_xray_root_trace_id(None) is None
    assert _parse_xray_root_trace_id("") is None
    assert _parse_xray_root_trace_id("Parent=53995c3f42cd8ad8;Sampled=1") is None
    assert (
        _parse_xray_root_trace_id(
            "Root=1-5759e988-not-enough-hex;Parent=53995c3f42cd8ad8"
        )
        is None
    )


def test_xray_trace_id_to_otel_removes_xray_prefix_and_normalizes_case():
    """Verify X-Ray trace IDs are converted into OTel-compatible integers."""
    trace_id = "1-5759E988-BD862E3FE1BE46A994272793"

    assert _xray_trace_id_to_otel(trace_id) == int(
        "5759e988bd862e3fe1be46a994272793", 16
    )


def test_to_otel_trace_id_uses_xray_root_header_when_available(monkeypatch):
    """Verify Lambda's X-Ray trace header takes precedence over fallback IDs."""
    monkeypatch.setenv(
        "_X_AMZN_TRACE_ID",
        "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1",
    )
    start_timestamp = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)

    assert _to_otel_trace_id("different-execution-arn", start_timestamp) == int(
        "5759e988bd862e3fe1be46a994272793", 16
    )


def test_to_otel_trace_id_falls_back_to_timestamp_and_execution_arn(monkeypatch):
    """Verify fallback trace IDs are deterministic for the same execution."""
    monkeypatch.delenv("_X_AMZN_TRACE_ID", raising=False)
    execution_arn = "arn:aws:lambda:us-west-2:123456789012:function:workflow:$LATEST"
    start_timestamp = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)

    assert _to_otel_trace_id(execution_arn, start_timestamp) == int(
        "65937d253aa8c3f7ffe36c50d65b1a6d", 16
    )


def test_operation_id_to_span_id_returns_deterministic_64_bit_id():
    """Verify operation IDs map to stable 64-bit span IDs."""
    assert operation_id_to_span_id("my-operation") == int("ab1f94a6d3c668f3", 16)


def test_deterministic_id_generator_returns_cached_trace_id(monkeypatch):
    """Verify trace IDs are cached after being set for an execution."""
    monkeypatch.delenv("_X_AMZN_TRACE_ID", raising=False)
    generator = DeterministicIdGenerator()

    generator.set_trace_id(
        "arn:aws:lambda:us-west-2:123456789012:function:workflow:$LATEST",
        datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC),
    )

    assert generator.generate_trace_id() == int("65937d253aa8c3f7ffe36c50d65b1a6d", 16)


def test_deterministic_id_generator_falls_back_to_random_trace_id(monkeypatch):
    """Verify trace IDs are random until an execution trace ID is set."""
    expected_trace_id = int("1" * 32, 16)
    generator = DeterministicIdGenerator()
    monkeypatch.setattr(
        generator._fallback_id_generator,
        "generate_trace_id",
        lambda: expected_trace_id,
    )

    assert generator.generate_trace_id() == expected_trace_id


def test_deterministic_id_generator_uses_next_span_id_once(monkeypatch):
    """Verify a configured span ID only applies to the next generated span."""
    deterministic_span_id = int("2" * 16, 16)
    random_span_id = int("3" * 16, 16)
    generator = DeterministicIdGenerator()
    monkeypatch.setattr(
        generator._fallback_id_generator,
        "generate_span_id",
        lambda: random_span_id,
    )

    generator.set_next_span_id(deterministic_span_id)

    assert generator.generate_span_id() == deterministic_span_id
    assert generator.generate_span_id() == random_span_id


def test_deterministic_id_generator_accepts_cleared_next_span_id(monkeypatch):
    """Verify clearing the next span ID preserves random span generation."""
    expected_span_id = int("4" * 16, 16)
    generator = DeterministicIdGenerator()
    monkeypatch.setattr(
        generator._fallback_id_generator,
        "generate_span_id",
        lambda: expected_span_id,
    )

    generator.set_next_span_id(None)

    assert generator.generate_span_id() == expected_span_id


def test_deterministic_id_generator_defaults_to_random_fallback():
    """Verify the fallback defaults to a RandomIdGenerator when none is given."""
    generator = DeterministicIdGenerator()

    assert isinstance(generator._fallback_id_generator, RandomIdGenerator)


def test_deterministic_id_generator_uses_provided_fallback_for_trace_id(monkeypatch):
    """Verify the supplied fallback generator produces trace IDs when no
    execution trace ID is set."""
    monkeypatch.delenv("_X_AMZN_TRACE_ID", raising=False)
    fallback = _StubIdGenerator(trace_id=int("a" * 32, 16), span_id=int("b" * 16, 16))
    generator = DeterministicIdGenerator(fallback_id_generator=fallback)

    assert generator.generate_trace_id() == int("a" * 32, 16)


def test_deterministic_id_generator_uses_provided_fallback_for_span_id():
    """Verify the supplied fallback generator produces span IDs when no
    deterministic span ID is pending."""
    fallback = _StubIdGenerator(trace_id=int("a" * 32, 16), span_id=int("b" * 16, 16))
    generator = DeterministicIdGenerator(fallback_id_generator=fallback)

    assert generator.generate_span_id() == int("b" * 16, 16)


def test_deterministic_id_generator_prefers_execution_trace_id_over_fallback(
    monkeypatch,
):
    """Verify a configured execution trace ID takes precedence over the fallback."""
    monkeypatch.delenv("_X_AMZN_TRACE_ID", raising=False)
    fallback = _StubIdGenerator(trace_id=int("a" * 32, 16), span_id=int("b" * 16, 16))
    generator = DeterministicIdGenerator(fallback_id_generator=fallback)

    generator.set_trace_id(
        "arn:aws:lambda:us-west-2:123456789012:function:workflow:$LATEST",
        datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC),
    )

    assert generator.generate_trace_id() == int("65937d253aa8c3f7ffe36c50d65b1a6d", 16)


def test_deterministic_id_generator_prefers_next_span_id_over_fallback():
    """Verify a pending deterministic span ID takes precedence over the fallback."""
    deterministic_span_id = int("c" * 16, 16)
    fallback = _StubIdGenerator(trace_id=int("a" * 32, 16), span_id=int("b" * 16, 16))
    generator = DeterministicIdGenerator(fallback_id_generator=fallback)

    generator.set_next_span_id(deterministic_span_id)

    assert generator.generate_span_id() == deterministic_span_id
    # Subsequent calls fall back to the provided generator.
    assert generator.generate_span_id() == int("b" * 16, 16)


def test_pending_span_id_is_isolated_across_threads():
    """Verify a span ID set in one thread is not consumed by another thread.

    The pending span ID is stored in a ContextVar, so each worker thread has
    its own value. Without this isolation a concurrent operation could steal
    another operation's deterministic span ID, producing the wrong span ID.
    """
    random_span_id = int("f" * 16, 16)
    fallback = _StubIdGenerator(trace_id=int("a" * 32, 16), span_id=random_span_id)
    generator = DeterministicIdGenerator(fallback_id_generator=fallback)

    # The main thread sets a deterministic span ID but never consumes it.
    main_deterministic_span_id = int("1" * 16, 16)
    generator.set_next_span_id(main_deterministic_span_id)

    barrier = threading.Barrier(2)
    results: dict[str, int] = {}

    def worker(name: str, span_id: int) -> None:
        # Each worker starts with a fresh context (default None), so it must
        # not see the main thread's pending span ID.
        barrier.wait()
        results[f"{name}-before-set"] = generator.generate_span_id()
        generator.set_next_span_id(span_id)
        results[f"{name}-after-set"] = generator.generate_span_id()

    worker_a_span_id = int("2" * 16, 16)
    worker_b_span_id = int("3" * 16, 16)
    thread_a = threading.Thread(target=worker, args=("a", worker_a_span_id))
    thread_b = threading.Thread(target=worker, args=("b", worker_b_span_id))
    thread_a.start()
    thread_b.start()
    thread_a.join()
    thread_b.join()

    # Workers never observed the main thread's value; they fell back to random.
    assert results["a-before-set"] == random_span_id
    assert results["b-before-set"] == random_span_id
    # Each worker consumed only its own deterministic span ID.
    assert results["a-after-set"] == worker_a_span_id
    assert results["b-after-set"] == worker_b_span_id
    # The main thread's pending span ID was untouched by the workers.
    assert generator.generate_span_id() == main_deterministic_span_id


def test_pending_span_id_is_isolated_across_async_tasks():
    """Verify a span ID set in one async task is not consumed by another.

    Each asyncio task runs with its own copied context, so the pending span ID
    stays scoped to the task that set it even across await boundaries on the
    same thread.
    """
    fallback_span_id = int("e" * 16, 16)
    fallback = _StubIdGenerator(trace_id=int("a" * 32, 16), span_id=fallback_span_id)
    generator = DeterministicIdGenerator(fallback_id_generator=fallback)

    task_a_span_id = int("4" * 16, 16)
    task_b_span_id = int("5" * 16, 16)

    async def task(span_id: int) -> int:
        generator.set_next_span_id(span_id)
        # Yield control so the other task interleaves between set and consume.
        await asyncio.sleep(0)
        return generator.generate_span_id()

    async def main() -> tuple[int, int]:
        return await asyncio.gather(task(task_a_span_id), task(task_b_span_id))

    result_a, result_b = asyncio.run(main())

    # Despite interleaving, each task consumed only its own deterministic ID.
    assert result_a == task_a_span_id
    assert result_b == task_b_span_id
