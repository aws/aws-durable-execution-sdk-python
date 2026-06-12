"""Tests for deterministic OpenTelemetry ID generation."""

from __future__ import annotations

from datetime import UTC, datetime

from aws_durable_execution_sdk_python_otel.deterministic_id_generator import (
    HASHED_ID_PATTERN,
    DeterministicIdGenerator,
    _parse_xray_root_trace_id,
    _to_otel_trace_id,
    _xray_trace_id_to_otel,
    operation_id_to_span_id,
)


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
        generator._random_id_generator,
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
        generator._random_id_generator,
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
        generator._random_id_generator,
        "generate_span_id",
        lambda: expected_span_id,
    )

    generator.set_next_span_id(None)

    assert generator.generate_span_id() == expected_span_id
