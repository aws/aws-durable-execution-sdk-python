"""Tests for trace context extraction helpers."""

from __future__ import annotations

from opentelemetry.context import Context

from aws_durable_execution_sdk_python_otel import context_extractors


def test_xray_context_extractor_returns_current_context_without_trace_header(
    monkeypatch,
):
    """Verify absent X-Ray trace headers leave the active context unchanged."""
    current_context = Context({"durable": "current"})
    monkeypatch.delenv("_X_AMZN_TRACE_ID", raising=False)
    monkeypatch.setattr(
        context_extractors.otel_context,
        "get_current",
        lambda: current_context,
    )

    assert context_extractors.xray_context_extractor(object()) is current_context


def test_xray_context_extractor_extracts_trace_header_from_environment(
    monkeypatch,
):
    """Verify X-Ray trace headers are passed through OpenTelemetry propagation."""
    trace_header = (
        "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"
    )
    current_context = Context({"durable": "current"})
    extracted_context = Context({"durable": "extracted"})
    extract_calls = []
    monkeypatch.setenv("_X_AMZN_TRACE_ID", trace_header)
    monkeypatch.setattr(
        context_extractors.otel_context,
        "get_current",
        lambda: current_context,
    )

    def extract(*, carrier, context):
        extract_calls.append({"carrier": carrier, "context": context})
        return extracted_context

    monkeypatch.setattr(context_extractors.propagate, "extract", extract)

    assert context_extractors.xray_context_extractor(object()) is extracted_context
    assert extract_calls == [
        {
            "carrier": {"X-Amzn-Trace-Id": trace_header},
            "context": current_context,
        }
    ]


def test_w3c_client_context_extractor_returns_current_context(monkeypatch):
    """Verify the placeholder W3C extractor leaves the active context unchanged."""
    current_context = Context({"durable": "current"})
    monkeypatch.setattr(
        context_extractors.otel_context,
        "get_current",
        lambda: current_context,
    )

    assert context_extractors.w3c_client_context_extractor(object()) is current_context
