"""Tests for trace context extraction helpers."""

from __future__ import annotations

import pytest

from aws_durable_execution_sdk_python_otel import context_extractors
from aws_durable_execution_sdk_python_otel.context_extractors import (
    ExtractedContext,
    Sampling,
)


def test_xray_context_extractor_returns_none_without_trace_header(monkeypatch):
    monkeypatch.delenv("_X_AMZN_TRACE_ID", raising=False)

    assert context_extractors.xray_context_extractor(object()) is None


def test_xray_context_extractor_extracts_trace_parent_and_sampling(monkeypatch):
    monkeypatch.setenv(
        "_X_AMZN_TRACE_ID",
        "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1",
    )

    extracted = context_extractors.xray_context_extractor(object())

    assert extracted is not None
    assert extracted.trace_id == int("5759e988bd862e3fe1be46a994272793", 16)
    assert extracted.parent_span_id == int("53995c3f42cd8ad8", 16)
    assert extracted.sampling is Sampling.SAMPLED
    assert extracted.has_complete_remote_parent


def test_xray_context_extractor_preserves_valid_root_without_parent(monkeypatch):
    monkeypatch.setenv(
        "_X_AMZN_TRACE_ID",
        "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=0000000000000000;Sampled=0",
    )

    extracted = context_extractors.xray_context_extractor(object())

    assert extracted is not None
    assert extracted.trace_id == int("5759e988bd862e3fe1be46a994272793", 16)
    assert extracted.parent_span_id is None
    assert extracted.sampling is Sampling.NOT_SAMPLED
    assert not extracted.has_complete_remote_parent


def test_xray_context_extractor_drops_all_zero_root(monkeypatch):
    monkeypatch.setenv(
        "_X_AMZN_TRACE_ID",
        "Root=1-00000000-000000000000000000000000;Parent=53995c3f42cd8ad8;Sampled=1",
    )

    extracted = context_extractors.xray_context_extractor(object())

    assert extracted is not None
    assert extracted.trace_id is None
    assert extracted.parent_span_id == int("53995c3f42cd8ad8", 16)
    assert extracted.sampling is Sampling.SAMPLED


def test_ensure_extracted_context_rejects_invalid_extractor_result():
    with pytest.raises(TypeError, match="ExtractedContext or None"):
        context_extractors._ensure_extracted_context(object())


def test_ensure_extracted_context_accepts_structured_context():
    extracted = ExtractedContext(
        trace_id=int("5759e988bd862e3fe1be46a994272793", 16),
        parent_span_id=int("53995c3f42cd8ad8", 16),
        sampling=Sampling.SAMPLED,
    )

    assert context_extractors._ensure_extracted_context(extracted) is extracted


@pytest.mark.parametrize(
    "root",
    [
        "1-5759e988",  # too few segments
        "2-5759e988-bd862e3fe1be46a994272793",  # unsupported version
        "1-5759e988-bd862e",  # trace-id hex too short
        "1-5759e988-zzzzzzzzzzzzzzzzzzzzzzzz",  # non-hex trace id
    ],
)
def test_xray_context_extractor_ignores_malformed_root(monkeypatch, root):
    monkeypatch.setenv(
        "_X_AMZN_TRACE_ID",
        f"Root={root};Parent=53995c3f42cd8ad8;Sampled=1",
    )

    extracted = context_extractors.xray_context_extractor(object())

    assert extracted is not None
    assert extracted.trace_id is None
    assert extracted.parent_span_id == int("53995c3f42cd8ad8", 16)


@pytest.mark.parametrize(
    "parent",
    [
        "53995c3f",  # wrong length
        "zzzzzzzzzzzzzzzz",  # non-hex
    ],
)
def test_xray_context_extractor_ignores_malformed_parent(monkeypatch, parent):
    monkeypatch.setenv(
        "_X_AMZN_TRACE_ID",
        f"Root=1-5759e988-bd862e3fe1be46a994272793;Parent={parent};Sampled=1",
    )

    extracted = context_extractors.xray_context_extractor(object())

    assert extracted is not None
    assert extracted.trace_id == int("5759e988bd862e3fe1be46a994272793", 16)
    assert extracted.parent_span_id is None


def test_xray_context_extractor_returns_none_for_unparseable_header(monkeypatch):
    monkeypatch.setenv("_X_AMZN_TRACE_ID", "garbage-without-known-fields")

    assert context_extractors.xray_context_extractor(object()) is None


def test_w3c_client_context_extractor_returns_none():
    assert context_extractors.w3c_client_context_extractor(object()) is None
