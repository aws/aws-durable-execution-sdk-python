"""Tests for X-Ray span retrieval."""

from datetime import UTC, datetime
from unittest.mock import Mock, patch

from test.conftest import XRaySpanFetcher


START_TIME = datetime(2026, 7, 17, 20, 0, tzinfo=UTC)
END_TIME = datetime(2026, 7, 17, 20, 1, tzinfo=UTC)
TRACE_ID = "1-test"
TRACE_SUMMARIES = {"TraceSummaries": [{"Id": TRACE_ID}]}
INCOMPLETE_TRACE = {
    "Traces": [
        {
            "Id": TRACE_ID,
            "Segments": [{"Document": '{"name":"invocation"}'}],
        }
    ]
}
COMPLETE_TRACE = {
    "Traces": [
        {
            "Id": TRACE_ID,
            "Segments": [{"Document": '{"name":"top-greet"}'}],
        }
    ]
}


def test_fetch_trace_with_span_retries_incomplete_trace():
    client = Mock()
    client.get_trace_summaries.return_value = TRACE_SUMMARIES
    client.batch_get_traces.side_effect = [INCOMPLETE_TRACE, COMPLETE_TRACE]
    fetcher = XRaySpanFetcher(client)

    with patch("test.conftest.time.sleep") as sleep:
        trace_id, segment_text = fetcher.fetch_trace_with_span(
            START_TIME,
            END_TIME,
            marker_span="top-greet",
        )

    assert trace_id == TRACE_ID
    assert "top-greet" in segment_text
    assert client.get_trace_summaries.call_count == 2
    assert client.batch_get_traces.call_count == 2
    sleep.assert_called_once_with(10)


def test_fetch_trace_with_span_retries_missing_summary():
    client = Mock()
    client.get_trace_summaries.side_effect = [
        {"TraceSummaries": []},
        TRACE_SUMMARIES,
    ]
    client.batch_get_traces.return_value = COMPLETE_TRACE
    fetcher = XRaySpanFetcher(client)

    with patch("test.conftest.time.sleep") as sleep:
        trace_id, segment_text = fetcher.fetch_trace_with_span(
            START_TIME,
            END_TIME,
            marker_span="top-greet",
        )

    assert trace_id == TRACE_ID
    assert "top-greet" in segment_text
    assert client.get_trace_summaries.call_count == 2
    assert client.batch_get_traces.call_count == 1
    sleep.assert_called_once_with(10)
