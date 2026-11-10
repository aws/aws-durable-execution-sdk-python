# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the decider operations, run on the local test runner."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest
from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.retries import RetryDecision

from aws_durable_execution_sdk_python_extras.ai import (
    MAX_CLASSIFY_OPTIONS,
    AiTimeoutError,
    DeciderError,
    DeciderInvalidResponseError,
    DeciderRequest,
    _shared,
    decider,
    default_ai_retry_strategy,
)


def answers(values: dict[str, Any]) -> dict[str, Any]:
    """A ``/v1/systemone`` reply with these answers."""
    return {
        "model": "fake",
        "answers": values,
        "usage": {"input_tokens": 1, "output_tokens": 1},
    }


def noul(probability: float) -> dict[str, Any]:
    return {"type": "noul", "noul": probability}


def choice(name: Any) -> dict[str, Any]:
    return {"type": "choice", "choice": name, "probabilities": {}, "confidence": 0.9}


def score(position: Any) -> dict[str, Any]:
    return {"type": "score", "score": position, "legend": {}, "probabilities": {}}


class FakeDecider:
    """A transport that records requests and answers from a list.

    Each reply is a dict to return, or an exception to raise.
    """

    def __init__(self, *replies: Any) -> None:
        self.replies = list(replies)
        self.requests: list[DeciderRequest] = []

    def __call__(self, request: DeciderRequest) -> Any:
        self.requests.append(request)
        reply = self.replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply


def no_retry(error: Exception, attempt: int) -> RetryDecision:
    return RetryDecision.no_retry()


def steps(result: Any) -> list[tuple[str | None, str | None]]:
    """The name and subtype of each top-level operation."""
    return [(operation.name, operation.sub_type) for operation in result.operations]


WAIT_LIMIT = 20
"""Seconds that a test of an unreachable endpoint may take."""


def test_check_answers_yes_at_the_threshold_and_records_one_step(run_ai: Any) -> None:
    fake = FakeDecider(answers({"answer": noul(0.5)}), answers({"answer": noul(0.49)}))
    decide = decider(transport=fake)

    value, result = run_ai(
        lambda context: [
            decide.check(context, "Is it urgent?", "help"),
            decide.check(context, "Is it urgent?", {"body": "help"}, name="second"),
        ]
    )

    assert value == {"result": [True, False]}
    assert steps(result) == [
        ("decider.check", "DeciderCheck"),
        ("second", "DeciderCheck"),
    ]
    assert fake.requests[0] == {
        "model": "strands-decider-latest",
        "state": "help",
        "questions": {"answer": {"type": "noul", "instructions": "Is it urgent?"}},
    }
    # A non-string input is sent as JSON.
    assert fake.requests[1]["state"] == '{"body": "help"}'


def test_check_uses_the_configured_threshold_and_model(run_ai: Any) -> None:
    fake = FakeDecider(answers({"answer": noul(0.6)}))
    decide = decider(transport=fake, threshold=0.7, model="hobson-latest")

    value, _ = run_ai(lambda context: decide.check(context, "Q?", "x"))

    assert value == {"result": False}
    assert fake.requests[0]["model"] == "hobson-latest"


def test_classify_returns_the_callers_spelling(run_ai: Any) -> None:
    fake = FakeDecider(answers({"answer": choice(" BILLING ")}))
    decide = decider(transport=fake)

    value, result = run_ai(
        lambda context: decide.classify(
            context, "Which team?", ["Billing", "Sales"], "x"
        )
    )

    assert value == {"result": "Billing"}
    assert steps(result) == [("decider.classify", "DeciderClassify")]
    assert fake.requests[0]["questions"] == {
        "answer": {
            "type": "choice",
            "instructions": "Which team?",
            "criteria": {"Billing": None, "Sales": None},
        }
    }


def test_label_asks_one_question_per_choice_and_keeps_their_order(run_ai: Any) -> None:
    fake = FakeDecider(answers({"o0": noul(0.9), "o1": noul(0.1), "o2": noul(0.8)}))
    decide = decider(transport=fake)

    value, result = run_ai(
        lambda context: decide.label(
            context, "Topics?", ["refund", "bug", "praise"], "x"
        )
    )

    assert value == {"result": ["refund", "praise"]}
    assert steps(result) == [("decider.label", "DeciderLabel")]
    assert fake.requests[0]["questions"]["o1"] == {
        "type": "noul",
        "instructions": "Topics?\nDoes this label apply: bug",
    }


@pytest.mark.parametrize(
    ("position", "expected"), [(0, 0.0), (1, 0.5), (2, 1.0), (2.005, 1.0)]
)
def test_score_maps_the_position_to_zero_to_one(
    run_ai: Any, position: float, expected: float
) -> None:
    fake = FakeDecider(answers({"answer": score(position)}))
    decide = decider(transport=fake)

    value, result = run_ai(
        lambda context: decide.score(
            context, "How angry?", ["calm", "upset", "furious"], "x"
        )
    )

    assert value == {"result": expected}
    assert steps(result) == [("decider.score", "DeciderScore")]
    assert fake.requests[0]["questions"]["answer"]["criteria"] == [
        "calm",
        "upset",
        "furious",
    ]


@pytest.mark.parametrize(
    ("reply", "call"),
    [
        ("not a dict", "check"),
        ({"answers": None}, "check"),
        (answers({"answer": {"type": "choice", "noul": 0.9}}), "check"),
        (answers({"answer": {"type": "noul", "noul": True}}), "check"),
        (answers({"answer": {"type": "noul", "noul": float("inf")}}), "check"),
        (answers({"answer": noul(0.9)}), "classify"),
        (answers({"answer": choice("other")}), "classify"),
        (answers({"answer": score(3)}), "score"),
        (answers({"answer": score(-0.5)}), "score"),
    ],
)
def test_an_unusable_reply_raises_decider_invalid_response_error(
    run_ai: Any, reply: Any, call: str
) -> None:
    decide = decider(transport=FakeDecider(reply))

    def body(context: Any) -> Any:
        if call == "classify":
            return decide.classify(context, "Q?", ["a", "b"], "x")
        if call == "score":
            return decide.score(context, "Q?", ["low", "high"], "x")
        return decide.check(context, "Q?", "x")

    value, _ = run_ai(body)

    assert value["error"] == "DeciderInvalidResponseError"
    assert not default_ai_retry_strategy(DeciderInvalidResponseError(), 1).should_retry


def test_a_failed_step_raises_the_same_class_with_its_status(run_ai: Any) -> None:
    error = DeciderError(message="HTTP 422: too long", status=422)
    decide = decider(transport=FakeDecider(error))

    value, _ = run_ai(lambda context: decide.check(context, "Q?", "x"))

    assert value == {
        "error": "DeciderError",
        "message": "HTTP 422: too long",
        "status": 422,
        "stop_reason": None,
    }


def test_the_default_strategy_retries_a_server_error_then_succeeds(
    run_ai: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[tuple[str, int]] = []

    def quick(error: Exception, attempt: int) -> RetryDecision:
        seen.append((type(error).__name__, attempt))
        decision = default_ai_retry_strategy(error, attempt)
        if decision.should_retry:
            return RetryDecision.retry(Duration.from_seconds(1))
        return decision

    fake = FakeDecider(
        DeciderError(message="busy", status=503), answers({"answer": noul(0.9)})
    )
    decide = decider(transport=fake, retry_strategy=quick)

    value, _ = run_ai(lambda context: decide.check(context, "Q?", "x"))

    assert value == {"result": True}
    assert seen == [("DeciderError", 1)]
    assert len(fake.requests) == 2


def test_another_error_stays_a_step_error(run_ai: Any) -> None:
    decide = decider(
        transport=FakeDecider(RuntimeError("boom")), retry_strategy=no_retry
    )

    value, _ = run_ai(lambda context: decide.check(context, "Q?", "x"))

    assert value["error"] == "StepError"
    assert "boom" in value["message"]


def test_a_request_past_the_reserve_raises_ai_timeout_error(
    run_ai: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_shared, "remaining_seconds", lambda context: 10.05)
    release = threading.Event()

    def slow(request: Any) -> Any:
        release.wait(5)
        return answers({"answer": noul(0.9)})

    decide = decider(transport=slow, retry_strategy=no_retry)
    try:
        value, _ = run_ai(lambda context: decide.check(context, "Q?", "x"))
    finally:
        release.set()

    assert value["error"] == "AiTimeoutError"
    assert default_ai_retry_strategy(AiTimeoutError(), 1).should_retry


def test_no_time_left_raises_ai_timeout_error_without_a_call(
    run_ai: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_shared, "remaining_seconds", lambda context: 9.0)
    fake = FakeDecider()
    decide = decider(transport=fake, retry_strategy=no_retry)

    value, _ = run_ai(lambda context: decide.check(context, "Q?", "x"))

    assert value["error"] == "AiTimeoutError"
    assert fake.requests == []


@pytest.mark.parametrize(
    ("call", "error"),
    [
        (lambda d, c: d.check(c, "", "x"), TypeError),
        (lambda d, c: d.check(c, 1, "x"), TypeError),
        (lambda d, c: d.check(c, "Q?", object()), TypeError),
        (lambda d, c: d.classify(c, "Q?", ["a"], "x"), ValueError),
        (lambda d, c: d.classify(c, "Q?", "ab", "x"), TypeError),
        (lambda d, c: d.classify(c, "Q?", ["a", " A "], "x"), TypeError),
        (lambda d, c: d.classify(c, "Q?", ["a", ""], "x"), TypeError),
        (
            lambda d, c: d.classify(
                c, "Q?", [f"o{i}" for i in range(MAX_CLASSIFY_OPTIONS + 1)], "x"
            ),
            ValueError,
        ),
        (lambda d, c: d.label(c, "Q?", ["a", "a"], "x"), TypeError),
        (lambda d, c: d.score(c, "Q?", ["only"], "x"), ValueError),
        (lambda d, c: d.score(c, "Q?", [str(i) for i in range(11)], "x"), ValueError),
        (lambda d, c: d.score(c, "Q?", ["a", 2], "x"), TypeError),
    ],
)
def test_an_invalid_argument_raises_and_creates_no_operation(
    run_ai: Any, call: Any, error: type[Exception]
) -> None:
    fake = FakeDecider()
    decide = decider(transport=fake)

    value, result = run_ai(lambda context: call(decide, context))

    assert value["error"] == error.__name__
    assert result.operations == []
    assert fake.requests == []


@pytest.mark.parametrize("threshold", [-0.1, 1.1])
def test_a_threshold_outside_zero_to_one_raises(threshold: float) -> None:
    with pytest.raises(ValueError, match="threshold"):
        decider(transport=FakeDecider(), threshold=threshold)


def test_takes_only_one_of_endpoint_and_transport() -> None:
    with pytest.raises(TypeError, match="only one of"):
        decider(endpoint="https://x.example/v1", transport=FakeDecider())


def test_needs_a_transport_or_an_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("STRANDS_DECIDER_ENDPOINT", raising=False)
    with pytest.raises(TypeError, match="STRANDS_DECIDER_ENDPOINT"):
        decider()


@pytest.mark.parametrize(
    ("endpoint", "message"),
    [
        ("not a url", "absolute"),
        ("ftp://x.example/v1", "absolute"),
        ("/v1/systemone", "absolute"),
        ("https://user:secret@x.example/v1", "credentials"),
    ],
)
def test_refuses_an_endpoint_that_is_not_valid(endpoint: str, message: str) -> None:
    with pytest.raises(TypeError, match=message):
        decider(endpoint=endpoint)


class _Server:
    """A local ``/v1/systemone`` server that answers from a list."""

    def __init__(self) -> None:
        self.replies: list[tuple[int, dict[str, str], bytes]] = []
        self.requests: list[tuple[str, dict[str, str], Any]] = []
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 - the http.server name
                length = int(self.headers["content-length"])
                body = json.loads(self.rfile.read(length))
                server.requests.append(
                    (self.path, {k.lower(): v for k, v in self.headers.items()}, body)
                )
                status, headers, reply = server.replies.pop(0)
                self.send_response(status)
                for key, value in headers.items():
                    self.send_header(key, value)
                self.send_header("content-length", str(len(reply)))
                self.end_headers()
                self.wfile.write(reply)

            def log_message(self, *args: Any) -> None:
                pass

        self._http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self._http.serve_forever, daemon=True).start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._http.server_port}/v1/systemone"

    def close(self) -> None:
        self._http.shutdown()
        self._http.server_close()


@pytest.fixture
def server() -> Iterator[_Server]:
    endpoint = _Server()
    yield endpoint
    endpoint.close()


def test_posts_to_the_endpoint_with_the_headers(run_ai: Any, server: _Server) -> None:
    server.replies = [(200, {}, json.dumps(answers({"answer": noul(0.9)})).encode())]
    decide = decider(endpoint=server.url, headers={"authorization": "Bearer t"})

    value, _ = run_ai(lambda context: decide.check(context, "Q?", "x"))

    assert value == {"result": True}
    [(path, headers, body)] = server.requests
    assert path == "/v1/systemone"
    assert headers["authorization"] == "Bearer t"
    assert headers["content-type"] == "application/json"
    assert body["questions"]["answer"]["instructions"] == "Q?"


def test_reads_the_endpoint_from_the_environment(
    run_ai: Any, server: _Server, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STRANDS_DECIDER_ENDPOINT", server.url)
    server.replies = [(200, {}, json.dumps(answers({"answer": noul(0.1)})).encode())]
    decide = decider()

    value, _ = run_ai(lambda context: decide.check(context, "Q?", "x"))

    assert value == {"result": False}
    assert len(server.requests) == 1


def test_an_http_error_raises_decider_error_with_its_status(
    run_ai: Any, server: _Server
) -> None:
    server.replies = [(422, {}, b'{"detail":"exceeds the context window"}')]
    decide = decider(endpoint=f"{server.url}?key=secret", retry_strategy=no_retry)

    value, _ = run_ai(lambda context: decide.check(context, "Q?", "x"))

    assert value["error"] == "DeciderError"
    assert value["status"] == 422
    assert "context window" in value["message"]
    # The checkpointed message names the endpoint without its query.
    assert "secret" not in value["message"]


def test_does_not_follow_a_redirect(run_ai: Any, server: _Server) -> None:
    server.replies = [(307, {"location": "https://other.example/v1"}, b"")]
    decide = decider(endpoint=server.url, retry_strategy=no_retry)

    value, _ = run_ai(lambda context: decide.check(context, "Q?", "x"))

    assert value["error"] == "DeciderError"
    assert value["status"] == 307
    assert "Redirects are not followed" in value["message"]
    assert len(server.requests) == 1


def test_a_reply_that_is_not_json_raises_decider_invalid_response_error(
    run_ai: Any, server: _Server
) -> None:
    server.replies = [(200, {}, b"<html>")]
    decide = decider(endpoint=server.url, retry_strategy=no_retry)

    value, _ = run_ai(lambda context: decide.check(context, "Q?", "x"))

    assert value["error"] == "DeciderInvalidResponseError"


def test_an_unreachable_endpoint_raises_a_retryable_decider_error(run_ai: Any) -> None:
    probe = ThreadingHTTPServer(("127.0.0.1", 0), BaseHTTPRequestHandler)
    port = probe.server_port
    probe.server_close()
    decide = decider(
        endpoint=f"http://127.0.0.1:{port}/v1/systemone?key=secret",
        retry_strategy=no_retry,
    )

    start = time.monotonic()
    value, _ = run_ai(lambda context: decide.check(context, "Q?", "x"))

    assert time.monotonic() - start < WAIT_LIMIT
    assert value["error"] == "DeciderError"
    assert value["status"] is None
    assert "could not be reached" in value["message"]
    assert "secret" not in value["message"]
    assert default_ai_retry_strategy(DeciderError(message="x"), 1).should_retry
