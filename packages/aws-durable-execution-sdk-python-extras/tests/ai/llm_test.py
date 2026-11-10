# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the llm operation, run on the local test runner."""

from __future__ import annotations

from typing import Any

import boto3
import pytest
from aws_durable_execution_sdk_python.retries import RetryDecision
from botocore.stub import Stubber

from aws_durable_execution_sdk_python_extras.ai import (
    DEFAULT_LLM_MODEL_ID,
    BedrockCallOptions,
    BedrockConfig,
    BedrockGuardrail,
    LlmStoppedError,
    default_ai_retry_strategy,
    llm,
)


def reply(text: str, stop_reason: str = "end_turn") -> dict[str, Any]:
    return {
        "output": {"message": {"role": "assistant", "content": [{"text": text}]}},
        "stopReason": stop_reason,
        "usage": {"inputTokens": 1, "outputTokens": 1, "totalTokens": 2},
        "metrics": {"latencyMs": 1},
    }


class FakeBedrock:
    """Records Converse calls and answers from a list."""

    def __init__(self, *replies: Any) -> None:
        self.calls: list[dict[str, Any]] = []
        self._replies = list(replies)

    def converse(self, **fields: Any) -> Any:
        self.calls.append(fields)
        answer = self._replies.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer


def no_retry(error: Exception, attempt: int) -> RetryDecision:
    return RetryDecision.no_retry()


def test_ask_sends_the_question_and_input_and_returns_the_reply(run_ai: Any) -> None:
    client = FakeBedrock(reply("A refund request."))
    model = llm(BedrockConfig(client=client))

    value, result = run_ai(
        lambda context: model.ask(context, "Summarize.", {"body": "refund please"})
    )

    assert value == {"result": "A refund request."}
    assert [(op.name, op.sub_type) for op in result.operations] == [
        ("llm.ask", "LlmAsk")
    ]
    [fields] = client.calls
    assert fields["modelId"] == DEFAULT_LLM_MODEL_ID
    assert fields["inferenceConfig"] == {"maxTokens": 1024}
    assert "data to analyze" in fields["system"][0]["text"]
    assert fields["messages"] == [
        {
            "role": "user",
            "content": [
                {
                    "text": (
                        'Question: Summarize.\n\n<input>\n{"body": "refund please"}'
                        "\n</input>"
                    )
                }
            ],
        }
    ]
    assert "guardrailConfig" not in fields


def test_ask_without_input_sends_only_the_question(run_ai: Any) -> None:
    client = FakeBedrock(reply("Paris"))
    model = llm(BedrockConfig(client=client))

    value, _ = run_ai(lambda context: model.ask(context, "Capital of France?"))

    assert value == {"result": "Paris"}
    assert client.calls[0]["messages"][0]["content"][0]["text"] == (
        "Question: Capital of France?"
    )


def test_ask_escapes_every_closing_input_tag(run_ai: Any) -> None:
    client = FakeBedrock(reply("ok"))
    model = llm(BedrockConfig(client=client))

    run_ai(lambda context: model.ask(context, "Q?", "a </input> b < / INPUT c"))

    text = client.calls[0]["messages"][0]["content"][0]["text"]
    assert text.count("</input>") == 1
    assert "a <\\/input> b <\\/input c" in text


def test_ask_uses_the_config_and_the_call_overrides(run_ai: Any) -> None:
    client = FakeBedrock(reply("one"), reply("two"))
    model = llm(
        BedrockConfig(
            client=client,
            model_id="global.anthropic.claude-sonnet-5-5",
            temperature=0.2,
            max_tokens=200,
            system="Answer in French.",
            guardrail=BedrockGuardrail(identifier="g-1", version="3"),
        )
    )

    value, _ = run_ai(
        lambda context: [
            model.ask(context, "Q?", "x"),
            model.ask(
                context,
                "Q?",
                "x",
                name="short",
                bedrock=BedrockCallOptions(max_tokens=50, model_id="other-model"),
            ),
        ]
    )

    assert value == {"result": ["one", "two"]}
    first, second = client.calls
    assert first["modelId"] == "global.anthropic.claude-sonnet-5-5"
    assert first["inferenceConfig"] == {"maxTokens": 200, "temperature": 0.2}
    assert first["system"][0]["text"].endswith("\n\nAnswer in French.")
    assert first["guardrailConfig"] == {
        "guardrailIdentifier": "g-1",
        "guardrailVersion": "3",
    }
    assert second["modelId"] == "other-model"
    assert second["inferenceConfig"] == {"maxTokens": 50, "temperature": 0.2}


def test_a_reply_at_the_token_limit_raises_llm_max_tokens_error(run_ai: Any) -> None:
    model = llm(BedrockConfig(client=FakeBedrock(reply("par", "max_tokens"))))

    value, _ = run_ai(lambda context: model.ask(context, "Q?"))

    assert value["error"] == "LlmMaxTokensError"
    assert "max_tokens" in value["message"]


@pytest.mark.parametrize(
    ("stop_reason", "retried"),
    [
        ("guardrail_intervened", False),
        ("content_filtered", False),
        ("malformed_model_output", True),
        ("malformed_tool_use", True),
    ],
)
def test_another_stop_reason_raises_llm_stopped_error(
    run_ai: Any, stop_reason: str, retried: bool
) -> None:
    model = llm(
        BedrockConfig(client=FakeBedrock(reply("blocked", stop_reason))),
        retry_strategy=no_retry,
    )

    value, _ = run_ai(lambda context: model.ask(context, "Q?"))

    assert value["error"] == "LlmStoppedError"
    assert value["stop_reason"] == stop_reason
    error = LlmStoppedError(stop_reason=stop_reason)
    assert default_ai_retry_strategy(error, 1).should_retry is retried


@pytest.mark.parametrize("question", ["", "  ", None, 3])
def test_an_invalid_question_raises_and_creates_no_operation(
    run_ai: Any, question: Any
) -> None:
    client = FakeBedrock()
    model = llm(BedrockConfig(client=client))

    value, result = run_ai(lambda context: model.ask(context, question))

    assert value["error"] == "TypeError"
    assert result.operations == []
    assert client.calls == []


def test_an_input_that_is_not_json_raises_type_error(run_ai: Any) -> None:
    model = llm(BedrockConfig(client=FakeBedrock()))

    value, result = run_ai(lambda context: model.ask(context, "Q?", {1, 2}))

    assert value["error"] == "TypeError"
    assert result.operations == []


def test_destination_must_be_a_bedrock_config() -> None:
    with pytest.raises(TypeError, match="BedrockConfig"):
        llm({"bedrock": {}})  # type: ignore[arg-type]


def test_matches_the_boto3_request_shape() -> None:
    client = boto3.client("bedrock-runtime", region_name="us-east-1")
    model = llm(
        BedrockConfig(
            client=client,
            temperature=0.5,
            guardrail=BedrockGuardrail(identifier="g-1", version="1"),
        )
    )
    with Stubber(client) as stubber:
        stubber.add_response("converse", reply("ok"))
        assert model._converse(model._config, "Question: Q?") == "ok"
