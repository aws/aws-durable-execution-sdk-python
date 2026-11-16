# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the AgentCore, SageMaker, and Lambda transports."""

from __future__ import annotations

import io
import json
from typing import Any

import boto3
import pytest
from aws_durable_execution_sdk_python.retries import RetryDecision
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from aws_durable_execution_sdk_python_extras.ai import (
    DeciderError,
    DeciderInvalidResponseError,
    agentcore_transport,
    decider,
    default_ai_retry_strategy,
    lambda_transport,
    sagemaker_transport,
)


ARN = "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/strands-decider-abc"
ENDPOINT = "strands-decider-2b-qwen3-5-v1-2610"
FUNCTION = "strands-decider-2b-qwen3-5-v1-2610"

REQUEST: Any = {
    "model": "strands-decider-latest",
    "state": "help",
    "questions": {"answer": {"type": "noul", "instructions": "Is it urgent?"}},
}
REPLY = {"model": "fake", "answers": {"answer": {"type": "noul", "noul": 0.9}}}


def stream(body: str | bytes) -> io.BytesIO:
    """A body as boto3 returns it: an object with ``read()``."""
    return io.BytesIO(body.encode() if isinstance(body, str) else body)


def client_error(code: str, status: int = 400, **extra: Any) -> ClientError:
    response: Any = {
        "Error": {"Code": code, "Message": f"{code} message"},
        "ResponseMetadata": {"HTTPStatusCode": status},
        **extra,
    }
    return ClientError(response, "Call")


class FakeClient:
    """Records calls of one boto3 method and answers from a list."""

    def __init__(self, method: str, *replies: Any) -> None:
        self.calls: list[dict[str, Any]] = []
        self._replies = list(replies)
        setattr(self, method, self._call)

    def _call(self, **fields: Any) -> Any:
        self.calls.append(fields)
        reply = self._replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply


def body_of(fields: dict[str, Any], key: str) -> Any:
    return json.loads(fields[key])


# ---------------------------------------------------------------- AgentCore


def test_agentcore_sends_the_request_and_returns_the_reply() -> None:
    client = FakeClient(
        "invoke_agent_runtime",
        {"statusCode": 200, "response": stream(json.dumps(REPLY))},
    )

    reply = agentcore_transport(ARN, qualifier="prod", client=client)(REQUEST)

    assert reply == REPLY
    [fields] = client.calls
    assert body_of(fields, "payload") == REQUEST
    assert {k: v for k, v in fields.items() if k != "payload"} == {
        "agentRuntimeArn": ARN,
        "qualifier": "prod",
        "contentType": "application/json",
        "accept": "application/json",
    }


def test_agentcore_omits_the_qualifier_when_none_is_set() -> None:
    client = FakeClient(
        "invoke_agent_runtime",
        {"statusCode": 200, "response": stream(json.dumps(REPLY))},
    )

    agentcore_transport(ARN, client=client)(REQUEST)

    assert "qualifier" not in client.calls[0]


def test_agentcore_turns_a_non_2xx_status_into_decider_error() -> None:
    client = FakeClient(
        "invoke_agent_runtime",
        {
            "statusCode": 422,
            "response": stream('{"detail":"exceeds the context window"}'),
        },
    )

    with pytest.raises(DeciderError) as caught:
        agentcore_transport(ARN, client=client)(REQUEST)

    assert caught.value.status == 422
    assert "context window" in str(caught.value)
    assert not default_ai_retry_strategy(caught.value, 1).should_retry


def test_agentcore_turns_a_reply_that_is_not_json_into_invalid_response() -> None:
    client = FakeClient(
        "invoke_agent_runtime", {"statusCode": 200, "response": stream("<html>")}
    )

    with pytest.raises(DeciderInvalidResponseError):
        agentcore_transport(ARN, client=client)(REQUEST)


def test_agentcore_matches_the_boto3_request_shape() -> None:
    client = boto3.client("bedrock-agentcore", region_name="us-east-1")
    with Stubber(client) as stubber:
        stubber.add_response(
            "invoke_agent_runtime",
            {
                "contentType": "application/json",
                "statusCode": 200,
                "response": stream(json.dumps(REPLY)),
            },
        )
        assert agentcore_transport(ARN, client=client)(REQUEST) == REPLY


# ---------------------------------------------------------------- SageMaker


def test_sagemaker_sends_the_request_and_returns_the_reply() -> None:
    client = FakeClient("invoke_endpoint", {"Body": stream(json.dumps(REPLY))})

    reply = sagemaker_transport(ENDPOINT, client=client)(REQUEST)

    assert reply == REPLY
    [fields] = client.calls
    assert body_of(fields, "Body") == REQUEST
    assert {k: v for k, v in fields.items() if k != "Body"} == {
        "EndpointName": ENDPOINT,
        "ContentType": "application/json",
        "Accept": "application/json",
    }


@pytest.mark.parametrize(("status", "retried"), [(422, False), (503, True)])
def test_sagemaker_turns_a_model_error_into_decider_error_with_its_status(
    status: int, retried: bool
) -> None:
    error = client_error(
        "ModelError",
        424,
        OriginalStatusCode=status,
        OriginalMessage='{"detail":"exceeds the context window"}',
    )
    client = FakeClient("invoke_endpoint", error)

    with pytest.raises(DeciderError) as caught:
        sagemaker_transport(ENDPOINT, client=client)(REQUEST)

    assert caught.value.status == status
    assert "context window" in str(caught.value)
    assert caught.value.__cause__ is error
    assert default_ai_retry_strategy(caught.value, 1).should_retry is retried


@pytest.mark.parametrize(
    ("code", "status", "retried"),
    [
        ("ModelNotReadyException", 429, True),
        ("ServiceUnavailable", 503, True),
        ("InternalFailure", 500, True),
        ("InternalDependencyException", 530, True),
        ("ValidationError", 400, False),
    ],
)
def test_sagemaker_passes_other_service_errors_through(
    code: str, status: int, retried: bool
) -> None:
    error = client_error(code, status)
    client = FakeClient("invoke_endpoint", error)

    with pytest.raises(ClientError) as caught:
        sagemaker_transport(ENDPOINT, client=client)(REQUEST)

    assert caught.value is error
    assert default_ai_retry_strategy(error, 1).should_retry is retried


def test_sagemaker_turns_a_reply_that_is_not_json_into_invalid_response() -> None:
    client = FakeClient("invoke_endpoint", {"Body": stream("<html>")})

    with pytest.raises(DeciderInvalidResponseError):
        sagemaker_transport(ENDPOINT, client=client)(REQUEST)


def test_sagemaker_matches_the_boto3_request_shape() -> None:
    client = boto3.client("sagemaker-runtime", region_name="us-east-1")
    with Stubber(client) as stubber:
        stubber.add_response("invoke_endpoint", {"Body": stream(json.dumps(REPLY))})
        assert sagemaker_transport(ENDPOINT, client=client)(REQUEST) == REPLY


# ---------------------------------------------------------------- Lambda


def test_lambda_invokes_with_the_request_as_the_event() -> None:
    client = FakeClient(
        "invoke", {"StatusCode": 200, "Payload": stream(json.dumps(REPLY))}
    )

    reply = lambda_transport(FUNCTION, qualifier="live", client=client)(REQUEST)

    assert reply == REPLY
    [fields] = client.calls
    assert body_of(fields, "Payload") == REQUEST
    assert fields["FunctionName"] == FUNCTION
    assert fields["Qualifier"] == "live"


def test_lambda_omits_the_qualifier_when_none_is_set() -> None:
    client = FakeClient(
        "invoke", {"StatusCode": 200, "Payload": stream(json.dumps(REPLY))}
    )

    lambda_transport(FUNCTION, client=client)(REQUEST)

    assert "Qualifier" not in client.calls[0]


@pytest.mark.parametrize("error_type", ["ValueError", "ValidationError"])
def test_lambda_turns_a_refused_request_into_status_422(error_type: str) -> None:
    payload = {"errorType": error_type, "errorMessage": "exceeds the context window"}
    client = FakeClient(
        "invoke",
        {
            "StatusCode": 200,
            "FunctionError": "Unhandled",
            "Payload": stream(json.dumps(payload)),
        },
    )

    with pytest.raises(DeciderError) as caught:
        lambda_transport(FUNCTION, client=client)(REQUEST)

    assert caught.value.status == 422
    assert error_type in str(caught.value)
    assert "context window" in str(caught.value)
    assert not default_ai_retry_strategy(caught.value, 1).should_retry


@pytest.mark.parametrize(
    ("payload", "kind"),
    [
        (
            json.dumps(
                {"errorType": "Runtime.ExitError", "errorMessage": "exit status 1"}
            ),
            "Runtime.ExitError",
        ),
        ("oops", "Unhandled"),
        ("[1]", "Unhandled"),
    ],
)
def test_lambda_turns_another_function_error_into_a_retryable_error(
    payload: str, kind: str
) -> None:
    client = FakeClient(
        "invoke",
        {"StatusCode": 200, "FunctionError": "Unhandled", "Payload": stream(payload)},
    )

    with pytest.raises(DeciderError) as caught:
        lambda_transport(FUNCTION, client=client)(REQUEST)

    assert caught.value.status is None
    assert kind in str(caught.value)
    assert default_ai_retry_strategy(caught.value, 1).should_retry


@pytest.mark.parametrize(
    ("code", "status"),
    [("TooManyRequestsException", 429), ("ServiceException", 500)],
)
def test_lambda_passes_throttling_and_server_errors_through_for_a_retry(
    code: str, status: int
) -> None:
    error = client_error(code, status)
    client = FakeClient("invoke", error)

    with pytest.raises(ClientError) as caught:
        lambda_transport(FUNCTION, client=client)(REQUEST)

    assert caught.value is error
    assert default_ai_retry_strategy(error, 1).should_retry


def test_lambda_turns_a_reply_that_is_not_json_into_invalid_response() -> None:
    client = FakeClient("invoke", {"StatusCode": 200, "Payload": stream("<html>")})

    with pytest.raises(DeciderInvalidResponseError):
        lambda_transport(FUNCTION, client=client)(REQUEST)


def test_lambda_matches_the_boto3_request_shape() -> None:
    client = boto3.client("lambda", region_name="us-east-1")
    with Stubber(client) as stubber:
        stubber.add_response(
            "invoke", {"StatusCode": 200, "Payload": stream(json.dumps(REPLY))}
        )
        assert lambda_transport(FUNCTION, client=client)(REQUEST) == REPLY


# ---------------------------------------------------------------- decider()


def test_decider_runs_a_transport_in_a_durable_step(run_ai: Any) -> None:
    client = FakeClient("invoke_endpoint", {"Body": stream(json.dumps(REPLY))})
    decide = decider(transport=sagemaker_transport(ENDPOINT, client=client))

    value, result = run_ai(
        lambda context: decide.check(context, "Is it urgent?", "help")
    )

    assert value == {"result": True}
    assert [(op.name, op.sub_type) for op in result.operations] == [
        ("decider.check", "DeciderCheck")
    ]
    assert body_of(client.calls[0], "Body") == REQUEST


def test_decider_raises_the_transport_error_with_its_status(run_ai: Any) -> None:
    payload = {"errorType": "ValueError", "errorMessage": "too long"}
    client = FakeClient(
        "invoke",
        {
            "StatusCode": 200,
            "FunctionError": "Unhandled",
            "Payload": stream(json.dumps(payload)),
        },
    )
    decide = decider(
        transport=lambda_transport(FUNCTION, client=client),
        retry_strategy=lambda error, attempt: RetryDecision.no_retry(),
    )

    value, _ = run_ai(lambda context: decide.check(context, "Q?", "x"))

    assert value["error"] == "DeciderError"
    assert value["status"] == 422


def test_creates_the_default_client_on_first_use(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    made: list[str] = []
    client = FakeClient("invoke_endpoint", {"Body": stream(json.dumps(REPLY))})

    def fake_client(service: str) -> Any:
        made.append(service)
        return client

    monkeypatch.setattr(boto3, "client", fake_client)
    send = sagemaker_transport(ENDPOINT)
    assert made == []

    send(REQUEST)

    assert made == ["sagemaker-runtime"]
