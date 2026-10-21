"""Temporary diagnostics must observe real serialization without leaking data."""

import importlib.util
import io
import json
from pathlib import Path
import sys

import boto3
from botocore.awsrequest import AWSRequest, AWSResponse
from botocore.config import Config
import pytest


SPEC = importlib.util.spec_from_file_location(
    "chain_wire_observer",
    Path(__file__).resolve().parents[1] / "src/chain_wire_observer.py",
)
observer = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = observer
SPEC.loader.exec_module(observer)

ROOT = "1-6ad0049a-2ee7ec7a5f0694681ab47ffa"
PARENT = "1ae88917648ea5bc"
HEADER = f"Root={ROOT};Parent={PARENT};Sampled=1"
OPERATION_ID = "a" * 64


def update(header):
    return {
        "Id": OPERATION_ID,
        "Type": "CHAINED_INVOKE",
        "Action": "START",
        "Name": "otel-invoke",
        "Payload": "PRIVATE_PAYLOAD",
        "ChainedInvokeOptions": {
            "FunctionName": "local-target",
            **({} if header is None else {"XAmznTraceId": header}),
        },
    }


@pytest.mark.parametrize("header", [None, "", "PRIVATE_INVALID_HEADER", HEADER])
def test_prepared_request_unchanged_and_only_trace_fields_logged(header, capsys):
    body = json.dumps(
        {
            "CheckpointToken": "PRIVATE_CHECKPOINT_TOKEN",
            "ClientToken": "PRIVATE_CLIENT_TOKEN",
            "Updates": [update(header)],
        }
    ).encode()
    request = AWSRequest(
        method="POST",
        url="https://lambda.example.invalid",
        data=body,
        headers={
            "Authorization": "PRIVATE_AUTHORIZATION",
            "X-Amz-Security-Token": "PRIVATE_SESSION_TOKEN",
            "X-Amzn-Trace-Id": HEADER + ";Lineage=PRIVATE_EXTENSION",
        },
    ).prepare()
    original_body, original_headers = request.body, dict(request.headers)
    assert observer.observe_request(request, model_has_field=True) is None
    assert request.body is original_body and dict(request.headers) == original_headers
    output = capsys.readouterr().out
    assert "PRIVATE_" not in output
    record = json.loads(output.removeprefix("DURABLE_CHAIN_WIRE "))
    assert record["event"] == "prepared_invoke_start"
    assert record["operation_id"] == OPERATION_ID
    assert record["model_has_field"] is True
    assert record["body_trace"]["present"] is (header is not None)
    assert record["body_trace"]["valid"] is (header == HEADER)
    assert record["http_trace"] == {
        "present": True,
        "valid": True,
        "Root": ROOT,
        "Parent": PARENT,
        "Sampled": "1",
    }


def test_stream_body_is_never_consumed(capsys):
    stream = io.BytesIO(b"PRIVATE_REQUEST")
    request = AWSRequest(
        method="POST", url="https://lambda.example.invalid", data=stream
    ).prepare()
    assert observer.observe_request(request, model_has_field=True) is None
    assert stream.tell() == 0
    assert "PRIVATE_REQUEST" not in capsys.readouterr().out


def test_real_botocore_prepared_send_is_observed_last_once(monkeypatch, capsys):
    client = boto3.client(
        "lambda",
        region_name="us-west-2",
        aws_access_key_id="local",
        aws_secret_access_key="local",
        endpoint_url="https://lambda.example.invalid",
        config=Config(retries={"max_attempts": 0}),
    )
    monkeypatch.setattr(observer.LambdaClient, "_cached_boto_client", client)
    seen = []

    class ResponseBody:
        def stream(self, *_args, **_kwargs):
            yield b'{"CheckpointToken":"PRIVATE_NEXT_TOKEN","NewExecutionState":{"Operations":[]}}'

    def send(request):
        seen.append(json.loads(request.body))
        return AWSResponse(
            request.url, 200, {"content-type": "application/json"}, ResponseBody()
        )

    monkeypatch.setattr(client._endpoint.http_session, "send", send)
    try:
        observer.install_checkpoint_observer()
        observer.install_checkpoint_observer()

        def instrument_http_carrier(request, **_kwargs):
            request.headers["X-Amzn-Trace-Id"] = HEADER

        # Model a normal instrumentation callback registered after the observer.
        # It exists only in this local test; the fixture never installs it.
        client.meta.events.register(observer._EVENT, instrument_http_carrier)
        result = client.checkpoint_durable_execution(
            DurableExecutionArn="arn:aws:lambda:us-west-2:123456789012:function:local:$LATEST/durable-execution/12345678-1234-1234-1234-123456789012/12345678-1234-1234-1234-123456789012",
            CheckpointToken="PRIVATE_CHECKPOINT_TOKEN",
            Updates=[update(HEADER)],
        )
        assert result["CheckpointToken"] == "PRIVATE_NEXT_TOKEN"
        assert (
            len(seen) == 1
            and seen[0]["Updates"][0]["ChainedInvokeOptions"]["XAmznTraceId"] == HEADER
        )
        output = capsys.readouterr().out
        assert "PRIVATE_" not in output
        records = [
            json.loads(line.removeprefix("DURABLE_CHAIN_WIRE "))
            for line in output.splitlines()
        ]
        assert len(records) == 1
        assert records[0]["body_trace"]["Parent"] == PARENT
        assert records[0]["http_trace"]["Parent"] == PARENT
        assert records[0]["model_has_field"] is True
    finally:
        client.close()
