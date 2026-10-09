"""Owned-model round trips and the real installed Lambda wire serializer.

The header-bearing client tests intentionally fail until botocore publishes
ChainedInvokeOptions.XAmznTraceId. They are not skipped or given preview models.
"""

from __future__ import annotations

import json
from io import BytesIO
from collections.abc import Iterator
from unittest.mock import patch
from typing import Any

import boto3
import pytest
from botocore.awsrequest import AWSResponse
from botocore.config import Config
from botocore.compat import HTTPHeaders

from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import (
    ChainedInvokeOptions,
    LambdaClient,
    OperationSubType,
    OperationUpdate,
)


ARN = (
    "arn:aws:lambda:us-west-2:123456789012:function:parent:1/durable-execution/test/id"
)
HEADER = "Root=1-68e1be00-0123456789abcdef01234567;Parent=0123456789abcdef;Sampled=1"


@pytest.mark.parametrize("header", [None, "", HEADER])
@pytest.mark.parametrize("tenant", [None, "tenant-a"])
def test_chained_options_and_update_round_trip(
    header: str | None, tenant: str | None
) -> None:
    options = ChainedInvokeOptions("child:live", tenant, header)
    expected = {"FunctionName": "child:live"}
    if tenant is not None:
        expected["TenantId"] = tenant
    if header is not None:
        expected["XAmznTraceId"] = header
    assert options.to_dict() == expected
    assert ChainedInvokeOptions.from_dict(expected) == options
    update = OperationUpdate.create_invoke_start(
        OperationIdentifier(
            "invoke-a", OperationSubType.CHAINED_INVOKE, "group", "child"
        ),
        '{"preserved": true}',
        options,
    )
    assert OperationUpdate.from_dict(update.to_dict()) == update
    assert update.to_dict()["ChainedInvokeOptions"] == expected
    assert update.to_dict()["Payload"] == '{"preserved": true}'


class _ResponseBody(BytesIO):
    def stream(self, amt: int = 1024, decode_content: bool = False) -> Iterator[bytes]:
        yield self.read()


@pytest.mark.parametrize("parameter_validation", [True, False])
@pytest.mark.parametrize("with_header", [False, True])
def test_public_botocore_serializes_each_invokes_header(
    parameter_validation: bool, with_header: bool
) -> None:
    """Exercise LambdaClient -> ordinary boto client -> HTTP request, without AWS I/O."""
    client = boto3.client(
        "lambda",
        region_name="us-west-2",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        endpoint_url="https://lambda.example.invalid",
        config=Config(
            parameter_validation=parameter_validation, retries={"max_attempts": 0}
        ),
    )
    requests: list[dict[str, Any]] = []

    def capture(request: Any, **_kwargs: Any) -> AWSResponse:
        requests.append(json.loads(request.body))
        response_headers = HTTPHeaders()
        response_headers["content-type"] = "application/json"
        return AWSResponse(
            request.url,
            200,
            response_headers,
            _ResponseBody(
                b'{"CheckpointToken":"next","NewExecutionState":{"Operations":[]}}'
            ),
        )

    headers = [
        HEADER,
        HEADER.replace("0123456789abcdef;Sampled", "fedcba9876543210;Sampled"),
    ]
    updates = [
        OperationUpdate.create_invoke_start(
            OperationIdentifier(
                f"invoke-{index}",
                OperationSubType.CHAINED_INVOKE,
                "group",
                f"child-{index}",
            ),
            json.dumps({"index": index}),
            ChainedInvokeOptions(
                f"child-{index}:live", "tenant-a", header if with_header else None
            ),
        )
        for index, header in enumerate(headers)
    ]
    try:
        # Replace only network I/O; parameter validation and request serialization
        # are the unmodified installed botocore path.
        with patch("botocore.httpsession.URLLib3Session.send", side_effect=capture):
            output = LambdaClient(client).checkpoint(
                ARN, "checkpoint", updates, "client-token"
            )
        assert output.checkpoint_token == "next"
        assert len(requests) == 1
        assert requests[0]["CheckpointToken"] == "checkpoint"
        assert requests[0]["ClientToken"] == "client-token"
        assert requests[0]["Updates"] == [update.to_dict() for update in updates]
        for index, update in enumerate(requests[0]["Updates"]):
            options = update["ChainedInvokeOptions"]
            assert options["FunctionName"] == f"child-{index}:live"
            assert options["TenantId"] == "tenant-a"
            assert json.loads(update["Payload"]) == {"index": index}
            if with_header:
                assert options["XAmznTraceId"] == headers[index]
            else:
                assert "XAmznTraceId" not in options
    finally:
        client.close()
