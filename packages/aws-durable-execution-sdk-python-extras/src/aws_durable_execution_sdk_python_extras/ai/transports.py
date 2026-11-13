# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Transports that send a ``decider`` request to a Strands Decider.

This module is experimental. Its API can change in any release.

Every place that hosts a Strands Decider takes the same request body and
returns the same reply. Only the way the call is sent and signed changes. So
each place has one transport factory:

- :func:`sagemaker_transport` calls ``InvokeEndpoint`` on Amazon SageMaker.
- :func:`lambda_transport` calls ``Invoke`` on AWS Lambda.
- :func:`agentcore_transport` calls ``InvokeAgentRuntime`` on Amazon Bedrock
  AgentCore Runtime.

``decider(endpoint=...)`` sends the request over plain HTTP. Any callable that
takes a :class:`DeciderRequest` and returns the JSON reply is a transport too.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from typing import Any, TypedDict

from botocore.exceptions import ClientError

from aws_durable_execution_sdk_python_extras.ai._shared import LazyClient, parse_reply
from aws_durable_execution_sdk_python_extras.ai.errors import DeciderError


class DeciderRequest(TypedDict):
    """A request to a decision model, in the format of its
    ``POST /v1/systemone`` route.

    Experimental. It can change or be removed in any release.
    """

    model: str
    state: str
    questions: dict[str, Any]


DeciderTransport = Callable[[DeciderRequest], Any]
"""Sends one request to a decision model and returns its JSON reply.

Experimental. It can change or be removed in any release.

``decider()`` runs the transport in the step, and stops waiting for it 10
seconds before the invocation ends. An exception that the transport raises
goes to the step retry strategy.
"""


def _encode(request: DeciderRequest) -> bytes:
    return json.dumps(request, ensure_ascii=False).encode()


def agentcore_transport(
    runtime_arn: str,
    *,
    qualifier: str | None = None,
    client: Any = None,
) -> DeciderTransport:
    """Return a transport for a Strands Decider on Amazon Bedrock AgentCore
    Runtime.

    Experimental. It can change or be removed in any release.

    The transport calls ``InvokeAgentRuntime``. AgentCore Runtime forwards the
    payload to the server's ``POST /invocations`` route. The Strands Decider
    server answers that route the same way as ``POST /v1/systemone``. The
    0.2.0 release on PyPI has no ``/invocations`` route, so deploy a later
    version.

    boto3 signs the call with the function's credentials. So the function's
    role needs ``bedrock-agentcore:InvokeAgentRuntime`` on the runtime.

    A status outside 2xx becomes a :class:`DeciderError` with that status.

    Example::

        decide = decider(
            transport=agentcore_transport(
                "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/strands-decider-abc"
            )
        )

    Args:
        runtime_arn: The ARN of the AgentCore Runtime that serves the model.
        qualifier: The runtime endpoint name. The default is the runtime's
            default endpoint.
        client: A ``bedrock-agentcore`` boto3 client. The default client uses
            the function's Region and credentials.
    """
    lazy = LazyClient("bedrock-agentcore", client)

    def send(request: DeciderRequest) -> Any:
        fields: dict[str, Any] = {
            "agentRuntimeArn": runtime_arn,
            "contentType": "application/json",
            "accept": "application/json",
            "payload": _encode(request),
        }
        if qualifier is not None:
            fields["qualifier"] = qualifier
        output = lazy.get().invoke_agent_runtime(**fields)
        stream = output.get("response")
        body = stream.read() if stream is not None else b""
        status = output.get("statusCode")
        if isinstance(status, int) and not 200 <= status < 300:
            text = body.decode(errors="replace")[:200]
            raise DeciderError(
                message=(
                    "The decision model on AgentCore Runtime answered HTTP "
                    f"{status}: {text}"
                ),
                status=status,
            )
        return parse_reply(body)

    return send


def sagemaker_transport(endpoint_name: str, *, client: Any = None) -> DeciderTransport:
    """Return a transport for a Strands Decider on an Amazon SageMaker
    real-time endpoint.

    Experimental. It can change or be removed in any release.

    The transport calls ``InvokeEndpoint``. SageMaker forwards the request to
    the server's ``POST /invocations`` route. The Strands Decider server
    answers that route the same way as ``POST /v1/systemone``. The Strands
    Decider repository has a CDK stack that deploys such an endpoint
    (``deploy/sagemaker``).

    boto3 signs the call with the function's credentials. So the function's
    role needs ``sagemaker:InvokeEndpoint`` on the endpoint.

    When the server answers with an HTTP error, SageMaker raises
    ``ModelError``. The transport turns it into a :class:`DeciderError` with
    the server's status. So a 422 for an input that is too long is not
    retried.

    Example::

        decide = decider(
            transport=sagemaker_transport("strands-decider-2b-qwen3-5-v1-2610")
        )

    Args:
        endpoint_name: The name of the SageMaker endpoint that serves the
            model.
        client: A ``sagemaker-runtime`` boto3 client. The default client uses
            the function's Region and credentials.
    """
    lazy = LazyClient("sagemaker-runtime", client)

    def send(request: DeciderRequest) -> Any:
        try:
            output = lazy.get().invoke_endpoint(
                EndpointName=endpoint_name,
                ContentType="application/json",
                Accept="application/json",
                Body=_encode(request),
            )
        except ClientError as error:
            response = error.response
            status = response.get("OriginalStatusCode")
            code = response.get("Error", {}).get("Code")
            if code == "ModelError" and isinstance(status, int):
                text = str(response.get("OriginalMessage") or "")[:200]
                raise DeciderError(
                    message=(
                        f"The decision model on SageMaker answered HTTP {status}: "
                        f"{text}"
                    ),
                    status=status,
                ) from error
            raise
        return parse_reply(output["Body"].read())

    return send


_REFUSED_ERROR_TYPES = frozenset({"ValidationError", "ValueError"})
"""The error types that the Strands Decider function raises for a request
that it refuses. The same request fails the same way again."""


def lambda_transport(
    function_name: str,
    *,
    qualifier: str | None = None,
    client: Any = None,
) -> DeciderTransport:
    """Return a transport for a Strands Decider deployed as an AWS Lambda
    function.

    Experimental. It can change or be removed in any release.

    The transport calls ``Invoke`` directly. The function takes the request as
    its event and returns the reply. The Strands Decider repository has a CDK
    stack that deploys such a function (``deploy/lambda``). The transport does
    not use the function's Function URL.

    boto3 signs the call with the function's credentials. So the calling
    function's role needs ``lambda:InvokeFunction`` on the decider function.

    The function fails the invocation when it refuses a request, for example
    when the input does not fit the model's window. The transport turns that
    failure into a :class:`DeciderError` with status 422, the status that the
    server answers over HTTP. So it is not retried. Any other function error
    becomes a :class:`DeciderError` with no status, which is retried.

    A request waits while a new execution environment loads the model, up to
    30 seconds without SnapStart.

    Example::

        decide = decider(
            transport=lambda_transport(
                "strands-decider-2b-qwen3-5-v1-2610", qualifier="live"
            )
        )

    Args:
        function_name: The name or ARN of the function that serves the model.
        qualifier: The version or alias to invoke, such as ``live``. The
            default is ``$LATEST``.
        client: A ``lambda`` boto3 client. The default client uses the
            function's Region and credentials.
    """
    lazy = LazyClient("lambda", client)

    def send(request: DeciderRequest) -> Any:
        fields: dict[str, Any] = {
            "FunctionName": function_name,
            "Payload": _encode(request),
        }
        if qualifier is not None:
            fields["Qualifier"] = qualifier
        output = lazy.get().invoke(**fields)
        payload = output.get("Payload")
        body = payload.read() if payload is not None else b""
        function_error = output.get("FunctionError")
        if function_error is not None:
            raise _function_error(function_error, body)
        return parse_reply(body)

    return send


def _function_error(function_error: str, body: bytes) -> DeciderError:
    try:
        fields = json.loads(body)
    except ValueError:
        fields = None
    if not isinstance(fields, dict):
        fields = {}
    error_type = fields.get("errorType")
    error_message = fields.get("errorMessage")
    kind = error_type if isinstance(error_type, str) else function_error
    detail = error_message if isinstance(error_message, str) else function_error
    return DeciderError(
        message=f"The decision model on Lambda failed ({kind}): {detail[:200]}",
        status=422 if kind in _REFUSED_ERROR_TYPES else None,
    )


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuses redirects. A redirect would send the headers to another
    server."""

    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


_OPENER = urllib.request.build_opener(_NoRedirect)


def _parse_endpoint(endpoint: str) -> urllib.parse.SplitResult:
    """Parse the endpoint URL.

    Raises:
        TypeError: When it is not an absolute ``http:`` or ``https:`` URL, or
            when it holds a user name or password.
    """
    url = urllib.parse.urlsplit(endpoint) if isinstance(endpoint, str) else None
    if url is None or url.scheme not in {"http", "https"} or not url.hostname:
        msg = "The decider endpoint must be an absolute http: or https: URL."
        raise TypeError(msg)
    if url.username or url.password:
        msg = (
            "The decider endpoint must not contain credentials. "
            "Pass them in headers instead."
        )
        raise TypeError(msg)
    return url


def _http_transport(
    url: urllib.parse.SplitResult, headers: Mapping[str, str] | None
) -> DeciderTransport:
    """Return a transport that posts the request to ``url``.

    Errors are checkpointed. So they name the endpoint without its query or
    fragment, which can hold a secret.
    """
    where = f"{url.scheme}://{url.netloc}{url.path}"
    href = urllib.parse.urlunsplit(url)

    def send(request: DeciderRequest) -> Any:
        http_request = urllib.request.Request(  # noqa: S310 - the scheme is checked
            href,
            data=_encode(request),
            headers={"content-type": "application/json", **(headers or {})},
            method="POST",
        )
        try:
            with _OPENER.open(http_request) as response:
                return parse_reply(response.read())
        except urllib.error.HTTPError as error:
            status = error.code
            if 300 <= status < 400:
                message = (
                    f"The decision model at {where} answered HTTP {status}. "
                    "Redirects are not followed."
                )
            else:
                text = error.read().decode(errors="replace")[:200]
                message = (
                    f"The decision model at {where} answered HTTP {status}: {text}"
                )
            raise DeciderError(message=message, status=status) from error
        except (urllib.error.URLError, OSError) as error:
            # The cause's message can repeat the full URL, and the message is
            # checkpointed. So the message names only the cause's type.
            reason = getattr(error, "reason", error)
            raise DeciderError(
                message=(
                    f"The decision model at {where} could not be reached "
                    f"({type(reason).__name__})."
                )
            ) from error

    return send
