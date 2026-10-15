# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The configuration of :func:`microvm`."""

from __future__ import annotations

import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.retries import RetryDecision


if TYPE_CHECKING:
    from mypy_boto3_lambda_microvms import LambdaMicroVMsClient as LambdaMicrovmsClient
else:
    LambdaMicrovmsClient = Any


MAX_MICROVM_DURATION_SECONDS = 28_800
"""A MicroVM lives at most 8 hours, so ``RunMicrovm`` accepts
``maximumDurationInSeconds`` of at most 28,800."""


@dataclass(frozen=True, kw_only=True)
class MicrovmRequestConfig:
    """How :func:`microvm` delivers a job over HTTP.

    An HTTP delivery POSTs the job document to a route of the application in
    the MicroVM. The route must answer with a 2xx status at once, and run the
    job in the background.

    The same job can arrive again, also after it ended. So a route must
    ignore a second request for a callback ID that it already ran. The
    worker package does this. A route served without the worker package must
    do it itself.
    """

    path: str | None = None
    """The route path, for example ``/clone-build``.

    It must be an RFC 3986 path that starts with ``/``, without a ``.`` or
    ``..`` segment. The operation sends it exactly as written. Percent-encode
    any other character, for example a space as ``%20``. Register the same
    string as the worker's route, because the worker matches the raw request
    path.

    ``None`` sends the job to :data:`DEFAULT_MICROVM_JOB_PATH`. The worker
    package serves that path with its ``handler``. A path sends the job to one
    of your own routes instead, and it selects HTTP delivery for every input
    size. The worker runs a job that arrives in the ``run`` hook with its
    ``handler``, not with a route. So a job for a route must go over HTTP.
    """

    port: int | None = None
    """The port of the route in the MicroVM, between 1 and 65535.

    ``None`` uses port 8080, the port that the MicroVM endpoint forwards to by
    default. The port applies only when the job goes over HTTP. Lambda sends
    the ``run`` hook to the port in the image's hook configuration, not to
    this port. So without a ``path``, the port is used only for an input too
    large for the ``run`` hook.
    """

    retry_window: Duration | None = None
    """How long the request step retries inside one step attempt.

    ``None`` means 60 seconds. It must be between 1 second and 10 minutes.
    After the window, the step fails the attempt, and the step retry strategy
    takes over. The step also stops 10 seconds before the Lambda invocation
    times out.
    """


@dataclass(frozen=True, kw_only=True)
class MicrovmConfig:
    """The MicroVM, and how long to wait for its result.

    :func:`microvm` validates the config before it starts any durable
    operation.
    """

    image_identifier: str
    """The ARN or ID of the MicroVM image to run.

    The image must enable the ``run`` lifecycle hook. The service then also
    requires the ``ready`` image hook. The worker package
    ``aws-durable-execution-sdk-python-microvm-worker`` answers both.
    """

    execution_role_arn: str
    """The IAM role that the MicroVM assumes.

    The MicroVM reports its result through the durable callback APIs. So the
    role needs ``lambda:SendDurableExecutionCallbackSuccess``,
    ``lambda:SendDurableExecutionCallbackFailure``, and
    ``lambda:SendDurableExecutionCallbackHeartbeat`` on the durable function's
    ARN.
    """

    timeout: Duration
    """The longest time to wait for the MicroVM to report a result.

    It must be between 1 second and 8 hours, because a MicroVM lives at most
    8 hours. The MicroVM's ``maximumDurationInSeconds`` is this timeout plus 5
    minutes, at most 8 hours. So a timeout above 7 hours 55 minutes leaves the
    MicroVM less than 5 minutes beyond the callback's timeout.
    """

    heartbeat_timeout: Duration | None = None
    """The longest time between two heartbeats from the MicroVM.

    When it is set, a MicroVM that fails to boot or stops responding fails the
    operation after this time, instead of after ``timeout``. The MicroVM
    receives the value in the job document.

    The heartbeat timeout starts when the callback is created, before the
    launch. The worker sends its first heartbeat when the job arrives. So the
    first period must cover the launch, the boot, and, for HTTP delivery, the
    request step, each with its retries. A throttled launch can take several
    minutes, and the request step retries for up to ``request.retry_window``
    in each attempt. A heartbeat timeout that is shorter fails the operation
    with ``MicrovmTimeoutError`` before the MicroVM can send its first
    heartbeat.
    """

    image_version: str | None = None
    """The image version to run. Defaults to the latest active version."""

    ingress_network_connectors: tuple[str, ...] | None = None
    """Ingress network connector ARNs.

    ``None`` gets a Lambda-provided connector that depends on the delivery:

    1. ``NO_INGRESS`` when the job goes in the ``run`` hook. The job then
       needs no inbound connection.
    2. ``ALL_INGRESS`` when the job goes over HTTP. The request must reach
       the MicroVM's endpoint.

    An empty tuple sends no ingress connector. Connectors that you set must
    allow ingress whenever an input can exceed the ``run`` hook limit, because
    such an input goes over HTTP.
    """

    egress_network_connectors: tuple[str, ...] | None = None
    """Egress network connector ARNs.

    ``None`` gets the Lambda-provided ``INTERNET_EGRESS`` connector. The
    MicroVM needs egress to reach the Lambda callback APIs. An empty tuple
    sends no egress connector.
    """

    logging: Mapping[str, Any] | None = None
    """The ``logging`` member of ``RunMicrovm``. Defaults to the service
    default."""

    request: MicrovmRequestConfig | None = None
    """How to deliver the job over HTTP.

    ``None`` delivers a job in the ``run`` hook when it fits, and over HTTP to
    :data:`DEFAULT_MICROVM_JOB_PATH` on port 8080 when it does not.
    """

    retry_strategy: Callable[[Exception, int], RetryDecision] | None = None
    """The step retry strategy of the launch, request, and terminate steps.

    It is the second retry tier. The first tier retries inside one step
    attempt: the boto3 client retries each call, and the request step retries
    the POST. Defaults to :func:`default_microvm_retry_strategy`.
    """

    opener: urllib.request.OpenerDirector | None = None
    """The urllib opener that POSTs the job for HTTP delivery.

    ``None`` uses an opener that ignores the proxy settings in the
    environment and follows no redirect. boto3 reads ``HTTPS_PROXY``, so in
    a network that reaches the MicroVM endpoint only through a proxy, pass
    an opener with that proxy:

    .. code-block:: python

        urllib.request.build_opener(
            urllib.request.ProxyHandler({"https": "http://proxy.internal:3128"})
        )

    The operation does not follow redirects. When an opener follows one, the
    delivery fails at once with ``MicrovmRequestRejectedError``. The followed
    request carries no auth token, because the operation adds its headers as
    unredirected headers.
    """

    client: LambdaMicrovmsClient | None = None
    """The boto3 ``lambda-microvms`` client.

    Defaults to one client per Region, created on first use. The Region comes
    from the durable execution ARN. The default client has a 5-second connect
    timeout, a 30-second read timeout, and 3 attempts in botocore's standard
    retry mode. A client that you pass keeps its own settings. Keep each call
    shorter than the function's timeout, because a call that runs past it
    records no outcome for the step attempt.
    """
