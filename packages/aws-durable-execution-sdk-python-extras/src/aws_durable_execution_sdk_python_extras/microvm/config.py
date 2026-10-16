# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The configuration of :func:`microvm` and :func:`microvm_session`."""

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
    """How :func:`microvm` and :meth:`MicrovmSession.invoke` deliver a job over
    HTTP.

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
    of your own routes instead. For :func:`microvm`, a path also selects HTTP
    delivery for every input size. The worker runs a job that arrives in the
    ``run`` hook with its ``handler``, not with a route. So a job for a route
    must go over HTTP. A session job always goes over HTTP.
    """

    port: int | None = None
    """The port of the route in the MicroVM, between 1 and 65535.

    ``None`` uses port 8080, the port that the MicroVM endpoint forwards to by
    default. The port applies only when the job goes over HTTP. Lambda sends
    the ``run`` hook to the port in the image's hook configuration, not to
    this port. So for :func:`microvm` without a ``path``, the port is used
    only for an input too large for the ``run`` hook. A session job always
    uses the port.
    """

    retry_window: Duration | None = None
    """How long the request step retries inside one step attempt.

    ``None`` means 60 seconds. It must be between 1 second and 10 minutes.
    After the window, the step fails the attempt, and the step retry strategy
    takes over. The step also stops 10 seconds before the Lambda invocation
    times out.
    """


@dataclass(frozen=True, kw_only=True)
class MicrovmBaseConfig:
    """The settings that :class:`MicrovmConfig` and
    :class:`MicrovmSessionConfig` share."""

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

    image_version: str | None = None
    """The image version to run. Defaults to the latest active version."""

    ingress_network_connectors: tuple[str, ...] | None = None
    """Ingress network connector ARNs.

    ``None`` gets a Lambda-provided connector that depends on the delivery:

    1. ``NO_INGRESS`` when the job goes in the ``run`` hook. The job then
       needs no inbound connection.
    2. ``ALL_INGRESS`` when the job goes over HTTP. The request must reach
       the MicroVM's endpoint. Every job of a session goes over HTTP.

    An empty tuple sends no ingress connector. Connectors that you set must
    allow ingress whenever a job goes over HTTP: for :func:`microvm`,
    whenever an input can exceed the ``run`` hook limit, and for a session,
    always.
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


@dataclass(frozen=True, kw_only=True)
class MicrovmConfig(MicrovmBaseConfig):
    """The MicroVM of :func:`microvm`, and how long to wait for its result.

    :func:`microvm` validates the config before it starts any durable
    operation.
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

    request: MicrovmRequestConfig | None = None
    """How to deliver the job over HTTP.

    ``None`` delivers a job in the ``run`` hook when it fits, and over HTTP to
    :data:`DEFAULT_MICROVM_JOB_PATH` on port 8080 when it does not.
    """


@dataclass(frozen=True, kw_only=True)
class MicrovmSessionConfig(MicrovmBaseConfig):
    """The MicroVM of :func:`microvm_session`.

    :func:`microvm_session` validates the config before it starts any durable
    operation.
    """

    timeout: Duration
    """The lifetime of the session's MicroVM, including durable waits between
    jobs.

    The MicroVM's ``maximumDurationInSeconds`` is this timeout plus 5
    minutes, at most 8 hours. The service then terminates the MicroVM. A
    MicroVM lives at most 8 hours, running or suspended. So the timeout must
    be between 1 second and 8 hours. The session also uses it to cap the
    default :attr:`auto_suspend_idle_time`.

    The timeout does not limit the handler or its jobs. Each job waits for
    its own ``timeout``. For example, a session with a 1-hour timeout starts
    a job with an 8-hour timeout at 0:58. The service terminates the MicroVM
    at about 1:05.

    1. When the image enables the ``terminate`` hook, a worker that acts on
       it fails the job at about 1:05.
    2. Lambda does not call the hook for a suspended MicroVM, or when the
       image disables it. The job then fails at its ``heartbeat_timeout``, or
       at 8:58 without one.

    So set a ``heartbeat_timeout`` on every :meth:`MicrovmSession.invoke`.
    """

    idle_policy: Mapping[str, Any] | None = None
    """The ``idlePolicy`` member of ``RunMicrovm``. Not set by default.

    It needs all three members, for example ``{"maxIdleDurationSeconds": 300,
    "suspendedDurationSeconds": 3600, "autoResumeEnabled": True}``.

    An idle policy counts only inbound traffic to the endpoint, and a running
    job receives none after its delivery. So ``maxIdleDurationSeconds`` must
    be longer than the longest job, or the service suspends the MicroVM
    during that job. Set it only for a MicroVM that serves inbound traffic of
    its own. With an idle policy, the request step checks the MicroVM state
    before each job, and resumes a suspended MicroVM. That needs
    ``lambda:GetMicrovm`` and ``lambda:ResumeMicrovm`` in the function's
    role.
    """

    auto_suspend_on_idle: bool = False
    """Whether the worker suspends the MicroVM when no job has run for
    :attr:`auto_suspend_idle_time`. Defaults to ``False``.

    A running MicroVM pays compute charges. A suspended MicroVM pays only for
    snapshot storage, and keeps its memory and files. With this setting, the
    session asks the worker in the ``run`` hook payload to suspend its own
    MicroVM when idle. Before each job, the request step checks the MicroVM
    state, and resumes a suspended MicroVM.

    It is off by default, because it needs more permissions and changes what
    the MicroVM does between jobs:

    1. The function's role needs ``lambda:GetMicrovm`` and
       ``lambda:ResumeMicrovm``. Without them, every job fails.
    2. The MicroVM's role needs ``lambda:SuspendMicrovm`` on the image. That
       permission lets code in any MicroVM of the image suspend any other.
       Use one image per trust boundary.
    3. Background processes that an earlier job started stop while the
       MicroVM is suspended. "Idle" means only that no job runs in the
       worker.

    A resume adds latency to the next job, about 1 second in us-east-1, and
    the job's ``timeout`` and ``heartbeat_timeout`` count it. Suspending does
    not extend the MicroVM's lifetime.

    The worker must be a version that supports the setting. The JavaScript
    worker ``@aws/durable-execution-sdk-js-microvm-worker`` does. The Python
    worker in this repository does not yet: it accepts the setting, and its
    MicroVM keeps running.
    """

    auto_suspend_idle_time: Duration | None = None
    """How long the worker waits with no running job before it suspends the
    MicroVM.

    ``None`` means 60 seconds, or the session ``timeout`` if that is shorter.
    A session whose ``timeout`` is under 10 seconds then does not suspend.
    It applies only when :attr:`auto_suspend_on_idle` is ``True``, and
    setting it otherwise fails validation. It must be at least 10 seconds,
    and not longer than the session ``timeout``. A resumed MicroVM needs a
    few seconds before its job arrives, so a shorter time could suspend it
    again first.
    """


@dataclass(frozen=True, kw_only=True)
class MicrovmInvokeOptions(MicrovmRequestConfig):
    """One job of :meth:`MicrovmSession.invoke`.

    The job always goes over HTTP: to ``path`` when it is set, and otherwise
    to :data:`DEFAULT_MICROVM_JOB_PATH`, which the worker serves with its
    ``handler``.
    """

    timeout: Duration
    """The longest time to wait for this job's result. At most 8 hours.

    The time starts before the job is delivered. So it includes a resume of
    a suspended MicroVM. The session's ``timeout`` does not shorten it.
    """

    heartbeat_timeout: Duration | None = None
    """The longest time between two heartbeats from the MicroVM for this job.

    The first period starts before the job is delivered, and the worker sends
    its first heartbeat when the job arrives. So the first period includes a
    resume of a suspended MicroVM and the request step with its retries.
    Keep it longer than both together.

    Set it on every job. A MicroVM that ends while suspended, or whose image
    disables the ``terminate`` hook, reports nothing. The heartbeat timeout
    is then the only way that the job fails soon after the MicroVM ends.
    """
