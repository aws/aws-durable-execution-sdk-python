# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The ``microvm`` operation: run one job in a new AWS Lambda MicroVM."""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import boto3
from botocore.config import Config
from aws_durable_execution_sdk_python.config import (
    CallbackConfig,
    Duration,
    StepConfig,
)
from aws_durable_execution_sdk_python.exceptions import (
    CallbackError,
    CallbackTimeoutError,
    ChildContextError,
    ExecutionError,
    InvocationError,
    StepError,
    ValidationError,
)
from aws_durable_execution_sdk_python.retries import RetryDecision
from aws_durable_execution_sdk_python.serdes import JsonSerDes
from botocore.exceptions import ClientError

from aws_durable_execution_sdk_python_extras.microvm.aws_errors import (
    add_request_id,
    decide_on_original_error,
    error_code,
    with_request_id,
)
from aws_durable_execution_sdk_python_extras.microvm.config import (
    MAX_MICROVM_DURATION_SECONDS,
    LambdaMicrovmsClient,
    MicrovmBaseConfig,
    MicrovmConfig,
    MicrovmRequestConfig,
)
from aws_durable_execution_sdk_python_extras.microvm.errors import (
    MicrovmDeliveryError,
    MicrovmError,
    MicrovmJobFailedError,
    MicrovmLaunchError,
    MicrovmNotRunningError,
    MicrovmTimeoutError,
)
from aws_durable_execution_sdk_python_extras.microvm.job import (
    JobRequest,
    RunHookPayload,
    fits_run_hook,
    run_hook_payload_without_job,
)
from aws_durable_execution_sdk_python_extras.microvm.lifecycle import (
    EnsureRunningOptions,
    MicrovmStateError,
    ensure_running,
    microvm_not_found,
)
from aws_durable_execution_sdk_python_extras.microvm.request import (
    DEFAULT_MICROVM_JOB_PATH,
    DEFAULT_MICROVM_PORT,
    DEFAULT_REQUEST_RETRY_WINDOW_SECONDS,
    MAX_REQUEST_RETRY_WINDOW_SECONDS,
    SendJobOptions,
    require_time_for_call,
    send_job,
)
from aws_durable_execution_sdk_python_extras.microvm.retry import (
    default_microvm_retry_strategy,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.context import Callback, DurableContext
    from aws_durable_execution_sdk_python.types import StepContext


LAUNCH_MARGIN_SECONDS = 300
"""The time added to the operation timeout to get the MicroVM's maximum
duration.

The MicroVM should outlive the job's callback, or the platform terminates it
before the job can report. The margin covers the launch. The maximum duration
is capped at 8 hours. So with a timeout near 8 hours, the margin shrinks, and
at 8 hours only the launch delay remains: the callback's timeout starts before
the launch, and the MicroVM's lifetime starts after it.
"""

_CONNECT_TIMEOUT_SECONDS = 5
_READ_TIMEOUT_SECONDS = 30
_CLIENT_ATTEMPTS = 3
"""The default client's limits.

botocore defaults to 60-second connect and read timeouts, with its legacy
retries. A launch or terminate call that runs past the Lambda invocation's
timeout records no outcome for its step attempt. These limits keep one step
attempt to about 2 minutes. The launch is idempotent through its client token,
so an attempt that ran out of time is repeated, not doubled.
"""

RUN_HOOK_DELIVERY = "run-hook"
"""The delivery in which the job goes in the ``runHookPayload`` of the launch."""

HTTP_DELIVERY = "http"
"""The delivery in which a request step POSTs the job to the MicroVM's
endpoint after the launch."""

_HOOK_PATH_PREFIX = "/aws/lambda-microvms/"
"""Lambda sends the lifecycle hooks to paths under this prefix."""

_MAX_PORT = 65_535

_ROUTE_PATH = re.compile(r"(?:/(?:[A-Za-z0-9\-._~!$&'()*+,;=:@]|%[0-9A-Fa-f]{2})*)+")
"""An RFC 3986 path, section 3.3: segments of pchar characters and %XX escapes.

The operation sends the path as written. Both workers match the raw request
path, so the config and the route use the same string.
"""

_DOT_SEGMENT = re.compile(r"/(?:\.|%2e){1,2}(?=/|$)", re.IGNORECASE)
"""A ``.`` or ``..`` segment, also with ``%2e`` for a dot.

A segment with other characters next to its dots, such as ``/.well-known``,
and a segment of three dots are not dot segments.
"""


@dataclass(frozen=True, kw_only=True)
class LaunchResult:
    """What the launch step checkpoints.

    Wire names: ``microvmId``, ``endpoint``, ``delivery``, and
    ``resumeBeforeDelivery``. The JavaScript operation checkpoints the same
    document.
    """

    microvm_id: str
    delivery: str
    """How the job reaches the MicroVM. Replay reads it from the checkpoint,
    so every replay follows the delivery that the launch prepared for."""
    endpoint: str | None = None
    """The MicroVM's endpoint. Present only for HTTP delivery."""
    resume_before_delivery: bool | None = None
    """Whether each session job checks the MicroVM state, and resumes a
    suspended MicroVM, before delivery. Only a session launch records it.
    Replay reads it from the checkpoint. So every replay follows the mode that
    the launch prepared the MicroVM for. A ``microvm`` launch leaves it
    ``None``. ``from_dict`` keeps only a bool, and reads any other value as
    ``None``. ``run_session`` reads only ``True`` as a resume mode."""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LaunchResult:
        # A checkpoint without a delivery gets an empty one. The operation
        # then terminates the MicroVM as for any delivery it does not know.
        # The field is typed bool | None. to_dict() writes it back. So
        # from_dict keeps only a bool, and reads any other value as None.
        resume_before_delivery = data.get("resumeBeforeDelivery")
        return cls(
            microvm_id=data["microvmId"],
            delivery=data.get("delivery", ""),
            endpoint=data.get("endpoint"),
            resume_before_delivery=(
                resume_before_delivery
                if isinstance(resume_before_delivery, bool)
                else None
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"microvmId": self.microvm_id}
        if self.endpoint is not None:
            data["endpoint"] = self.endpoint
        data["delivery"] = self.delivery
        # A microvm() launch records no resume mode. So its checkpoint has no
        # such key, as in the JavaScript operation.
        if self.resume_before_delivery is not None:
            data["resumeBeforeDelivery"] = self.resume_before_delivery
        return data


def microvm(
    context: DurableContext,
    name: str,
    *,
    job_input: Any,
    config: MicrovmConfig,
) -> Any:
    """Run one job in a new AWS Lambda MicroVM, and return the job's result.

    The operation runs its durable operations in one child context named
    ``name``:

    1. ``<name>.callback`` creates a durable callback. The service generates
       the callback ID, and the SDK checkpoints it. So the ID is unique to this
       call and stays the same on every replay.
    2. ``<name>.launch`` calls ``RunMicrovm``. The client token is the
       SHA-256 hex digest of the callback ID. So a retried or replayed launch
       returns the same MicroVM instead of starting a second one. With
       ``run`` hook delivery, the job goes in the ``runHookPayload``, and
       Lambda passes it to the worker's ``run`` hook.
    3. With HTTP delivery, ``<name>.request`` creates an auth token and POSTs
       the job to the MicroVM's endpoint.
    4. The child context waits on the callback. The invocation ends during the
       wait, so no Lambda compute is billed. The worker in the MicroVM
       completes the callback with the job's result or failure.
    5. ``<name>.terminate`` calls ``TerminateMicrovm``. It runs after success,
       failure, and timeout alike, and after a failed delivery.

    The operation picks the delivery:

    1. A job whose ``run`` hook payload fits in
       :data:`MAX_RUN_HOOK_PAYLOAD_LENGTH` code points goes in the ``run``
       hook. The MicroVM then gets no ingress.
    2. A larger job, or any job when ``config.request.path`` is set, goes
       over HTTP. The MicroVM then gets the ``ALL_INGRESS`` connector.

    A terminate failure does not fail the operation. The job's outcome is
    already recorded when terminate runs, and the MicroVM's maximum duration
    bounds how long it keeps running. So the operation logs an error and
    returns the job's outcome.

    Pass the context that the call runs in. Inside a ``map`` item or a child
    context, pass that context, so the operation's steps belong to it.

    ``job_input`` and ``config`` are keyword-only. A job can be a string, and
    the name is a string. So positional arguments in the wrong order would
    pass validation and run the wrong job.

    Args:
        context: The context to create the durable operations in.
        name: The child context name. It also prefixes the inner operations.
        job_input: The job's input. It must be JSON-serializable, and the same
            on every replay. It is sent as JSON, so the MicroVM receives what
            ``json.dumps`` writes: a tuple arrives as a list, and a dict key
            that is not a string arrives as a string.
        config: The MicroVM and the timeouts.

    Returns:
        The value that the MicroVM passed to
        ``SendDurableExecutionCallbackSuccess``, parsed as JSON.

    Raises:
        ValidationError: When ``name`` or ``config`` is not valid, or the
            Region cannot be determined. No durable operation starts. It is
            the SDK's class, which ``Duration`` also raises for a negative
            value.
        TypeError: When ``job_input`` is not JSON-serializable. No durable
            operation starts.
        MicrovmLaunchError: When ``RunMicrovm`` fails after all retries.
        MicrovmNotRunningError: When ``CreateMicrovmAuthToken`` reports that
            the MicroVM no longer exists. It is a ``MicrovmDeliveryError``.
        MicrovmDeliveryError: When the HTTP delivery fails after all retries,
            or the route answers a 4xx status such as 404, or a redirect.
        MicrovmJobFailedError: When the MicroVM reports a failure, or sends a
            result that is not valid JSON.
        MicrovmTimeoutError: When no result or heartbeat arrives in time.

    The message of a failed Lambda MicroVMs call ends with its request ID,
    for example ``RunMicrovm request ID: <id>.``.
    """
    scope = _OperationScope.create(context, name, job_input, config)

    def run(child: DurableContext) -> Any:
        return scope.run_job(child, job_input)

    try:
        return context.run_in_child_context(run, name=name)
    except ChildContextError as error:
        raise MicrovmError.from_child_context_error(name, error) from error


_STATE_ERROR_TYPE = f"{MicrovmStateError.__module__}.{MicrovmStateError.__qualname__}"
"""The error type that a request step checkpoints for a MicroVM that can no
longer run a job. The SDK records the fully-qualified class name."""


def step_retry_strategy(
    config: MicrovmBaseConfig,
) -> Callable[[Exception, int], RetryDecision]:
    """Return the retry strategy of the launch, request, and terminate steps.

    It is the configured strategy, or :func:`default_microvm_retry_strategy`.
    It decides on the error that botocore raised, without the request ID in
    the message. See :func:`decide_on_original_error`. A session passes the
    same strategy to each job. So this applies to every AWS call of both
    operations.
    """
    return decide_on_original_error(
        config.retry_strategy or default_microvm_retry_strategy
    )


def _validate_base(name: str, config: MicrovmBaseConfig, timeout: Duration) -> int:
    """Validate the settings that every MicroVM operation shares.

    Returns:
        The timeout in seconds.

    Raises:
        ValidationError: When ``name`` or a setting is not valid.
    """
    if not isinstance(name, str) or not name:
        msg = "a MicroVM operation requires a non-empty name"
        raise ValidationError(msg)
    if not config.image_identifier:
        msg = f'MicroVM "{name}": image_identifier is required'
        raise ValidationError(msg)
    if not config.execution_role_arn:
        msg = f'MicroVM "{name}": execution_role_arn is required'
        raise ValidationError(msg)
    return _validate_timeout(name, "timeout", timeout)


def _validate_timeout(name: str, field: str, timeout: Duration) -> int:
    """Validate a job or MicroVM timeout, and return it in seconds.

    Raises:
        ValidationError: When it is not between 1 second and 8 hours.
    """
    seconds = timeout.to_seconds()
    if not 1 <= seconds <= MAX_MICROVM_DURATION_SECONDS:
        msg = (
            f'MicroVM "{name}": {field} must be between 1 second and '
            f"{MAX_MICROVM_DURATION_SECONDS} seconds (8 hours), because a "
            f"MicroVM lives at most 8 hours. Got {seconds} seconds."
        )
        raise ValidationError(msg)
    return seconds


def _validate_heartbeat(name: str, heartbeat_timeout: Duration | None) -> int | None:
    """Validate a heartbeat timeout, and return it in seconds.

    Raises:
        ValidationError: When it is shorter than 1 second.
    """
    if heartbeat_timeout is None:
        return None
    seconds = heartbeat_timeout.to_seconds()
    if seconds < 1:
        msg = f'MicroVM "{name}": heartbeat_timeout must be at least 1 second'
        raise ValidationError(msg)
    return seconds


def _validate_input(name: str, job_input: Any) -> None:
    """Check that the job input is JSON-serializable.

    Raises:
        TypeError: When it is not.
    """
    try:
        json.dumps(job_input, allow_nan=False)
    except (TypeError, ValueError) as error:
        msg = f'MicroVM "{name}": the job input must be JSON-serializable: {error}'
        raise TypeError(msg) from error


def _validate_region(context: DurableContext, name: str) -> tuple[str, str]:
    """Return the partition and the Region of the durable execution.

    Raises:
        ValidationError: When the Region cannot be determined.
    """
    partition, region = _partition_and_region(
        context.execution_context.durable_execution_arn
    )
    if not region:
        # The worker rejects a payload without a Region. So the operation
        # fails before any durable operation instead.
        msg = (
            f'MicroVM "{name}": cannot determine the AWS Region. The durable '
            "execution ARN has none, and AWS_REGION is not set."
        )
        raise ValidationError(msg)
    return partition, region


@dataclass(frozen=True, kw_only=True)
class _MicrovmScope:
    """The validated values and the steps that every MicroVM operation uses.

    :func:`microvm` and :func:`microvm_session` share the launch, request,
    and terminate steps.
    """

    name: str
    config: MicrovmBaseConfig
    partition: str
    region: str
    timeout_seconds: int
    retry_strategy: Callable[[Exception, int], RetryDecision]

    def launch(
        self,
        child: DurableContext,
        *,
        token_source: str,
        payload: str,
        delivery: str,
        idle_policy: Mapping[str, Any] | None = None,
        resume_before_delivery: bool | None = None,
    ) -> LaunchResult:
        """Run the launch step, ``<name>.launch``.

        The client token is the SHA-256 hex digest of ``token_source``. It
        must be unique per launch and stable across retries and replays.

        ``resume_before_delivery`` is the resume mode to record. Only a
        session launch sets it.

        Raises:
            MicrovmLaunchError: When ``RunMicrovm`` fails after all retries.
        """
        try:
            return LaunchResult.from_dict(
                child.step(
                    lambda _: self._launch_call(
                        token_source,
                        payload,
                        delivery,
                        idle_policy,
                        resume_before_delivery,
                    ).to_dict(),
                    name=f"{self.name}.launch",
                    config=StepConfig(
                        retry_strategy=self.retry_strategy,
                        # Plain JSON, as the JavaScript SDK checkpoints it. So
                        # a reader of the execution history finds the
                        # MicroVM ID. Changing the serdes later would break
                        # the replay of executions in flight.
                        serdes=JsonSerDes(),
                    ),
                )
            )
        except StepError as error:
            msg = f'MicroVM "{self.name}": the launch failed: {_detail(error)}'
            raise MicrovmLaunchError(
                message=msg, data=error.data, stack_trace=error.stack_trace
            ) from error

    def deliver(
        self,
        child: DurableContext,
        *,
        job_name: str,
        launched: LaunchResult,
        callback_id: str,
        job_input: Any,
        heartbeat_timeout_seconds: int | None,
        request: MicrovmRequestConfig,
        retry_window_seconds: int,
        resume: bool = False,
    ) -> None:
        """Run the request step, ``<job_name>.request``: POST the job to the
        MicroVM's endpoint.

        The step retries inside each attempt with :func:`send_job`, the first
        tier. The step retry strategy is the second tier.

        With ``resume``, each attempt first makes sure the MicroVM runs, and
        resumes a suspended one. It then sends the job to the endpoint that
        ``GetMicrovm`` reports. A session sets it when its MicroVM can be
        suspended.

        ``GetMicrovm``, ``ResumeMicrovm``, and ``CreateMicrovmAuthToken``
        answer ``ResourceNotFoundException`` for a removed MicroVM. An
        attempt raises ``MicrovmStateError`` for that answer of any of the
        three calls. The default retry strategy does not retry it.

        Raises:
            MicrovmNotRunningError: When the MicroVM can no longer run a job.
            MicrovmDeliveryError: When the delivery fails after all retries.
        """
        body = (
            JobRequest(
                region=self.region,
                microvm_id=launched.microvm_id,
                callback_id=callback_id,
                job_input=job_input,
                heartbeat_timeout_seconds=heartbeat_timeout_seconds,
            )
            .to_json()
            .encode()
        )
        remaining_seconds = _remaining_seconds(child)
        path = DEFAULT_MICROVM_JOB_PATH if request.path is None else request.path
        port = DEFAULT_MICROVM_PORT if request.port is None else request.port

        def attempt(step_context: StepContext) -> None:
            # The launch records an endpoint for every HTTP delivery. So a
            # missing endpoint is a defect, not a transient failure. The
            # default retry strategy does not retry RuntimeError.
            if launched.endpoint is None:
                msg = (
                    f"the launch recorded no endpoint for MicroVM "
                    f"{launched.microvm_id}, so the job cannot be sent over HTTP"
                )
                raise RuntimeError(msg)
            client = self._client()
            # The state check before delivery and the request's retries share
            # one window. So the wait for a resume counts toward it.
            window_started_at = time.monotonic()
            endpoint = launched.endpoint
            recheck: Callable[[float], str | None] | None = None
            if resume:
                require_time_for_call(remaining_seconds)

                def running(max_wait_seconds: float) -> str | None:
                    return ensure_running(
                        EnsureRunningOptions(
                            client=client,
                            microvm_id=launched.microvm_id,
                            max_wait_seconds=max_wait_seconds,
                            remaining_seconds=remaining_seconds,
                            logger=step_context.logger,
                        )
                    )

                endpoint = running(retry_window_seconds) or endpoint
                recheck = running
            # Only CreateMicrovmAuthToken can raise ResourceNotFoundException
            # here:
            #
            # 1. _create_auth_token raises that answer unchanged.
            # 2. send_job calls new_token() outside any try. So the error
            #    leaves send_job unchanged.
            # 3. _run_with_limit re-raises the same error object.
            # 4. ensure_running turns the same answer of GetMicrovm and
            #    ResumeMicrovm into MicrovmStateError. So neither call raises
            #    a ClientError with that code.
            #
            # So the step raises MicrovmStateError for CreateMicrovmAuthToken.
            # The default retry strategy does not retry that error.
            try:
                send_job(
                    SendJobOptions(
                        client=client,
                        microvm_id=launched.microvm_id,
                        endpoint=endpoint,
                        path=path,
                        port=port,
                        body=body,
                        retry_window_seconds=retry_window_seconds,
                        remaining_seconds=remaining_seconds,
                        logger=step_context.logger,
                        opener=self.config.opener,
                        window_started_at=window_started_at,
                        recheck=recheck,
                    )
                )
            except ClientError as error:
                if error_code(error) == "ResourceNotFoundException":
                    raise microvm_not_found(
                        launched.microvm_id, "CreateMicrovmAuthToken", error
                    ) from error
                raise

        try:
            child.step(
                attempt,
                name=f"{job_name}.request",
                config=StepConfig(retry_strategy=self.retry_strategy),
            )
        except StepError as error:
            error_class = (
                MicrovmNotRunningError
                if error.error_type == _STATE_ERROR_TYPE
                else MicrovmDeliveryError
            )
            msg = f'MicroVM "{job_name}": the job could not be delivered: {_detail(error)}'
            raise error_class(
                message=msg, data=error.data, stack_trace=error.stack_trace
            ) from error

    def terminate(self, child: DurableContext, microvm_id: str) -> None:
        """Run the terminate step, and log a final failure instead of raising.

        The job's outcome is already recorded when terminate runs, and the
        MicroVM's maximum duration bounds how long it can keep running. So a
        terminate failure must not discard the outcome. The step's failure is
        checkpointed. So on replay the step fails again at once, with the same
        outcome.
        """
        try:
            child.step(
                lambda _: self._terminate_call(microvm_id),
                name=f"{self.name}.terminate",
                config=StepConfig(retry_strategy=self.retry_strategy),
            )
        except StepError as error:
            # The MicroVM keeps running, and is billed, until the platform
            # ends it. A missing permission fails every terminate this way,
            # and the default retry strategy does not retry
            # AccessDeniedException. This log is the only sign of it. So it
            # is an error, not a warning.
            child.logger.error(
                f'MicroVM "{self.name}": terminate failed for {microvm_id}. The '
                "MicroVM keeps running, and is billed, until the platform ends "
                "it at its maximum duration: the operation timeout plus 5 "
                "minutes, at most 8 hours. A missing permission needs "
                "lambda:TerminateMicrovm in the function's execution role.",
                extra={"microvmId": microvm_id, "error": error.message},
            )

    def _launch_call(
        self,
        token_source: str,
        payload: str,
        delivery: str,
        idle_policy: Mapping[str, Any] | None,
        resume_before_delivery: bool | None = None,
    ) -> LaunchResult:
        """Call ``RunMicrovm``, and return what the launch step checkpoints."""
        over_http = delivery == HTTP_DELIVERY
        config = self.config
        request: dict[str, Any] = {
            "imageIdentifier": config.image_identifier,
            "executionRoleArn": config.execution_role_arn,
            # A callback ID can be 1,024 characters, and the client token
            # allows 128. The SHA-256 hex digest is 64 characters.
            "clientToken": hashlib.sha256(token_source.encode()).hexdigest(),
            # An explicit empty tuple stays empty. The service accepts 0 to
            # 10 connectors, and only None gets the default.
            # An HTTP delivery must reach the MicroVM's endpoint, and a run
            # hook delivery needs no inbound connection.
            "ingressNetworkConnectors": list(
                (self._connector_arn("ALL_INGRESS" if over_http else "NO_INGRESS"),)
                if config.ingress_network_connectors is None
                else config.ingress_network_connectors
            ),
            "egressNetworkConnectors": list(
                (self._connector_arn("INTERNET_EGRESS"),)
                if config.egress_network_connectors is None
                else config.egress_network_connectors
            ),
            "maximumDurationInSeconds": min(
                self.timeout_seconds + LAUNCH_MARGIN_SECONDS,
                MAX_MICROVM_DURATION_SECONDS,
            ),
            "runHookPayload": payload,
        }
        if config.image_version is not None:
            request["imageVersion"] = config.image_version
        if config.logging is not None:
            request["logging"] = dict(config.logging)
        if idle_policy is not None:
            request["idlePolicy"] = dict(idle_policy)
        try:
            response = self._client().run_microvm(**request)
        except ClientError as error:
            # add_request_id returns the error, or a copy whose __cause__ is
            # the error. So the raise has no `from`.
            raise add_request_id("RunMicrovm", error)
        # A missing field fails the step. So the message names the request
        # ID of the answer.
        microvm_id = response.get("microvmId")
        if not microvm_id:
            msg = "RunMicrovm returned no microvmId"
            raise RuntimeError(with_request_id(msg, "RunMicrovm", response))
        endpoint = response.get("endpoint")
        if over_http and not endpoint:
            msg = "RunMicrovm returned no endpoint"
            raise RuntimeError(with_request_id(msg, "RunMicrovm", response))
        return LaunchResult(
            microvm_id=microvm_id,
            delivery=delivery,
            endpoint=endpoint if over_http else None,
            resume_before_delivery=resume_before_delivery,
        )

    def _terminate_call(self, microvm_id: str) -> None:
        try:
            self._client().terminate_microvm(microvmIdentifier=microvm_id)
        except ClientError as error:
            # TerminateMicrovm succeeds for a terminated MicroVM. The service
            # answers ResourceNotFoundException for a MicroVM whose record it
            # has removed. Either way, the MicroVM is gone. Any other error
            # gets the request ID. See add_request_id.
            if error_code(error) != "ResourceNotFoundException":
                raise add_request_id("TerminateMicrovm", error)

    def _connector_arn(self, connector: str) -> str:
        return (
            f"arn:{self.partition}:lambda:{self.region}:aws:network-connector:"
            f"aws-network-connector:{connector}"
        )

    def _client(self) -> LambdaMicrovmsClient:
        if self.config.client is not None:
            return self.config.client
        return _DEFAULT_CLIENTS.get(self.region)


@dataclass(frozen=True, kw_only=True)
class _OperationScope(_MicrovmScope):
    """The validated values of one :func:`microvm` call."""

    heartbeat_timeout_seconds: int | None
    request: MicrovmRequestConfig
    retry_window_seconds: int

    @classmethod
    def create(
        cls,
        context: DurableContext,
        name: str,
        job_input: Any,
        config: MicrovmConfig,
    ) -> _OperationScope:
        """Validate the call, before any durable operation starts.

        Raises:
            ValidationError: When ``name`` or ``config`` is not valid, or the
                Region cannot be determined.
            TypeError: When ``job_input`` is not JSON-serializable.
        """
        timeout_seconds = _validate_base(name, config, config.timeout)
        heartbeat_timeout_seconds = _validate_heartbeat(name, config.heartbeat_timeout)
        request = config.request or MicrovmRequestConfig()
        retry_window_seconds = _validate_request(name, request)
        _validate_input(name, job_input)
        partition, region = _validate_region(context, name)
        return cls(
            name=name,
            config=config,
            partition=partition,
            region=region,
            timeout_seconds=timeout_seconds,
            heartbeat_timeout_seconds=heartbeat_timeout_seconds,
            request=request,
            retry_window_seconds=retry_window_seconds,
            retry_strategy=step_retry_strategy(config),
        )

    def run_job(self, child: DurableContext, job_input: Any) -> Any:
        """Run the operation's durable operations in its child context."""
        name = self.name
        callback = create_job_callback(
            child, name, self.timeout_seconds, self.heartbeat_timeout_seconds
        )
        # The operation picks the delivery:
        #
        # 1. The run hook is preferred. It opens no ingress, and it needs no
        #    extra durable step.
        # 2. A config with a request path goes over HTTP. The worker runs a
        #    run hook job with its handler, not with a route.
        # 3. A payload larger than the run hook limit goes over HTTP.
        #    RunMicrovm rejects it with ValidationException, which the
        #    default retry strategy does not retry.
        #
        # The payload contains the callback ID, so the size is known only
        # after the callback exists. The ID comes from the checkpoint on
        # replay, and the input must not change. So every replay picks the
        # same delivery as the first attempt.
        with_job = RunHookPayload(
            region=self.region,
            callback_id=callback.callback_id,
            job_input=job_input,
            heartbeat_timeout_seconds=self.heartbeat_timeout_seconds,
        ).to_json()
        over_http = self.request.path is not None or not fits_run_hook(with_job)
        delivery = HTTP_DELIVERY if over_http else RUN_HOOK_DELIVERY
        payload = run_hook_payload_without_job(self.region) if over_http else with_job

        launched = self.launch(
            child, token_source=callback.callback_id, payload=payload, delivery=delivery
        )
        microvm_id = launched.microvm_id
        # Replay computes the delivery again from the current input and
        # config. A deploy between the launch and a replay can change both.
        # The launch result comes from the checkpoint on replay, and it
        # records the delivery that the launch prepared for. So the operation
        # follows the launch's delivery, not the new computation.
        if launched.delivery not in {RUN_HOOK_DELIVERY, HTTP_DELIVERY}:
            # The launch checkpoint comes from a release with a delivery that
            # this release does not know, for example after a rollback. This
            # release cannot deliver the job. So it terminates the MicroVM and
            # fails, the same way on every replay.
            self.terminate(child, microvm_id)
            raise unknown_delivery_error(name, launched)

        if launched.delivery == HTTP_DELIVERY:
            try:
                self.deliver(
                    child,
                    job_name=name,
                    launched=launched,
                    callback_id=callback.callback_id,
                    job_input=job_input,
                    heartbeat_timeout_seconds=self.heartbeat_timeout_seconds,
                    request=self.request,
                    retry_window_seconds=self.retry_window_seconds,
                )
            except MicrovmDeliveryError:
                self.terminate(child, microvm_id)
                raise

        try:
            result = callback.result()
        except ExecutionError:
            # The execution fails without a retry, and the core SDK records
            # no outcome for the child context. A terminate step recorded now
            # would change that history. So the error passes through.
            raise
        except InvocationError as error:
            # An SDK invocation error is not a job outcome. A retryable one
            # makes Lambda run the invocation again, and the replay waits for
            # the same MicroVM. So the MicroVM stays up for it.
            if not error.is_retryable():
                self.terminate(child, microvm_id)
            raise
        except Exception as error:
            # SuspendExecution is a BaseException, so a wait for the callback
            # passes through here without terminating the MicroVM.
            self.terminate(child, microvm_id)
            raise job_error(name, callback, error) from error
        self.terminate(child, microvm_id)
        return result


def create_job_callback(
    child: DurableContext,
    job_name: str,
    timeout_seconds: int,
    heartbeat_timeout_seconds: int | None,
) -> Callback:
    """Create the callback ``<job_name>.callback`` that the MicroVM completes."""
    return child.create_callback(
        name=f"{job_name}.callback",
        config=CallbackConfig(
            timeout=Duration.from_seconds(timeout_seconds),
            heartbeat_timeout=Duration.from_seconds(heartbeat_timeout_seconds or 0),
            # The worker sends the result as a JSON string. The default
            # callback serdes would return that string unparsed.
            serdes=JsonSerDes(),
        ),
    )


def unknown_delivery_error(name: str, launched: LaunchResult) -> MicrovmDeliveryError:
    """The error for a launch checkpoint whose delivery this release does not
    know."""
    msg = (
        f'MicroVM "{name}": the launch prepared the delivery '
        f'"{launched.delivery}", which this release does not support'
    )
    return MicrovmDeliveryError(msg)


def job_error(name: str, callback: Callback, error: Exception) -> Exception:
    """Convert a failure of the callback wait into its MicroVM error."""
    if isinstance(error, CallbackTimeoutError):
        return MicrovmTimeoutError(
            message=f'MicroVM "{name}": the job timed out: {error.message}',
            data=error.data,
            stack_trace=error.stack_trace,
        )
    if isinstance(error, CallbackError):
        # The SDK raises CallbackExternalError with the message and the
        # data. The error type that the MicroVM reported stays on the
        # callback's checkpoint, so the message carries it from there.
        reported = _reported_error_type(callback)
        detail = f" ({reported})" if reported else ""
        return MicrovmJobFailedError(
            message=f'MicroVM "{name}": the job failed{detail}: {error.message}',
            data=error.data,
            stack_trace=error.stack_trace,
        )
    # Any other failure, such as a result that is not valid JSON. The
    # MicroVM sent the result, so the job failed. The JavaScript operation
    # differs here: its SDK fails the invocation with SERDES_FAILED before
    # the operation sees an error. The SDK checkpoints the class of a
    # MicroVM error as its type. So the message names the original type.
    detail = getattr(error, "message", None) or str(error)
    return MicrovmJobFailedError(
        message=(
            f'MicroVM "{name}": the job result could not be read '
            f"({type(error).__name__}): {detail}"
        )
    )


class _DefaultClients:
    """One ``lambda-microvms`` client per Region, created on first use.

    A boto3 client is safe to use from several threads. Creating one from a
    shared session is not. So creation runs under a lock, and each client gets
    its own session.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._clients: dict[str, LambdaMicrovmsClient] = {}

    def get(self, region: str) -> LambdaMicrovmsClient:
        with self._lock:
            client = self._clients.get(region)
            if client is None:
                client = boto3.session.Session().client(
                    "lambda-microvms",
                    region_name=region,
                    config=Config(
                        connect_timeout=_CONNECT_TIMEOUT_SECONDS,
                        read_timeout=_READ_TIMEOUT_SECONDS,
                        retries={
                            "mode": "standard",
                            # total_max_attempts counts the first attempt too.
                            # max_attempts would count only the retries.
                            "total_max_attempts": _CLIENT_ATTEMPTS,
                        },
                    ),
                )
                self._clients[region] = client
            return client


_DEFAULT_CLIENTS = _DefaultClients()


def _reported_error_type(callback: Callback) -> str | None:
    """The error type that the MicroVM reported for a failed callback.

    ``CallbackExternalError`` carries the message and the data, not the type.
    The callback's checkpoint holds the type, and it is the same on the first
    run and on replay. ``ExecutionState`` is internal to the SDK. So the
    package's dependency has an upper bound on the SDK's major version.

    ``Error`` is the type that a worker reports for a value that is not an
    exception. It names nothing, so it is left out.
    """
    try:
        error = callback.state.get_checkpoint_result(callback.operation_id).error
        reported = None if error is None else error.type
    except (AttributeError, KeyError, TypeError):
        # The internal shape changed in an SDK release. The type is only
        # detail in the message, so the operation goes on without it.
        return None
    if not isinstance(reported, str) or reported in {"", "Error"}:
        return None
    return reported


def _partition_and_region(arn: str) -> tuple[str, str]:
    """Read the partition and Region from a durable execution ARN.

    The ARN has the form ``arn:<partition>:lambda:<region>:<account>:...``.
    Without that form, the partition is ``aws`` and the Region comes from
    ``AWS_REGION``.
    """
    parts = arn.split(":")
    if len(parts) > 3 and parts[0] == "arn" and parts[1] and parts[3]:
        return parts[1], parts[3]
    return "aws", os.environ.get("AWS_REGION", "")


def _validate_request(name: str, request: MicrovmRequestConfig) -> int:
    """Validate the HTTP delivery settings.

    Returns:
        The retry window in seconds.

    Raises:
        ValidationError: When a setting is not valid.
    """
    path = request.path
    if path is not None and (
        not isinstance(path, str) or not _ROUTE_PATH.fullmatch(path)
    ):
        msg = (
            f'MicroVM "{name}": the request path must be an RFC 3986 path that '
            'starts with "/". Percent-encode any other character, for example a '
            "space as %20."
        )
        raise ValidationError(msg)
    # The JavaScript SDK's fetch resolves a dot segment before it sends the
    # request, so the JavaScript operation rejects such a path. urllib sends
    # the path unchanged, but a proxy or server in front of the worker can
    # resolve it too. So both SDKs accept the same paths, and each path
    # reaches the worker as written.
    if path is not None and _DOT_SEGMENT.search(path):
        msg = (
            f'MicroVM "{name}": the request path must not have a "." or ".." '
            "segment, because an HTTP client or server can resolve it before "
            "the route sees it."
        )
        raise ValidationError(msg)
    if path is not None and path.startswith(_HOOK_PATH_PREFIX):
        msg = (
            f'MicroVM "{name}": the request path must not use the lifecycle hook '
            f"prefix {_HOOK_PATH_PREFIX}"
        )
        raise ValidationError(msg)
    port = request.port
    # bool is an int subclass, and True would pass as port 1.
    if port is not None and (
        isinstance(port, bool)
        or not isinstance(port, int)
        or not 1 <= port <= _MAX_PORT
    ):
        msg = f'MicroVM "{name}": the request port must be an integer between 1 and {_MAX_PORT}'
        raise ValidationError(msg)
    if request.retry_window is None:
        return DEFAULT_REQUEST_RETRY_WINDOW_SECONDS
    seconds = request.retry_window.to_seconds()
    # A window over 10 minutes would outlive the auth token and its one
    # refresh.
    if not 1 <= seconds <= MAX_REQUEST_RETRY_WINDOW_SECONDS:
        msg = (
            f'MicroVM "{name}": the request retry_window must be between 1 second '
            f"and {MAX_REQUEST_RETRY_WINDOW_SECONDS // 60} minutes"
        )
        raise ValidationError(msg)
    return seconds


def _remaining_seconds(context: DurableContext) -> Callable[[], float | None]:
    """Return a reader of the invocation's remaining time, in seconds.

    The first retry tier uses it to stop before the invocation times out.
    ``None`` means that the compute reports no deadline. The core passes the
    Lambda handler's context, whose ``get_remaining_time_in_millis()``
    returns an ``int``. A failure of that call raises, so the reserve is
    never turned off without a sign.
    """
    lambda_context = context.lambda_context
    if lambda_context is None:
        return lambda: None
    return lambda: lambda_context.get_remaining_time_in_millis() / 1000


def _detail(error: StepError) -> str:
    # An exception without a message checkpoints none. Its type is then the
    # only detail. DurableOperationError always sets a type.
    return error.message or error.error_type
