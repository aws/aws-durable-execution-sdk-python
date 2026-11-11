# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The ``agentcore`` operation: run one job in a new AgentCore Runtime
session."""

from __future__ import annotations

import json
import os
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import boto3
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
from botocore.config import Config
from botocore.exceptions import ClientError

from aws_durable_execution_sdk_python_extras.compute.agentcore.config import (
    AgentcoreConfig,
    BedrockAgentcoreClient,
)
from aws_durable_execution_sdk_python_extras.compute.agentcore.errors import (
    AgentcoreDeliveryError,
    AgentcoreError,
    AgentcoreJobFailedError,
    AgentcoreTimeoutError,
)
from aws_durable_execution_sdk_python_extras.compute.agentcore.job import (
    AgentcoreJobRequest,
)
from aws_durable_execution_sdk_python_extras.compute.agentcore.retry import (
    default_agentcore_retry_strategy,
)
from aws_durable_execution_sdk_python_extras.compute.microvm.job import (
    MAX_JOB_INPUT_BYTES,
    dumps,
)


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.context import Callback, DurableContext


_CONNECT_TIMEOUT_SECONDS = 5
_READ_TIMEOUT_SECONDS = 60
_CLIENT_ATTEMPTS = 3
"""The default client's limits.

The first ``InvokeAgentRuntime`` of a session starts the session's microVM
before the agent answers. So the read timeout is 60 seconds, longer than the
30 seconds of the MicroVM client. One step attempt then takes at most about
3 minutes.
"""

MAX_ANSWER_BYTES = 64 * 1024
"""The most bytes of the agent's answer that the request step reads.

The worker answers ``{"accepted": true}``. The step reads no more than this
limit, so a long answer from other code does not fill the memory.
"""


class AgentcoreNotAcceptedError(Exception):
    """The agent answered the job request with something other than
    ``{"accepted": true}``.

    The default retry strategy does not retry it. The same agent gives the
    same answer to the same job.
    """


def agentcore(
    context: DurableContext,
    name: str,
    *,
    job_input: Any,
    config: AgentcoreConfig,
) -> Any:
    """Run one job in a new AgentCore Runtime session, and return the job's
    result.

    The operation runs its durable operations in one child context named
    ``name``:

    1. ``<name>.session`` generates the runtime session ID, a UUID. The SDK
       checkpoints it. So every retry and replay uses the same session.
    2. ``<name>.callback`` creates a durable callback.
    3. ``<name>.request`` calls ``InvokeAgentRuntime`` with the session ID
       and the job document as the payload. AgentCore starts the session on
       this first request. The agent answers ``{"accepted": true}`` at once,
       and runs the job in the background.
    4. The child context waits on the callback. The invocation ends during
       the wait, so no Lambda compute is billed. The agent completes the
       callback with the job's result or failure.
    5. ``<name>.stop`` calls ``StopRuntimeSession``. It runs after success,
       failure, and timeout alike, and after a failed delivery.

    A stop failure does not fail the operation. The job's outcome is already
    recorded when stop runs, and AgentCore ends an idle session at the agent
    runtime's idle timeout. So the operation logs an error and returns the
    job's outcome.

    ``InvokeAgentRuntime`` has no idempotency token. A retried request step
    can deliver the same job twice to the same session. The worker answers a
    repeated callback ID with ``{"accepted": true}``, and does not run the
    job again.

    ``job_input`` and ``config`` are keyword-only, as in ``microvm``.

    Args:
        context: The context to create the durable operations in.
        name: The child context name. It also prefixes the inner operations.
        job_input: The job's input. It must be JSON-serializable, and the
            same on every replay.
        config: The agent runtime and the timeouts.

    Returns:
        The value that the agent passed to
        ``SendDurableExecutionCallbackSuccess``, parsed as JSON.

    Raises:
        ValidationError: When ``name`` or a timeout is not valid, or the
            Region of the durable function cannot be determined. No durable
            operation starts.
        TypeError: When ``job_input`` is not JSON-serializable. No durable
            operation starts.
        ValueError: When the JSON of ``job_input`` is over
            ``MAX_JOB_INPUT_BYTES`` UTF-8 bytes. No durable operation starts.
        AgentcoreDeliveryError: When ``InvokeAgentRuntime`` fails after all
            retries, or the agent does not accept the job.
        AgentcoreJobFailedError: When the agent reports a failure, or sends a
            result that is not valid JSON.
        AgentcoreTimeoutError: When no result or heartbeat arrives in time.
    """
    scope = _AgentcoreScope.create(context, name, job_input, config)

    def run(child: DurableContext) -> Any:
        return scope.run_job(child, job_input)

    try:
        return context.run_in_child_context(run, name=name)
    except ChildContextError as error:
        raise AgentcoreError.from_child_context_error(name, error) from error


@dataclass(frozen=True, kw_only=True)
class _AgentcoreScope:
    """The validated values of one :func:`agentcore` call."""

    name: str
    config: AgentcoreConfig
    region: str
    """The Region of the durable function."""
    timeout_seconds: int
    heartbeat_timeout_seconds: int | None
    retry_strategy: Callable[[Exception, int], RetryDecision]

    @classmethod
    def create(
        cls,
        context: DurableContext,
        name: str,
        job_input: Any,
        config: AgentcoreConfig,
    ) -> _AgentcoreScope:
        """Validate the call, before any durable operation starts.

        ``agent_runtime_arn`` and ``qualifier`` are not checked here. The
        boto3 client and the service reject a value that is not valid. The
        request step then fails, and the operation raises
        :class:`AgentcoreDeliveryError`.

        Raises:
            ValidationError: When ``name`` or a timeout is not valid, or the
                Region cannot be determined.
            TypeError: When ``job_input`` is not JSON-serializable.
            ValueError: When the JSON of ``job_input`` is too large.
        """
        if not isinstance(name, str) or not name:
            msg = "an AgentCore operation requires a non-empty name"
            raise ValidationError(msg)
        timeout_seconds = config.timeout.to_seconds()
        if timeout_seconds < 1:
            msg = f'AgentCore "{name}": timeout must be at least 1 second'
            raise ValidationError(msg)
        heartbeat_timeout_seconds = (
            None
            if config.heartbeat_timeout is None
            else config.heartbeat_timeout.to_seconds()
        )
        if heartbeat_timeout_seconds is not None and heartbeat_timeout_seconds < 1:
            msg = f'AgentCore "{name}": heartbeat_timeout must be at least 1 second'
            raise ValidationError(msg)
        _validate_input(name, job_input)
        region = _function_region(context)
        if not region:
            # The worker needs the Region for the callback calls. So the
            # operation fails before any durable operation instead.
            msg = (
                f'AgentCore "{name}": cannot determine the AWS Region. The '
                "durable execution ARN has none, and AWS_REGION is not set."
            )
            raise ValidationError(msg)
        return cls(
            name=name,
            config=config,
            region=region,
            timeout_seconds=timeout_seconds,
            heartbeat_timeout_seconds=heartbeat_timeout_seconds,
            retry_strategy=config.retry_strategy or default_agentcore_retry_strategy,
        )

    def run_job(self, child: DurableContext, job_input: Any) -> Any:
        """Run the operation's durable operations in its child context."""
        name = self.name
        session_id: str = child.step(
            # A UUID has 36 characters. InvokeAgentRuntime requires a session
            # ID of at least 33.
            lambda _: str(uuid.uuid4()),
            name=f"{name}.session",
            # Plain JSON, so a reader of the execution history finds the
            # session ID.
            config=StepConfig(serdes=JsonSerDes()),
        )
        callback = child.create_callback(
            name=f"{name}.callback",
            config=CallbackConfig(
                timeout=Duration.from_seconds(self.timeout_seconds),
                heartbeat_timeout=Duration.from_seconds(
                    self.heartbeat_timeout_seconds or 0
                ),
                # The worker sends the result as a JSON string. The default
                # callback serdes would return that string unparsed.
                serdes=JsonSerDes(),
            ),
        )
        try:
            self._deliver(child, session_id, callback.callback_id, job_input)
        except AgentcoreDeliveryError:
            self._stop(child, session_id)
            raise

        try:
            result = callback.result()
        except ExecutionError:
            # The execution fails without a retry, and the core SDK records
            # no outcome for the child context. A stop step recorded now
            # would change that history. So the error passes through.
            raise
        except InvocationError as error:
            # An SDK invocation error is not a job outcome. A retryable one
            # makes Lambda run the invocation again, and the replay waits for
            # the same session. So the session stays up for it.
            if not error.is_retryable():
                self._stop(child, session_id)
            raise
        except Exception as error:
            # SuspendExecution is a BaseException, so a wait for the callback
            # passes through here without stopping the session.
            self._stop(child, session_id)
            raise _job_error(name, callback, error) from error
        self._stop(child, session_id)
        return result

    def _deliver(
        self,
        child: DurableContext,
        session_id: str,
        callback_id: str,
        job_input: Any,
    ) -> None:
        """Run the request step, ``<name>.request``.

        Raises:
            AgentcoreDeliveryError: When the delivery fails after all retries,
                or the agent does not accept the job.
        """
        payload = (
            AgentcoreJobRequest(
                region=self.region,
                callback_id=callback_id,
                job_input=job_input,
                heartbeat_timeout_seconds=self.heartbeat_timeout_seconds,
            )
            .to_json()
            .encode()
        )

        def attempt(_: object) -> None:
            request: dict[str, Any] = {
                "agentRuntimeArn": self.config.agent_runtime_arn,
                "runtimeSessionId": session_id,
                "contentType": "application/json",
                "accept": "application/json",
                "payload": payload,
            }
            if self.config.qualifier is not None:
                request["qualifier"] = self.config.qualifier
            response = self._client().invoke_agent_runtime(**request)
            _check_accepted(response)

        try:
            child.step(
                attempt,
                name=f"{self.name}.request",
                config=StepConfig(retry_strategy=self.retry_strategy),
            )
        except StepError as error:
            msg = (
                f'AgentCore "{self.name}": the job could not be delivered: '
                f"{error.message or error.error_type}"
            )
            raise AgentcoreDeliveryError(
                message=msg, data=error.data, stack_trace=error.stack_trace
            ) from error

    def _stop(self, child: DurableContext, session_id: str) -> None:
        """Run the stop step, and log a final failure instead of raising.

        The job's outcome is already recorded when stop runs. AgentCore ends
        an idle session at the agent runtime's idle timeout. So a stop
        failure must not discard the outcome. The step's failure is
        checkpointed. So on replay the step fails again at once, with the
        same outcome.
        """
        try:
            child.step(
                lambda _: self._stop_call(session_id),
                name=f"{self.name}.stop",
                config=StepConfig(retry_strategy=self.retry_strategy),
            )
        except StepError as error:
            # The session keeps running, and is billed, until AgentCore ends
            # it. A missing permission fails every stop this way. This log
            # is the only sign of it. So it is an error, not a warning.
            child.logger.error(
                f'AgentCore "{self.name}": StopRuntimeSession failed for '
                f"session {session_id}. The session keeps running until the "
                "agent runtime's idle timeout or maximum lifetime ends it. A "
                "missing permission needs bedrock-agentcore:StopRuntimeSession "
                "in the function's execution role.",
                extra={"runtimeSessionId": session_id, "error": error.message},
            )

    def _stop_call(self, session_id: str) -> None:
        request: dict[str, Any] = {
            "agentRuntimeArn": self.config.agent_runtime_arn,
            "runtimeSessionId": session_id,
        }
        if self.config.qualifier is not None:
            request["qualifier"] = self.config.qualifier
        try:
            self._client().stop_runtime_session(**request)
        except ClientError as error:
            # A session that has already ended, or that never started, is
            # not found. Either way, no session runs.
            if error.response.get("Error", {}).get("Code") != (
                "ResourceNotFoundException"
            ):
                raise

    def _client(self) -> BedrockAgentcoreClient:
        if self.config.client is not None:
            return self.config.client
        return _DEFAULT_CLIENTS.get(_arn_region(self.config.agent_runtime_arn))


def _check_accepted(response: Any) -> None:
    """Read the agent's answer, and check that it accepted the job.

    The answer of ``InvokeAgentRuntime`` is a stream in ``response``. The
    step reads at most :data:`MAX_ANSWER_BYTES` of it, and closes it. An
    unread stream would hold its connection.

    Raises:
        AgentcoreNotAcceptedError: When the answer is not
            ``{"accepted": true}``.
    """
    stream = response["response"]
    try:
        body = stream.read(MAX_ANSWER_BYTES)
    finally:
        stream.close()
    try:
        answer = json.loads(body)
    except ValueError:
        answer = None
    if not isinstance(answer, dict) or answer.get("accepted") is not True:
        text = body[:200].decode(errors="replace")
        msg = f'the agent did not answer {{"accepted": true}}. It answered: {text}'
        raise AgentcoreNotAcceptedError(msg)


def _job_error(name: str, callback: Callback, error: Exception) -> Exception:
    """Convert a failure of the callback wait into its AgentCore error."""
    if isinstance(error, CallbackTimeoutError):
        return AgentcoreTimeoutError(
            message=f'AgentCore "{name}": the job timed out: {error.message}',
            data=error.data,
            stack_trace=error.stack_trace,
        )
    if isinstance(error, CallbackError):
        reported = _reported_error_type(callback)
        detail = f" ({reported})" if reported else ""
        return AgentcoreJobFailedError(
            message=f'AgentCore "{name}": the job failed{detail}: {error.message}',
            data=error.data,
            stack_trace=error.stack_trace,
        )
    # Any other failure, such as a result that is not valid JSON. The agent
    # sent the result, so the job failed.
    detail = getattr(error, "message", None) or str(error)
    return AgentcoreJobFailedError(
        message=(
            f'AgentCore "{name}": the job result could not be read '
            f"({type(error).__name__}): {detail}"
        )
    )


def _reported_error_type(callback: Callback) -> str | None:
    """The error type that the agent reported for a failed callback.

    ``CallbackExternalError`` carries the message and the data, not the type.
    The callback's checkpoint holds the type. ``ExecutionState`` is internal
    to the SDK. The MicroVM operation reads it the same way, and the
    package's dependency has an upper bound on the SDK's major version.

    ``Error`` is the type that a worker reports for a value that is not an
    exception. It names nothing, so it is left out.
    """
    try:
        error = callback.state.get_checkpoint_result(callback.operation_id).error
        reported = None if error is None else error.type
    except (AttributeError, KeyError, TypeError):
        return None
    if not isinstance(reported, str) or reported in {"", "Error"}:
        return None
    return reported


def _validate_input(name: str, job_input: Any) -> None:
    """Check that the job input is JSON-serializable, and fits in a job body.

    The worker accepts a job body of at most 6 MiB, as the MicroVM worker
    does. So the limit is the MicroVM limit, :data:`MAX_JOB_INPUT_BYTES`.
    ``InvokeAgentRuntime`` accepts a payload of up to 100 MB, so the worker
    limit is the smaller one.

    Raises:
        TypeError: When it is not JSON-serializable.
        ValueError: When its JSON is over :data:`MAX_JOB_INPUT_BYTES` bytes.
    """
    try:
        text = dumps(job_input)
    except (TypeError, ValueError) as error:
        msg = f'AgentCore "{name}": the job input must be JSON-serializable: {error}'
        raise TypeError(msg) from error
    size = len(text.encode())
    if size > MAX_JOB_INPUT_BYTES:
        msg = (
            f'AgentCore "{name}": the job input is {size} bytes as JSON. The '
            f"limit is {MAX_JOB_INPUT_BYTES} bytes, because the worker accepts a "
            "job body of at most 6 MiB. Pass larger data by reference, for "
            "example as an S3 key."
        )
        raise ValueError(msg)


def _function_region(context: DurableContext) -> str:
    """The Region of the durable execution, or ``AWS_REGION``."""
    return _arn_region(context.execution_context.durable_execution_arn) or (
        os.environ.get("AWS_REGION", "")
    )


def _arn_region(arn: str) -> str:
    """The Region of an ARN of the form ``arn:<partition>:<service>:<region>:...``,
    or ``""``."""
    parts = arn.split(":")
    if len(parts) > 3 and parts[0] == "arn":
        return parts[3]
    return ""


class _DefaultClients:
    """One ``bedrock-agentcore`` client per Region, created on first use.

    A boto3 client is safe to use from several threads. Creating one from a
    shared session is not. So creation runs under a lock, and each client
    gets its own session. The key ``""`` gets boto3's default Region.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._clients: dict[str, BedrockAgentcoreClient] = {}

    def get(self, region: str) -> BedrockAgentcoreClient:
        with self._lock:
            client = self._clients.get(region)
            if client is None:
                client = boto3.session.Session().client(
                    "bedrock-agentcore",
                    region_name=region or None,
                    config=Config(
                        connect_timeout=_CONNECT_TIMEOUT_SECONDS,
                        read_timeout=_READ_TIMEOUT_SECONDS,
                        retries={
                            "mode": "standard",
                            "total_max_attempts": _CLIENT_ATTEMPTS,
                        },
                    ),
                )
                self._clients[region] = client
            return client


_DEFAULT_CLIENTS = _DefaultClients()
