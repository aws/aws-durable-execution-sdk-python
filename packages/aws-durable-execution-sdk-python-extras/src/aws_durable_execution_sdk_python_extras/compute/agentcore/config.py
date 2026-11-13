# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The configuration of :func:`agentcore`."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.retries import RetryDecision


if TYPE_CHECKING:
    from mypy_boto3_bedrock_agentcore import (
        BedrockAgentCoreClient as BedrockAgentcoreClient,
    )
else:
    BedrockAgentcoreClient = Any


@dataclass(frozen=True, kw_only=True)
class AgentcoreConfig:
    """The agent runtime of :func:`agentcore`, and how long to wait for its
    result.

    :func:`agentcore` validates the config before it starts any durable
    operation.
    """

    agent_runtime_arn: str
    """The ARN of the AgentCore Runtime agent runtime that runs the job.

    The agent runtime must run the worker
    ``aws_durable_execution_sdk_python_compute_worker.agentcore``, or code
    that follows the same contract. Its execution role needs
    ``lambda:SendDurableExecutionCallbackSuccess``,
    ``lambda:SendDurableExecutionCallbackFailure``, and
    ``lambda:SendDurableExecutionCallbackHeartbeat`` on the durable
    function's ARN.

    The durable function's execution role needs
    ``bedrock-agentcore:InvokeAgentRuntime`` and
    ``bedrock-agentcore:StopRuntimeSession`` on two resources: this ARN, and
    ``<this ARN>/runtime-endpoint/*``. A live run with only the runtime ARN
    in the policy was denied on ``.../runtime-endpoint/DEFAULT``. To grant
    one endpoint only, name the endpoint of ``qualifier`` instead of ``*``.

    The worker answers ``/ping`` with ``HealthyBusy`` while a job runs. A
    live run kept a session through a 180-second job with an idle timeout
    of 60 seconds. So the agent runtime's ``idleRuntimeSessionTimeout``
    does not end a running job. After the idle timeout, a session with no
    running job ends. A later request with the same session ID then gets a
    new microVM without an error, and the memory and files of the old one
    are lost. This operation sends one job per session, so it does not
    depend on state across that gap.
    """

    timeout: Duration
    """The longest time to wait for the agent to report a result.

    It must be at least 1 second. AgentCore ends a session at the agent
    runtime's ``maxLifetime``: at most 8 hours for a microVM session, and at
    most 14 days on Instances. A job that runs longer than its session can
    never report. Keep the timeout within the runtime's ``maxLifetime``.
    """

    qualifier: str | None = None
    """The endpoint qualifier of the agent runtime, such as ``DEFAULT``.

    ``None`` sends no qualifier, and AgentCore uses its default endpoint. The
    operation sends the same qualifier to ``InvokeAgentRuntime`` and
    ``StopRuntimeSession``.
    """

    heartbeat_timeout: Duration | None = None
    """The longest time between two heartbeats from the agent.

    Set it to at least 1 second. It ends the job when the worker crashes or
    stops reporting.

    When it is set, an agent session that crashes fails the operation after
    this time, instead of after ``timeout``. The agent receives the value in
    the job document.

    The heartbeat timeout starts when the callback is created, before the
    request step. The worker sends its first heartbeat when the job arrives.
    So the first period must cover the request step with its retries and the
    start of a new session.
    """

    retry_strategy: Callable[[Exception, int], RetryDecision] | None = None
    """The step retry strategy of the request and stop steps.

    It is the second retry tier. The boto3 client already retries each call
    inside one step attempt. Defaults to
    :func:`default_agentcore_retry_strategy`.

    ``InvokeAgentRuntime`` has no idempotency token. So a retried request
    step can deliver the same job twice. The worker answers a second
    delivery of a callback ID that it already accepted without running the
    job again. It remembers callback IDs only in its process memory. A new
    process after the idle timeout runs a repeated job again. So the job
    handler's side effects must be idempotent.
    """

    client: BedrockAgentcoreClient | None = None
    """The boto3 ``bedrock-agentcore`` client.

    Defaults to one client per Region, created on first use. The Region comes
    from the agent runtime ARN. The default client has a 5-second connect
    timeout, a 60-second read timeout, and 3 attempts in botocore's standard
    retry mode. A client that you pass keeps its own settings.
    """
