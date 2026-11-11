# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Runs jobs from AWS Lambda durable functions inside an Amazon Bedrock
AgentCore Runtime session.

This module is experimental. Its API can change in any release.

The exports match ``@aws/durable-execution-sdk-js-compute-worker/agentcore``.
"""

from aws_durable_execution_sdk_python_compute_worker.agentcore.worker import (
    AGENTCORE_HOST,
    AGENTCORE_PORT,
    INVOCATIONS_PATH,
    PING_PATH,
    AgentcoreJobContext,
    AgentcoreJobHandler,
    AgentcoreWorker,
    start_agentcore_worker,
)


__all__ = [
    "AGENTCORE_HOST",
    "AGENTCORE_PORT",
    "INVOCATIONS_PATH",
    "PING_PATH",
    "AgentcoreJobContext",
    "AgentcoreJobHandler",
    "AgentcoreWorker",
    "start_agentcore_worker",
]
