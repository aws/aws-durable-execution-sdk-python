"""Run one job in an AgentCore Runtime session and return its result.

The agent runtime runs agentcore-agent/app.py. Its handler counts the words
of the text and hashes it.
"""

import os
from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python_extras.compute.agentcore import (
    AgentcoreConfig,
    agentcore,
)


def _require_agent_runtime_arn() -> str:
    """Return the agent runtime ARN.

    The SAM template sets ``AGENT_RUNTIME_ARN`` on every example with
    ``"usesAgentcore": true``, from the ``AgentRuntimeArn`` parameter.
    """
    arn = os.environ.get("AGENT_RUNTIME_ARN")
    if not arn:
        msg = (
            "AGENT_RUNTIME_ARN must be set. Deploy an agent runtime that runs "
            "agentcore-agent/app.py, and pass its ARN as AgentRuntimeArn."
        )
        raise RuntimeError(msg)
    return arn


@durable_execution
def handler(event: dict[str, Any], context: DurableContext) -> Any:
    # The operation starts a new session with the job, and waits on a durable
    # callback. The worker in the session runs the job and completes the
    # callback. The operation then stops the session.
    return agentcore(
        context,
        "count-words",
        job_input={"text": event["text"]},
        config=AgentcoreConfig(
            agent_runtime_arn=_require_agent_runtime_arn(),
            qualifier="DEFAULT",
            timeout=Duration.from_minutes(5),
            # A session that fails to start fails the operation after this
            # time, instead of after the timeout.
            heartbeat_timeout=Duration.from_seconds(60),
        ),
    )
