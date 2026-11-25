"""Run two jobs in one AgentCore Runtime session, with a durable wait between
them.

The agent runtime runs agentcore-agent/app.py. Its handler counts the words
of the text and hashes it. It also returns the session ID that AgentCore
passed to it.
"""

import os
from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python_extensions.compute.agentcore import (
    AgentcoreInvokeOptions,
    AgentcoreSession,
    AgentcoreSessionConfig,
    agentcore_session,
)


def _require_agent_runtime_arn() -> str:
    """Return the agent runtime ARN.

    The SAM template sets ``AGENT_RUNTIME_ARN`` on every example with
    ``"usesAgentcore": true``, from the ``AgentRuntimeArn`` parameter.
    scripts/build_examples.py copies every file under src/ into one
    directory. So a shared helper module would need a different import in
    Lambda than in the local tests. So each AgentCore example defines this
    function itself.
    """
    arn = os.environ.get("AGENT_RUNTIME_ARN")
    if not arn:
        msg = (
            "AGENT_RUNTIME_ARN must be set. Deploy an agent runtime that runs "
            "agentcore-agent/app.py, and pass its ARN as AgentRuntimeArn."
        )
        raise RuntimeError(msg)
    return arn


JOB_OPTIONS = AgentcoreInvokeOptions(
    timeout=Duration.from_minutes(5),
    # A job that the agent stops answering fails after this time, instead of
    # after the timeout.
    heartbeat_timeout=Duration.from_seconds(60),
)


@durable_execution
def handler(event: dict[str, Any], context: DurableContext) -> Any:
    agent_runtime_arn = _require_agent_runtime_arn()

    def count_both(session: AgentcoreSession, session_context: DurableContext) -> Any:
        # Each session.invoke sends one job to the session, and waits on a
        # durable callback that the worker in the session completes.
        first = session.invoke(
            session_context,
            "count-first",
            job_input={"text": event["first"]},
            options=JOB_OPTIONS,
        )

        # The wait ends this invocation. The session keeps its session ID.
        # So the next job goes to the same session.
        session_context.wait(Duration.from_seconds(5), name="pause")

        second = session.invoke(
            session_context,
            "count-second",
            job_input={"text": event["second"]},
            options=JOB_OPTIONS,
        )

        return {
            "runtimeSessionId": session.runtime_session_id,
            "first": first,
            "second": second,
        }

    # The session stops with StopRuntimeSession when the handler returns.
    return agentcore_session(
        context,
        "count-session",
        config=AgentcoreSessionConfig(
            agent_runtime_arn=agent_runtime_arn,
            qualifier="DEFAULT",
        ),
        handler=count_both,
    )
