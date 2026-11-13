"""The agent that the AgentCore example runs in its agent runtime.

Deploy it as the entry point of an AgentCore Runtime agent runtime, with the
compute worker package installed. The agent runtime's execution role needs
lambda:SendDurableExecutionCallbackSuccess,
lambda:SendDurableExecutionCallbackFailure, and
lambda:SendDurableExecutionCallbackHeartbeat on the example function.
"""

import hashlib
from typing import Any

from aws_durable_execution_sdk_python_compute_worker.agentcore import (
    AgentcoreJobContext,
    start_agentcore_worker,
)


def count_words(job_input: dict[str, Any], context: AgentcoreJobContext) -> dict:
    """Count the words of the text, and hash it."""
    text: str = job_input["text"]
    return {
        "wordCount": len(text.split()),
        "sha256": hashlib.sha256(text.encode()).hexdigest(),
        "sessionId": context.session_id,
    }


if __name__ == "__main__":
    start_agentcore_worker(handler=count_words).wait()
