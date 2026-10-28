"""A step result above the checkpoint batch size limit completes (issue #871).

The step result is 800 KB. That is above ``max_batch_size_bytes`` (750 KB) and
below the service's 1 MB per-operation limit. With the default
``AT_LEAST_ONCE_PER_RETRY`` semantics the START checkpoint is async, so the
batcher still holds it when the sync SUCCEED arrives, and SUCCEED takes the
overflow path. Before the fix the SUCCEED operation was never sent and the
handler thread blocked forever, so ``run()`` raised ``TimeoutError``.
"""

from __future__ import annotations

import json

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)

RESULT_SIZE_BYTES = 800 * 1024


@durable_execution
def _large_step_handler(_event: dict[str, str], context: DurableContext) -> str:
    return context.step(lambda _sc: "y" * RESULT_SIZE_BYTES, name="big_step")


def test_step_result_above_batch_size_limit_completes() -> None:
    with DurableFunctionTestRunner(handler=_large_step_handler) as runner:
        result: DurableFunctionTestResult = runner.run(input="{}", execution_timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result is not None
    assert len(json.loads(result.result)) == RESULT_SIZE_BYTES
