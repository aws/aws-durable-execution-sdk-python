"""10-13: Plugin replay flag on operation-start.

Two sequential steps: step A succeeds on its first attempt (terminal); step B
fails once then succeeds via the SDK's real retry strategy. The plugin emits
from the SDK's real ``on_operation_start`` (carrying the ``is_replayed`` flag)
and ``on_operation_end`` hooks, filtering to step-type operations. Terminal step
A is never re-emitted on replay (replay=false only); non-terminal step B is
observed with replay=true on the retry invocation.
"""

import json
from typing import Any

from aws_durable_execution_sdk_python.config import Duration, StepConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
    OperationEndInfo,
    OperationStartInfo,
)
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)


def _emit(record: dict[str, Any], execution_arn: str | None) -> None:
    # Prefix every plugin record with the execution ARN as a top-level field so
    # the conformance runner's CloudWatch JSON filter can scope logs to a single
    # execution. Omit the field when the ARN is unset (never invent a value).
    if execution_arn:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


class ReplayFlagPlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        # Operation hooks do not carry the execution ARN, so capture it from the
        # invocation-start hook and reuse it for later operation emissions.
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self._execution_arn = info.execution_arn

    def on_operation_start(self, info: OperationStartInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        _emit(
            {
                "plugin": "CONFPLUGIN",
                "hook": "operation-start",
                "op": info.operation_id,
                "replay": info.is_replayed,
            },
            self._execution_arn,
        )

    def on_operation_end(self, info: OperationEndInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        _emit(
            {
                "plugin": "CONFPLUGIN",
                "hook": "operation-end",
                "op": info.operation_id,
                "status": info.status.name,
            },
            self._execution_arn,
        )


@durable_step
def step_a(_step_context: StepContext) -> str:
    return "a"


@durable_step
def step_b(step_context: StepContext) -> str:
    # Fail on the first attempt, succeed on the second, driven by the SDK's
    # built-in durable attempt counter (1-based).
    if step_context.attempt < 2:
        msg = f"Attempt {step_context.attempt} failed"
        raise RuntimeError(msg)
    return "Operation succeeded"


@durable_execution(plugins=[ReplayFlagPlugin()])
def handler(_event: Any, context: DurableContext) -> str:
    context.step(step_a())
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        initial_delay=Duration.from_seconds(1),
        retryable_error_types=[RuntimeError],
    )
    result: str = context.step(
        step_b(),
        config=StepConfig(create_retry_strategy(retry_config)),
    )
    return result
