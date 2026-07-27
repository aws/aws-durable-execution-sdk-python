"""10-3: Plugin attempt hooks fire per step attempt with attempt number/outcome.

Uses the SDK's real retry strategy (``RetryStrategyConfig`` + ``create_retry_strategy``)
so the step fails once then succeeds on the second attempt (mirrors handler
1-11). The plugin emits its lines from the SDK's real ``on_user_function_start`` /
``on_user_function_end`` hooks, filtering to STEP-type operations.
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
    UserFunctionEndInfo,
    UserFunctionStartInfo,
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


def _is_step(info: UserFunctionStartInfo | UserFunctionEndInfo) -> bool:
    return info.operation_type.name == "STEP"


class AttemptPlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        # User-function hooks do not carry the execution ARN, so capture it from
        # the invocation-start hook and reuse it for later attempt emissions.
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        # Capture only; this test asserts attempt-start/-end lines exclusively.
        self._execution_arn = info.execution_arn

    def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
        if not _is_step(info):
            return
        _emit(
            {
                "plugin": "CONFPLUGIN",
                "hook": "attempt-start",
                "n": info.attempt,
                "op": info.operation_id,
            },
            self._execution_arn,
        )

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        if not _is_step(info):
            return
        _emit(
            {
                "plugin": "CONFPLUGIN",
                "hook": "attempt-end",
                "n": info.attempt,
                "outcome": info.outcome.name,
                "op": info.operation_id,
            },
            self._execution_arn,
        )


@durable_step
def unreliable_operation(step_context: StepContext) -> str:
    # Fail on the first attempt, succeed on the second, using the SDK's built-in
    # durable attempt counter (1-based) from the step context.
    if step_context.attempt < 2:
        msg = f"Attempt {step_context.attempt} failed"
        raise RuntimeError(msg)
    return "Operation succeeded"


@durable_execution(plugins=[AttemptPlugin()])
def handler(_event: Any, context: DurableContext) -> str:
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        initial_delay=Duration.from_seconds(1),
        retryable_error_types=[RuntimeError],
    )
    result: str = context.step(
        unreliable_operation(),
        config=StepConfig(create_retry_strategy(retry_config)),
    )
    return result
