"""10-15: Plugin attempt hooks through retry exhaustion.

A single step always throws, configured with the SDK's real retry strategy
allowing 2 total attempts (~1s delay). The plugin emits from the SDK's real
``on_user_function_start`` / ``on_user_function_end`` (per-attempt) and
``on_operation_end`` (terminal) hooks, filtering to step-type operations. Every
attempt fails; retries exhaust; the terminal operation-end reports FAILED.
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


def _is_step(info: Any) -> bool:
    return info.operation_type.name == "STEP"


class RetryExhaustionPlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        # User-function / operation hooks do not carry the execution ARN, so
        # capture it from the invocation-start hook and reuse it.
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
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

    def on_operation_end(self, info: OperationEndInfo) -> None:
        if not _is_step(info):
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
def always_fail(_step_context: StepContext) -> str:
    msg = "boom"
    raise RuntimeError(msg)


@durable_execution(plugins=[RetryExhaustionPlugin()])
def handler(_event: Any, context: DurableContext) -> str:
    retry_config = RetryStrategyConfig(
        max_attempts=2,
        initial_delay=Duration.from_seconds(1),
        retryable_error_types=[RuntimeError],
    )
    result: str = context.step(
        always_fail(),
        config=StepConfig(create_retry_strategy(retry_config)),
    )
    return result
