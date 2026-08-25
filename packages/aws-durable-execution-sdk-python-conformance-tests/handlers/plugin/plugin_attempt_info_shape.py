"""10-21: Attempt hook info field shape.

The named step fails on its first built-in durable attempt and succeeds on its
second under the SDK's real retry strategy. Each user-function hook dumps only
fields carried by its own info object.
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
    OperationInfo,
    UserFunctionEndInfo,
    UserFunctionStartInfo,
)
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)


def _emit(record: dict[str, Any], execution_arn: str | None) -> None:
    if execution_arn is not None:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


def _attempt_record(hook: str, info: OperationInfo) -> dict[str, Any]:
    record: dict[str, Any] = {
        "plugin": "CONFPLUGIN",
        "hook": hook,
        "id": info.operation_id,
        "type": info.operation_type.name,
        "isReplay": info.is_replayed,
    }
    if info.name is not None:
        record["name"] = info.name
    if info.sub_type is not None:
        record["subType"] = info.sub_type.value
    if info.parent_id is not None:
        record["parentId"] = info.parent_id
    if info.attempt is not None:
        record["attempt"] = info.attempt
    if info.start_time is not None:
        record["startTimestamp"] = info.start_time.isoformat()
    if info.end_time is not None:
        record["endTimestamp"] = info.end_time.isoformat()
    if info.error is not None and info.error.message is not None:
        record["error"] = info.error.message
    return record


class AttemptInfoShapePlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self._execution_arn = info.execution_arn

    def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        _emit(_attempt_record("attempt-start", info), self._execution_arn)

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        record = _attempt_record("attempt-end", info)
        record["outcome"] = info.outcome.name
        _emit(record, self._execution_arn)


@durable_step
def flaky(step_context: StepContext) -> str:
    if step_context.attempt < 2:
        raise RuntimeError(f"Attempt {step_context.attempt} failed")
    return "ok"


@durable_execution(plugins=[AttemptInfoShapePlugin()])
def handler(_event: Any, context: DurableContext) -> str:
    retry_config = RetryStrategyConfig(
        max_attempts=3,
        initial_delay=Duration.from_seconds(1),
        retryable_error_types=[RuntimeError],
    )
    result: str = context.step(
        flaky(),
        name="flaky",
        config=StepConfig(create_retry_strategy(retry_config)),
    )
    return result
