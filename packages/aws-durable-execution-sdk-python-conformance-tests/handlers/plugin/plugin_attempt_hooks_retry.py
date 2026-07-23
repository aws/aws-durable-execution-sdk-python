"""10-3: Plugin attempt hooks fire per step attempt with attempt number/outcome.

Uses the SDK's real retry strategy (``RetryStrategyConfig`` + ``create_retry_strategy``)
so the step fails once then succeeds on the second attempt (mirrors handler
1-11). The plugin emits its lines from the SDK's real ``on_user_function_start`` /
``on_user_function_end`` hooks, filtering to STEP-type operations.
"""

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
    UserFunctionEndInfo,
    UserFunctionStartInfo,
)
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    create_retry_strategy,
)


def _is_step(info: UserFunctionStartInfo | UserFunctionEndInfo) -> bool:
    return info.operation_type.name == "STEP"


class AttemptPlugin(DurableInstrumentationPlugin):
    def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
        if not _is_step(info):
            return
        print(f"CONFPLUGIN attempt-start n={info.attempt}", flush=True)

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        if not _is_step(info):
            return
        print(
            f"CONFPLUGIN attempt-end n={info.attempt} outcome={info.outcome.name}",
            flush=True,
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
