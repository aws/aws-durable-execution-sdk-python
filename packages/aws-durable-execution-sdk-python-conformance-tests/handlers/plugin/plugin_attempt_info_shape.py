"""10-21: Attempt hook info field shape (interface-shape probe).

A single step named "flaky" throws on its first attempt and succeeds on the
second, using the SDK's real retry strategy (``RetryStrategyConfig`` +
``create_retry_strategy``, max_attempts >= 2, ~1s delay); it returns "ok". The
plugin emits from the SDK's real per-attempt hooks (``on_user_function_start`` /
``on_user_function_end``), filtering to step-type operations. Every logged field
is read from the CURRENT hook's own info parameter — never reconstructed from
another hook or from plugin state. When the Python info type does not expose a
field, the plugin logs the corresponding ``has_*`` flag as false; that omission
is the honest signal of a missing API surface.

Python surface note: ``UserFunctionStartInfo`` / ``UserFunctionEndInfo`` expose
operation_id, name, operation_type, attempt and start_time; the end info adds
``outcome`` (a ``UserFunctionOutcome`` enum) and ``error``. The failure is a
genuine thrown exception surfaced through the SDK's retry path — never a
hand-rolled outcome.
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


class AttemptInfoShapePlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        # User-function hooks do not carry the execution ARN, so capture it from
        # the invocation-start hook and stamp it on later attempt emissions.
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self._execution_arn = info.execution_arn

    def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        _emit(
            {
                "plugin": "CONFPLUGIN",
                "hook": "attempt-start",
                "op": info.operation_id,
                "name": info.name,
                "type": info.operation_type.name.upper(),
                "attempt": info.attempt,
                "has_start_time": info.start_time is not None,
            },
            self._execution_arn,
        )

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        _emit(
            {
                "plugin": "CONFPLUGIN",
                "hook": "attempt-end",
                "op": info.operation_id,
                "name": info.name,
                "type": info.operation_type.name.upper(),
                "attempt": info.attempt,
                # outcome as reported by the info's own outcome enum — a
                # presentation of the API's data, not a reconstruction.
                "outcome": info.outcome.name,
                "has_error": info.error is not None,
            },
            self._execution_arn,
        )


@durable_step
def flaky(step_context: StepContext) -> str:
    # Fail on the first attempt, succeed on the second, using the SDK's built-in
    # durable attempt counter (1-based) from the step context.
    if step_context.attempt < 2:
        msg = f"Attempt {step_context.attempt} failed"
        raise RuntimeError(msg)
    return "ok"


@durable_execution(plugins=[AttemptInfoShapePlugin()])
def handler(_event: Any, context: DurableContext) -> str:
    retry_config = RetryStrategyConfig(
        max_attempts=2,
        initial_delay=Duration.from_seconds(1),
        retryable_error_types=[RuntimeError],
    )
    result: str = context.step(
        flaky(),
        name="flaky",
        config=StepConfig(create_retry_strategy(retry_config)),
    )
    return result
