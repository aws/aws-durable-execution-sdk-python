"""10-21: Attempt hook info field shape (interface-shape probe).

A single step named "flaky" throws on its first attempt and succeeds on the
second, using the SDK's real retry strategy (``RetryStrategyConfig`` +
``create_retry_strategy``, max_attempts >= 2, ~1s delay); it returns "ok". The
plugin emits from the SDK's real per-attempt hooks (``on_user_function_start`` /
``on_user_function_end``, filtering to step-type operations) a CANONICAL DUMP of
the CURRENT hook's own info parameter: every field the Python attempt-info type
exposes is mapped one-to-one to its canonical camelCase name; unset fields
(value None) are OMITTED (a missing key fails its assertion — the honest parity
signal); timestamps ISO-8601, errors their message string, ``outcome`` the
info's own ``UserFunctionOutcome`` token.

Python surface note: ``UserFunctionStartInfo`` / ``UserFunctionEndInfo`` expose
operation_id, operation_type, sub_type, name, parent_id, start_time, attempt,
is_replayed and is_replay_children; the end info adds ``end_time``, ``outcome``
and ``error``. The failure is a genuine thrown exception surfaced through the
SDK's retry path — never a hand-rolled outcome. (``status`` is an inherited
constant STARTED on attempt infos and is not part of the attempt schema.)
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


def _dump_attempt(
    hook: str, info: UserFunctionStartInfo | UserFunctionEndInfo
) -> dict[str, Any]:
    # Canonical dump of an attempt info's own field surface. Identity + attempt
    # number + replay indicators are always present; optional fields are emitted
    # only when the info populates them (None -> omitted key = honest red).
    record: dict[str, Any] = {
        "plugin": "CONFPLUGIN",
        "hook": hook,
        "id": info.operation_id,
        "type": info.operation_type.name.upper(),
        "isReplay": info.is_replayed,
        "isReplayingChildren": info.is_replay_children,
    }
    if info.name is not None:
        record["name"] = info.name
    if info.sub_type is not None:
        record["subType"] = info.sub_type.name
    if info.parent_id is not None:
        record["parentId"] = info.parent_id
    if info.attempt is not None:
        record["attempt"] = info.attempt
    if info.start_time is not None:
        record["startTimestamp"] = info.start_time.isoformat()
    return record


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
        _emit(_dump_attempt("attempt-start", info), self._execution_arn)

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        record = _dump_attempt("attempt-end", info)
        # The end info adds its own outcome enum and (on failure) an error.
        record["outcome"] = info.outcome.name
        if info.end_time is not None:
            record["endTimestamp"] = info.end_time.isoformat()
        if info.error is not None and info.error.message is not None:
            record["error"] = info.error.message
        _emit(record, self._execution_arn)


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
