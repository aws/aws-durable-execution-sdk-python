"""10-17: Faulty plugin does not affect a healthy plugin.

Two plugins are registered together, in order: a faulty plugin whose every
exercised hook logs a line and then raises, and a healthy plugin that logs
normally. The exercised hooks span the full lifecycle: invocation-start,
operation-start, attempt-start, attempt-end, operation-end, and invocation-end.
In the Python SDK the per-attempt hooks are the real ``on_user_function_start`` /
``on_user_function_end`` callbacks (the latter carries the attempt ``outcome``),
and the operation hooks are ``on_operation_start`` / ``on_operation_end``.

The SDK must isolate each plugin (the faulty plugin's exceptions are swallowed)
so the healthy plugin still receives every hook and the execution
result/history are identical to running without the faulty plugin. No mocking:
the isolation guarantee under test is the SDK's own per-plugin hook-dispatch
try/except, which we exercise by genuinely raising from every exercised hook.
"""

import json
from typing import Any

from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    OperationEndInfo,
    OperationStartInfo,
    UserFunctionEndInfo,
    UserFunctionStartInfo,
)


def _emit(record: dict[str, Any], execution_arn: str | None) -> None:
    # Prefix every plugin record with the execution ARN as a top-level field so
    # the conformance runner's CloudWatch JSON filter can scope logs to a single
    # execution. Omit the field when the ARN is unset (never invent a value).
    if execution_arn:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


class FaultyPlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        # Operation/attempt hooks do not carry the execution ARN; capture it
        # from invocation-start (before raising) and reuse it so every faulty
        # record is still execution-scoped for CloudWatch retrieval.
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self._execution_arn = info.execution_arn
        _emit(
            {"plugin": "CONFPLUGIN-FAULTY", "hook": "invocation-start"},
            info.execution_arn,
        )
        raise RuntimeError("faulty invocation-start")

    def on_operation_start(self, info: OperationStartInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        _emit(
            {"plugin": "CONFPLUGIN-FAULTY", "hook": "operation-start"},
            self._execution_arn,
        )
        raise RuntimeError("faulty operation-start")

    def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        _emit(
            {"plugin": "CONFPLUGIN-FAULTY", "hook": "attempt-start"},
            self._execution_arn,
        )
        raise RuntimeError("faulty attempt-start")

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        _emit(
            {"plugin": "CONFPLUGIN-FAULTY", "hook": "attempt-end"},
            self._execution_arn,
        )
        raise RuntimeError("faulty attempt-end")

    def on_operation_end(self, info: OperationEndInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        _emit(
            {"plugin": "CONFPLUGIN-FAULTY", "hook": "operation-end"},
            self._execution_arn,
        )
        raise RuntimeError("faulty operation-end")

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        _emit(
            {"plugin": "CONFPLUGIN-FAULTY", "hook": "invocation-end"},
            info.execution_arn,
        )
        raise RuntimeError("faulty invocation-end")


class HealthyPlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        # Operation/attempt hooks do not carry the execution ARN, so capture it
        # from the invocation-start hook and reuse it for later emissions.
        self._execution_arn: str | None = None

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self._execution_arn = info.execution_arn
        _emit(
            {
                "plugin": "CONFPLUGIN-HEALTHY",
                "hook": "invocation-start",
                "first": info.is_first_invocation,
            },
            info.execution_arn,
        )

    def on_operation_start(self, info: OperationStartInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        _emit(
            {
                "plugin": "CONFPLUGIN-HEALTHY",
                "hook": "operation-start",
                "op": info.operation_id,
            },
            self._execution_arn,
        )

    def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        _emit(
            {
                "plugin": "CONFPLUGIN-HEALTHY",
                "hook": "attempt-start",
                "op": info.operation_id,
            },
            self._execution_arn,
        )

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        _emit(
            {
                "plugin": "CONFPLUGIN-HEALTHY",
                "hook": "attempt-end",
                "op": info.operation_id,
                "outcome": info.outcome.name,
            },
            self._execution_arn,
        )

    def on_operation_end(self, info: OperationEndInfo) -> None:
        if info.operation_type.name != "STEP":
            return
        _emit(
            {
                "plugin": "CONFPLUGIN-HEALTHY",
                "hook": "operation-end",
                "op": info.operation_id,
                "status": info.status.name,
            },
            self._execution_arn,
        )

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        status = info.status.name if info.status is not None else "NONE"
        _emit(
            {
                "plugin": "CONFPLUGIN-HEALTHY",
                "hook": "invocation-end",
                "status": status,
            },
            info.execution_arn,
        )


@durable_step
def greet(_step_context: StepContext, name: str) -> str:
    return f"Hello, {name}!"


@durable_execution(plugins=[FaultyPlugin(), HealthyPlugin()])
def handler(event: Any, context: DurableContext) -> str:
    result: str = context.step(greet(event))
    return result
