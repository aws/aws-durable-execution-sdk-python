"""10-19: Invocation hook info field shape (interface-shape probe).

A single 2-second wait suspends on the first invocation and completes on
replay; the handler returns "done-<input>". The plugin emits from the SDK's
real ``on_invocation_start`` / ``on_invocation_end`` hooks a CANONICAL DUMP of
the CURRENT hook's own info parameter: every field the Python ``InvocationInfo``
type exposes is mapped one-to-one to its canonical camelCase name; a field the
type does not expose is simply OMITTED (a missing key fails its assertion — the
honest parity signal). One derived scalar is added on the end record:
``terminal`` := status in (SUCCEEDED, FAILED). No cross-hook reconstruction —
``isFirstInvocation`` on the end record comes from the invocation-end info.

Python surface note: ``InvocationInfo`` carries ``request_id``,
``execution_arn``, ``is_first_invocation``, ``execution_start_time``, the
``operations`` map and ``execution_input``; ``InvocationStartInfo`` adds
``updated_operations`` and the end info adds ``status``, ``error`` and
``execution_result``. The payload surfaces (``execution_input`` /
``execution_result``) are deliberately NOT dumped here: they are out of GA
conformance scope, so this requirement's canonical schema omits
``executionInput`` / ``executionResult`` and asserts nothing about them.
"""

import json
from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
)


def _emit(record: dict[str, Any], execution_arn: str | None) -> None:
    # Prefix every plugin record with the execution ARN as a top-level field so
    # the conformance runner's CloudWatch JSON filter can scope logs to a single
    # execution. Omit the field when the ARN is unset (never invent a value).
    if execution_arn:
        record = {"durableExecutionArn": execution_arn, **record}
    print(json.dumps(record), flush=True)


class InvocationInfoShapePlugin(DurableInstrumentationPlugin):
    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        # Canonical dump of InvocationStartInfo, minus execution_input: the
        # payload surfaces are out of GA scope and this requirement asserts
        # nothing about them.
        record: dict[str, Any] = {
            "plugin": "CONFPLUGIN",
            "hook": "invocation-start",
            "isFirstInvocation": info.is_first_invocation,
            "operationsCount": len(info.operations),
            "updatedOperationsCount": len(info.updated_operations),
        }
        if info.request_id is not None:
            record["requestId"] = info.request_id
        if info.execution_start_time is not None:
            record["executionStartTimestamp"] = info.execution_start_time.isoformat()
        _emit(record, info.execution_arn)

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        # isFirstInvocation MUST come from the END info itself. execution_input
        # and execution_result are omitted for the same out-of-scope reason.
        status = info.status.name
        record: dict[str, Any] = {
            "plugin": "CONFPLUGIN",
            "hook": "invocation-end",
            "isFirstInvocation": info.is_first_invocation,
            "operationsCount": len(info.operations),
            "status": status,
            "terminal": status in ("SUCCEEDED", "FAILED"),
        }
        if info.request_id is not None:
            record["requestId"] = info.request_id
        if info.execution_start_time is not None:
            record["executionStartTimestamp"] = info.execution_start_time.isoformat()
        if info.error is not None and info.error.message is not None:
            record["executionError"] = info.error.message
        _emit(record, info.execution_arn)


@durable_execution(plugins=[InvocationInfoShapePlugin()])
def handler(event: Any, context: DurableContext) -> str:
    context.wait(Duration.from_seconds(2))
    return f"done-{event}"
