"""10-19: Invocation hook info field shape (interface-shape probe).

A single 2-second wait suspends on the first invocation and completes on
replay; the handler returns "done-<input>". The plugin emits from the SDK's
real ``on_invocation_start`` / ``on_invocation_end`` hooks. Every logged field
is read from the CURRENT hook's own info parameter — never reconstructed from
another hook or from plugin state. When the Python ``InvocationInfo`` type does
not expose a field, the plugin logs the corresponding ``has_*`` flag as false
and omits the value key; that omission is the honest signal of a missing API
surface (the reference field set is the union across SDKs).

Python surface note: ``InvocationInfo`` carries only ``request_id``,
``execution_arn``, ``is_first_invocation`` and ``execution_start_time``. It has
NO execution-input, operations-map, or externally-updated-operations field, and
``InvocationEndInfo`` adds only ``status`` + ``error`` (no execution-result
field). Those absences are surfaced faithfully as has_*: false.
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
        # Probe the invocation-start info surface. Python's InvocationInfo has
        # no execution-input, operations-map, or updated-operations field, so
        # those has_* flags are honestly false and the "input" key is omitted.
        _emit(
            {
                "plugin": "CONFPLUGIN",
                "hook": "invocation-start",
                "first": info.is_first_invocation,
                "has_request_id": info.request_id is not None,
                "has_input": False,
                "has_operations": False,
                "updated_nonempty": False,
                "has_start_time": info.execution_start_time is not None,
            },
            info.execution_arn,
        )

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        # "first" MUST come from the END info itself (its presence there is what
        # is under test) — never captured at invocation-start.
        status = info.status.name if info.status is not None else "NONE"
        terminal = status in ("SUCCEEDED", "FAILED")
        _emit(
            {
                "plugin": "CONFPLUGIN",
                "hook": "invocation-end",
                "first": info.is_first_invocation,
                "terminal": terminal,
                "status": status,
                # InvocationEndInfo exposes no execution-result field.
                "has_result": False,
                "has_error": info.error is not None,
            },
            info.execution_arn,
        )


@durable_execution(plugins=[InvocationInfoShapePlugin()])
def handler(event: Any, context: DurableContext) -> str:
    context.wait(Duration.from_seconds(2))
    return f"done-{event}"
