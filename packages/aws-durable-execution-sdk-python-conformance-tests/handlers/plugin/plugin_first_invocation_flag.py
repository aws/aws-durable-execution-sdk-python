"""10-6: Plugin sees is-first-invocation true once, then false on replay.

Uses the SDK's real ``context.wait`` (mirrors handler 2-1) so the execution
suspends on the first invocation and replays after the timer completes. The
plugin emits its lines from the invocation-start / invocation-end hooks; the
terminal invocation-end (SUCCEEDED) fires on the replay invocation.
"""

from typing import Any

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
)


class FirstInvocationPlugin(DurableInstrumentationPlugin):
    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        first = str(info.is_first_invocation).lower()
        print(f"CONFPLUGIN invocation-start first={first}", flush=True)

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        status = info.status.name if info.status is not None else "NONE"
        print(f"CONFPLUGIN invocation-end status={status}", flush=True)


@durable_execution(plugins=[FirstInvocationPlugin()])
def handler(_event: Any, context: DurableContext) -> str:
    context.wait(Duration.from_seconds(2))
    return "Wait completed"
