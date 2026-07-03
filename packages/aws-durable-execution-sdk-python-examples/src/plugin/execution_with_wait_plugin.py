"""Demonstrates plugin hooks for a wait that completes while suspended."""

from typing import Any, ClassVar

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationStartInfo,
    OperationEndInfo,
)


class RecordingWaitPlugin(DurableInstrumentationPlugin):
    operation_end_infos: ClassVar[list[dict[str, Any]]] = []

    @classmethod
    def reset(cls) -> None:
        cls.operation_end_infos.clear()

    @classmethod
    def get_wait_end_infos(cls) -> list[dict[str, Any]]:
        return [
            info
            for info in cls.operation_end_infos
            if info["operation_type"] == "WAIT" and info["name"] == "plugin-wait"
        ]

    def on_invocation_start(self, _info: InvocationStartInfo) -> None:
        self.reset()

    def on_operation_end(self, info: OperationEndInfo) -> None:
        self.operation_end_infos.append(
            {
                "operation_type": info.operation_type.value,
                "name": info.name,
                "status": info.status.value,
                "is_replayed": info.is_replayed,
                "has_end_time": info.end_time is not None,
            }
        )


@durable_execution(plugins=[RecordingWaitPlugin()])
def handler(_event: Any, context: DurableContext) -> dict[str, Any]:
    context.wait(Duration.from_seconds(1), name="plugin-wait")
    return {
        "message": "Plugin wait completed",
        "wait_end_infos": RecordingWaitPlugin.get_wait_end_infos(),
    }
