"""An in-memory service client, that can replace the boto lambda service client."""

import datetime

from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    DurableServiceClient,
    OperationUpdate,
    StateOutput,
)

from aws_durable_execution_sdk_python_testing.checkpoint.processor import (
    CheckpointProcessor,
)
from aws_durable_execution_sdk_python_testing.worker.checkpoint_tasks import (
    CheckpointTask,
    GetStateTask,
)
from aws_durable_execution_sdk_python_testing.worker.registry import ExecutionRegistry


class InMemoryServiceClient(DurableServiceClient):
    """An in-memory service client, that can replace the boto lambda service client."""

    def __init__(
        self,
        checkpoint_processor: CheckpointProcessor,
        registry: ExecutionRegistry,
    ):
        self._checkpoint_processor: CheckpointProcessor = checkpoint_processor
        self._registry: ExecutionRegistry = registry

    def checkpoint(
        self,
        durable_execution_arn: str,
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        task = CheckpointTask(
            self._checkpoint_processor, checkpoint_token, updates, client_token
        )
        return self._registry.submit(durable_execution_arn, task).result()

    def get_execution_state(
        self,
        durable_execution_arn: str,
        checkpoint_token: str,
        next_marker: str,
        max_items: int = 1000,
    ) -> StateOutput:
        task = GetStateTask(
            self._checkpoint_processor, checkpoint_token, next_marker, max_items
        )
        return self._registry.submit(durable_execution_arn, task).result()

    def stop(self, execution_arn: str, payload: bytes | None) -> datetime.datetime:  # noqa: ARG002
        # TODO: implement
        # Return current time for in-memory testing
        return datetime.datetime.now(tz=datetime.UTC)
