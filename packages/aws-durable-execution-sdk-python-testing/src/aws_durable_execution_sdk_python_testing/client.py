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


class InMemoryServiceClient(DurableServiceClient):
    """An in-memory service client, that can replace the boto lambda service client."""

    def __init__(self, checkpoint_processor: CheckpointProcessor):
        self._checkpoint_processor: CheckpointProcessor = checkpoint_processor

    def checkpoint(
        self,
        durable_execution_arn: str,  # noqa: ARG002
        checkpoint_token: str,
        updates: list[OperationUpdate],
        client_token: str | None,
    ) -> CheckpointOutput:
        # durable_execution_arn is not used in in-memory testing
        return self._checkpoint_processor.process_checkpoint(
            checkpoint_token, updates, client_token
        )

    def get_execution_state(
        self,
        durable_execution_arn: str,  # noqa: ARG002
        checkpoint_token: str,
        next_marker: str,
        max_items: int = 1000,
    ) -> StateOutput:
        # durable_execution_arn is not used in in-memory testing
        return self._checkpoint_processor.get_execution_state(
            checkpoint_token, next_marker, max_items
        )

    def stop(self, execution_arn: str, payload: bytes | None) -> datetime.datetime:  # noqa: ARG002
        # TODO: implement
        # Return current time for in-memory testing
        return datetime.datetime.now(tz=datetime.UTC)
