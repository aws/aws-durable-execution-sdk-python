"""Implementation for Durable Map operation."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Generic, TypeVar

from aws_durable_execution_sdk_python.concurrency import (
    BatchResult,
    ConcurrentExecutor,
    Executable,
)
from aws_durable_execution_sdk_python.config import MapConfig
from aws_durable_execution_sdk_python.lambda_service import OperationSubType

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.identifier import OperationIdentifier
    from aws_durable_execution_sdk_python.serdes import SerDes
    from aws_durable_execution_sdk_python.state import (
        CheckpointedResult,
        ExecutionState,
    )
    from aws_durable_execution_sdk_python.types import SummaryGenerator

logger = logging.getLogger(__name__)

# Input item type
T = TypeVar("T")
# Result type
R = TypeVar("R")


class MapExecutor(Generic[T, R], ConcurrentExecutor[Callable, R]):  # noqa: PYI059
    def __init__(
        self,
        executables: list[Executable[Callable]],
        items: Sequence[T],
        max_concurrency: int | None,
        completion_config,
        top_level_sub_type: OperationSubType,
        iteration_sub_type: OperationSubType,
        name_prefix: str,
        serdes: SerDes | None,
        summary_generator: SummaryGenerator | None = None,
        item_serdes: SerDes | None = None,
    ):
        super().__init__(
            executables=executables,
            max_concurrency=max_concurrency,
            completion_config=completion_config,
            sub_type_top=top_level_sub_type,
            sub_type_iteration=iteration_sub_type,
            name_prefix=name_prefix,
            serdes=serdes,
            summary_generator=summary_generator,
            item_serdes=item_serdes,
        )
        self.items = items

    @classmethod
    def from_items(
        cls,
        items: Sequence[T],
        func: Callable,
        config: MapConfig,
    ) -> MapExecutor[T, R]:
        """Create MapExecutor from items and a callable."""
        executables: list[Executable[Callable]] = [
            Executable(index=i, func=func) for i in range(len(items))
        ]

        return cls(
            executables=executables,
            items=items,
            max_concurrency=config.max_concurrency,
            completion_config=config.completion_config,
            top_level_sub_type=OperationSubType.MAP,
            iteration_sub_type=OperationSubType.MAP_ITERATION,
            name_prefix="map-item-",
            serdes=config.serdes,
            summary_generator=config.summary_generator,
        )

    def execute_item(self, child_context, executable: Executable[Callable]) -> R:
        logger.debug("🗺️ Processing map item: %s", executable.index)
        item = self.items[executable.index]
        result: R = executable.func(child_context, item, executable.index, self.items)
        logger.debug("✅ Processed map item: %s", executable.index)
        return result


def map_handler(
    items: Sequence[T],
    func: Callable,
    config: MapConfig | None,
    execution_state: ExecutionState,
    map_context: DurableContext,
    operation_identifier: OperationIdentifier,
) -> BatchResult[R]:
    """Execute a callable for each item in parallel."""
    # Summary Generator Construction (matches TypeScript implementation):
    # Construct the summary generator at the handler level, just like TypeScript does in map-handler.ts.
    # This matches the pattern where handlers are responsible for configuring operation-specific behavior.
    #
    # See TypeScript reference: aws-durable-execution-sdk-js/src/handlers/map-handler/map-handler.ts (~line 79)

    executor: MapExecutor[T, R] = MapExecutor.from_items(
        items=items,
        func=func,
        config=config or MapConfig(summary_generator=MapSummaryGenerator()),
    )

    checkpoint: CheckpointedResult = execution_state.get_checkpoint_result(
        operation_identifier.operation_id
    )
    if checkpoint.is_succeeded():
        # if we've reached this point, then not only is the step succeeded, but it is also `replay_children`.
        return executor.replay(execution_state, map_context)
    # we are making it explicit that we are now executing within the map_context
    return executor.execute(execution_state, executor_context=map_context)


class MapSummaryGenerator:
    def __call__(self, result: BatchResult) -> str:
        fields = {
            "totalCount": result.total_count,
            "successCount": result.success_count,
            "failureCount": result.failure_count,
            "completionReason": result.completion_reason.value,
            "status": result.status.value,
            "type": "MapResult",
        }
        return json.dumps(fields)
