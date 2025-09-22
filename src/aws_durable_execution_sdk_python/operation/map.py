"""Implementation for Durable Map operation."""

from __future__ import annotations

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
    from aws_durable_execution_sdk_python.config import ChildConfig
    from aws_durable_execution_sdk_python.serdes import SerDes
    from aws_durable_execution_sdk_python.state import ExecutionState
    from aws_durable_execution_sdk_python.types import DurableContext


logger = logging.getLogger(__name__)

# Input item type
T = TypeVar("T")
# Result type
R = TypeVar("R")


class MapExecutor(Generic[T, R], ConcurrentExecutor[Callable, R]):
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
    ):
        super().__init__(
            executables=executables,
            max_concurrency=max_concurrency,
            completion_config=completion_config,
            sub_type_top=top_level_sub_type,
            sub_type_iteration=iteration_sub_type,
            name_prefix=name_prefix,
            serdes=serdes,
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
    run_in_child_context: Callable[
        [Callable[[DurableContext], R], str | None, ChildConfig | None], R
    ],
) -> BatchResult[R]:
    """Execute a callable for each item in parallel."""
    executor: MapExecutor[T, R] = MapExecutor.from_items(
        items=items, func=func, config=config if config else MapConfig()
    )
    return executor.execute(execution_state, run_in_child_context)
