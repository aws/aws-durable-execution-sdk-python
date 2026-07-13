"""Implementation for Durable Map operation."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Generic, TypeVar

from aws_durable_execution_sdk_python.concurrency.executor import ConcurrentExecutor
from aws_durable_execution_sdk_python.concurrency.models import (
    BatchResult,
    Executable,
)
from aws_durable_execution_sdk_python.config import MapConfig, NestingType
from aws_durable_execution_sdk_python.lambda_service import OperationSubType


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.identifier import (
        OperationIdentifier,
        OperationIdNamespace,
    )
    from aws_durable_execution_sdk_python.serdes import SerDes
    from aws_durable_execution_sdk_python.state import (
        CheckpointedResult,
        ExecutionState,
    )

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
        operation_id_namespace: OperationIdNamespace,
        item_serdes: SerDes | None = None,
        nesting_type: NestingType = NestingType.NESTED,
    ):
        super().__init__(
            executables=executables,
            max_concurrency=max_concurrency,
            completion_config=completion_config,
            sub_type_top=top_level_sub_type,
            sub_type_iteration=iteration_sub_type,
            name_prefix=name_prefix,
            serdes=serdes,
            operation_id_namespace=operation_id_namespace,
            item_serdes=item_serdes,
            nesting_type=nesting_type,
        )
        self.items = items

    @classmethod
    def from_items(
        cls,
        items: Sequence[T],
        func: Callable,
        config: MapConfig[T],
        operation_id_namespace: OperationIdNamespace,
    ) -> MapExecutor[T, R]:
        """Create MapExecutor from items and a callable."""

        def bind(i: int, item: T) -> Callable:
            def run(child_context: DurableContext) -> R:
                result: R = func(child_context, item, i, items)
                return result

            return run

        executables: list[Executable[Callable]] = [
            Executable(
                index=i,
                func=bind(i, item),
                name=config.item_namer(item, i) if config.item_namer else None,
            )
            for i, item in enumerate(items)
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
            operation_id_namespace=operation_id_namespace,
            item_serdes=config.item_serdes,
            nesting_type=config.nesting_type,
        )


def map_handler(
    items: Sequence[T],
    func: Callable,
    config: MapConfig | None,
    execution_state: ExecutionState,
    map_context: DurableContext,
    operation_identifier: OperationIdentifier,
    operation_id_namespace: OperationIdNamespace,
) -> BatchResult[R]:
    """Execute a callable for each item in parallel."""
    map_config: MapConfig = config or MapConfig()

    executor: MapExecutor[T, R] = MapExecutor.from_items(
        items=items,
        func=func,
        config=map_config,
        operation_id_namespace=operation_id_namespace,
    )

    checkpoint: CheckpointedResult = execution_state.get_checkpoint_result(
        operation_identifier.operation_id
    )
    if checkpoint.is_succeeded():
        # if we've reached this point, then not only is the step succeeded, but it is also `replay_children`.
        return executor.replay(execution_state, map_context, checkpoint)
    # we are making it explicit that we are now executing within the map_context
    return executor.execute(execution_state, executor_context=map_context)
