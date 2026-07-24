"""Implementation for Durable Parallel operation."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, TypeVar

from aws_durable_execution_sdk_python.concurrency.executor import ConcurrentExecutor
from aws_durable_execution_sdk_python.concurrency.models import Executable
from aws_durable_execution_sdk_python.config import (
    NestingType,
    ParallelBranch,
    ParallelConfig,
)
from aws_durable_execution_sdk_python.lambda_service import OperationSubType


if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.concurrency.models import BatchResult
    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.identifier import (
        OperationIdentifier,
        OperationIdNamespace,
    )
    from aws_durable_execution_sdk_python.serdes import SerDes
    from aws_durable_execution_sdk_python.state import ExecutionState

logger = logging.getLogger(__name__)

# Result type
R = TypeVar("R")


class ParallelExecutor(ConcurrentExecutor[Callable, R]):
    def __init__(
        self,
        executables: list[Executable[Callable]],
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

    @classmethod
    def from_callables(
        cls,
        callables: Sequence[Callable | ParallelBranch],
        config: ParallelConfig,
        operation_id_namespace: OperationIdNamespace,
    ) -> ParallelExecutor:
        """Create ParallelExecutor from a sequence of callables or ParallelBranch instances.

        Since ParallelBranch is callable, it is stored directly as the func in
        each Executable, with its name bound when provided.
        """
        executables: list[Executable[Callable]] = [
            Executable(
                index=i,
                func=func,
                name=func.name if isinstance(func, ParallelBranch) else None,
            )
            for i, func in enumerate(callables)
        ]

        return cls(
            executables=executables,
            max_concurrency=config.max_concurrency,
            completion_config=config.completion_config,
            top_level_sub_type=OperationSubType.PARALLEL,
            iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
            name_prefix="parallel-branch-",
            serdes=config.serdes,
            operation_id_namespace=operation_id_namespace,
            item_serdes=config.item_serdes,
            nesting_type=config.nesting_type,
        )


def parallel_handler(
    callables: Sequence[Callable | ParallelBranch],
    config: ParallelConfig | None,
    execution_state: ExecutionState,
    parallel_context: DurableContext,
    operation_identifier: OperationIdentifier,
    operation_id_namespace: OperationIdNamespace,
) -> BatchResult[R]:
    """Execute multiple operations in parallel."""
    parallel_config: ParallelConfig = config or ParallelConfig()

    executor = ParallelExecutor.from_callables(
        callables,
        parallel_config,
        operation_id_namespace=operation_id_namespace,
    )

    checkpoint = execution_state.get_checkpoint_result(
        operation_identifier.operation_id
    )
    if checkpoint.is_succeeded():
        return executor.replay(execution_state, parallel_context, checkpoint)
    return executor.execute(execution_state, executor_context=parallel_context)
