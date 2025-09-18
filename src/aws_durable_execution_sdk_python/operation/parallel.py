"""Implementation for Durable Parallel operation."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, TypeVar

from aws_durable_execution_sdk_python.concurrency import ConcurrentExecutor, Executable
from aws_durable_execution_sdk_python.config import ParallelConfig
from aws_durable_execution_sdk_python.lambda_service import OperationSubType

if TYPE_CHECKING:
    from aws_durable_execution_sdk_python.concurrency import BatchResult
    from aws_durable_execution_sdk_python.config import ChildConfig
    from aws_durable_execution_sdk_python.serdes import SerDes
    from aws_durable_execution_sdk_python.state import ExecutionState
    from aws_durable_execution_sdk_python.types import DurableContext

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

    @classmethod
    def from_callables(
        cls,
        callables: Sequence[Callable],
        config: ParallelConfig,
    ) -> ParallelExecutor:
        """Create ParallelExecutor from a sequence of callables."""
        executables: list[Executable[Callable]] = [
            Executable(index=i, func=func) for i, func in enumerate(callables)
        ]
        return cls(
            executables=executables,
            max_concurrency=config.max_concurrency,
            completion_config=config.completion_config,
            top_level_sub_type=OperationSubType.PARALLEL,
            iteration_sub_type=OperationSubType.PARALLEL_BRANCH,
            name_prefix="parallel-branch-",
            serdes=config.serdes,
        )

    def execute_item(self, child_context, executable: Executable[Callable]) -> R:
        logger.debug("🔀 Processing parallel branch: %s", executable.index)
        result: R = executable.func(child_context)
        logger.debug("✅ Processed parallel branch: %s", executable.index)
        return result


def parallel_handler(
    callables: Sequence[Callable],
    config: ParallelConfig | None,
    execution_state: ExecutionState,
    run_in_child_context: Callable[
        [Callable[[DurableContext], R], str | None, ChildConfig | None], R
    ],
) -> BatchResult[R]:
    """Execute multiple operations in parallel."""
    executor = ParallelExecutor.from_callables(
        callables, config if config else ParallelConfig()
    )
    return executor.execute(execution_state, run_in_child_context)
