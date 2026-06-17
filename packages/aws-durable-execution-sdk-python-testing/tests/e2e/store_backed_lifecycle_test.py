"""End-to-end lifecycle on each store backend.

The in-memory store returns the same object on every load, so an in
place mutation of a loaded execution survives even with no save. The
filesystem and sqlite stores deserialize a fresh object on every load,
so any mutation the lifecycle fails to persist is lost on the next load.

Every other test runs on the in-memory store, which hides that class of
bug. This test drives one full durable execution through the executor,
the worker, and the checkpoint path on each store backend. The
execution uses steps, a wait, a child context, and completion, so it
exercises several checkpoints and the completion effect. A mutation that
is not saved would change the result or stall the run on a disk store.
"""

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import (
    InvocationStatus,
    durable_execution,
)
from aws_durable_execution_sdk_python.types import StepContext

from aws_durable_execution_sdk_python_testing.runner import (
    ContextOperation,
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
    StepOperation,
)
from aws_durable_execution_sdk_python_testing.stores.base import ExecutionStore
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)
from aws_durable_execution_sdk_python_testing.stores.sqlite import (
    SQLiteExecutionStore,
)


StoreFactory = Callable[[Path], ExecutionStore]


def _memory_store(_tmp: Path) -> ExecutionStore:
    return InMemoryExecutionStore()


def _filesystem_store(tmp: Path) -> ExecutionStore:
    return FileSystemExecutionStore.create(tmp / "fs-store")


def _sqlite_store(tmp: Path) -> ExecutionStore:
    return SQLiteExecutionStore.create_and_initialize(str(tmp / "exec.db"))


@pytest.mark.parametrize(
    "store_factory",
    [_memory_store, _filesystem_store, _sqlite_store],
    ids=["memory", "filesystem", "sqlite"],
)
def test_full_lifecycle_persists_on_each_store(
    store_factory: StoreFactory, tmp_path: Path
) -> None:
    @durable_step
    def one(step_context: StepContext, a: int, b: int) -> str:
        return f"{a} {b}"

    @durable_step
    def two_1(step_context: StepContext, a: int, b: int) -> str:
        return f"{a} {b}"

    @durable_step
    def two_2(step_context: StepContext, a: int, b: int) -> str:
        return f"{b} {a}"

    @durable_with_child_context
    def two(ctx: DurableContext, a: int, b: int) -> str:
        two_1_result: str = ctx.step(two_1(a, b))
        two_2_result: str = ctx.step(two_2(a, b))
        return f"{two_1_result} {two_2_result}"

    @durable_step
    def three(step_context: StepContext, a: int, b: int) -> str:
        return f"{a} {b}"

    @durable_execution
    def function_under_test(event: Any, context: DurableContext) -> list[str]:
        results: list[str] = []
        results.append(context.step(one(1, 2)))
        context.wait(Duration.from_seconds(1))
        results.append(context.run_in_child_context(two(3, 4)))
        results.append(context.step(three(5, 6)))
        return results

    store: ExecutionStore = store_factory(tmp_path)
    with DurableFunctionTestRunner(
        handler=function_under_test, store=store, execution_timeout=10
    ) as runner:
        result: DurableFunctionTestResult = runner.run(input="input str")

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == json.dumps(["1 2", "3 4 4 3", "5 6"])

    one_op: StepOperation = result.get_step("one")
    assert one_op.result == json.dumps("1 2")

    two_op: ContextOperation = result.get_context("two")
    assert two_op.result == json.dumps("3 4 4 3")

    three_op: StepOperation = result.get_step("three")
    assert three_op.result == json.dumps("5 6")
