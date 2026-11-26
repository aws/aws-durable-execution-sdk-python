"""default_step_semantics=AT_LEAST_ONCE_ASYNC through the local test runner.

The steps do not wait for their SUCCEED checkpoints. Each invocation still
sends them before it returns, so the history records every step as SUCCEEDED,
including the steps before a wait that suspends the execution.
"""

from __future__ import annotations

import json
from typing import Any

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from aws_durable_execution_sdk_python.config import Duration, StepSemantics
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import (
    OperationStatus,
    OperationType,
)
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner

_calls: list[str] = []


def _step(context: DurableContext, name: str) -> str:
    def run(_step_context: Any) -> str:
        _calls.append(name)
        return name

    return context.step(run, name=name)


@durable_execution(default_step_semantics=StepSemantics.AT_LEAST_ONCE_ASYNC)
def _handler(_event: Any, context: DurableContext) -> list[str]:
    results = [_step(context, "root")]
    results.append(
        context.run_in_child_context(lambda child: _step(child, "child"), name="group")
    )
    results.extend(
        context.map(
            ["a", "b"],
            lambda item_context, item, _index, _items: _step(
                item_context, f"item-{item}"
            ),
            name="items",
        ).get_results()
    )
    context.wait(Duration.from_seconds(1), name="pause")
    results.append(_step(context, "after-wait"))
    return results


def test_async_default_saves_every_step_before_the_invocation_returns() -> None:
    _calls.clear()
    with DurableFunctionTestRunner(handler=_handler) as runner:
        result = runner.run(input="{}", execution_timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED
    assert json.loads(result.result) == [
        "root",
        "child",
        "item-a",
        "item-b",
        "after-wait",
    ]
    # Each step ran once: the replay after the wait read the saved results.
    assert sorted(_calls) == sorted(json.loads(result.result))
    steps = {
        operation.name: operation.status
        for operation in result.get_all_operations()
        if operation.operation_type is OperationType.STEP
    }
    assert steps == dict.fromkeys(
        ["root", "child", "item-a", "item-b", "after-wait"],
        OperationStatus.SUCCEEDED,
    )
