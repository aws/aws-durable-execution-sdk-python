"""Functional tests, covering end-to-end DurableTestRunner."""

import json
from typing import Any

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
from aws_durable_execution_sdk_python.config import Duration


# brazil-test-exec pytest test/runner_int_test.py
def test_basic_durable_function() -> None:
    @durable_step
    def one(step_context: StepContext, a: int, b: int) -> str:
        # print("[DEBUG] one called")
        return f"{a} {b}"

    @durable_step
    def two_1(step_context: StepContext, a: int, b: int) -> str:
        # print("[DEBUG] two_1 called")
        return f"{a} {b}"

    @durable_step
    def two_2(step_context: StepContext, a: int, b: int) -> str:
        # print("[DEBUG] two_2 called")
        return f"{b} {a}"

    @durable_with_child_context
    def two(ctx: DurableContext, a: int, b: int) -> str:
        # print("[DEBUG] two called")
        two_1_result: str = ctx.step(two_1(a, b))
        two_2_result: str = ctx.step(two_2(a, b))
        return f"{two_1_result} {two_2_result}"

    @durable_step
    def three(step_context: StepContext, a: int, b: int) -> str:
        # print("[DEBUG] three called")
        return f"{a} {b}"

    @durable_execution
    def function_under_test(event: Any, context: DurableContext) -> list[str]:
        results: list[str] = []

        result_one: str = context.step(one(1, 2))
        results.append(result_one)

        context.wait(Duration.from_seconds(1))

        result_two: str = context.run_in_child_context(two(3, 4))
        results.append(result_two)

        result_three: str = context.step(three(5, 6))
        results.append(result_three)

        return results

    with DurableFunctionTestRunner(handler=function_under_test) as runner:
        result: DurableFunctionTestResult = runner.run(input="input str", timeout=10)

    assert result.status is InvocationStatus.SUCCEEDED
    assert result.result == json.dumps(["1 2", "3 4 4 3", "5 6"])

    one_result: StepOperation = result.get_step("one")
    assert one_result.result == json.dumps("1 2")

    two_result: ContextOperation = result.get_context("two")
    assert two_result.result == json.dumps("3 4 4 3")

    three_result: StepOperation = result.get_step("three")
    assert three_result.result == json.dumps("5 6")

    # currently has the optimization where it's not saving child checkpoints after parent done
    # prob should unpick that for test
    # two_one_op = cast(StepOperation, two_result_op.get_operation_by_name("two_1"))
    # assert two_one_op.result == '"3 4"'

    # print("done")
