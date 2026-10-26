"""Operation categories stay consistent across public APIs and replay hooks."""

from __future__ import annotations

from tests.test_helpers import plugin_factory


from typing import Any

import pytest

from aws_durable_execution_sdk_python.config import ChildConfig, Duration
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.lambda_service import OperationSubType
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
    OperationEndInfo,
    OperationInfo,
    OperationStartInfo,
    OperationType,
    UserFunctionEndInfo,
    UserFunctionOutcome,
    UserFunctionStartInfo,
)
from aws_durable_execution_sdk_python.waits import (
    WaitForConditionConfig,
    WaitForConditionDecision,
)
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner


class _TypeRecordingPlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        self.invocation_starts: list[InvocationStartInfo] = []
        self.invocation_ends: list[InvocationEndInfo] = []
        self.operation_starts: list[OperationStartInfo] = []
        self.operation_ends: list[OperationEndInfo] = []
        self.user_function_starts: list[UserFunctionStartInfo] = []
        self.user_function_ends: list[UserFunctionEndInfo] = []

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self.invocation_starts.append(info)

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        self.invocation_ends.append(info)

    def on_operation_start(self, info: OperationStartInfo) -> None:
        self.operation_starts.append(info)

    def on_operation_end(self, info: OperationEndInfo) -> None:
        self.operation_ends.append(info)

    def on_user_function_start(self, info: UserFunctionStartInfo) -> None:
        self.user_function_starts.append(info)

    def on_user_function_end(self, info: UserFunctionEndInfo) -> None:
        self.user_function_ends.append(info)


def test_builtin_operation_categories_survive_suspend_and_replay() -> None:
    plugin = _TypeRecordingPlugin()
    step_calls: list[str] = []

    def step(_context: Any) -> str:
        step_calls.append("called")
        return "stepped"

    def handler(_event: Any, context: DurableContext) -> str:
        context.step(step, name="step")
        context.wait_for_condition(
            lambda state, _ctx: state + 1,
            config=WaitForConditionConfig(
                initial_state=0,
                wait_strategy=lambda _state, _attempt: (
                    WaitForConditionDecision.stop_polling()
                ),
            ),
            name="condition",
        )
        context.run_in_child_context(lambda _ctx: "child", name="child")
        context.map([1], lambda _ctx, item, _index, _items: item, name="map")
        context.parallel([lambda _ctx: "branch"], name="parallel")
        context.wait_for_callback(
            lambda callback_id, _ctx: runner.send_callback_success(
                callback_id, b'"callback result"'
            ),
            name="callback",
        )
        context.invoke("echo", {"message": "invoked"}, name="invoke")
        context.wait(Duration.from_seconds(60), name="wait")
        return "done"

    wrapped = durable_execution(handler, plugins=[plugin_factory(plugin)])
    with DurableFunctionTestRunner(handler=wrapped) as runner:
        runner.register_function("echo", lambda event, _context: event)
        result = runner.run(input="{}", execution_timeout=120)

    assert result.result == '"done"'
    assert step_calls == ["called"]
    assert len(plugin.invocation_starts) >= 2
    assert any(
        info.status is InvocationStatus.PENDING for info in plugin.invocation_ends
    )
    assert plugin.invocation_ends[-1].status is InvocationStatus.SUCCEEDED

    expected: dict[str, OperationType] = {
        OperationSubType.STEP: OperationType.STEP,
        OperationSubType.WAIT_FOR_CONDITION: OperationType.STEP,
        OperationSubType.WAIT: OperationType.WAIT,
        OperationSubType.CALLBACK: OperationType.CALLBACK,
        OperationSubType.CHAINED_INVOKE: OperationType.CHAINED_INVOKE,
        OperationSubType.RUN_IN_CHILD_CONTEXT: OperationType.CONTEXT,
        OperationSubType.WAIT_FOR_CALLBACK: OperationType.CONTEXT,
        OperationSubType.MAP: OperationType.CONTEXT,
        OperationSubType.MAP_ITERATION: OperationType.CONTEXT,
        OperationSubType.PARALLEL: OperationType.CONTEXT,
        OperationSubType.PARALLEL_BRANCH: OperationType.CONTEXT,
    }
    assert {info.sub_type for info in plugin.operation_starts} == set(expected)
    assert {info.sub_type for info in plugin.operation_ends} == set(expected)

    # The final replay starts with every earlier operation checkpointed,
    # including the execution root, whose service record has no subtype.
    replay_operations = plugin.invocation_starts[-1].operations.values()
    assert {info.sub_type for info in replay_operations} == {*expected, None}
    all_infos: list[OperationInfo] = [
        *plugin.operation_starts,
        *plugin.operation_ends,
        *plugin.user_function_starts,
        *plugin.user_function_ends,
        *replay_operations,
        *plugin.invocation_ends[-1].operations.values(),
    ]
    for info in all_infos:
        if info.sub_type is None:
            assert info.operation_type is OperationType.EXECUTION
        else:
            assert info.operation_type is expected[info.sub_type], info


@pytest.mark.parametrize("is_virtual", [False, True], ids=["checkpointed", "virtual"])
def test_child_category_is_independent_of_subtype_on_resume(is_virtual: bool) -> None:
    plugin = _TypeRecordingPlugin()
    child_calls: list[str] = []

    def child(context: DurableContext) -> str:
        child_calls.append("entered")
        context.wait(Duration.from_seconds(60), name="inside-child")
        return "child result"

    def handler(_event: Any, context: DurableContext) -> str:
        return context.run_in_child_context(
            child,
            name="custom-subtype-child",
            config=ChildConfig(sub_type="OrderSaga", is_virtual=is_virtual),
        )

    wrapped = durable_execution(handler, plugins=[plugin_factory(plugin)])
    with DurableFunctionTestRunner(handler=wrapped) as runner:
        result = runner.run(input="{}", execution_timeout=120)

    assert result.result == '"child result"'
    assert len(child_calls) >= 2
    assert plugin.invocation_ends[-1].status is InvocationStatus.SUCCEEDED

    starts = [
        info
        for info in plugin.user_function_starts
        if info.name == "custom-subtype-child"
    ]
    ends = [
        info
        for info in plugin.user_function_ends
        if info.name == "custom-subtype-child"
    ]
    completed = [
        info for info in plugin.operation_ends if info.name == "custom-subtype-child"
    ]
    assert len(starts) == len(child_calls)
    assert len(ends) == len(starts)
    assert any(info.outcome is UserFunctionOutcome.INCOMPLETE for info in ends)
    assert ends[-1].outcome is UserFunctionOutcome.SUCCEEDED
    assert completed
    for info in [*starts, *ends, *completed]:
        assert info.operation_type is OperationType.CONTEXT
        assert info.sub_type == "OrderSaga"
        assert info.operation_id == starts[0].operation_id

    snapshots = [
        info
        for invocation in [*plugin.invocation_starts, *plugin.invocation_ends]
        for info in invocation.operations.values()
        if info.name == "custom-subtype-child"
    ]
    if is_virtual:
        assert snapshots == []
    else:
        assert snapshots
        for info in snapshots:
            assert info.operation_type is OperationType.CONTEXT
            assert info.sub_type == "OrderSaga"
