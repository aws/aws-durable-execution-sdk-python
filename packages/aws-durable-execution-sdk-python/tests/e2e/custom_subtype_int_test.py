"""Custom subtypes on steps, callbacks, and child contexts.

A function that composes several durable operations can label each part with
its own subtype. The subtype reaches the checkpoint, the execution history,
and plugin events, and replay compares it with the checkpoint.
"""

from __future__ import annotations

from typing import Any

from aws_durable_execution_sdk_python.config import (
    CallbackConfig,
    ChildConfig,
    Duration,
    StepConfig,
    WaitForCallbackConfig,
)
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.lambda_service import (
    OperationStatus,
    OperationSubType,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus as PluginInvocationStatus,
    OperationEndInfo,
    OperationStartInfo,
    OperationType,
)
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from tests.test_helpers import plugin_factory


class _RecordingPlugin(DurableInstrumentationPlugin):
    def __init__(self) -> None:
        self.invocation_starts: list[InvocationStartInfo] = []
        self.invocation_ends: list[InvocationEndInfo] = []
        self.operation_starts: list[OperationStartInfo] = []
        self.operation_ends: list[OperationEndInfo] = []

    def on_invocation_start(self, info: InvocationStartInfo) -> None:
        self.invocation_starts.append(info)

    def on_invocation_end(self, info: InvocationEndInfo) -> None:
        self.invocation_ends.append(info)

    def on_operation_start(self, info: OperationStartInfo) -> None:
        self.operation_starts.append(info)

    def on_operation_end(self, info: OperationEndInfo) -> None:
        self.operation_ends.append(info)


def test_custom_subtypes_survive_suspend_and_replay() -> None:
    """Each part of a composed operation records its own subtype.

    The wait inside the child context suspends the execution, so the second
    invocation replays every operation against its checkpoint.
    """
    plugin = _RecordingPlugin()
    step_calls: list[str] = []

    def charge(_step_context: Any) -> str:
        step_calls.append("charged")
        return "receipt"

    def order_saga(child: DurableContext) -> str:
        receipt = child.step(
            charge, name="charge", config=StepConfig(sub_type="OrderCharge")
        )
        callback = child.create_callback(
            name="approval", config=CallbackConfig(sub_type="OrderApproval")
        )
        approval = callback.result()
        child.wait(Duration.from_seconds(60), name="settle")
        return f"{receipt}:{approval}"

    def handler(_event: Any, context: DurableContext) -> str:
        return context.run_in_child_context(
            order_saga, name="saga", config=ChildConfig(sub_type="OrderSaga")
        )

    wrapped = durable_execution(handler, plugins=[plugin_factory(plugin)])
    with DurableFunctionTestRunner(handler=wrapped) as runner:
        arn = runner.run_async(input="{}")
        callback_id = runner.wait_for_callback(arn, name="approval", timeout=60)
        runner.send_callback_success(callback_id, b"approved")
        result = runner.wait_for_result(execution_arn=arn, timeout=120)
        history = runner.get_execution_history(arn)

    assert result.result == '"receipt:approved"'
    assert step_calls == ["charged"]
    assert len(plugin.invocation_starts) >= 2, "the callback must suspend the execution"
    assert plugin.invocation_ends[-1].status is PluginInvocationStatus.SUCCEEDED

    # The testing runner reads the recorded subtype as a plain string.
    saga = result.get_context("saga")
    assert saga.status is OperationStatus.SUCCEEDED
    assert saga.sub_type == "OrderSaga"
    by_name = {operation.name: operation for operation in saga.child_operations}
    assert by_name["charge"].sub_type == "OrderCharge"
    assert by_name["approval"].sub_type == "OrderApproval"
    assert by_name["settle"].sub_type == OperationSubType.WAIT

    # The execution history carries the subtype on every event of the operation.
    def history_sub_types(name: str) -> set[str | None]:
        return {event.sub_type for event in history.events if event.name == name}

    assert history_sub_types("saga") == {"OrderSaga"}
    assert history_sub_types("charge") == {"OrderCharge"}
    assert history_sub_types("approval") == {"OrderApproval"}

    # Plugin events report the subtype, with the operation type unchanged.
    expected = {
        "saga": ("OrderSaga", OperationType.CONTEXT),
        "charge": ("OrderCharge", OperationType.STEP),
        "approval": ("OrderApproval", OperationType.CALLBACK),
        "settle": (OperationSubType.WAIT, OperationType.WAIT),
    }
    for info in [*plugin.operation_starts, *plugin.operation_ends]:
        assert info.name is not None
        sub_type, operation_type = expected[info.name]
        assert info.sub_type == sub_type, info
        assert info.operation_type is operation_type, info

    # The replaying invocation receives every recorded subtype as a plain string.
    replayed = plugin.invocation_starts[-1].operations.values()
    replayed_by_name = {info.name: info for info in replayed if info.name}
    assert replayed_by_name["saga"].sub_type == "OrderSaga"
    assert replayed_by_name["charge"].sub_type == "OrderCharge"
    assert replayed_by_name["approval"].sub_type == "OrderApproval"
    assert replayed_by_name["settle"].sub_type == OperationSubType.WAIT
    assert type(replayed_by_name["saga"].sub_type) is str


def test_wait_for_callback_subtype_labels_the_inner_callback() -> None:
    """The callback inside wait_for_callback takes the configured subtype.

    The surrounding context keeps the subtype "WaitForCallback", and the
    submitter step keeps "Step".
    """

    def handler(_event: Any, context: DurableContext) -> str:
        return context.wait_for_callback(
            lambda callback_id, _ctx: runner.send_callback_success(
                callback_id, b"approved"
            ),
            name="approval",
            config=WaitForCallbackConfig(sub_type="OrderApproval"),
        )

    wrapped = durable_execution(handler)
    with DurableFunctionTestRunner(handler=wrapped) as runner:
        result = runner.run(input="{}", execution_timeout=120)

    assert result.result == '"approved"'
    context_operation = result.get_context("approval")
    assert context_operation.sub_type == OperationSubType.WAIT_FOR_CALLBACK
    by_name = {op.name: op for op in context_operation.child_operations}
    assert by_name["approval create callback id"].sub_type == "OrderApproval"
    assert by_name["approval submitter"].sub_type == OperationSubType.STEP
