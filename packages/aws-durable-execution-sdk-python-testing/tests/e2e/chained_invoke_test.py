"""End-to-end tests for chained invoke through the in-process runner."""

import json
import time
from typing import Any

from aws_durable_execution_sdk_python.config import Duration, InvokeConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.exceptions import InvokeError
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.lambda_service import (
    ErrorObject,
    OperationStatus,
)

from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)


@durable_execution
def child_doubler(event: Any, context: DurableContext) -> int:
    return event["n"] * 2


@durable_execution
def child_failer(event: Any, context: DurableContext) -> int:
    msg: str = "child exploded"
    raise ValueError(msg)


def plain_adder(event: Any, context: Any) -> dict:
    return {"total": event["a"] + event["b"]}


def test_invoke_durable_child_returns_result() -> None:
    @durable_execution
    def parent(event: Any, context: DurableContext) -> int:
        doubled: int = context.invoke("child-doubler", {"n": 21}, name="double")
        return doubled + 1

    with DurableFunctionTestRunner(handler=parent) as runner:
        runner.register_durable_function("child-doubler", child_doubler)
        result: DurableFunctionTestResult = runner.run(input=json.dumps({}))

    assert result.result == "43"
    invoke_op = result.get_invoke("double")
    assert invoke_op.status == OperationStatus.SUCCEEDED
    assert invoke_op.result == "42"


def test_invoke_non_durable_child_returns_result() -> None:
    @durable_execution
    def parent(event: Any, context: DurableContext) -> int:
        summed: dict = context.invoke("adder", {"a": 2, "b": 3}, name="add")
        return summed["total"]

    with DurableFunctionTestRunner(handler=parent) as runner:
        runner.register_function("adder", plain_adder)
        result = runner.run(input=json.dumps({}))

    assert result.result == "5"
    assert result.get_invoke("add").status == OperationStatus.SUCCEEDED


def test_invoke_failing_durable_child_raises_in_parent() -> None:
    @durable_execution
    def parent(event: Any, context: DurableContext) -> str:
        try:
            context.invoke("child-failer", {}, name="fail-target")
        except InvokeError as err:
            return f"caught:{err.message}"
        return "not-reached"

    with DurableFunctionTestRunner(handler=parent) as runner:
        runner.register_durable_function("child-failer", child_failer)
        result = runner.run(input=json.dumps({}))

    assert "caught:" in (result.result or "")
    assert result.get_invoke("fail-target").status == OperationStatus.FAILED


def test_invoke_unknown_function_fails_to_start() -> None:
    """The service resolves a target before scheduling it and returns a
    target it cannot resolve as FAILED in the checkpoint response, so the
    handler raises without suspending. The runner does the same: the
    parent finishes in its first invocation, and the history carries the
    started and failed events of the operation."""
    invocations: list[int] = []

    @durable_execution
    def parent(event: Any, context: DurableContext) -> str:
        invocations.append(1)
        try:
            context.invoke(
                "nowhere-to-be-found", {"secret": "retain-me"}, name="missing"
            )
        except InvokeError as err:
            return f"type:{err.error_type}"
        return "not-reached"

    with DurableFunctionTestRunner(handler=parent) as runner:
        parent_arn: str = runner.run_async(input=json.dumps({}))
        result = runner.wait_for_result(execution_arn=parent_arn, timeout=60)
        history = runner.get_execution_history(parent_arn, include_execution_data=True)

    # An unknown target surfaces Lambda's error code for a missing function.
    assert result.result == '"type:ResourceNotFoundException"'
    assert result.get_invoke("missing").status == OperationStatus.FAILED
    assert len(invocations) == 1
    chained_events = [
        e.event_type for e in history.events if e.event_type.startswith("ChainedInvoke")
    ]
    assert chained_events == ["ChainedInvokeStarted", "ChainedInvokeFailed"]
    # The service keeps no input for a target it cannot resolve: the
    # started event names the function and carries no Input.
    started = next(e for e in history.events if e.event_type == "ChainedInvokeStarted")
    assert started.chained_invoke_started_details is not None
    assert started.chained_invoke_started_details.function_name == "nowhere-to-be-found"
    assert started.chained_invoke_started_details.input is None


def test_invoke_stopped_child_surfaces_stop_error() -> None:
    @durable_execution
    def waiting_child(event: Any, context: DurableContext) -> str:
        context.wait(Duration.from_hours(1), name="hold")
        return "never"

    @durable_execution
    def parent(event: Any, context: DurableContext) -> str:
        try:
            context.invoke("waiting-child", {}, name="held")
        except InvokeError as err:
            return f"{err.error_type}:{err.message}"
        return "not-reached"

    with DurableFunctionTestRunner(handler=parent, skip_time=False) as runner:
        runner.register_durable_function("waiting-child", waiting_child)
        parent_arn: str = runner.run_async(input=json.dumps({}))

        # Find the child through the parent's started event, then stop it
        # with a caller-supplied error.
        child_arn: str | None = None
        deadline = time.monotonic() + 30
        while child_arn is None and time.monotonic() < deadline:
            history = runner.get_execution_history(parent_arn)
            for event in history.events:
                details = event.chained_invoke_started_details
                if details is not None and details.durable_execution_arn:
                    child_arn = details.durable_execution_arn
            if child_arn is None:
                time.sleep(0.05)
        assert child_arn

        stop_error = ErrorObject(
            message="operator stopped the child",
            type="OperatorStop",
            data=None,
            stack_trace=None,
        )
        runner._executor.stop_execution(child_arn, stop_error)  # noqa: SLF001
        result = runner.wait_for_result(execution_arn=parent_arn, timeout=30)

    # The parent operation is STOPPED and carries the child's own stop error.
    assert result.result == '"OperatorStop:operator stopped the child"'
    held = result.get_invoke("held")
    assert held.status == OperationStatus.STOPPED
    assert held.error is not None
    assert held.error.type == "OperatorStop"
    assert held.error.message == "operator stopped the child"


def test_invoke_fan_out_multiple_children() -> None:
    @durable_execution
    def parent(event: Any, context: DurableContext) -> int:
        first: int = context.invoke("child-doubler", {"n": 1}, name="one")
        second: int = context.invoke("child-doubler", {"n": 2}, name="two")
        third: dict = context.invoke("adder", {"a": first, "b": second}, name="three")
        return third["total"]

    with DurableFunctionTestRunner(handler=parent) as runner:
        runner.register_durable_function("child-doubler", child_doubler)
        runner.register_function("adder", plain_adder)
        result = runner.run(input=json.dumps({}))

    assert result.result == "6"
    for name in ("one", "two", "three"):
        assert result.get_invoke(name).status == OperationStatus.SUCCEEDED


def test_invoke_nested_child_invokes_grandchild() -> None:
    @durable_execution
    def middle(event: Any, context: DurableContext) -> int:
        doubled: int = context.invoke("child-doubler", {"n": event["n"]}, name="inner")
        return doubled + 100

    @durable_execution
    def parent(event: Any, context: DurableContext) -> int:
        return context.invoke("middle", {"n": 5}, name="outer")

    with DurableFunctionTestRunner(handler=parent) as runner:
        runner.register_durable_function("middle", middle)
        runner.register_durable_function("child-doubler", child_doubler)
        result = runner.run(input=json.dumps({}))

    assert result.result == "110"


def test_invoke_history_events_and_child_history() -> None:
    @durable_execution
    def parent(event: Any, context: DurableContext) -> int:
        return context.invoke("child-doubler", {"n": 4}, name="double")

    with DurableFunctionTestRunner(handler=parent) as runner:
        runner.register_durable_function("child-doubler", child_doubler)
        parent_arn: str = runner.run_async(input=json.dumps({}))
        result = runner.wait_for_result(execution_arn=parent_arn, timeout=60)

        parent_history = runner.get_execution_history(
            parent_arn, include_execution_data=True
        )
        started_events = [
            e for e in parent_history.events if e.event_type == "ChainedInvokeStarted"
        ]
        assert len(started_events) == 1
        details = started_events[0].chained_invoke_started_details
        assert details is not None
        assert details.function_name == "child-doubler"
        assert details.input is not None
        assert json.loads(details.input.payload) == {"n": 4}
        child_arn: str | None = details.durable_execution_arn
        assert child_arn

        succeeded_events = [
            e for e in parent_history.events if e.event_type == "ChainedInvokeSucceeded"
        ]
        assert len(succeeded_events) == 1

        # The child is a first-class execution with its own history.
        child_history = runner.get_execution_history(
            child_arn, include_execution_data=True
        )
        child_event_types = [e.event_type for e in child_history.events]
        assert "ExecutionStarted" in child_event_types
        assert "ExecutionSucceeded" in child_event_types

        # Without execution data the started event redacts the payload.
        redacted_history = runner.get_execution_history(
            parent_arn, include_execution_data=False
        )
        redacted_started = next(
            e for e in redacted_history.events if e.event_type == "ChainedInvokeStarted"
        )
        assert redacted_started.chained_invoke_started_details is not None
        redacted_input = redacted_started.chained_invoke_started_details.input
        assert redacted_input is not None
        assert redacted_input.payload is None
        assert redacted_input.truncated is True

    assert result.result == "8"


def test_invoke_durable_child_with_wait_under_skip_time() -> None:
    @durable_execution
    def slow_child(event: Any, context: DurableContext) -> str:
        context.wait(Duration.from_hours(2), name="nap")
        return "rested"

    @durable_execution
    def parent(event: Any, context: DurableContext) -> str:
        return context.invoke("slow-child", {}, name="patient")

    with DurableFunctionTestRunner(handler=parent) as runner:
        runner.register_durable_function("slow-child", slow_child)
        result = runner.run(input=json.dumps({}), execution_timeout=60)

    assert result.result == '"rested"'


def test_invoke_child_timeout_maps_to_timed_out() -> None:
    @durable_execution
    def hanging_child(event: Any, context: DurableContext) -> str:
        time.sleep(2)
        return "never"

    @durable_execution
    def parent(event: Any, context: DurableContext) -> str:
        try:
            context.invoke("hanging-child", {}, name="hang")
        except InvokeError as err:
            return f"type:{err.error_type}"
        return "not-reached"

    with DurableFunctionTestRunner(handler=parent, skip_time=False) as runner:
        runner.register_durable_function(
            "hanging-child", hanging_child, execution_timeout=1
        )
        result = runner.run(input=json.dumps({}), execution_timeout=30)

    assert result.result == '"type:ChainedInvoke.Timeout"'
    assert result.get_invoke("hang").status == OperationStatus.TIMED_OUT


def test_invoke_child_timeout_message_carries_the_child_execution_timeout() -> None:
    @durable_execution
    def hanging_child(event: Any, context: DurableContext) -> str:
        time.sleep(2)
        return "never"

    @durable_execution
    def parent(event: Any, context: DurableContext) -> str:
        try:
            context.invoke("hanging-child", {}, name="hang")
        except InvokeError as err:
            return str(err)
        return "not-reached"

    with DurableFunctionTestRunner(handler=parent, skip_time=False) as runner:
        runner.register_durable_function(
            "hanging-child", hanging_child, execution_timeout=1
        )
        result = runner.run(input=json.dumps({}), execution_timeout=30)

    operation = result.get_invoke("hang")
    assert operation.status == OperationStatus.TIMED_OUT
    assert operation.error is not None
    assert operation.error.type == "ChainedInvoke.Timeout"
    assert operation.error.message == "CHAINED_INVOKE timed out after 1 seconds."


def test_invoke_plain_target_exceeding_invocation_timeout_fails_as_lambda_does() -> (
    None
):
    """A non-durable target running past the invocation timeout fails with
    Lambda's function-timeout error, as the service reports it."""

    def slow_adder(event: Any, context: Any) -> dict:
        time.sleep(3)
        return {"total": 0}

    @durable_execution
    def parent(event: Any, context: DurableContext) -> str:
        try:
            context.invoke("slow-adder", {}, name="slow")
        except InvokeError as err:
            return f"type:{err.error_type}"
        return "not-reached"

    with DurableFunctionTestRunner(
        handler=parent, skip_time=False, invocation_timeout=1
    ) as runner:
        runner.register_function("slow-adder", slow_adder)
        result = runner.run(input=json.dumps({}), execution_timeout=30)

    assert result.result == '"type:Sandbox.Timedout"'
    operation = result.get_invoke("slow")
    assert operation.status == OperationStatus.FAILED
    assert operation.error is not None
    assert operation.error.message is not None
    assert operation.error.message.endswith(" Task timed out after 1.00 seconds")


def test_invoke_target_given_as_arn_resolves_the_registered_name() -> None:
    @durable_execution
    def parent(event: Any, context: DurableContext) -> int:
        return context.invoke(
            "arn:aws:lambda:us-west-2:123456789012:function:child-doubler",
            {"n": 4},
            name="double",
        )

    with DurableFunctionTestRunner(handler=parent) as runner:
        runner.register_durable_function("child-doubler", child_doubler)
        result = runner.run(input=json.dumps({}))

    assert result.result == "8"
    assert result.get_invoke("double").status == OperationStatus.SUCCEEDED


def test_invoke_target_in_another_account_fails_the_checkpoint() -> None:
    @durable_execution
    def parent(event: Any, context: DurableContext) -> str:
        context.invoke(
            "arn:aws:lambda:us-west-2:999999999999:function:child-doubler",
            {"n": 4},
            name="double",
        )
        return "not-reached"

    with DurableFunctionTestRunner(handler=parent) as runner:
        runner.register_durable_function("child-doubler", child_doubler)
        result = runner.run(input=json.dumps({}))

    assert result.error is not None
    assert "Cannot start a CHAINED_INVOKE on a function in another account." in (
        result.error.message or ""
    )


def test_invoke_input_over_one_mebibyte_fails_the_checkpoint() -> None:
    @durable_execution
    def parent(event: Any, context: DurableContext) -> str:
        context.invoke("child-doubler", {"blob": "x" * 1_048_577}, name="big")
        return "not-reached"

    with DurableFunctionTestRunner(handler=parent) as runner:
        runner.register_durable_function("child-doubler", child_doubler)
        result = runner.run(input=json.dumps({}))

    assert result.error is not None
    assert (
        "CHAINED_INVOKE input payload size must be less than or equal to "
        "1048576 bytes." in (result.error.message or "")
    )


def test_invoke_durable_child_result_over_one_mebibyte_fails_the_child() -> None:
    @durable_execution
    def big_child(event: Any, context: DurableContext) -> str:
        return "x" * 1_048_577

    @durable_execution
    def parent(event: Any, context: DurableContext) -> str:
        try:
            context.invoke("big-child", {}, name="big")
        except InvokeError as err:
            return str(err)
        return "not-reached"

    with DurableFunctionTestRunner(handler=parent) as runner:
        runner.register_durable_function("big-child", big_child)
        result = runner.run(input=json.dumps({}))

    operation = result.get_invoke("big")
    assert operation.status == OperationStatus.FAILED
    assert operation.error is not None
    assert (
        "Execution output payload size must be less than or equal to 1048576 bytes."
        in (operation.error.message or "")
    )


def test_invoke_tenant_reaches_durable_and_plain_target_handlers() -> None:
    """A tenant given on the invoke is what the target handlers see."""

    @durable_execution
    def tenant_echo(event: Any, context: DurableContext) -> str | None:
        assert context.lambda_context is not None
        return context.lambda_context.tenant_id

    def plain_tenant_echo(event: Any, context: Any) -> str | None:
        return context.tenant_id

    @durable_execution
    def parent(event: Any, context: DurableContext) -> dict:
        durable_seen: Any = context.invoke(
            "tenant-echo", {}, name="durable", config=InvokeConfig(tenant_id="tenant-a")
        )
        plain_seen: Any = context.invoke(
            "plain-echo", {}, name="plain", config=InvokeConfig(tenant_id="tenant-b")
        )
        return {"durable": durable_seen, "plain": plain_seen}

    with DurableFunctionTestRunner(handler=parent) as runner:
        runner.register_durable_function("tenant-echo", tenant_echo)
        runner.register_function("plain-echo", plain_tenant_echo)
        result = runner.run(input=json.dumps({}))

    assert json.loads(result.result or "{}") == {
        "durable": "tenant-a",
        "plain": "tenant-b",
    }


def test_invoke_region_of_the_runner_is_reported_and_enforced() -> None:
    @durable_execution
    def arn_echo(event: Any, context: DurableContext) -> str:
        assert context.lambda_context is not None
        return str(context.lambda_context.invoked_function_arn)

    @durable_execution
    def parent(event: Any, context: DurableContext) -> str:
        return context.invoke(
            "arn:aws:lambda:eu-west-1:123456789012:function:arn-echo",
            {},
            name="echo",
        )

    with DurableFunctionTestRunner(handler=parent, region="eu-west-1") as runner:
        runner.register_durable_function("arn-echo", arn_echo)
        result = runner.run(input=json.dumps({}))

    assert result.result == '"arn:aws:lambda:eu-west-1:123456789012:function:arn-echo"'


def test_invoke_without_tenant_gives_handlers_no_tenant() -> None:
    @durable_execution
    def tenant_echo(event: Any, context: DurableContext) -> str | None:
        assert context.lambda_context is not None
        return context.lambda_context.tenant_id

    def plain_tenant_echo(event: Any, context: Any) -> str | None:
        return context.tenant_id

    @durable_execution
    def parent(event: Any, context: DurableContext) -> dict:
        assert context.lambda_context is not None
        durable_seen: Any = context.invoke("tenant-echo", {}, name="durable")
        plain_seen: Any = context.invoke("plain-echo", {}, name="plain")
        return {
            "parent": context.lambda_context.tenant_id,
            "durable": durable_seen,
            "plain": plain_seen,
        }

    with DurableFunctionTestRunner(handler=parent) as runner:
        runner.register_durable_function("tenant-echo", tenant_echo)
        runner.register_function("plain-echo", plain_tenant_echo)
        result = runner.run(input=json.dumps({}))
        with_tenant = runner.run(input=json.dumps({}), tenant_id="tenant-p")

    assert json.loads(result.result or "{}") == {
        "parent": None,
        "durable": None,
        "plain": None,
    }
    # A tenant on run() reaches the parent; children invoked without one
    # have none, as their invokes carried none.
    assert json.loads(with_tenant.result or "{}") == {
        "parent": "tenant-p",
        "durable": None,
        "plain": None,
    }


def test_invoke_account_of_the_run_is_reported_to_handlers() -> None:
    @durable_execution
    def arn_echo(event: Any, context: DurableContext) -> str:
        assert context.lambda_context is not None
        return str(context.lambda_context.invoked_function_arn)

    def plain_arn_echo(event: Any, context: Any) -> str:
        return str(context.invoked_function_arn)

    @durable_execution
    def parent(event: Any, context: DurableContext) -> dict:
        durable_seen: Any = context.invoke("arn-echo", {}, name="durable")
        plain_seen: Any = context.invoke("plain-arn-echo", {}, name="plain")
        return {"durable": durable_seen, "plain": plain_seen}

    with DurableFunctionTestRunner(handler=parent) as runner:
        runner.register_durable_function("arn-echo", arn_echo)
        runner.register_function("plain-arn-echo", plain_arn_echo)
        result = runner.run(input=json.dumps({}), account_id="999999999999")

    assert json.loads(result.result or "{}") == {
        "durable": "arn:aws:lambda:us-west-2:999999999999:function:arn-echo",
        "plain": "arn:aws:lambda:us-west-2:999999999999:function:plain-arn-echo",
    }


def test_invoke_qualified_target_runs_the_qualified_registration_if_any() -> None:
    """``child:prod`` runs a handler registered as ``child:prod``; with only
    ``child`` registered, that single handler serves every qualifier."""

    @durable_execution
    def latest(event: Any, context: DurableContext) -> str:
        return "latest"

    @durable_execution
    def prod(event: Any, context: DurableContext) -> str:
        return "prod"

    @durable_execution
    def parent(event: Any, context: DurableContext) -> dict:
        return {
            "prod": context.invoke("child:prod", {}, name="prod"),
            "staging": context.invoke("child:staging", {}, name="staging"),
            "bare": context.invoke("child", {}, name="bare"),
        }

    with DurableFunctionTestRunner(handler=parent) as runner:
        runner.register_durable_function("child", latest)
        runner.register_durable_function("child:prod", prod)
        result = runner.run(input=json.dumps({}))

    assert json.loads(result.result or "{}") == {
        "prod": "prod",
        "staging": "latest",
        "bare": "latest",
    }


def test_plain_target_context_carries_the_requested_qualifier() -> None:
    """A plain target invoked as ``child:prod`` sees a qualified function
    ARN, whether it is registered under the qualified key or the bare name."""

    def target(event: Any, context: Any) -> str:
        return context.invoked_function_arn

    @durable_execution
    def parent(event: Any, context: DurableContext) -> dict:
        return {
            "exact": context.invoke("exact:prod", {}, name="exact"),
            "fallback": context.invoke("bare:prod", {}, name="fallback"),
            "bare": context.invoke("bare", {}, name="bare"),
            "latest": context.invoke("bare:$LATEST", {}, name="latest"),
        }

    with DurableFunctionTestRunner(handler=parent) as runner:
        runner.register_function("exact:prod", target)
        runner.register_function("bare", target)
        result = runner.run(input=json.dumps({}))

    prefix = "arn:aws:lambda:us-west-2:123456789012:function:"
    assert json.loads(result.result or "{}") == {
        "exact": prefix + "exact:prod",
        "fallback": prefix + "bare:prod",
        "bare": prefix + "bare",
        "latest": prefix + "bare:$LATEST",
    }


def test_targets_see_the_function_name_and_version_lambda_would_give() -> None:
    """Lambda fills function_name, function_version and invoked_function_arn
    on every invocation. Plain and durable targets, bare and qualified,
    see the same three values here; an alias reports $LATEST because the
    runner keeps no versions."""

    def identity(context: Any) -> list:
        return [
            context.function_name,
            context.function_version,
            context.invoked_function_arn.rsplit(":function:", 1)[1],
        ]

    def plain(event: Any, context: Any) -> list:
        return identity(context)

    @durable_execution
    def durable(event: Any, context: DurableContext) -> list:
        return identity(context.lambda_context)

    @durable_execution
    def parent(event: Any, context: DurableContext) -> dict:
        return {
            "plain": context.invoke("plain", {}, name="p"),
            "plain_version": context.invoke("plain:3", {}, name="pv"),
            "durable": context.invoke("durable", {}, name="d"),
            "durable_alias": context.invoke("durable:prod", {}, name="da"),
        }

    with DurableFunctionTestRunner(handler=parent) as runner:
        runner.register_function("plain", plain)
        runner.register_durable_function("durable", durable)
        result = runner.run(input=json.dumps({}))

    assert json.loads(result.result or "{}") == {
        "plain": ["plain", "$LATEST", "plain"],
        "plain_version": ["plain", "3", "plain:3"],
        "durable": ["durable", "$LATEST", "durable"],
        "durable_alias": ["durable", "$LATEST", "durable:prod"],
    }
