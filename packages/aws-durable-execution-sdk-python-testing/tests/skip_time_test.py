"""End-to-end tests for skip-time behaviour on the local runner."""

from __future__ import annotations

from typing import Any

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

from aws_durable_execution_sdk_python_testing.clock import RealClock, SkipClock
from aws_durable_execution_sdk_python_testing.runner import (
    DurableChildContextTestRunner,
    DurableFunctionTestRunner,
)


_MODELED_WAIT_SECONDS = 300

_MODELED_MIDDLE_WAIT_SECONDS = 60


@durable_execution
def _waits_then_returns(event: Any, context: DurableContext) -> str:  # noqa: ARG001
    context.wait(Duration.from_seconds(_MODELED_WAIT_SECONDS))
    return "done"


def test_skip_time_completes_long_wait_and_keeps_faithful_history() -> None:
    # skip_time defaults to True. A modeled 300s wait must complete well
    # within the 30s execution timeout, which is only possible if the
    # runner does not spend the modeled duration in wall-clock time.
    with DurableFunctionTestRunner(handler=_waits_then_returns) as runner:
        result = runner.run(input="x", execution_timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED

    wait_ops = [op for op in result.operations if op.operation_type.value == "WAIT"]
    assert len(wait_ops) == 1
    wait_op = wait_ops[0]

    assert wait_op.status.value == "SUCCEEDED"
    assert wait_op.start_timestamp is not None
    assert wait_op.scheduled_end_timestamp is not None

    # History records the real modeled duration, not the skipped
    # wall-clock time.
    modeled = (
        wait_op.scheduled_end_timestamp - wait_op.start_timestamp
    ).total_seconds()
    assert abs(modeled - _MODELED_WAIT_SECONDS) < 1.0


@durable_step
def _step_a(step_context: StepContext) -> str:  # noqa: ARG001
    return "a"


@durable_step
def _step_b(step_context: StepContext) -> str:  # noqa: ARG001
    return "b"


@durable_execution
def _step_wait_step(event: Any, context: DurableContext) -> str:  # noqa: ARG001
    context.step(_step_a())
    context.wait(Duration.from_seconds(_MODELED_MIDDLE_WAIT_SECONDS))
    context.step(_step_b())
    return "done"


def test_skip_time_keeps_history_monotonic_across_wait() -> None:
    # stepA -> wait(60) -> stepB. Under skip, the clock is the single
    # source of "now" for stamping, so the operation after the wait must
    # start no earlier than the wait's modeled end. The history stays
    # monotonic even though it is future-dated relative to wall-clock.
    with DurableFunctionTestRunner(handler=_step_wait_step) as runner:
        result = runner.run(input="x", execution_timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED

    step_a = result.get_step("_step_a")
    step_b = result.get_step("_step_b")
    wait_ops = [op for op in result.operations if op.operation_type.value == "WAIT"]
    assert len(wait_ops) == 1
    wait_op = wait_ops[0]

    assert wait_op.start_timestamp is not None
    assert wait_op.scheduled_end_timestamp is not None
    assert step_a.start_timestamp is not None
    assert step_b.start_timestamp is not None

    # Faithful: the modeled wait duration is recorded exactly.
    modeled = (
        wait_op.scheduled_end_timestamp - wait_op.start_timestamp
    ).total_seconds()
    assert abs(modeled - _MODELED_MIDDLE_WAIT_SECONDS) < 1.0

    # Monotonic: the post-wait step cannot start before the wait's
    # modeled end (the inversion the clock fix removes).
    assert step_b.start_timestamp >= wait_op.scheduled_end_timestamp
    # And the pre-wait step starts no later than the wait.
    assert step_a.start_timestamp <= wait_op.start_timestamp


@durable_with_child_context
def _child_waits(child_ctx: DurableContext) -> str:
    child_ctx.wait(Duration.from_seconds(_MODELED_WAIT_SECONDS), name="child-wait")
    return "child-done"


def test_child_context_runner_forwards_skip_time() -> None:
    child_runner = DurableChildContextTestRunner(_child_waits)  # type: ignore[arg-type]
    real_runner = DurableChildContextTestRunner(
        _child_waits,  # type: ignore[arg-type]
        skip_time=False,
    )

    assert isinstance(child_runner._clock, SkipClock)  # noqa: SLF001
    assert isinstance(real_runner._clock, RealClock)  # noqa: SLF001

    child_runner.close()
    real_runner.close()


def test_child_context_runner_skips_modeled_wait() -> None:
    # A modeled 300s wait inside a child context must complete well within
    # the 30s execution timeout under the default skip_time=True.
    with DurableChildContextTestRunner(_child_waits) as runner:  # type: ignore[arg-type]
        result = runner.run(input="x", execution_timeout=30)

    assert result.status is InvocationStatus.SUCCEEDED
