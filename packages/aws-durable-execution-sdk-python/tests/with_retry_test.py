"""Unit tests for with_retry helper function."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, TypeVar
from unittest.mock import MagicMock

import pytest

from aws_durable_execution_sdk_python.config import Duration, JitterStrategy
from aws_durable_execution_sdk_python.exceptions import SuspendExecution
from aws_durable_execution_sdk_python.retries import (
    RetryStrategyConfig,
    WithRetryConfig,
    create_retry_strategy,
    with_retry,
)


if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_execution_sdk_python.config import ChildConfig
    from aws_durable_execution_sdk_python.types import DurableContext

_T = TypeVar("_T")


# region Mock DurableContext


@dataclass
class WaitCall:
    """Record of a wait() call."""

    duration: Duration
    name: str | None


@dataclass
class RunInChildContextCall:
    """Record of a run_in_child_context() call."""

    name: str | None
    config: ChildConfig | None
    result: object = None


@dataclass
class MockDurableContext:
    """A fake DurableContext that records wait() and run_in_child_context() calls."""

    wait_calls: list[WaitCall] = field(default_factory=list)
    child_context_calls: list[RunInChildContextCall] = field(default_factory=list)

    def wait(self, duration: Duration, name: str | None = None) -> None:
        self.wait_calls.append(WaitCall(duration=duration, name=name))

    def run_in_child_context(
        self,
        func: Callable[[DurableContext], _T],
        name: str | None = None,
        config: ChildConfig | None = None,
    ) -> _T:
        result: _T = func(self)  # type: ignore[arg-type]
        self.child_context_calls.append(
            RunInChildContextCall(name=name, config=config, result=result)
        )
        return result

    def step(self, *args, **kwargs):
        raise NotImplementedError("step not used in with_retry tests")

    def map(self, *args, **kwargs):
        raise NotImplementedError("map not used in with_retry tests")

    def parallel(self, *args, **kwargs):
        raise NotImplementedError("parallel not used in with_retry tests")

    def create_callback(self, *args, **kwargs):
        raise NotImplementedError("create_callback not used in with_retry tests")


# endregion


# region Helper fixtures


def _make_config(
    max_attempts: int = 3,
    initial_delay: Duration | None = None,
    wrap_with_run_in_child_context: bool = True,
    child_context_config: ChildConfig | None = None,
) -> WithRetryConfig:
    """Create a WithRetryConfig with no jitter for deterministic tests."""
    return WithRetryConfig(
        retry_strategy=create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=max_attempts,
                initial_delay=initial_delay or Duration.from_seconds(1),
                jitter_strategy=JitterStrategy.NONE,
            )
        ),
        wrap_with_run_in_child_context=wrap_with_run_in_child_context,
        child_context_config=child_context_config,
    )


# endregion


# region Tests


def test_success_on_first_attempt_returns_result_without_retry():
    """Function succeeds on first attempt returns result without invoking retry strategy."""
    ctx = MockDurableContext()
    config = _make_config(wrap_with_run_in_child_context=False)

    def tracking_func(ctx: DurableContext, attempt: int) -> str:
        return "success"

    result = with_retry(ctx, tracking_func, config)

    assert result == "success"
    assert len(ctx.wait_calls) == 0


def test_function_fails_then_succeeds_returns_successful_result():
    """Function fails then succeeds returns result from successful attempt."""
    ctx = MockDurableContext()
    config = _make_config(max_attempts=3, wrap_with_run_in_child_context=False)

    call_count = 0

    def failing_then_succeeding(ctx: DurableContext, attempt: int) -> str:
        nonlocal call_count
        call_count += 1
        if attempt < 3:
            raise ValueError(f"fail on attempt {attempt}")
        return "eventual success"

    result = with_retry(ctx, failing_then_succeeding, config)

    assert result == "eventual success"
    assert call_count == 3
    assert len(ctx.wait_calls) == 2


def test_retry_strategy_returns_should_retry_false_reraises_exception():
    """Retry strategy returns should_retry=False re-raises exception."""
    ctx = MockDurableContext()
    # max_attempts=1 means the strategy will return should_retry=False on first failure
    config = _make_config(max_attempts=1, wrap_with_run_in_child_context=False)

    def always_fails(ctx: DurableContext, attempt: int) -> None:
        raise RuntimeError("permanent failure")

    with pytest.raises(RuntimeError, match="permanent failure"):
        with_retry(ctx, always_fails, config)

    assert len(ctx.wait_calls) == 0


def test_suspend_execution_is_reraised_immediately():
    """SuspendExecution is re-raised immediately without invoking retry strategy."""
    ctx = MockDurableContext()
    config = _make_config(max_attempts=5, wrap_with_run_in_child_context=False)

    def raises_suspend(ctx: DurableContext, attempt: int) -> None:
        raise SuspendExecution("suspending")

    with pytest.raises(SuspendExecution, match="suspending"):
        with_retry(ctx, raises_suspend, config)

    # No waits should have been called - strategy was never invoked
    assert len(ctx.wait_calls) == 0


def test_default_config_wraps_in_child_context():
    """Default config wraps in child context."""
    ctx = MockDurableContext()
    config = _make_config(wrap_with_run_in_child_context=True)

    def simple_func(ctx: DurableContext, attempt: int) -> str:
        return "child result"

    result = with_retry(ctx, simple_func, config)

    assert result == "child result"
    assert len(ctx.child_context_calls) == 1


def test_wrap_with_run_in_child_context_false_skips_child_context():
    """wrap_with_run_in_child_context=False skips child context."""
    ctx = MockDurableContext()
    config = _make_config(wrap_with_run_in_child_context=False)

    def simple_func(ctx: DurableContext, attempt: int) -> str:
        return "direct result"

    result = with_retry(ctx, simple_func, config)

    assert result == "direct result"
    assert len(ctx.child_context_calls) == 0


def test_no_name_creates_anonymous_child_context_and_anonymous_waits():
    """No name creates anonymous child context and anonymous waits."""
    ctx = MockDurableContext()
    config = _make_config(max_attempts=3, wrap_with_run_in_child_context=True)

    call_count = 0

    def fails_once(ctx: DurableContext, attempt: int) -> str:
        nonlocal call_count
        call_count += 1
        if attempt == 1:
            raise ValueError("transient")
        return "ok"

    result = with_retry(ctx, fails_once, config, name=None)

    assert result == "ok"
    # Child context should have been called with name=None
    assert len(ctx.child_context_calls) == 1
    assert ctx.child_context_calls[0].name is None
    # Wait should have been called with name=None
    assert len(ctx.wait_calls) == 1
    assert ctx.wait_calls[0].name is None


def test_name_is_forwarded_to_child_context_and_backoff_waits():
    """Name is forwarded to child context and backoff waits."""
    ctx = MockDurableContext()
    config = _make_config(max_attempts=3, wrap_with_run_in_child_context=True)

    call_count = 0

    def fails_twice(ctx: DurableContext, attempt: int) -> str:
        nonlocal call_count
        call_count += 1
        if attempt <= 2:
            raise ValueError("transient")
        return "done"

    result = with_retry(ctx, fails_twice, config, name="my-retry")

    assert result == "done"
    # Child context should have been called with the name
    assert len(ctx.child_context_calls) == 1
    assert ctx.child_context_calls[0].name == "my-retry"
    # Waits should be named "{name}-backoff-{attempt}"
    assert len(ctx.wait_calls) == 2
    assert ctx.wait_calls[0].name == "my-retry-backoff-1"
    assert ctx.wait_calls[1].name == "my-retry-backoff-2"


def test_child_context_config_is_forwarded():
    """child_context_config is forwarded to run_in_child_context."""
    ctx = MockDurableContext()

    mock_child_config = MagicMock()

    config = WithRetryConfig(
        retry_strategy=create_retry_strategy(RetryStrategyConfig(max_attempts=3)),
        wrap_with_run_in_child_context=True,
        child_context_config=mock_child_config,
    )

    def simple_func(ctx: DurableContext, attempt: int) -> str:
        return "result"

    with_retry(ctx, simple_func, config, name="test")

    assert len(ctx.child_context_calls) == 1
    assert ctx.child_context_calls[0].config is mock_child_config


def test_attempt_number_starts_at_1_and_increments():
    """Attempt number starts at 1 and increments."""
    ctx = MockDurableContext()
    config = _make_config(max_attempts=5, wrap_with_run_in_child_context=False)

    recorded_attempts: list[int] = []

    def record_attempts(ctx: DurableContext, attempt: int) -> str:
        recorded_attempts.append(attempt)
        if attempt < 4:
            raise ValueError("not yet")
        return "done"

    result = with_retry(ctx, record_attempts, config)

    assert result == "done"
    assert recorded_attempts == [1, 2, 3, 4]


def test_with_retry_and_config_importable_from_package():
    """with_retry and WithRetryConfig are importable from package."""
    from aws_durable_execution_sdk_python import WithRetryConfig as ImportedConfig
    from aws_durable_execution_sdk_python import with_retry as imported_with_retry

    assert ImportedConfig is WithRetryConfig
    assert imported_with_retry is with_retry


def test_integration_with_create_retry_strategy():
    """Integration with create_retry_strategy produces correct retry behavior."""
    ctx = MockDurableContext()

    config = WithRetryConfig(
        retry_strategy=create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=4,
                initial_delay=Duration.from_seconds(2),
                backoff_rate=2.0,
                jitter_strategy=JitterStrategy.NONE,
            )
        ),
        wrap_with_run_in_child_context=False,
    )

    call_count = 0

    def fails_three_times(ctx: DurableContext, attempt: int) -> str:
        nonlocal call_count
        call_count += 1
        if attempt <= 3:
            raise ValueError(f"fail {attempt}")
        return "success after retries"

    result = with_retry(ctx, fails_three_times, config)

    assert result == "success after retries"
    assert call_count == 4

    # Verify backoff delays: 2*2^0=2, 2*2^1=4, 2*2^2=8
    assert len(ctx.wait_calls) == 3
    assert ctx.wait_calls[0].duration.to_seconds() == 2
    assert ctx.wait_calls[1].duration.to_seconds() == 4
    assert ctx.wait_calls[2].duration.to_seconds() == 8


def test_integration_retries_exhausted_raises_last_exception():
    """When all retries are exhausted, the last exception is raised."""
    ctx = MockDurableContext()

    config = WithRetryConfig(
        retry_strategy=create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=3,
                initial_delay=Duration.from_seconds(1),
                jitter_strategy=JitterStrategy.NONE,
            )
        ),
        wrap_with_run_in_child_context=False,
    )

    def always_fails(ctx: DurableContext, attempt: int) -> None:
        raise RuntimeError(f"error on attempt {attempt}")

    with pytest.raises(RuntimeError, match="error on attempt 3"):
        with_retry(ctx, always_fails, config)

    # Should have waited between attempts 1->2 and 2->3
    assert len(ctx.wait_calls) == 2


# endregion
