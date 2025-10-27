"""Ready-made wait strategies and wait creators."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic

from aws_durable_execution_sdk_python.config import JitterStrategy, T

if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_execution_sdk_python.serdes import SerDes

Numeric = int | float


@dataclass
class WaitDecision:
    """Decision about whether to wait a step and with what delay."""

    should_wait: bool
    delay_seconds: int

    @classmethod
    def wait(cls, delay_seconds: int) -> WaitDecision:
        """Create a wait decision."""
        return cls(should_wait=True, delay_seconds=delay_seconds)

    @classmethod
    def no_wait(cls) -> WaitDecision:
        """Create a no-wait decision."""
        return cls(should_wait=False, delay_seconds=0)


@dataclass
class WaitStrategyConfig(Generic[T]):
    should_continue_polling: Callable[[T], bool]
    max_attempts: int = 60
    initial_delay_seconds: int = 5
    max_delay_seconds: int = 300  # 5 minutes
    backoff_rate: Numeric = 1.5
    jitter_strategy: JitterStrategy = field(default=JitterStrategy.FULL)
    timeout_seconds: int | None = None  # Not implemented yet


def create_wait_strategy(
    config: WaitStrategyConfig[T],
) -> Callable[[T, int], WaitDecision]:
    def wait_strategy(result: T, attempts_made: int) -> WaitDecision:
        # Check if condition is met
        if not config.should_continue_polling(result):
            return WaitDecision.no_wait()

        # Check if we've exceeded max attempts
        if attempts_made >= config.max_attempts:
            return WaitDecision.no_wait()

        # Calculate delay with exponential backoff
        base_delay = min(
            config.initial_delay_seconds * (config.backoff_rate ** (attempts_made - 1)),
            config.max_delay_seconds,
        )

        # Apply jitter (add jitter to base delay)
        jitter = config.jitter_strategy.compute_jitter(base_delay)
        delay_with_jitter = base_delay + jitter

        # Ensure delay is an integer >= 1
        final_delay = max(1, round(delay_with_jitter))

        return WaitDecision.wait(final_delay)

    return wait_strategy


@dataclass(frozen=True)
class WaitForConditionDecision:
    """Decision about whether to continue waiting."""

    should_continue: bool
    delay_seconds: int

    @classmethod
    def continue_waiting(cls, delay_seconds: int) -> WaitForConditionDecision:
        """Create a decision to continue waiting for delay_seconds."""
        return cls(should_continue=True, delay_seconds=delay_seconds)

    @classmethod
    def stop_polling(cls) -> WaitForConditionDecision:
        """Create a decision to stop polling."""
        return cls(should_continue=False, delay_seconds=-1)


@dataclass(frozen=True)
class WaitForConditionConfig(Generic[T]):
    """Configuration for wait_for_condition."""

    wait_strategy: Callable[[T, int], WaitForConditionDecision]
    initial_state: T
    serdes: SerDes | None = None
