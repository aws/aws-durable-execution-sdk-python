"""Ready-made retry strategies and retry creators."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from aws_durable_execution_sdk_python.config import Duration, JitterStrategy

if TYPE_CHECKING:
    from collections.abc import Callable

Numeric = int | float


@dataclass
class RetryDecision:
    """Decision about whether to retry a step and with what delay."""

    should_retry: bool
    delay: Duration

    @property
    def delay_seconds(self) -> int:
        """Get delay in seconds."""
        return self.delay.to_seconds()

    @classmethod
    def retry(cls, delay: Duration) -> RetryDecision:
        """Create a retry decision."""
        return cls(should_retry=True, delay=delay)

    @classmethod
    def no_retry(cls) -> RetryDecision:
        """Create a no-retry decision."""
        return cls(should_retry=False, delay=Duration())


@dataclass
class RetryStrategyConfig:
    max_attempts: int = 3
    initial_delay: Duration = field(default_factory=lambda: Duration.from_seconds(5))
    max_delay: Duration = field(
        default_factory=lambda: Duration.from_minutes(5)
    )  # 5 minutes
    backoff_rate: Numeric = 2.0
    jitter_strategy: JitterStrategy = field(default=JitterStrategy.FULL)
    retryable_errors: list[str | re.Pattern] | None = None
    retryable_error_types: list[type[Exception]] | None = None

    @property
    def initial_delay_seconds(self) -> int:
        """Get initial delay in seconds."""
        return self.initial_delay.to_seconds()

    @property
    def max_delay_seconds(self) -> int:
        """Get max delay in seconds."""
        return self.max_delay.to_seconds()


def create_retry_strategy(
    config: RetryStrategyConfig | None = None,
) -> Callable[[Exception, int], RetryDecision]:
    if config is None:
        config = RetryStrategyConfig()

    # Apply default retryableErrors only if user didn't specify either filter
    should_use_default_errors: bool = (
        config.retryable_errors is None and config.retryable_error_types is None
    )

    retryable_errors: list[str | re.Pattern] = (
        config.retryable_errors
        if config.retryable_errors is not None
        else ([re.compile(r".*")] if should_use_default_errors else [])
    )
    retryable_error_types: list[type[Exception]] = config.retryable_error_types or []

    def retry_strategy(error: Exception, attempts_made: int) -> RetryDecision:
        # Check if we've exceeded max attempts
        if attempts_made >= config.max_attempts:
            return RetryDecision.no_retry()

        # Check if error is retryable based on error message
        is_retryable_error_message: bool = any(
            pattern.search(str(error))
            if isinstance(pattern, re.Pattern)
            else pattern in str(error)
            for pattern in retryable_errors
        )

        # Check if error is retryable based on error type
        is_retryable_error_type: bool = any(
            isinstance(error, error_type) for error_type in retryable_error_types
        )

        if not is_retryable_error_message and not is_retryable_error_type:
            return RetryDecision.no_retry()

        # Calculate delay with exponential backoff
        base_delay: float = min(
            config.initial_delay_seconds * (config.backoff_rate ** (attempts_made - 1)),
            config.max_delay_seconds,
        )
        # Apply jitter to get final delay
        delay_with_jitter: float = config.jitter_strategy.apply_jitter(base_delay)
        # Round up and ensure minimum of 1 second
        final_delay: int = max(1, math.ceil(delay_with_jitter))

        return RetryDecision.retry(Duration(seconds=final_delay))

    return retry_strategy


class RetryPresets:
    """Default retry presets."""

    @classmethod
    def none(cls) -> Callable[[Exception, int], RetryDecision]:
        """No retries."""
        return create_retry_strategy(RetryStrategyConfig(max_attempts=1))

    @classmethod
    def default(cls) -> Callable[[Exception, int], RetryDecision]:
        """Default retries, will be used automatically if retryConfig is missing"""
        return create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=6,
                initial_delay=Duration.from_seconds(5),
                max_delay=Duration.from_minutes(1),
                backoff_rate=2,
                jitter_strategy=JitterStrategy.FULL,
            )
        )

    @classmethod
    def transient(cls) -> Callable[[Exception, int], RetryDecision]:
        """Quick retries for transient errors"""
        return create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=3, backoff_rate=2, jitter_strategy=JitterStrategy.HALF
            )
        )

    @classmethod
    def resource_availability(cls) -> Callable[[Exception, int], RetryDecision]:
        """Longer retries for resource availability"""
        return create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=5,
                initial_delay=Duration.from_seconds(5),
                max_delay=Duration.from_minutes(5),
                backoff_rate=2,
            )
        )

    @classmethod
    def critical(cls) -> Callable[[Exception, int], RetryDecision]:
        """Aggressive retries for critical operations"""
        return create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=10,
                initial_delay=Duration.from_seconds(1),
                max_delay=Duration.from_minutes(1),
                backoff_rate=1.5,
                jitter_strategy=JitterStrategy.NONE,
            )
        )
