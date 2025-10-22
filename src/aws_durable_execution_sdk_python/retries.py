"""Ready-made retry strategies and retry creators."""

from __future__ import annotations

from enum import StrEnum
import random
import re
import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable
    from aws_durable_execution_sdk_python.types import JitterStrategy

Numeric = int | float

# region Jitter

class JitterStrategy(StrEnum):
    """
    Jitter strategies are used to introduce noise when attempting to retry
    an invoke. We introduce noise to prevent a thundering-herd effect where
    a group of accesses (e.g. invokes) happen at once.

    Jitter is meant to be used to spread operations across time. 

    members:
        :NONE: No jitter; use the exact calculated delay
        :FULL: Full jitter; random delay between 0 and calculated delay
        :HALF: Half jitter; random delay between 0.5x and 1.0x of the calculated delay
    """
    NONE = "NONE"
    FULL = "FULL"
    HALF = "HALF"

    def compute_jitter(self, delay) -> int:
        match self:
            case JitterStrategy.NONE:
                return 0
            case JitterStrategy.FULL:
                return random.random() * delay  # noqa: S311
            case JitterStrategy.HALF:
                return (random.random() * 0.5 + 0.5)  # noqa: S311

# endregion Jitter


@dataclass
class RetryDecision:
    """Decision about whether to retry a step and with what delay."""

    should_retry: bool
    delay_seconds: int

    @classmethod
    def retry(cls, delay_seconds: int) -> RetryDecision:
        """Create a retry decision."""
        return cls(should_retry=True, delay_seconds=delay_seconds)

    @classmethod
    def no_retry(cls) -> RetryDecision:
        """Create a no-retry decision."""
        return cls(should_retry=False, delay_seconds=0)


@dataclass
class RetryStrategyConfig:
    max_attempts: int = sys.maxsize  # "infinite", practically
    initial_delay_seconds: int = 5
    max_delay_seconds: int = 300  # 5 minutes
    backoff_rate: Numeric = 2.0
    jitter_strategy: JitterStrategy = field(default=JitterStrategy.FULL)
    retryable_errors: list[str | re.Pattern] = field(
        default_factory=lambda: [re.compile(r".*")]
    )
    retryable_error_types: list[type[Exception]] = field(default_factory=list)


def create_retry_strategy(
    config: RetryStrategyConfig,
) -> Callable[[Exception, int], RetryDecision]:
    if config is None:
        config = RetryStrategyConfig()

    def retry_strategy(error: Exception, attempts_made: int) -> RetryDecision:
        # Check if we've exceeded max attempts
        if attempts_made >= config.max_attempts:
            return RetryDecision.no_retry()

        # Check if error is retryable based on error message
        is_retryable_error_message = any(
            pattern.search(str(error))
            if isinstance(pattern, re.Pattern)
            else pattern in str(error)
            for pattern in config.retryable_errors
        )

        # Check if error is retryable based on error type
        is_retryable_error_type = any(
            isinstance(error, error_type) for error_type in config.retryable_error_types
        )

        if not is_retryable_error_message and not is_retryable_error_type:
            return RetryDecision.no_retry()

        # Calculate delay with exponential backoff
        delay = min(
            config.initial_delay_seconds * (config.backoff_rate ** (attempts_made - 1)),
            config.max_delay_seconds,
        )
        jitter = config.jitter_strategy.compute_jitter(delay)        
        final_delay = max(1, delay + jitter)

        return RetryDecision.retry(round(final_delay))

    return retry_strategy


class RetryPresets:
    """Default retry presets."""

    @classmethod
    def none(cls) -> Callable[[Exception, int], RetryDecision]:
        """No retries."""
        return create_retry_strategy(RetryStrategyConfig(max_attempts=0))

    @classmethod
    def default(cls) -> Callable[[Exception, int], RetryDecision]:
        """Default retries, will be used automatically if retryConfig is missing"""
        return create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=sys.maxsize,
                initial_delay_seconds=5,
                max_delay_seconds=60,
                backoff_rate=2,
            )
        )

    @classmethod
    def transient(cls) -> Callable[[Exception, int], RetryDecision]:
        """Quick retries for transient errors"""
        return create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=3,
                backoff_rate=2,
                jitter_seconds=0.5,
            )
        )

    @classmethod
    def resource_availability(cls) -> Callable[[Exception, int], RetryDecision]:
        """Longer retries for resource availability"""
        return create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=5,
                initial_delay_seconds=5,
                max_delay_seconds=300,
                backoff_rate=2,
            )
        )

    @classmethod
    def critical(cls) -> Callable[[Exception, int], RetryDecision]:
        """Aggressive retries for critical operations"""
        return create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=10,
                initial_delay_seconds=1,
                max_delay_seconds=60,
                backoff_rate=1.5,
            )
        )
