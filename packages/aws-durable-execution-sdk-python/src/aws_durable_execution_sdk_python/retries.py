"""Ready-made retry strategies and retry creators."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, TypeVar

from aws_durable_execution_sdk_python.config import Duration, JitterStrategy
from aws_durable_execution_sdk_python.exceptions import SuspendExecution


if TYPE_CHECKING:
    from collections.abc import Callable

    from aws_durable_execution_sdk_python.config import ChildConfig
    from aws_durable_execution_sdk_python.types import DurableContext

T = TypeVar("T")

Numeric = int | float

# Default pattern that matches all error messages
_DEFAULT_RETRYABLE_ERROR_PATTERN = re.compile(r".*")


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


@dataclass
class LinearRetryStrategyConfig:
    max_attempts: int = 6
    initial_delay: Duration = field(default_factory=lambda: Duration.from_seconds(1))
    increment: Duration = field(default_factory=lambda: Duration.from_seconds(1))
    max_delay: Duration = field(default_factory=lambda: Duration.from_minutes(5))
    jitter_strategy: JitterStrategy = field(default=JitterStrategy.FULL)
    retryable_errors: list[str | re.Pattern] | None = None
    retryable_error_types: list[type[Exception]] | None = None

    @property
    def initial_delay_seconds(self) -> int:
        """Get initial delay in seconds."""
        return self.initial_delay.to_seconds()

    @property
    def increment_seconds(self) -> int:
        """Get increment in seconds."""
        return self.increment.to_seconds()

    @property
    def max_delay_seconds(self) -> int:
        """Get max delay in seconds."""
        return self.max_delay.to_seconds()


def _resolve_retryable_errors(
    retryable_errors: list[str | re.Pattern] | None,
    retryable_error_types: list[type[Exception]] | None,
) -> tuple[list[str | re.Pattern], list[type[Exception]]]:
    """Resolve the error filters, applying the match-all default only when neither is set."""
    should_use_default_errors: bool = (
        retryable_errors is None and retryable_error_types is None
    )
    resolved_errors: list[str | re.Pattern] = (
        retryable_errors
        if retryable_errors is not None
        else ([_DEFAULT_RETRYABLE_ERROR_PATTERN] if should_use_default_errors else [])
    )
    resolved_error_types: list[type[Exception]] = retryable_error_types or []
    return resolved_errors, resolved_error_types


def _is_error_retryable(
    error: Exception,
    retryable_errors: list[str | re.Pattern],
    retryable_error_types: list[type[Exception]],
) -> bool:
    """Return True when the error matches one of the message patterns or types."""
    is_retryable_error_message: bool = any(
        pattern.search(str(error))
        if isinstance(pattern, re.Pattern)
        else pattern in str(error)
        for pattern in retryable_errors
    )
    is_retryable_error_type: bool = any(
        isinstance(error, error_type) for error_type in retryable_error_types
    )
    return is_retryable_error_message or is_retryable_error_type


def create_retry_strategy(
    config: RetryStrategyConfig | None = None,
) -> Callable[[Exception, int], RetryDecision]:
    if config is None:
        config = RetryStrategyConfig()

    retryable_errors, retryable_error_types = _resolve_retryable_errors(
        config.retryable_errors, config.retryable_error_types
    )

    def retry_strategy(error: Exception, attempts_made: int) -> RetryDecision:
        # Check if we've exceeded max attempts
        if attempts_made >= config.max_attempts:
            return RetryDecision.no_retry()

        if not _is_error_retryable(error, retryable_errors, retryable_error_types):
            return RetryDecision.no_retry()

        # Calculate delay with exponential backoff
        base_delay: float = min(
            config.initial_delay_seconds * (config.backoff_rate ** (attempts_made - 1)),
            config.max_delay_seconds,
        )
        final_delay: int = config.jitter_strategy.finalize_delay(base_delay)

        return RetryDecision.retry(Duration(seconds=final_delay))

    return retry_strategy


def create_linear_retry_strategy(
    config: LinearRetryStrategyConfig | None = None,
) -> Callable[[Exception, int], RetryDecision]:
    """Linearly increasing delay between retries.

    The base delay is ``initial_delay + increment * (attempts_made - 1)``,
    capped at ``max_delay``, with jitter and error filtering applied the same
    way as :func:`create_retry_strategy`. Mirrors the JS SDK's
    ``createLinearRetryStrategy``.
    """
    if config is None:
        config = LinearRetryStrategyConfig()

    retryable_errors, retryable_error_types = _resolve_retryable_errors(
        config.retryable_errors, config.retryable_error_types
    )

    def linear_retry_strategy(error: Exception, attempts_made: int) -> RetryDecision:
        if attempts_made >= config.max_attempts:
            return RetryDecision.no_retry()

        if not _is_error_retryable(error, retryable_errors, retryable_error_types):
            return RetryDecision.no_retry()

        base_delay: float = min(
            config.initial_delay_seconds
            + config.increment_seconds * (attempts_made - 1),
            config.max_delay_seconds,
        )
        final_delay: int = config.jitter_strategy.finalize_delay(base_delay)

        return RetryDecision.retry(Duration(seconds=final_delay))

    return linear_retry_strategy


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

    @classmethod
    def linear(cls) -> Callable[[Exception, int], RetryDecision]:
        """Linearly increasing delay between retries: 1s, 2s, 3s, 4s, 5s."""
        return create_linear_retry_strategy(
            LinearRetryStrategyConfig(
                max_attempts=6,
                initial_delay=Duration.from_seconds(1),
                increment=Duration.from_seconds(1),
                jitter_strategy=JitterStrategy.NONE,
            )
        )

    @classmethod
    def fixed(
        cls, interval: Duration | None = None
    ) -> Callable[[Exception, int], RetryDecision]:
        """Constant delay between retries (5s by default, no jitter)."""
        delay: Duration = interval if interval is not None else Duration.from_seconds(5)
        return create_retry_strategy(
            RetryStrategyConfig(
                max_attempts=5,
                initial_delay=delay,
                max_delay=delay,
                backoff_rate=1,
                jitter_strategy=JitterStrategy.NONE,
            )
        )


@dataclass(frozen=True)
class WithRetryConfig(Generic[T]):
    """Configuration for with_retry.

    Holds a retry strategy callable (same type used by StepConfig) and
    adds execution-mode options specific to with_retry.

    Attributes:
        retry_strategy: A callable that decides whether to retry and with
            what delay. Accepts (Exception, int) and returns RetryDecision.
            Use create_retry_strategy(RetryStrategyConfig(...)) to build one,
            or provide a custom callable. If None, the default retry strategy
            (RetryStrategyConfig defaults) is used.
        wrap_with_run_in_child_context: Whether to wrap the retry loop in
            a child context for isolation. Default True. When True, final
            failure is rethrown as CallableRuntimeError with the original
            exception on `cause`. When False, the original error is
            rethrown unchanged.
        child_context_config: Optional ChildConfig forwarded to
            run_in_child_context when wrapping is enabled. Ignored when
            wrap_with_run_in_child_context is False.
    """

    retry_strategy: Callable[[Exception, int], RetryDecision] | None = None
    wrap_with_run_in_child_context: bool = True
    child_context_config: ChildConfig[T] | None = None


def with_retry(
    context: DurableContext,
    func: Callable[[DurableContext, int], T],
    config: WithRetryConfig[T],
    name: str | None = None,
) -> T:
    """Retry a block of durable logic with configurable backoff.

    Semantically a run_in_child_context with a retry policy wrapped around
    it — on failure the whole function body is re-run from the beginning
    with configurable backoff.

    Unlike context.step() which retries a single atomic operation,
    with_retry retries an entire function body that may contain multiple
    durable operations (steps, waits, invokes, callbacks, etc.).

    Args:
        context: The DurableContext to execute within.
        func: A callable that accepts (DurableContext, attempt: int) and
              returns T. The function body may contain multiple durable
              operations.
        config: WithRetryConfig containing a retry strategy callable plus
              execution-mode options.
        name: Optional name for the child context and backoff waits.
              When provided, backoff waits are named
              "{name}-backoff-{attempt}".

    Returns:
        The result of func on successful execution.

    Raises:
        The exception from the last failed attempt when retries are
        exhausted or the retry strategy returns should_retry=False.
        When wrap_with_run_in_child_context is True (default),
        ChildOperationExecutor.process wraps non-InvocationError /
        SuspendExecution exceptions as CallableRuntimeError with the
        original error in cause.
        When wrap_with_run_in_child_context is False, the original
        exception propagates unchanged.
        SuspendExecution: Re-raised immediately (SDK control flow).
    """
    retry_strategy = config.retry_strategy or create_retry_strategy()

    def run_loop(ctx: DurableContext) -> T:
        attempt = 0
        while True:
            attempt += 1
            try:
                return func(ctx, attempt)
            except SuspendExecution:
                raise  # SDK control flow - never intercept
            except Exception as err:
                decision = retry_strategy(err, attempt)
                if not decision.should_retry:
                    raise
                wait_name = f"{name}-backoff-{attempt}" if name else None
                ctx.wait(duration=decision.delay, name=wait_name)

    if config.wrap_with_run_in_child_context:
        return context.run_in_child_context(
            run_loop,
            name=name,
            config=config.child_context_config,
        )
    else:
        return run_loop(context)
