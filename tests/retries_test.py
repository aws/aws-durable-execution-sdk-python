"""Tests for retry strategies and jitter implementations."""

import re
from unittest.mock import patch

import pytest

from aws_durable_execution_sdk_python.config import BackoffStrategy, Duration
from aws_durable_execution_sdk_python.retries import (
    JitterStrategy,
    RetryDecision,
    RetryPresets,
    RetryStrategyConfig,
    create_retry_strategy,
)

# region Jitter Strategy Tests


def test_none_jitter_returns_delay():
    """Test NONE jitter returns the original delay unchanged."""
    strategy = JitterStrategy.NONE
    assert strategy.apply_jitter(10) == 10
    assert strategy.apply_jitter(100) == 100


@patch("random.random")
def test_full_jitter_range(mock_random):
    """Test FULL jitter returns value between 0 and delay."""
    mock_random.return_value = 0.5
    strategy = JitterStrategy.FULL
    delay = 10
    result = strategy.apply_jitter(delay)
    assert result == 5.0  # 0.5 * 10


@patch("random.random")
def test_half_jitter_range(mock_random):
    """Test HALF jitter returns value between delay/2 and delay."""
    mock_random.return_value = 0.5
    strategy = JitterStrategy.HALF
    result = strategy.apply_jitter(10)
    assert result == 7.5  # 10/2 + 0.5 * (10/2) = 5 + 2.5


@patch("random.random")
def test_half_jitter_boundary_values(mock_random):
    """Test HALF jitter boundary values."""
    strategy = JitterStrategy.HALF

    # Minimum value (random = 0): delay/2 + 0 = delay/2
    mock_random.return_value = 0.0
    result = strategy.apply_jitter(100)
    assert result == 50

    # Maximum value (random = 1): delay/2 + delay/2 = delay
    mock_random.return_value = 1.0
    result = strategy.apply_jitter(100)
    assert result == 100


def test_invalid_jitter_strategy():
    """Test behavior with invalid jitter strategy."""
    # Create an invalid enum value by bypassing normal construction
    invalid_strategy = "INVALID"

    # This should raise an exception or return None
    with pytest.raises((ValueError, AttributeError)):
        JitterStrategy(invalid_strategy).apply_jitter(10)


# endregion


# region Retry Decision Tests


def test_retry_factory():
    """Test retry factory method."""
    decision = RetryDecision.retry(Duration.from_seconds(30))
    assert decision.should_retry is True
    assert decision.delay_seconds == 30


def test_no_retry_factory():
    """Test no_retry factory method."""
    decision = RetryDecision.no_retry()
    assert decision.should_retry is False
    assert decision.delay_seconds == 0


# endregion


# region Retry Strategy Config Tests


def test_default_config():
    """Test default configuration values."""
    config = RetryStrategyConfig()
    assert config.max_attempts == 3
    assert config.initial_delay_seconds == 5
    assert config.max_delay_seconds == 300
    assert config.backoff_rate == 2.0
    assert config.jitter_strategy == JitterStrategy.FULL
    assert config.retryable_errors is None
    assert config.retryable_error_types is None


# endregion


# region Create Retry Strategy Tests


def test_max_attempts_exceeded():
    """Test strategy returns no_retry when max attempts exceeded."""
    config = RetryStrategyConfig(max_attempts=2)
    strategy = create_retry_strategy(config)

    error = Exception("test error")
    decision = strategy(error, 2)
    assert decision.should_retry is False


def test_retryable_error_message_string():
    """Test retry based on error message string match."""
    config = RetryStrategyConfig(retryable_errors=["timeout"])
    strategy = create_retry_strategy(config)

    error = Exception("connection timeout")
    decision = strategy(error, 1)
    assert decision.should_retry is True


def test_retryable_error_message_regex():
    """Test retry based on error message regex match."""
    config = RetryStrategyConfig(retryable_errors=[re.compile(r"timeout|error")])
    strategy = create_retry_strategy(config)

    error = Exception("network timeout occurred")
    decision = strategy(error, 1)
    assert decision.should_retry is True


def test_retryable_error_type():
    """Test retry based on error type."""
    config = RetryStrategyConfig(retryable_error_types=[ValueError])
    strategy = create_retry_strategy(config)

    error = ValueError("invalid value")
    decision = strategy(error, 1)
    assert decision.should_retry is True


def test_non_retryable_error():
    """Test no retry for non-retryable error."""
    config = RetryStrategyConfig(retryable_errors=["timeout"])
    strategy = create_retry_strategy(config)

    error = Exception("permission denied")
    decision = strategy(error, 1)
    assert decision.should_retry is False


@patch("random.random")
def test_exponential_backoff_calculation(mock_random):
    """Test exponential backoff delay calculation with jitter."""
    mock_random.return_value = 0.5
    config = RetryStrategyConfig(
        initial_delay=Duration.from_seconds(2),
        backoff_rate=2.0,
        jitter_strategy=JitterStrategy.FULL,
    )
    strategy = create_retry_strategy(config)

    error = Exception("test error")

    # First attempt: base = 2 * (2^0) = 2, full jitter = 0.5 * 2 = 1
    decision = strategy(error, 1)
    assert decision.delay_seconds == 1

    # Second attempt: base = 2 * (2^1) = 4, full jitter = 0.5 * 4 = 2
    decision = strategy(error, 2)
    assert decision.delay_seconds == 2


def test_max_delay_cap():
    """Test delay is capped at max_delay_seconds."""
    config = RetryStrategyConfig(
        initial_delay=Duration.from_seconds(100),
        max_delay=Duration.from_seconds(50),
        backoff_rate=2.0,
        jitter_strategy=JitterStrategy.NONE,
    )
    strategy = create_retry_strategy(config)

    error = Exception("test error")
    decision = strategy(error, 2)  # Would be 200 without cap
    assert decision.delay_seconds == 50


def test_minimum_delay_one_second():
    """Test delay is at least 1 second."""
    config = RetryStrategyConfig(
        initial_delay=Duration.from_seconds(0), jitter_strategy=JitterStrategy.NONE
    )
    strategy = create_retry_strategy(config)

    error = Exception("test error")
    decision = strategy(error, 1)
    assert decision.delay_seconds == 1


def test_delay_ceiling_applied():
    """Test delay is rounded up using math.ceil."""
    with patch("random.random", return_value=0.3):
        config = RetryStrategyConfig(
            initial_delay=Duration.from_seconds(3),
            jitter_strategy=JitterStrategy.FULL,
        )
        strategy = create_retry_strategy(config)

        error = Exception("test error")
        decision = strategy(error, 1)
        # base = 3, full jitter = 0.3 * 3 = 0.9, ceil(0.9) = 1
        assert decision.delay_seconds == 1


# endregion


# region Retry Presets Tests


def test_none_preset():
    """Test none preset allows no retries."""
    strategy = RetryPresets.none()
    error = Exception("test error")

    decision = strategy(error, 1)
    assert decision.should_retry is False


def test_default_preset_config():
    """Test default preset configuration."""
    strategy = RetryPresets.default()
    error = Exception("test error")

    # Should retry within max attempts
    decision = strategy(error, 1)
    assert decision.should_retry is True

    # Should not retry after max attempts
    decision = strategy(error, 6)
    assert decision.should_retry is False


def test_transient_preset_config():
    """Test transient preset configuration."""
    strategy = RetryPresets.transient()
    error = Exception("test error")

    # Should retry within max attempts
    decision = strategy(error, 1)
    assert decision.should_retry is True

    # Should not retry after max attempts
    decision = strategy(error, 3)
    assert decision.should_retry is False


def test_resource_availability_preset():
    """Test resource availability preset allows longer retries."""
    strategy = RetryPresets.resource_availability()
    error = Exception("test error")

    # Should retry within max attempts
    decision = strategy(error, 1)
    assert decision.should_retry is True

    # Should not retry after max attempts
    decision = strategy(error, 5)
    assert decision.should_retry is False


def test_critical_preset_config():
    """Test critical preset allows many retries."""
    strategy = RetryPresets.critical()
    error = Exception("test error")

    # Should retry within max attempts
    decision = strategy(error, 5)
    assert decision.should_retry is True

    # Should not retry after max attempts
    decision = strategy(error, 10)
    assert decision.should_retry is False


@patch("random.random")
def test_critical_preset_no_jitter(mock_random):
    """Test critical preset uses no jitter."""
    mock_random.return_value = 0.5  # Should be ignored
    strategy = RetryPresets.critical()
    error = Exception("test error")

    decision = strategy(error, 1)
    # With no jitter: 1 * (1.5^0) = 1
    assert decision.delay_seconds == 1


# endregion


# region Jitter Integration Tests


@patch("random.random")
def test_full_jitter_integration(mock_random):
    """Test full jitter integration in retry strategy."""
    mock_random.return_value = 0.8
    config = RetryStrategyConfig(
        initial_delay=Duration.from_seconds(10), jitter_strategy=JitterStrategy.FULL
    )
    strategy = create_retry_strategy(config)

    error = Exception("test error")
    decision = strategy(error, 1)
    # base = 10, full jitter = 0.8 * 10 = 8
    assert decision.delay_seconds == 8


@patch("random.random")
def test_half_jitter_integration(mock_random):
    """Test half jitter integration in retry strategy."""
    mock_random.return_value = 0.6
    config = RetryStrategyConfig(
        initial_delay=Duration.from_seconds(10), jitter_strategy=JitterStrategy.HALF
    )
    strategy = create_retry_strategy(config)

    error = Exception("test error")
    decision = strategy(error, 1)
    # base = 10, half jitter = 10/2 + 0.6 * (10/2) = 5 + 3 = 8
    assert decision.delay_seconds == 8


@patch("random.random")
def test_half_jitter_integration_corrected(mock_random):
    """Test half jitter with minimum random value."""
    mock_random.return_value = 0.0  # Minimum jitter
    config = RetryStrategyConfig(
        initial_delay=Duration.from_seconds(10), jitter_strategy=JitterStrategy.HALF
    )
    strategy = create_retry_strategy(config)

    error = Exception("test error")
    decision = strategy(error, 1)
    # base = 10, half jitter = 10/2 + 0.0 * (10/2) = 5
    assert decision.delay_seconds == 5


def test_none_jitter_integration():
    """Test no jitter integration in retry strategy."""
    config = RetryStrategyConfig(
        initial_delay=Duration.from_seconds(10), jitter_strategy=JitterStrategy.NONE
    )
    strategy = create_retry_strategy(config)

    error = Exception("test error")
    decision = strategy(error, 1)
    assert decision.delay_seconds == 10


# endregion


# region Default Behavior Tests


def test_no_filters_retries_all_errors():
    """Test that when neither filter is specified, all errors are retried."""
    config = RetryStrategyConfig()
    strategy = create_retry_strategy(config)

    # Should retry any error
    error1 = Exception("any error message")
    decision1 = strategy(error1, 1)
    assert decision1.should_retry is True

    error2 = ValueError("different error type")
    decision2 = strategy(error2, 1)
    assert decision2.should_retry is True


def test_only_retryable_errors_specified():
    """Test that when only retryable_errors is specified, only matching messages are retried."""
    config = RetryStrategyConfig(retryable_errors=["timeout"])
    strategy = create_retry_strategy(config)

    # Should retry matching error
    error1 = Exception("connection timeout")
    decision1 = strategy(error1, 1)
    assert decision1.should_retry is True

    # Should NOT retry non-matching error
    error2 = Exception("permission denied")
    decision2 = strategy(error2, 1)
    assert decision2.should_retry is False


def test_only_retryable_error_types_specified():
    """Test that when only retryable_error_types is specified, only matching types are retried."""
    config = RetryStrategyConfig(retryable_error_types=[ValueError, TypeError])
    strategy = create_retry_strategy(config)

    # Should retry matching type
    error1 = ValueError("invalid value")
    decision1 = strategy(error1, 1)
    assert decision1.should_retry is True

    error2 = TypeError("type error")
    decision2 = strategy(error2, 1)
    assert decision2.should_retry is True

    # Should NOT retry non-matching type (even though message might match default pattern)
    error3 = Exception("some error")
    decision3 = strategy(error3, 1)
    assert decision3.should_retry is False


def test_both_filters_specified_or_logic():
    """Test that when both filters are specified, errors matching either are retried (OR logic)."""
    config = RetryStrategyConfig(
        retryable_errors=["timeout"], retryable_error_types=[ValueError]
    )
    strategy = create_retry_strategy(config)

    # Should retry on message match
    error1 = Exception("connection timeout")
    decision1 = strategy(error1, 1)
    assert decision1.should_retry is True

    # Should retry on type match
    error2 = ValueError("some value error")
    decision2 = strategy(error2, 1)
    assert decision2.should_retry is True

    # Should NOT retry when neither matches
    error3 = RuntimeError("runtime error")
    decision3 = strategy(error3, 1)
    assert decision3.should_retry is False


def test_empty_retryable_errors_with_types():
    """Test that empty retryable_errors list with types specified only retries matching types."""
    config = RetryStrategyConfig(
        retryable_errors=[], retryable_error_types=[ValueError]
    )
    strategy = create_retry_strategy(config)

    # Should retry matching type
    error1 = ValueError("value error")
    decision1 = strategy(error1, 1)
    assert decision1.should_retry is True

    # Should NOT retry non-matching type
    error2 = Exception("some error")
    decision2 = strategy(error2, 1)
    assert decision2.should_retry is False


def test_empty_retryable_error_types_with_errors():
    """Test that empty retryable_error_types list with errors specified only retries matching messages."""
    config = RetryStrategyConfig(retryable_errors=["timeout"], retryable_error_types=[])
    strategy = create_retry_strategy(config)

    # Should retry matching message
    error1 = Exception("connection timeout")
    decision1 = strategy(error1, 1)
    assert decision1.should_retry is True

    # Should NOT retry non-matching message
    error2 = Exception("permission denied")
    decision2 = strategy(error2, 1)
    assert decision2.should_retry is False


# endregion


# region Edge Cases Tests


def test_none_config():
    """Test behavior when config is None."""
    strategy = create_retry_strategy(None)
    error = Exception("test error")
    decision = strategy(error, 1)
    assert decision.should_retry is True
    assert decision.delay_seconds >= 1


def test_zero_backoff_rate():
    """Test behavior with zero backoff rate."""
    config = RetryStrategyConfig(
        initial_delay=Duration.from_seconds(5),
        backoff_rate=0,
        jitter_strategy=JitterStrategy.NONE,
    )
    strategy = create_retry_strategy(config)

    error = Exception("test error")
    decision = strategy(error, 1)
    # 5 * (0^0) = 5 * 1 = 5
    assert decision.delay_seconds == 5


def test_fractional_backoff_rate():
    """Test behavior with fractional backoff rate."""
    config = RetryStrategyConfig(
        initial_delay=Duration.from_seconds(8),
        backoff_rate=0.5,
        jitter_strategy=JitterStrategy.NONE,
    )
    strategy = create_retry_strategy(config)

    error = Exception("test error")
    decision = strategy(error, 2)
    # 8 * (0.5^1) = 4
    assert decision.delay_seconds == 4


def test_empty_retryable_errors_list():
    """Test behavior with empty retryable errors list."""
    config = RetryStrategyConfig(retryable_errors=[])
    strategy = create_retry_strategy(config)

    error = Exception("test error")
    decision = strategy(error, 1)
    assert decision.should_retry is False


def test_multiple_error_patterns():
    """Test multiple error patterns matching."""
    config = RetryStrategyConfig(
        retryable_errors=["timeout", re.compile(r"network.*error")]
    )
    strategy = create_retry_strategy(config)

    # Test string match
    error1 = Exception("connection timeout")
    decision1 = strategy(error1, 1)
    assert decision1.should_retry is True

    # Test regex match
    error2 = Exception("network connection error")
    decision2 = strategy(error2, 1)
    assert decision2.should_retry is True


def test_mixed_error_types_and_patterns():
    """Test combination of error types and patterns."""
    config = RetryStrategyConfig(
        retryable_errors=["timeout"], retryable_error_types=[ValueError]
    )
    strategy = create_retry_strategy(config)

    # Should retry on ValueError even without message match
    error = ValueError("some value error")
    decision = strategy(error, 1)
    assert decision.should_retry is True


# endregion


# region Backoff Strategy Tests


def test_exponential_backoff_strategy():
    """Test EXPONENTIAL backoff strategy calculates delay correctly."""
    strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    # initial=5, rate=2, attempt=3: 5 * 2^(3-1) = 5 * 4 = 20
    result: float = strategy.calculate_base_delay(
        initial_delay_seconds=5,
        backoff_rate=2.0,
        attempts_made=3,
        max_delay_seconds=300,
    )
    assert result == 20.0


def test_linear_backoff_strategy():
    """Test LINEAR backoff strategy calculates delay correctly."""
    strategy: BackoffStrategy = BackoffStrategy.LINEAR
    # initial=5, attempt=1: 5 * 1 = 5
    result_1: float = strategy.calculate_base_delay(
        initial_delay_seconds=5,
        backoff_rate=2.0,
        attempts_made=1,
        max_delay_seconds=300,
    )
    assert result_1 == 5.0

    # initial=5, attempt=3: 5 * 3 = 15
    result_3: float = strategy.calculate_base_delay(
        initial_delay_seconds=5,
        backoff_rate=2.0,
        attempts_made=3,
        max_delay_seconds=300,
    )
    assert result_3 == 15.0


def test_fixed_backoff_strategy():
    """Test FIXED backoff strategy returns constant delay."""
    strategy: BackoffStrategy = BackoffStrategy.FIXED
    # Always returns initial_delay regardless of attempt number
    result_1: float = strategy.calculate_base_delay(
        initial_delay_seconds=5,
        backoff_rate=2.0,
        attempts_made=1,
        max_delay_seconds=300,
    )
    result_5: float = strategy.calculate_base_delay(
        initial_delay_seconds=5,
        backoff_rate=2.0,
        attempts_made=5,
        max_delay_seconds=300,
    )
    assert result_1 == 5.0
    assert result_5 == 5.0


def test_backoff_strategy_respects_max_delay():
    """Test all backoff strategies cap delay at max_delay_seconds."""
    max_delay: int = 10

    # EXPONENTIAL: 5 * 2^4 = 80, capped to 10
    exp_result: float = BackoffStrategy.EXPONENTIAL.calculate_base_delay(
        initial_delay_seconds=5,
        backoff_rate=2.0,
        attempts_made=5,
        max_delay_seconds=max_delay,
    )
    assert exp_result == max_delay

    # LINEAR: 5 * 5 = 25, capped to 10
    lin_result: float = BackoffStrategy.LINEAR.calculate_base_delay(
        initial_delay_seconds=5,
        backoff_rate=2.0,
        attempts_made=5,
        max_delay_seconds=max_delay,
    )
    assert lin_result == max_delay

    # FIXED: 5, not capped (already under)
    fix_result: float = BackoffStrategy.FIXED.calculate_base_delay(
        initial_delay_seconds=5,
        backoff_rate=2.0,
        attempts_made=5,
        max_delay_seconds=max_delay,
    )
    assert fix_result == 5.0


def test_default_backoff_strategy_is_exponential():
    """Test RetryStrategyConfig defaults to EXPONENTIAL backoff."""
    config: RetryStrategyConfig = RetryStrategyConfig()
    assert config.backoff_strategy == BackoffStrategy.EXPONENTIAL


# endregion


# region New Preset Tests


def test_fixed_wait_preset():
    """Test fixed_wait preset uses constant delay and allows retries within max attempts."""
    strategy = RetryPresets.fixed_wait()
    error: Exception = Exception("test error")

    # Should retry within max attempts
    decision = strategy(error, 1)
    assert decision.should_retry is True

    # Should not retry after max attempts
    decision = strategy(error, 5)
    assert decision.should_retry is False


def test_fixed_wait_preset_constant_delay():
    """Test fixed_wait preset returns constant delay across attempts."""
    strategy = RetryPresets.fixed_wait()
    error: Exception = Exception("test error")

    # FIXED strategy + NONE jitter = constant delay of 5s for all attempts
    decision_1 = strategy(error, 1)
    decision_2 = strategy(error, 2)
    decision_3 = strategy(error, 3)
    assert decision_1.delay_seconds == 5
    assert decision_2.delay_seconds == 5
    assert decision_3.delay_seconds == 5


def test_linear_backoff_preset():
    """Test linear_backoff preset allows retries within max attempts."""
    strategy = RetryPresets.linear_backoff()
    error: Exception = Exception("test error")

    # Should retry within max attempts
    decision = strategy(error, 1)
    assert decision.should_retry is True

    # Should not retry after max attempts
    decision = strategy(error, 5)
    assert decision.should_retry is False


@patch("random.random")
def test_linear_backoff_preset_increasing_delay(mock_random):
    """Test linear_backoff preset has linearly increasing delay."""
    mock_random.return_value = 1.0  # Max jitter to get predictable values
    strategy = RetryPresets.linear_backoff()
    error: Exception = Exception("test error")

    # LINEAR: initial_delay * attempts_made, with FULL jitter (random=1.0 means full delay)
    # attempt 1: 5 * 1 = 5
    decision_1 = strategy(error, 1)
    assert decision_1.delay_seconds == 5

    # attempt 2: 5 * 2 = 10
    decision_2 = strategy(error, 2)
    assert decision_2.delay_seconds == 10

    # attempt 3: 5 * 3 = 15
    decision_3 = strategy(error, 3)
    assert decision_3.delay_seconds == 15


def test_slow_preset():
    """Test slow preset allows retries within max attempts."""
    strategy = RetryPresets.slow()
    error: Exception = Exception("test error")

    # Should retry within max attempts
    decision = strategy(error, 1)
    assert decision.should_retry is True

    # Should not retry after max attempts
    decision = strategy(error, 8)
    assert decision.should_retry is False


@patch("random.random")
def test_slow_preset_high_initial_delay(mock_random):
    """Test slow preset starts with high initial delay."""
    mock_random.return_value = 1.0  # Max jitter
    strategy = RetryPresets.slow()
    error: Exception = Exception("test error")

    # Attempt 1: 30 * (2^0) = 30, full jitter with random=1.0 = 30
    decision = strategy(error, 1)
    assert decision.delay_seconds == 30


# endregion


# region Backoff Strategy Integration Tests


@patch("random.random")
def test_fixed_backoff_in_create_retry_strategy(mock_random):
    """Test FIXED backoff works end-to-end through create_retry_strategy."""
    mock_random.return_value = 0.5
    config: RetryStrategyConfig = RetryStrategyConfig(
        initial_delay=Duration.from_seconds(10),
        backoff_strategy=BackoffStrategy.FIXED,
        jitter_strategy=JitterStrategy.FULL,
    )
    strategy = create_retry_strategy(config)

    error: Exception = Exception("test error")
    # FIXED = 10, FULL jitter with 0.5 = 5
    decision_1 = strategy(error, 1)
    assert decision_1.delay_seconds == 5

    # Same delay at attempt 2 (FIXED ignores attempt number)
    decision_2 = strategy(error, 2)
    assert decision_2.delay_seconds == 5


@patch("random.random")
def test_linear_backoff_in_create_retry_strategy(mock_random):
    """Test LINEAR backoff works end-to-end through create_retry_strategy."""
    mock_random.return_value = 1.0  # Full jitter = full delay
    config: RetryStrategyConfig = RetryStrategyConfig(
        max_attempts=5,
        initial_delay=Duration.from_seconds(5),
        backoff_strategy=BackoffStrategy.LINEAR,
        jitter_strategy=JitterStrategy.FULL,
    )
    strategy = create_retry_strategy(config)

    error: Exception = Exception("test error")
    # LINEAR: 5 * 1 = 5, jitter(1.0) = 5
    decision_1 = strategy(error, 1)
    assert decision_1.delay_seconds == 5

    # LINEAR: 5 * 2 = 10, jitter(1.0) = 10
    decision_2 = strategy(error, 2)
    assert decision_2.delay_seconds == 10

    # LINEAR: 5 * 3 = 15, jitter(1.0) = 15
    decision_3 = strategy(error, 3)
    assert decision_3.delay_seconds == 15


def test_linear_backoff_respects_max_delay():
    """Test LINEAR backoff caps delay at max_delay."""
    config: RetryStrategyConfig = RetryStrategyConfig(
        max_attempts=5,
        initial_delay=Duration.from_seconds(10),
        max_delay=Duration.from_seconds(25),
        backoff_strategy=BackoffStrategy.LINEAR,
        jitter_strategy=JitterStrategy.NONE,
    )
    strategy = create_retry_strategy(config)

    error: Exception = Exception("test error")
    # LINEAR: 10 * 3 = 30, capped to 25
    decision = strategy(error, 3)
    assert decision.delay_seconds == 25


# endregion
