"""Tests for retry strategies and jitter implementations."""

import re
from unittest.mock import patch

import pytest

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.retries import (
    JitterStrategy,
    LinearRetryStrategyConfig,
    RetryDecision,
    RetryPresets,
    RetryStrategyConfig,
    create_linear_retry_strategy,
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


# region create_linear_retry_strategy


@patch("random.random")
def test_linear_retry_strategy_uses_additive_formula(mock_random):
    """Default config yields additive delays of 1s, 2s, 3s, 4s, 5s."""
    mock_random.return_value = 1.0  # FULL jitter at the upper bound keeps the base
    strategy = create_linear_retry_strategy()

    delays = [
        strategy(Exception("e"), attempt).delay_seconds for attempt in range(1, 6)
    ]

    assert delays == [1, 2, 3, 4, 5]


def test_linear_retry_strategy_stops_at_max_attempts():
    """No retry once attempts_made reaches max_attempts."""
    strategy = create_linear_retry_strategy(LinearRetryStrategyConfig(max_attempts=3))

    assert strategy(Exception("e"), 1).should_retry is True
    assert strategy(Exception("e"), 2).should_retry is True
    assert strategy(Exception("e"), 3).should_retry is False


def test_linear_retry_strategy_respects_custom_initial_and_increment():
    """Custom initial_delay and increment shift the additive sequence."""
    strategy = create_linear_retry_strategy(
        LinearRetryStrategyConfig(
            max_attempts=10,
            initial_delay=Duration.from_seconds(2),
            increment=Duration.from_seconds(3),
            jitter_strategy=JitterStrategy.NONE,
        )
    )

    delays = [
        strategy(Exception("e"), attempt).delay_seconds for attempt in range(1, 5)
    ]

    # 2 + 3*0, 2 + 3*1, 2 + 3*2, 2 + 3*3
    assert delays == [2, 5, 8, 11]


def test_linear_retry_strategy_caps_at_max_delay():
    """The additive delay is capped at max_delay before jitter."""
    strategy = create_linear_retry_strategy(
        LinearRetryStrategyConfig(
            max_attempts=10,
            initial_delay=Duration.from_seconds(10),
            increment=Duration.from_seconds(10),
            max_delay=Duration.from_seconds(25),
            jitter_strategy=JitterStrategy.NONE,
        )
    )

    # 10, 20, then capped at 25 for the third attempt (would be 30).
    delays = [
        strategy(Exception("e"), attempt).delay_seconds for attempt in range(1, 4)
    ]
    assert delays == [10, 20, 25]


@patch("random.random")
def test_linear_retry_strategy_applies_jitter(mock_random):
    """FULL jitter scales the additive base delay by random()."""
    mock_random.return_value = 0.5
    strategy = create_linear_retry_strategy(
        LinearRetryStrategyConfig(
            initial_delay=Duration.from_seconds(4),
            increment=Duration.from_seconds(4),
            jitter_strategy=JitterStrategy.FULL,
        )
    )

    # base = 4 + 4*1 = 8, full jitter = 0.5 * 8 = 4
    assert strategy(Exception("e"), 2).delay_seconds == 4


def test_linear_retry_strategy_filters_by_error_message():
    """Only errors matching retryable_errors are retried."""
    strategy = create_linear_retry_strategy(
        LinearRetryStrategyConfig(retryable_errors=["timeout"])
    )

    assert strategy(Exception("connection timeout"), 1).should_retry is True
    assert strategy(Exception("permission denied"), 1).should_retry is False


def test_linear_retry_strategy_filters_by_error_type():
    """Only errors matching retryable_error_types are retried."""
    strategy = create_linear_retry_strategy(
        LinearRetryStrategyConfig(retryable_error_types=[ValueError])
    )

    assert strategy(ValueError("bad"), 1).should_retry is True
    assert strategy(KeyError("missing"), 1).should_retry is False


# endregion


# region RetryPresets.linear / RetryPresets.fixed


def test_retry_presets_linear_matches_js_defaults():
    """RetryPresets.linear() yields 1s, 2s, 3s, 4s, 5s and stops after 6 attempts."""
    strategy = RetryPresets.linear()

    delays = [
        strategy(Exception("e"), attempt).delay_seconds for attempt in range(1, 6)
    ]
    assert delays == [1, 2, 3, 4, 5]
    assert strategy(Exception("e"), 6).should_retry is False


def test_retry_presets_fixed_uses_default_interval():
    """RetryPresets.fixed() defaults to a 5s constant delay with no jitter."""
    strategy = RetryPresets.fixed()

    for attempt in range(1, 5):
        decision = strategy(Exception("e"), attempt)
        assert decision.should_retry is True
        assert decision.delay_seconds == 5


def test_retry_presets_fixed_respects_custom_interval():
    """A caller-supplied interval is used as the constant delay."""
    strategy = RetryPresets.fixed(interval=Duration.from_seconds(12))

    assert strategy(Exception("e"), 1).delay_seconds == 12
    assert strategy(Exception("e"), 3).delay_seconds == 12


def test_retry_presets_fixed_stops_at_max_attempts():
    """RetryPresets.fixed() stops retrying after 5 attempts."""
    strategy = RetryPresets.fixed()

    assert strategy(Exception("e"), 4).should_retry is True
    assert strategy(Exception("e"), 5).should_retry is False


# endregion
