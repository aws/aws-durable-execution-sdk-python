"""Tests for retry strategies and jitter implementations."""

import re
from unittest.mock import patch

import pytest

from aws_durable_execution_sdk_python.config import Duration
from aws_durable_execution_sdk_python.retries import (
    JitterStrategy,
    RetryDecision,
    RetryPresets,
    RetryStrategyConfig,
    create_retry_strategy,
)


class TestJitterStrategy:
    """Test jitter strategy implementations."""

    def test_none_jitter_returns_zero(self):
        """Test NONE jitter always returns 0."""
        strategy = JitterStrategy.NONE
        assert strategy.compute_jitter(10) == 0
        assert strategy.compute_jitter(100) == 0

    @patch("random.random")
    def test_full_jitter_range(self, mock_random):
        """Test FULL jitter returns value between 0 and delay."""
        mock_random.return_value = 0.5
        strategy = JitterStrategy.FULL
        delay = 10
        result = strategy.compute_jitter(delay)
        assert result == 5.0  # 0.5 * 10

    @patch("random.random")
    def test_half_jitter_range(self, mock_random):
        """Test HALF jitter returns value between 0.5 and 1.0 (multiplier)."""
        mock_random.return_value = 0.5
        strategy = JitterStrategy.HALF
        result = strategy.compute_jitter(10)
        assert result == 7.5  # 10 * (0.5 + 0.5*0.5)

    @patch("random.random")
    def test_half_jitter_boundary_values(self, mock_random):
        """Test HALF jitter boundary values."""
        strategy = JitterStrategy.HALF

        # Minimum value (random = 0)
        mock_random.return_value = 0.0
        result = strategy.compute_jitter(100)
        assert result == 50

        # Maximum value (random = 1)
        mock_random.return_value = 1.0
        result = strategy.compute_jitter(100)
        assert result == 100

    def test_invalid_jitter_strategy(self):
        """Test behavior with invalid jitter strategy."""
        # Create an invalid enum value by bypassing normal construction
        invalid_strategy = "INVALID"

        # This should raise an exception or return None
        with pytest.raises((ValueError, AttributeError)):
            JitterStrategy(invalid_strategy).compute_jitter(10)


class TestRetryDecision:
    """Test RetryDecision factory methods."""

    def test_retry_factory(self):
        """Test retry factory method."""
        decision = RetryDecision.retry(Duration.from_seconds(30))
        assert decision.should_retry is True
        assert decision.delay_seconds == 30

    def test_no_retry_factory(self):
        """Test no_retry factory method."""
        decision = RetryDecision.no_retry()
        assert decision.should_retry is False
        assert decision.delay_seconds == 0


class TestRetryStrategyConfig:
    """Test RetryStrategyConfig defaults and behavior."""

    def test_default_config(self):
        """Test default configuration values."""
        config = RetryStrategyConfig()
        assert config.max_attempts == 3
        assert config.initial_delay_seconds == 5
        assert config.max_delay_seconds == 300
        assert config.backoff_rate == 2.0
        assert config.jitter_strategy == JitterStrategy.FULL
        assert config.retryable_errors is None
        assert config.retryable_error_types is None


class TestCreateRetryStrategy:
    """Test retry strategy creation and behavior."""

    def test_max_attempts_exceeded(self):
        """Test strategy returns no_retry when max attempts exceeded."""
        config = RetryStrategyConfig(max_attempts=2)
        strategy = create_retry_strategy(config)

        error = Exception("test error")
        decision = strategy(error, 2)
        assert decision.should_retry is False

    def test_retryable_error_message_string(self):
        """Test retry based on error message string match."""
        config = RetryStrategyConfig(retryable_errors=["timeout"])
        strategy = create_retry_strategy(config)

        error = Exception("connection timeout")
        decision = strategy(error, 1)
        assert decision.should_retry is True

    def test_retryable_error_message_regex(self):
        """Test retry based on error message regex match."""
        config = RetryStrategyConfig(retryable_errors=[re.compile(r"timeout|error")])
        strategy = create_retry_strategy(config)

        error = Exception("network timeout occurred")
        decision = strategy(error, 1)
        assert decision.should_retry is True

    def test_retryable_error_type(self):
        """Test retry based on error type."""
        config = RetryStrategyConfig(retryable_error_types=[ValueError])
        strategy = create_retry_strategy(config)

        error = ValueError("invalid value")
        decision = strategy(error, 1)
        assert decision.should_retry is True

    def test_non_retryable_error(self):
        """Test no retry for non-retryable error."""
        config = RetryStrategyConfig(retryable_errors=["timeout"])
        strategy = create_retry_strategy(config)

        error = Exception("permission denied")
        decision = strategy(error, 1)
        assert decision.should_retry is False

    @patch("random.random")
    def test_exponential_backoff_calculation(self, mock_random):
        """Test exponential backoff delay calculation."""
        mock_random.return_value = 0.5
        config = RetryStrategyConfig(
            initial_delay=Duration.from_seconds(2),
            backoff_rate=2.0,
            jitter_strategy=JitterStrategy.FULL,
        )
        strategy = create_retry_strategy(config)

        error = Exception("test error")

        # First attempt: 2 * (2^0) = 2, jitter adds 1, total = 3
        decision = strategy(error, 1)
        assert decision.delay_seconds == 3

        # Second attempt: 2 * (2^1) = 4, jitter adds 2, total = 6
        decision = strategy(error, 2)
        assert decision.delay_seconds == 6

    def test_max_delay_cap(self):
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

    def test_minimum_delay_one_second(self):
        """Test delay is at least 1 second."""
        config = RetryStrategyConfig(
            initial_delay=Duration.from_seconds(0), jitter_strategy=JitterStrategy.NONE
        )
        strategy = create_retry_strategy(config)

        error = Exception("test error")
        decision = strategy(error, 1)
        assert decision.delay_seconds == 1

    def test_delay_ceiling_applied(self):
        """Test delay is rounded up using math.ceil."""
        with patch("random.random", return_value=0.3):
            config = RetryStrategyConfig(
                initial_delay=Duration.from_seconds(3),
                jitter_strategy=JitterStrategy.FULL,
            )
            strategy = create_retry_strategy(config)

            error = Exception("test error")
            decision = strategy(error, 1)
            # 3 + (0.3 * 3) = 3.9, ceil(3.9) = 4
            assert decision.delay_seconds == 4


class TestRetryPresets:
    """Test predefined retry presets."""

    def test_none_preset(self):
        """Test none preset allows no retries."""
        strategy = RetryPresets.none()
        error = Exception("test error")

        decision = strategy(error, 1)
        assert decision.should_retry is False

    def test_default_preset_config(self):
        """Test default preset configuration."""
        strategy = RetryPresets.default()
        error = Exception("test error")

        # Should retry within max attempts
        decision = strategy(error, 1)
        assert decision.should_retry is True

        # Should not retry after max attempts
        decision = strategy(error, 6)
        assert decision.should_retry is False

    def test_transient_preset_config(self):
        """Test transient preset configuration."""
        strategy = RetryPresets.transient()
        error = Exception("test error")

        # Should retry within max attempts
        decision = strategy(error, 1)
        assert decision.should_retry is True

        # Should not retry after max attempts
        decision = strategy(error, 3)
        assert decision.should_retry is False

    def test_resource_availability_preset(self):
        """Test resource availability preset allows longer retries."""
        strategy = RetryPresets.resource_availability()
        error = Exception("test error")

        # Should retry within max attempts
        decision = strategy(error, 1)
        assert decision.should_retry is True

        # Should not retry after max attempts
        decision = strategy(error, 5)
        assert decision.should_retry is False

    def test_critical_preset_config(self):
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
    def test_critical_preset_no_jitter(self, mock_random):
        """Test critical preset uses no jitter."""
        mock_random.return_value = 0.5  # Should be ignored
        strategy = RetryPresets.critical()
        error = Exception("test error")

        decision = strategy(error, 1)
        # With no jitter: 1 * (1.5^0) = 1
        assert decision.delay_seconds == 1


class TestJitterIntegration:
    """Test jitter integration with retry strategies."""

    @patch("random.random")
    def test_full_jitter_integration(self, mock_random):
        """Test full jitter integration in retry strategy."""
        mock_random.return_value = 0.8
        config = RetryStrategyConfig(
            initial_delay=Duration.from_seconds(10), jitter_strategy=JitterStrategy.FULL
        )
        strategy = create_retry_strategy(config)

        error = Exception("test error")
        decision = strategy(error, 1)
        # 10 + (0.8 * 10) = 18
        assert decision.delay_seconds == 18

    @patch("random.random")
    def test_half_jitter_integration(self, mock_random):
        """Test half jitter integration in retry strategy."""
        mock_random.return_value = 0.6
        config = RetryStrategyConfig(
            initial_delay=Duration.from_seconds(10), jitter_strategy=JitterStrategy.HALF
        )
        strategy = create_retry_strategy(config)

        error = Exception("test error")
        decision = strategy(error, 1)
        # 10 + 10*(0.6 * 0.5 + 0.5) = 18
        assert decision.delay_seconds == 18

    @patch("random.random")
    def test_half_jitter_integration_corrected(self, mock_random):
        """Test half jitter with corrected understanding of implementation."""
        mock_random.return_value = 0.0  # Minimum jitter
        config = RetryStrategyConfig(
            initial_delay=Duration.from_seconds(10), jitter_strategy=JitterStrategy.HALF
        )
        strategy = create_retry_strategy(config)

        error = Exception("test error")
        decision = strategy(error, 1)
        # 10 + 10 * 0.5 = 15
        assert decision.delay_seconds == 15

    def test_none_jitter_integration(self):
        """Test no jitter integration in retry strategy."""
        config = RetryStrategyConfig(
            initial_delay=Duration.from_seconds(10), jitter_strategy=JitterStrategy.NONE
        )
        strategy = create_retry_strategy(config)

        error = Exception("test error")
        decision = strategy(error, 1)
        assert decision.delay_seconds == 10


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_none_config(self):
        """Test behavior when config is None."""
        strategy = create_retry_strategy(None)
        error = Exception("test error")
        decision = strategy(error, 1)
        assert decision.should_retry is True
        assert decision.delay_seconds >= 1

    def test_zero_backoff_rate(self):
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

    def test_fractional_backoff_rate(self):
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

    def test_empty_retryable_errors_list(self):
        """Test behavior with empty retryable errors list."""
        config = RetryStrategyConfig(retryable_errors=[])
        strategy = create_retry_strategy(config)

        error = Exception("test error")
        decision = strategy(error, 1)
        assert decision.should_retry is False

    def test_multiple_error_patterns(self):
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

    def test_mixed_error_types_and_patterns(self):
        """Test combination of error types and patterns."""
        config = RetryStrategyConfig(
            retryable_errors=["timeout"], retryable_error_types=[ValueError]
        )
        strategy = create_retry_strategy(config)

        # Should retry on ValueError even without message match
        error = ValueError("some value error")
        decision = strategy(error, 1)
        assert decision.should_retry is True

    def test_only_retryable_error_types_specified(self):
        """Test that specifying only retryable_error_types doesn't retry all errors."""
        class TimeoutError(Exception):
            pass

        config = RetryStrategyConfig(retryable_error_types=[TimeoutError])
        strategy = create_retry_strategy(config)

        # Should retry: matches error type
        timeout_error = TimeoutError("Operation timed out")
        decision = strategy(timeout_error, 1)
        assert decision.should_retry is True

        # Should NOT retry: generic Error doesn't match type
        generic_error = Exception("Some random error")
        decision = strategy(generic_error, 1)
        assert decision.should_retry is False

    def test_only_retryable_errors_specified(self):
        """Test that specifying only retryable_errors doesn't retry all errors."""
        config = RetryStrategyConfig(retryable_errors=[re.compile(r"timeout")])
        strategy = create_retry_strategy(config)

        # Should retry: matches pattern
        timeout_error = Exception("Request timeout")
        decision = strategy(timeout_error, 1)
        assert decision.should_retry is True

        # Should NOT retry: doesn't match pattern
        other_error = Exception("Invalid input")
        decision = strategy(other_error, 1)
        assert decision.should_retry is False

    def test_combine_retryable_errors_and_types_with_or_logic(self):
        """Test that retryable_errors and retryable_error_types use OR logic."""
        class NetworkError(Exception):
            pass

        config = RetryStrategyConfig(
            retryable_errors=[re.compile(r"timeout")],
            retryable_error_types=[NetworkError],
        )
        strategy = create_retry_strategy(config)

        # Should retry: matches error type
        network_error = NetworkError("Connection failed")
        decision = strategy(network_error, 1)
        assert decision.should_retry is True

        # Should retry: matches error message pattern
        timeout_error = Exception("Request timeout")
        decision = strategy(timeout_error, 1)
        assert decision.should_retry is True

        # Should NOT retry: matches neither
        other_error = Exception("Invalid input")
        decision = strategy(other_error, 1)
        assert decision.should_retry is False

    def test_default_retries_all_errors_when_no_filters_specified(self):
        """Test that default behavior retries all errors when no filters specified."""
        config = RetryStrategyConfig(max_attempts=5)
        strategy = create_retry_strategy(config)

        # Should retry: default regex matches all errors
        error1 = Exception("Network error")
        decision = strategy(error1, 1)
        assert decision.should_retry is True

        error2 = ValueError("Validation error")
        decision = strategy(error2, 1)
        assert decision.should_retry is True

        error3 = RuntimeError("Random error")
        decision = strategy(error3, 1)
        assert decision.should_retry is True
