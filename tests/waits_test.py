"""Tests for wait strategies and wait_for_condition implementations."""

from unittest.mock import patch

from aws_durable_execution_sdk_python.config import Duration, JitterStrategy
from aws_durable_execution_sdk_python.serdes import JsonSerDes
from aws_durable_execution_sdk_python.waits import (
    WaitDecision,
    WaitForConditionConfig,
    WaitForConditionDecision,
    WaitStrategyConfig,
    create_wait_strategy,
)


class TestWaitDecision:
    """Test WaitDecision factory methods."""

    def test_wait_factory(self):
        """Test wait factory method."""
        decision = WaitDecision.wait(Duration.from_seconds(30))
        assert decision.should_wait is True
        assert decision.delay_seconds == 30

    def test_no_wait_factory(self):
        """Test no_wait factory method."""
        decision = WaitDecision.no_wait()
        assert decision.should_wait is False
        assert decision.delay_seconds == 0


class TestWaitForConditionDecision:
    """Test WaitForConditionDecision factory methods."""

    def test_continue_waiting_factory(self):
        """Test continue_waiting factory method."""
        decision = WaitForConditionDecision.continue_waiting(Duration.from_seconds(45))
        assert decision.should_continue is True
        assert decision.delay_seconds == 45

    def test_stop_polling_factory(self):
        """Test stop_polling factory method."""
        decision = WaitForConditionDecision.stop_polling()
        assert decision.should_continue is False
        assert decision.delay_seconds == 0


class TestWaitStrategyConfig:
    """Test WaitStrategyConfig defaults and behavior."""

    def test_default_config(self):
        """Test default configuration values."""
        config = WaitStrategyConfig(should_continue_polling=lambda x: True)
        assert config.max_attempts == 60
        assert config.initial_delay_seconds == 5
        assert config.max_delay_seconds == 300
        assert config.backoff_rate == 1.5
        assert config.jitter_strategy == JitterStrategy.FULL
        assert config.timeout_seconds is None


class TestCreateWaitStrategy:
    """Test wait strategy creation and behavior."""

    def test_condition_met_returns_no_wait(self):
        """Test strategy returns no_wait when condition is met."""
        config = WaitStrategyConfig(should_continue_polling=lambda x: False)
        strategy = create_wait_strategy(config)

        result = "completed"
        decision = strategy(result, 1)
        assert decision.should_wait is False

    def test_max_attempts_exceeded(self):
        """Test strategy returns no_wait when max attempts exceeded."""
        config = WaitStrategyConfig(
            should_continue_polling=lambda x: True, max_attempts=5
        )
        strategy = create_wait_strategy(config)

        result = "pending"
        decision = strategy(result, 5)
        assert decision.should_wait is False

    def test_should_continue_polling(self):
        """Test strategy continues when condition not met."""
        config = WaitStrategyConfig(should_continue_polling=lambda x: x == "pending")
        strategy = create_wait_strategy(config)

        result = "pending"
        decision = strategy(result, 1)
        assert decision.should_wait is True

    @patch("random.random")
    def test_exponential_backoff_calculation(self, mock_random):
        """Test exponential backoff delay calculation."""
        mock_random.return_value = 0.5
        config = WaitStrategyConfig(
            should_continue_polling=lambda x: True,
            initial_delay=Duration.from_seconds(2),
            backoff_rate=2.0,
            jitter_strategy=JitterStrategy.FULL,
        )
        strategy = create_wait_strategy(config)

        result = "pending"

        # First attempt: 2 * (2^0) = 2, jitter adds 1, total = 3
        decision = strategy(result, 1)
        assert decision.delay_seconds == 3

        # Second attempt: 2 * (2^1) = 4, jitter adds 2, total = 6
        decision = strategy(result, 2)
        assert decision.delay_seconds == 6

    def test_max_delay_cap(self):
        """Test delay is capped at max_delay_seconds."""
        config = WaitStrategyConfig(
            should_continue_polling=lambda x: True,
            initial_delay=Duration.from_seconds(100),
            max_delay=Duration.from_seconds(50),
            backoff_rate=2.0,
            jitter_strategy=JitterStrategy.NONE,
        )
        strategy = create_wait_strategy(config)

        result = "pending"
        decision = strategy(result, 2)  # Would be 200 without cap
        assert decision.delay_seconds == 50

    def test_minimum_delay_one_second(self):
        """Test delay is at least 1 second."""
        config = WaitStrategyConfig(
            should_continue_polling=lambda x: True,
            initial_delay=Duration.from_seconds(0),
            jitter_strategy=JitterStrategy.NONE,
        )
        strategy = create_wait_strategy(config)

        result = "pending"
        decision = strategy(result, 1)
        assert decision.delay_seconds == 1

    @patch("random.random")
    def test_full_jitter_integration(self, mock_random):
        """Test full jitter integration in wait strategy."""
        mock_random.return_value = 0.8
        config = WaitStrategyConfig(
            should_continue_polling=lambda x: True,
            initial_delay=Duration.from_seconds(10),
            jitter_strategy=JitterStrategy.FULL,
        )
        strategy = create_wait_strategy(config)

        result = "pending"
        decision = strategy(result, 1)
        # 10 + (0.8 * 10) = 18
        assert decision.delay_seconds == 18

    @patch("random.random")
    def test_half_jitter_integration(self, mock_random):
        """Test half jitter integration in wait strategy."""
        mock_random.return_value = 0.0  # Minimum jitter
        config = WaitStrategyConfig(
            should_continue_polling=lambda x: True,
            initial_delay=Duration.from_seconds(10),
            jitter_strategy=JitterStrategy.HALF,
        )
        strategy = create_wait_strategy(config)

        result = "pending"
        decision = strategy(result, 1)
        # base: 10, jitter: 10 * (0.5 + 0.0 * 0.5) = 5, total: 10 + 5 = 15
        assert decision.delay_seconds == 15

    def test_none_jitter_integration(self):
        """Test no jitter integration in wait strategy."""
        config = WaitStrategyConfig(
            should_continue_polling=lambda x: True,
            initial_delay=Duration.from_seconds(10),
            jitter_strategy=JitterStrategy.NONE,
        )
        strategy = create_wait_strategy(config)

        result = "pending"
        decision = strategy(result, 1)
        assert decision.delay_seconds == 10


class TestWaitStrategyWithStatefulConditions:
    """Test wait strategy with stateful condition checks."""

    def test_stateful_condition_check(self):
        """Test condition check with stateful result."""

        class State:
            def __init__(self, count):
                self.count = count

        config = WaitStrategyConfig(
            should_continue_polling=lambda s: s.count < 3, max_attempts=10
        )
        strategy = create_wait_strategy(config)

        # Should continue when count < 3
        state1 = State(1)
        decision1 = strategy(state1, 1)
        assert decision1.should_wait is True

        # Should stop when count >= 3
        state2 = State(3)
        decision2 = strategy(state2, 1)
        assert decision2.should_wait is False

    def test_complex_condition_logic(self):
        """Test complex condition logic."""

        def complex_condition(result):
            return result.get("status") == "pending" and result.get("retries", 0) < 5

        config = WaitStrategyConfig(should_continue_polling=complex_condition)
        strategy = create_wait_strategy(config)

        # Should continue
        result1 = {"status": "pending", "retries": 2}
        decision1 = strategy(result1, 1)
        assert decision1.should_wait is True

        # Should stop - status changed
        result2 = {"status": "completed", "retries": 2}
        decision2 = strategy(result2, 1)
        assert decision2.should_wait is False

        # Should stop - retries exceeded
        result3 = {"status": "pending", "retries": 5}
        decision3 = strategy(result3, 1)
        assert decision3.should_wait is False


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_zero_backoff_rate(self):
        """Test behavior with zero backoff rate."""
        config = WaitStrategyConfig(
            should_continue_polling=lambda x: True,
            initial_delay=Duration.from_seconds(5),
            backoff_rate=0,
            jitter_strategy=JitterStrategy.NONE,
        )
        strategy = create_wait_strategy(config)

        result = "pending"
        decision = strategy(result, 1)
        # 5 * (0^0) = 5 * 1 = 5
        assert decision.delay_seconds == 5

    def test_fractional_backoff_rate(self):
        """Test behavior with fractional backoff rate."""
        config = WaitStrategyConfig(
            should_continue_polling=lambda x: True,
            initial_delay=Duration.from_seconds(8),
            backoff_rate=0.5,
            jitter_strategy=JitterStrategy.NONE,
        )
        strategy = create_wait_strategy(config)

        result = "pending"
        decision = strategy(result, 2)
        # 8 * (0.5^1) = 4
        assert decision.delay_seconds == 4

    def test_large_backoff_rate(self):
        """Test behavior with large backoff rate hits max delay."""
        config = WaitStrategyConfig(
            should_continue_polling=lambda x: True,
            initial_delay=Duration.from_seconds(10),
            max_delay=Duration.from_seconds(100),
            backoff_rate=10.0,
            jitter_strategy=JitterStrategy.NONE,
        )
        strategy = create_wait_strategy(config)

        result = "pending"
        decision = strategy(result, 3)
        # 10 * (10^2) = 1000, capped at 100
        assert decision.delay_seconds == 100

    def test_attempt_at_boundary(self):
        """Test behavior at max_attempts boundary."""
        config = WaitStrategyConfig(
            should_continue_polling=lambda x: True, max_attempts=3
        )
        strategy = create_wait_strategy(config)

        result = "pending"

        # At boundary - should not wait
        decision = strategy(result, 3)
        assert decision.should_wait is False

        # Just before boundary - should wait
        decision = strategy(result, 2)
        assert decision.should_wait is True

    def test_negative_delay_clamped_to_one(self):
        """Test negative delay is clamped to 1."""
        config = WaitStrategyConfig(
            should_continue_polling=lambda x: True,
            initial_delay=Duration.from_seconds(0),
            backoff_rate=0,
            jitter_strategy=JitterStrategy.NONE,
        )
        strategy = create_wait_strategy(config)

        result = "pending"
        decision = strategy(result, 1)
        assert decision.delay_seconds == 1

    @patch("random.random")
    def test_rounding_behavior(self, mock_random):
        """Test delay rounding behavior."""
        mock_random.return_value = 0.3
        config = WaitStrategyConfig(
            should_continue_polling=lambda x: True,
            initial_delay=Duration.from_seconds(3),
            jitter_strategy=JitterStrategy.FULL,
        )
        strategy = create_wait_strategy(config)

        result = "pending"
        decision = strategy(result, 1)
        # 3 + (0.3 * 3) = 3.9, round(3.9) = 4
        assert decision.delay_seconds == 4


class TestWaitForConditionConfig:
    """Test WaitForConditionConfig."""

    def test_config_creation(self):
        """Test creating WaitForConditionConfig."""

        def wait_strategy(state, attempt):
            return WaitForConditionDecision.continue_waiting(Duration.from_seconds(10))

        config = WaitForConditionConfig(
            wait_strategy=wait_strategy, initial_state={"count": 0}
        )

        assert config.wait_strategy is wait_strategy
        assert config.initial_state == {"count": 0}
        assert config.serdes is None

    def test_config_with_serdes(self):
        """Test WaitForConditionConfig with custom serdes."""

        def wait_strategy(state, attempt):
            return WaitForConditionDecision.stop_polling()

        serdes = JsonSerDes()
        config = WaitForConditionConfig(
            wait_strategy=wait_strategy, initial_state=0, serdes=serdes
        )

        assert config.serdes is serdes


class TestWaitStrategyCallableConditions:
    """Test wait strategy with various callable conditions."""

    def test_lambda_condition(self):
        """Test with lambda condition."""
        config = WaitStrategyConfig(should_continue_polling=lambda x: x < 10)
        strategy = create_wait_strategy(config)

        decision1 = strategy(5, 1)
        assert decision1.should_wait is True

        decision2 = strategy(10, 1)
        assert decision2.should_wait is False

    def test_function_condition(self):
        """Test with function condition."""

        def is_pending(status):
            return status == "pending"

        config = WaitStrategyConfig(should_continue_polling=is_pending)
        strategy = create_wait_strategy(config)

        decision1 = strategy("pending", 1)
        assert decision1.should_wait is True

        decision2 = strategy("completed", 1)
        assert decision2.should_wait is False

    def test_method_condition(self):
        """Test with method condition."""

        class Checker:
            def __init__(self, threshold):
                self.threshold = threshold

            def should_continue(self, value):
                return value < self.threshold

        checker = Checker(100)
        config = WaitStrategyConfig(should_continue_polling=checker.should_continue)
        strategy = create_wait_strategy(config)

        decision1 = strategy(50, 1)
        assert decision1.should_wait is True

        decision2 = strategy(100, 1)
        assert decision2.should_wait is False
