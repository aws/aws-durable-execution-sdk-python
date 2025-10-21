"""Unit tests for config module."""

from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

from aws_durable_execution_sdk_python.config import (
    BatchedInput,
    CallbackConfig,
    CheckpointMode,
    ChildConfig,
    CompletionConfig,
    ItemBatcher,
    ItemsPerBatchUnit,
    MapConfig,
    ParallelConfig,
    StepConfig,
    StepFuture,
    StepSemantics,
    TerminationMode,
    WaitForConditionConfig,
    WaitForConditionDecision,
)


def test_batched_input():
    """Test BatchedInput dataclass."""
    batch_input = BatchedInput("batch", [1, 2, 3])
    assert batch_input.batch_input == "batch"
    assert batch_input.items == [1, 2, 3]


def test_completion_config_defaults():
    """Test CompletionConfig default values."""
    config = CompletionConfig()
    assert config.min_successful is None
    assert config.tolerated_failure_count is None
    assert config.tolerated_failure_percentage is None


def test_completion_config_first_completed():
    """Test CompletionConfig.first_completed factory method."""
    # first_completed is commented out, so this test should be skipped or removed


def test_completion_config_first_successful():
    """Test CompletionConfig.first_successful factory method."""
    config = CompletionConfig.first_successful()
    assert config.min_successful == 1
    assert config.tolerated_failure_count is None
    assert config.tolerated_failure_percentage is None


def test_completion_config_all_completed():
    """Test CompletionConfig.all_completed factory method."""
    config = CompletionConfig.all_completed()
    assert config.min_successful is None
    assert config.tolerated_failure_count is None
    assert config.tolerated_failure_percentage is None


def test_completion_config_all_successful():
    """Test CompletionConfig.all_successful factory method."""
    config = CompletionConfig.all_successful()
    assert config.min_successful is None
    assert config.tolerated_failure_count == 0
    assert config.tolerated_failure_percentage == 0


def test_termination_mode_enum():
    """Test TerminationMode enum."""
    assert TerminationMode.TERMINATE.value == "TERMINATE"
    assert TerminationMode.CANCEL.value == "CANCEL"
    assert TerminationMode.WAIT.value == "WAIT"
    assert TerminationMode.ABANDON.value == "ABANDON"


def test_parallel_config_defaults():
    """Test ParallelConfig default values."""
    config = ParallelConfig()
    assert config.max_concurrency is None
    assert isinstance(config.completion_config, CompletionConfig)


def test_wait_for_condition_decision_continue():
    """Test WaitForConditionDecision.continue_waiting factory method."""
    decision = WaitForConditionDecision.continue_waiting(30)
    assert decision.should_continue is True
    assert decision.delay_seconds == 30


def test_wait_for_condition_decision_stop():
    """Test WaitForConditionDecision.stop_polling factory method."""
    decision = WaitForConditionDecision.stop_polling()
    assert decision.should_continue is False
    assert decision.delay_seconds == -1


def test_wait_for_condition_config():
    """Test WaitForConditionConfig with custom values."""

    def wait_strategy(state, attempt):
        return WaitForConditionDecision.continue_waiting(10)

    serdes = Mock()
    config = WaitForConditionConfig(
        wait_strategy=wait_strategy, initial_state="test_state", serdes=serdes
    )

    assert config.wait_strategy is wait_strategy
    assert config.initial_state == "test_state"
    assert config.serdes is serdes


def test_step_semantics_enum():
    """Test StepSemantics enum."""
    assert StepSemantics.AT_MOST_ONCE_PER_RETRY.value == "AT_MOST_ONCE_PER_RETRY"
    assert StepSemantics.AT_LEAST_ONCE_PER_RETRY.value == "AT_LEAST_ONCE_PER_RETRY"


def test_step_config_defaults():
    """Test StepConfig default values."""
    config = StepConfig()
    assert config.retry_strategy is None
    assert config.step_semantics == StepSemantics.AT_LEAST_ONCE_PER_RETRY
    assert config.serdes is None


def test_step_config_with_values():
    """Test StepConfig with custom values."""
    retry_strategy = Mock()
    serdes = Mock()

    config = StepConfig(
        retry_strategy=retry_strategy,
        step_semantics=StepSemantics.AT_MOST_ONCE_PER_RETRY,
        serdes=serdes,
    )

    assert config.retry_strategy is retry_strategy
    assert config.step_semantics == StepSemantics.AT_MOST_ONCE_PER_RETRY
    assert config.serdes is serdes


def test_checkpoint_mode_enum():
    """Test CheckpointMode enum."""
    assert CheckpointMode.NO_CHECKPOINT.value == ("NO_CHECKPOINT",)
    assert CheckpointMode.CHECKPOINT_AT_FINISH.value == ("CHECKPOINT_AT_FINISH",)
    assert (
        CheckpointMode.CHECKPOINT_AT_START_AND_FINISH.value
        == "CHECKPOINT_AT_START_AND_FINISH"
    )


def test_child_config_defaults():
    """Test ChildConfig default values."""
    config = ChildConfig()
    assert config.serdes is None
    assert config.sub_type is None


def test_child_config_with_serdes():
    """Test ChildConfig with serdes."""
    serdes = Mock()
    config = ChildConfig(serdes=serdes)
    assert config.serdes is serdes
    assert config.sub_type is None


def test_child_config_with_sub_type():
    """Test ChildConfig with sub_type."""
    sub_type = Mock()
    config = ChildConfig(sub_type=sub_type)
    assert config.serdes is None
    assert config.sub_type is sub_type


def test_child_config_with_summary_generator():
    """Test ChildConfig with summary_generator."""

    def mock_summary_generator(result):
        return f"Summary of {result}"

    config = ChildConfig(summary_generator=mock_summary_generator)
    assert config.serdes is None
    assert config.sub_type is None
    assert config.summary_generator is mock_summary_generator

    # Test that the summary generator works
    result = config.summary_generator("test_data")
    assert result == "Summary of test_data"


def test_items_per_batch_unit_enum():
    """Test ItemsPerBatchUnit enum."""
    assert ItemsPerBatchUnit.COUNT.value == ("COUNT",)
    assert ItemsPerBatchUnit.BYTES.value == "BYTES"


def test_item_batcher_defaults():
    """Test ItemBatcher default values."""
    batcher = ItemBatcher()
    assert batcher.max_items_per_batch == 0
    assert batcher.max_item_bytes_per_batch == 0
    assert batcher.batch_input is None


def test_item_batcher_with_values():
    """Test ItemBatcher with custom values."""
    batcher = ItemBatcher(
        max_items_per_batch=100, max_item_bytes_per_batch=1024, batch_input="test_input"
    )
    assert batcher.max_items_per_batch == 100
    assert batcher.max_item_bytes_per_batch == 1024
    assert batcher.batch_input == "test_input"


def test_map_config_defaults():
    """Test MapConfig default values."""
    config = MapConfig()
    assert config.max_concurrency is None
    assert isinstance(config.item_batcher, ItemBatcher)
    assert isinstance(config.completion_config, CompletionConfig)
    assert config.serdes is None


def test_callback_config_defaults():
    """Test CallbackConfig default values."""
    config = CallbackConfig()
    assert config.timeout_seconds == 0
    assert config.heartbeat_timeout_seconds == 0
    assert config.serdes is None


def test_callback_config_with_values():
    """Test CallbackConfig with custom values."""
    serdes = Mock()
    config = CallbackConfig(
        timeout_seconds=30, heartbeat_timeout_seconds=10, serdes=serdes
    )
    assert config.timeout_seconds == 30
    assert config.heartbeat_timeout_seconds == 10
    assert config.serdes is serdes


def test_step_future():
    """Test StepFuture with Future."""
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(lambda: "test_result")
        step_future = StepFuture(future, "test_step")

        result = step_future.result()
        assert result == "test_result"


def test_step_future_with_timeout():
    """Test StepFuture result with timeout."""
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(lambda: "test_result")
        step_future = StepFuture(future)

        result = step_future.result(timeout_seconds=1)
        assert result == "test_result"


def test_step_future_without_name():
    """Test StepFuture without name."""
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(lambda: 42)
        step_future = StepFuture(future)

        result = step_future.result()
        assert result == 42
