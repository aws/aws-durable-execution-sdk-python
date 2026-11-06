"""Unit tests for config module."""

import math
from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python.config import (
    CallbackConfig,
    ChildConfig,
    CompletionConfig,
    CompletionDecision,
    CompletionOutcome,
    Duration,
    InvokeConfig,
    MapConfig,
    ParallelConfig,
    StepConfig,
    StepSemantics,
    WaitForCallbackConfig,
)
from aws_durable_execution_sdk_python.exceptions import ValidationError
from aws_durable_execution_sdk_python.lambda_service import (
    DistributedMapCompletionConfig as WireCompletionConfig,
)
from aws_durable_execution_sdk_python.waits import (
    WaitForConditionConfig,
    WaitForConditionDecision,
)
from aws_durable_execution_sdk_python.config import (
    DistributedMapCompletionConfig,
    DistributedMapConfig,
    DistributedMapDestination,
    DistributedMapProcessor,
    DistributedMapProcessorRetryConfig,
    DistributedMapSource,
    ReaderSource,
)


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
    assert config.tolerated_failure_percentage == 100


def test_completion_config_all_successful():
    """Test CompletionConfig.all_successful factory method."""
    config = CompletionConfig.all_successful()
    assert config.min_successful is None
    assert config.tolerated_failure_count == 0
    assert config.tolerated_failure_percentage == 0


def test_parallel_config_defaults():
    """Test ParallelConfig default values."""
    config = ParallelConfig()
    assert config.max_concurrency is None
    assert isinstance(config.completion_config, CompletionConfig)


def test_wait_for_condition_decision_continue():
    """Test WaitForConditionDecision.continue_waiting factory method."""
    decision = WaitForConditionDecision.continue_waiting(Duration.from_seconds(30))
    assert decision.should_continue is True
    assert decision.delay_seconds == 30


def test_wait_for_condition_decision_stop():
    """Test WaitForConditionDecision.stop_polling factory method."""
    decision = WaitForConditionDecision.stop_polling()
    assert decision.should_continue is False
    assert decision.delay_seconds == 0


def test_wait_for_condition_config():
    """Test WaitForConditionConfig with custom values."""

    def wait_strategy(state, attempt):
        return WaitForConditionDecision.continue_waiting(Duration.from_seconds(10))

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
    config = ChildConfig(sub_type="OrderSaga")
    assert config.serdes is None
    assert config.sub_type == "OrderSaga"


def test_step_config_sub_type_defaults_to_none():
    assert StepConfig().sub_type is None
    assert StepConfig(sub_type="OrderCharge").sub_type == "OrderCharge"


def test_callback_config_sub_type_defaults_to_none():
    assert CallbackConfig().sub_type is None
    assert CallbackConfig(sub_type="Approval").sub_type == "Approval"
    # WaitForCallbackConfig inherits the field and labels the inner callback.
    assert WaitForCallbackConfig(sub_type="Approval").sub_type == "Approval"


def test_sub_type_is_keyword_only_so_positional_order_is_unchanged():
    """2.x positional construction keeps its meaning; sub_type never takes a slot."""
    retry = Mock()
    serdes = Mock()
    config = WaitForCallbackConfig(
        Duration.from_seconds(1), Duration.from_seconds(2), serdes, retry
    )
    assert config.retry_strategy is retry
    assert config.sub_type is None
    assert (
        StepConfig(retry, StepSemantics.AT_MOST_ONCE_PER_RETRY, serdes).serdes is serdes
    )
    # ChildConfig.sub_type keeps its 2.x positional slot; only its type changed.
    assert ChildConfig(serdes, "OrderSaga").sub_type == "OrderSaga"
    with pytest.raises(TypeError):
        StepConfig(retry, StepSemantics.AT_LEAST_ONCE_PER_RETRY, serdes, "OrderCharge")  # type: ignore[misc]


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


def test_map_config_defaults():
    """Test MapConfig default values."""
    config = MapConfig()
    assert config.max_concurrency is None
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
        timeout=Duration.from_seconds(30),
        heartbeat_timeout=Duration.from_seconds(10),
        serdes=serdes,
    )
    assert config.timeout_seconds == 30
    assert config.heartbeat_timeout_seconds == 10
    assert config.serdes is serdes


def test_invoke_config_defaults():
    """Test InvokeConfig defaults."""
    config = InvokeConfig()
    assert config.tenant_id is None


def test_invoke_config_with_tenant_id():
    """Test InvokeConfig with explicit tenant_id."""
    config = InvokeConfig(tenant_id="test-tenant")
    assert config.tenant_id == "test-tenant"


# region Config validation


def test_completion_config_rejects_min_successful_below_one():
    with pytest.raises(ValidationError, match="min_successful must be at least 1"):
        CompletionConfig(min_successful=0)


def test_completion_config_rejects_negative_tolerated_failure_count():
    with pytest.raises(
        ValidationError, match="tolerated_failure_count must be non-negative"
    ):
        CompletionConfig(tolerated_failure_count=-1)


def test_completion_config_rejects_out_of_range_failure_percentage():
    with pytest.raises(
        ValidationError, match="tolerated_failure_percentage must be between 0 and 100"
    ):
        CompletionConfig(tolerated_failure_percentage=101)
    with pytest.raises(
        ValidationError, match="tolerated_failure_percentage must be between 0 and 100"
    ):
        CompletionConfig(tolerated_failure_percentage=-0.1)


def test_completion_config_accepts_boundary_values():
    assert CompletionConfig(min_successful=1).min_successful == 1
    assert CompletionConfig(tolerated_failure_count=0).tolerated_failure_count == 0
    assert (
        CompletionConfig(tolerated_failure_percentage=0).tolerated_failure_percentage
        == 0
    )
    assert (
        CompletionConfig(tolerated_failure_percentage=100).tolerated_failure_percentage
        == 100
    )


def test_map_config_rejects_max_concurrency_below_one():
    with pytest.raises(ValidationError, match="max_concurrency must be at least 1"):
        MapConfig(max_concurrency=0)
    with pytest.raises(ValidationError, match="max_concurrency must be at least 1"):
        MapConfig(max_concurrency=-1)


def test_parallel_config_rejects_max_concurrency_below_one():
    with pytest.raises(ValidationError, match="max_concurrency must be at least 1"):
        ParallelConfig(max_concurrency=0)


def test_configs_accept_none_max_concurrency_as_unlimited():
    assert MapConfig().max_concurrency is None
    assert ParallelConfig(max_concurrency=1).max_concurrency == 1


def test_completion_decision_rejects_complete_without_outcome():
    with pytest.raises(
        ValidationError, match="outcome is required when complete is True"
    ):
        CompletionDecision(complete=True)


def test_completion_decision_rejects_outcome_when_not_complete():
    with pytest.raises(
        ValidationError, match="outcome must be None when complete is False"
    ):
        CompletionDecision(complete=False, outcome=CompletionOutcome.SUCCEEDED)


# endregion Config validation


# --- sub_type validation ---------------------------------------------------


@pytest.mark.parametrize(
    "sub_type",
    ["OrderCharge", "a", "A" * 32, "with-hyphen_and_underscore9", None, ""],
    ids=["custom", "one-char", "max-length", "all-classes", "none", "empty"],
)
def test_configs_accept_a_valid_or_default_sub_type(sub_type: str | None) -> None:
    assert StepConfig(sub_type=sub_type).sub_type == sub_type
    assert CallbackConfig(sub_type=sub_type).sub_type == sub_type
    assert ChildConfig(sub_type=sub_type).sub_type == sub_type


@pytest.mark.parametrize(
    "sub_type",
    ["A" * 33, "Order Charge", "order.charge", "order/charge", "ordér"],
    ids=["too-long", "space", "dot", "slash", "non-ascii"],
)
@pytest.mark.parametrize("config_class", [StepConfig, CallbackConfig, ChildConfig])
def test_configs_reject_a_sub_type_the_service_rejects(
    config_class: type, sub_type: str
) -> None:
    with pytest.raises(ValidationError, match="1 to 32 characters"):
        config_class(sub_type=sub_type)


def test_sub_type_rejection_names_the_value() -> None:
    with pytest.raises(ValidationError, match=r"got: 'Order Charge'"):
        StepConfig(sub_type="Order Charge")


# ==========================================================================
# Distributed map config validation
# ==========================================================================


def test_retry_duration_out_of_range_rejected():
    with pytest.raises(ValidationError, match="between 1 minute and 6 hours"):
        DistributedMapProcessorRetryConfig(max_retry_duration=Duration.from_seconds(30))
    with pytest.raises(ValidationError, match="between 1 minute and 6 hours"):
        DistributedMapProcessorRetryConfig(max_retry_duration=Duration.from_hours(7))


def test_expected_bucket_owner_must_be_12_digits():
    with pytest.raises(ValidationError, match="12-digit"):
        DistributedMapSource.s3_json_lines(
            "s3://b/k.jsonl", expected_bucket_owner="123"
        )


def test_csv_delimiter_invalid_string_rejected():
    with pytest.raises(ValidationError, match="delimiter must be one of"):
        DistributedMapSource.s3_csv("s3://b/data.csv", delimiter="BAR")


def test_csv_headers_duplicates_rejected():
    with pytest.raises(ValidationError, match="duplicates"):
        DistributedMapSource.s3_csv("s3://b/k.csv", headers=["a", "a"])


def test_json_lines_requires_key():
    with pytest.raises(ValidationError, match="object key"):
        DistributedMapSource.s3_json_lines("s3://bucket-only")


def test_timeout_out_of_range_rejected():
    with pytest.raises(ValidationError, match="at most 90 days"):
        DistributedMapConfig(timeout=Duration.from_days(91))


def test_empty_function_name_rejected():
    with pytest.raises(ValidationError, match="non-empty"):
        DistributedMapProcessor.report_batch_outcome("")


def test_valid_function_references_accepted():
    for ref in (
        "my-func",
        "my-func:PROD",
        "123456789012:function:my-func",
        "arn:aws:lambda:us-east-1:123456789012:function:my-func",
        "arn:aws:lambda:us-east-1:123456789012:function:my-func:1",
    ):
        # Should not raise.
        DistributedMapProcessor.report_batch_outcome(ref)


def test_function_name_over_max_length_rejected():
    with pytest.raises(ValidationError, match="at most 170 characters"):
        DistributedMapProcessor.report_batch_outcome("f" * 171)


def test_completion_count_and_percentage_mutually_exclusive():
    with pytest.raises(ValidationError, match="mutually exclusive"):
        DistributedMapCompletionConfig(
            tolerated_failure_count=1, tolerated_failure_percentage=5
        )


def test_completion_config_without_a_threshold_rejected():
    """Omitting the config means zero tolerated failures, so an empty one is an error."""
    with pytest.raises(ValidationError, match="must set tolerated_failure_count"):
        DistributedMapCompletionConfig()


def test_completion_sample_size_requires_percentage():
    with pytest.raises(ValidationError, match="minimum_sample_size"):
        DistributedMapCompletionConfig(
            tolerated_failure_count=1, minimum_sample_size=10
        )


def test_completion_negative_count_rejected():
    with pytest.raises(ValidationError, match="non-negative"):
        DistributedMapCompletionConfig(tolerated_failure_count=-1)


def test_completion_percentage_out_of_range_rejected():
    with pytest.raises(ValidationError, match="between 0 and 100"):
        DistributedMapCompletionConfig(tolerated_failure_percentage=150)


def test_completion_fractional_percentage_accepted():
    """The service's percentage is a number, so a fraction reaches the wire unchanged."""
    config = DistributedMapCompletionConfig(tolerated_failure_percentage=12.5)
    assert config.tolerated_failure_percentage == 12.5
    wire = WireCompletionConfig.from_config(config)
    assert wire.to_dict()["ToleratedFailurePercentage"] == 12.5


@pytest.mark.parametrize(
    "percentage",
    [math.nan, math.inf, -math.inf],
    ids=["nan", "inf", "-inf"],
)
def test_completion_non_finite_percentage_rejected(percentage):
    """The range check rejects these, since every comparison against NaN is False."""
    with pytest.raises(ValidationError, match="between 0 and 100"):
        DistributedMapCompletionConfig(tolerated_failure_percentage=percentage)


def test_completion_sample_size_below_one_rejected():
    with pytest.raises(ValidationError, match="at least 1"):
        DistributedMapCompletionConfig(
            tolerated_failure_percentage=5, minimum_sample_size=0
        )


def test_completion_failure_count_factory():
    assert DistributedMapCompletionConfig.failure_count(3).tolerated_failure_count == 3


def test_retry_duration_below_minimum_rejected():
    with pytest.raises(ValidationError, match="1 minute and 6 hours"):
        DistributedMapProcessorRetryConfig(max_retry_duration=Duration.from_seconds(30))


def test_max_items_below_one_rejected():
    with pytest.raises(ValidationError, match="between 1 and 100000000"):
        DistributedMapSource.s3_json_lines("s3://b/k.jsonl", max_items=0)


def test_max_items_above_the_service_maximum_rejected():
    with pytest.raises(ValidationError, match="between 1 and 100000000"):
        DistributedMapSource.s3_json_lines("s3://b/k.jsonl", max_items=100_000_001)


def test_csv_headers_above_the_count_limit_rejected():
    with pytest.raises(ValidationError, match="at most 1000 names"):
        DistributedMapSource.s3_csv(
            "s3://b/f.csv", headers=[f"c{i}" for i in range(1001)]
        )


def test_csv_header_name_above_the_length_limit_rejected():
    with pytest.raises(ValidationError, match="at most 256 characters"):
        DistributedMapSource.s3_csv("s3://b/f.csv", headers=["a" * 257])


def test_csv_headers_at_the_limits_accepted():
    source = DistributedMapSource.s3_csv(
        "s3://b/f.csv", headers=[f"c{i}" for i in range(999)] + ["a" * 256]
    )
    assert source.s3 is not None
    assert source.s3.headers is not None
    assert len(source.s3.headers) == 1000


def test_csv_requires_object_key():
    with pytest.raises(ValidationError, match="csv requires an S3 object key"):
        DistributedMapSource.s3_csv("s3://bucket")


def test_source_without_any_kind_rejected():
    with pytest.raises(ValidationError, match="exactly one of"):
        DistributedMapSource()


def test_source_with_two_kinds_rejected():
    with pytest.raises(ValidationError, match="exactly one of"):
        DistributedMapSource(
            inline_items=("a",),
            reader=ReaderSource(function_name="r"),
        )


def test_success_destination_all_false_rejected():
    with pytest.raises(ValidationError, match="success destination must include"):
        DistributedMapDestination.s3_successes(
            "s3://out/ok", include_input=False, include_output=False
        )


def test_failure_destination_all_false_rejected():
    with pytest.raises(ValidationError, match="failure destination must include"):
        DistributedMapDestination.s3_failures(
            "s3://out/bad", include_input=False, include_error=False
        )


@pytest.mark.parametrize(
    ("prefix", "size"),
    [
        ("a" * 901, 901),
        ("é" * 451, 902),
    ],
)
def test_destination_prefix_over_the_byte_limit_rejected(prefix: str, size: int):
    expected = f"prefix must be at most 900 UTF-8 bytes, got: {size}"
    with pytest.raises(ValidationError, match=expected):
        DistributedMapDestination.s3_successes(f"s3://out/{prefix}")
    with pytest.raises(ValidationError, match=expected):
        DistributedMapDestination.s3_failures(f"s3://out/{prefix}")


@pytest.mark.parametrize("prefix", ["a" * 900, "é" * 450, ""])
def test_destination_prefix_at_the_byte_limit_accepted(prefix: str):
    assert DistributedMapDestination.s3_successes(f"s3://out/{prefix}").prefix == prefix
    assert DistributedMapDestination.s3_failures(f"s3://out/{prefix}").prefix == prefix


def test_invalid_s3_uri_rejected():
    with pytest.raises(ValidationError, match="Invalid S3 URI"):
        DistributedMapSource.s3_json_lines("s3:///key.jsonl")


def test_non_s3_scheme_uri_rejected():
    with pytest.raises(ValidationError, match="must start with s3://"):
        DistributedMapSource.s3_json_lines("http://foo/bar")


def test_csv_empty_headers_rejected():
    with pytest.raises(ValidationError, match="must be non-empty"):
        DistributedMapSource.s3_csv("s3://b/f.csv", headers=[])


def test_negative_retry_attempts_rejected():
    with pytest.raises(ValidationError, match="non-negative"):
        DistributedMapProcessorRetryConfig(max_retry_attempts=-5)


@pytest.mark.parametrize("attempts", ["5", "forever", "UNLIMITED", 2.5])
def test_retry_attempts_that_is_not_an_int_or_unlimited_rejected(attempts):
    with pytest.raises(ValidationError, match="non-negative integer"):
        DistributedMapProcessorRetryConfig(max_retry_attempts=attempts)


def test_unlimited_retry_attempts_accepted() -> None:
    # Annotated so mypy checks this body. The constant is the only typed call site
    # that would catch UNLIMITED widening back to a plain str.
    processor = DistributedMapProcessor.report_batch_outcome(
        "p",
        retry=DistributedMapProcessorRetryConfig(
            max_retry_attempts=DistributedMapProcessorRetryConfig.UNLIMITED
        ),
    )
    assert processor.retry is not None
    assert (
        processor.retry.max_retry_attempts
        == DistributedMapProcessorRetryConfig.UNLIMITED
    )


def test_batch_size_out_of_range_rejected():
    with pytest.raises(ValidationError, match="between 1 and 10000"):
        DistributedMapProcessor.report_batch_outcome("p", batch_size=0)


def test_durable_execution_name_prefix_too_long_rejected():
    with pytest.raises(ValidationError, match="1 to 36 characters"):
        DistributedMapProcessor.report_batch_outcome(
            "p", durable_execution_name_prefix="x" * 37
        )
