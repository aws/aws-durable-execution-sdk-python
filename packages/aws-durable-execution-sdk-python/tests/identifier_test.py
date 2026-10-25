"""Operation identifiers carry a category independently of their subtype."""

import pytest

from aws_durable_execution_sdk_python.exceptions import NonDeterministicExecutionError
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationStatus,
    OperationSubType,
    OperationType,
)


@pytest.mark.parametrize(
    ("operation_type", "sub_type", "wrong_type"),
    [
        (OperationType.CONTEXT, OperationSubType.STEP, OperationType.STEP),
        (OperationType.STEP, OperationSubType.MAP, OperationType.CONTEXT),
    ],
)
def test_explicit_operation_type_controls_replay_validation(
    operation_type: OperationType,
    sub_type: OperationSubType,
    wrong_type: OperationType,
) -> None:
    identifier = OperationIdentifier(
        operation_id="operation",
        sub_type=sub_type,
        operation_type=operation_type,
    )

    assert identifier.type is operation_type
    identifier.validate_checkpoint(
        Operation(
            operation_id="operation",
            operation_type=operation_type,
            sub_type=sub_type,
            status=OperationStatus.SUCCEEDED,
        )
    )

    with pytest.raises(NonDeterministicExecutionError, match="type checkpoint="):
        identifier.validate_checkpoint(
            Operation(
                operation_id="operation",
                operation_type=wrong_type,
                sub_type=sub_type,
                status=OperationStatus.SUCCEEDED,
            )
        )


def test_operation_identifier_requires_explicit_operation_type() -> None:
    with pytest.raises(TypeError, match="operation_type"):
        OperationIdentifier("operation", OperationSubType.STEP)  # type: ignore[call-arg]


def _checkpoint(sub_type: str | None) -> Operation:
    """A checkpoint as the SDK loads it: the subtype is the stored string."""
    return Operation(
        operation_id="operation",
        operation_type=OperationType.STEP,
        sub_type=sub_type,
        status=OperationStatus.SUCCEEDED,
    )


@pytest.mark.parametrize(
    "sub_type",
    [OperationSubType.STEP, "OrderCharge"],
    ids=["sdk-subtype", "custom-subtype"],
)
def test_replay_accepts_checkpoint_with_same_subtype(sub_type: str) -> None:
    identifier = OperationIdentifier(
        "operation", sub_type, operation_type=OperationType.STEP
    )
    identifier.validate_checkpoint(_checkpoint(sub_type))


def test_replay_accepts_loaded_sdk_subtype_against_member() -> None:
    """A loaded "Step" string equals the member the SDK puts on the identifier."""
    identifier = OperationIdentifier(
        "operation", OperationSubType.STEP, operation_type=OperationType.STEP
    )
    identifier.validate_checkpoint(_checkpoint("Step"))


def test_replay_mismatch_message_prints_sdk_subtype_as_its_value() -> None:
    """The SDK assigns .value, so the message shows 'Step' like any other string."""
    identifier = OperationIdentifier(
        "operation", OperationSubType.STEP.value, operation_type=OperationType.STEP
    )
    with pytest.raises(NonDeterministicExecutionError) as exc_info:
        identifier.validate_checkpoint(_checkpoint("OrderCharge"))
    assert "subtype checkpoint='OrderCharge' current='Step'" in str(exc_info.value)


@pytest.mark.parametrize(
    ("current", "recorded", "expected_message"),
    [
        (
            "OrderCharge",
            "Step",
            "subtype checkpoint='Step' current='OrderCharge'",
        ),
        (
            "Step",
            "OrderCharge",
            "subtype checkpoint='OrderCharge' current='Step'",
        ),
        (
            "OrderCharge",
            "OrderNotify",
            "subtype checkpoint='OrderNotify' current='OrderCharge'",
        ),
        (
            "OrderCharge",
            None,
            "subtype checkpoint=None current='OrderCharge'",
        ),
    ],
    ids=["custom-vs-sdk", "sdk-vs-custom", "custom-vs-custom", "custom-vs-none"],
)
def test_replay_rejects_changed_subtype(
    current: str, recorded: str | None, expected_message: str
) -> None:
    """Changing a subtype is non-deterministic; the message shows plain values."""
    identifier = OperationIdentifier(
        "operation", current, operation_type=OperationType.STEP
    )
    with pytest.raises(NonDeterministicExecutionError) as exc_info:
        identifier.validate_checkpoint(_checkpoint(recorded))
    assert expected_message in str(exc_info.value)
