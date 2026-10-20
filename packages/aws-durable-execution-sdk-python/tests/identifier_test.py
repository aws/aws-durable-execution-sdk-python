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
