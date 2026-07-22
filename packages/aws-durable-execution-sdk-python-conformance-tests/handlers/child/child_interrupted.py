"""3-12: Child context interrupted and re-executed."""

import os
import time
from typing import Any

import boto3
from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.context import (
    DurableContext,
    StepContext,
    durable_step,
    durable_with_child_context,
)
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.retries import RetryPresets

ddb_client = boto3.client("dynamodb")
TABLE_NAME = os.environ.get("ATTEMPTS_TABLE_NAME", "Attempts")


@durable_step
def crashable_step(_step_context: StepContext, *, execution_id: str, value: str) -> str:
    response = ddb_client.update_item(
        TableName=TABLE_NAME,
        Key={"executionId": {"S": execution_id}},
        UpdateExpression="SET attemptCount = if_not_exists(attemptCount, :zero) + :inc",
        ExpressionAttributeValues={
            ":zero": {"N": "0"},
            ":inc": {"N": "1"},
        },
        ReturnValues="UPDATED_NEW",
    )

    attempt_count = int(response["Attributes"]["attemptCount"]["N"])

    if attempt_count < 2:
        # Sleep to allow checkpoint to be sent before crash
        time.sleep(1)
        # Simulate Lambda crash
        os._exit(1)
    return value


@durable_with_child_context
def interrupted_child(ctx: DurableContext, *, execution_id: str, value: str) -> str:
    return ctx.step(
        crashable_step(execution_id=execution_id, value=value),
        config=StepConfig(retry_strategy=RetryPresets.none()),
    )


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    execution_id = context.execution_context.durable_execution_arn

    result: str = context.run_in_child_context(
        interrupted_child(execution_id=execution_id, value=str(event))
    )
    return result
