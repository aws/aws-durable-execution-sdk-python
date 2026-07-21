"""5-8: Invoke with tenantId (tenant-isolated invocation)."""

import os
from typing import Any

from aws_durable_execution_sdk_python.config import InvokeConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution


@durable_execution
def handler(event: Any, context: DurableContext) -> str:
    function_name = os.environ["TARGET_FUNCTION_NAME"]
    tenant_id = event["tenantId"]
    payload = event["payload"]
    result: str = context.invoke(
        function_name,
        payload,
        config=InvokeConfig(tenant_id=tenant_id),
    )
    return result
