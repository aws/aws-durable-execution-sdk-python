"""Example file with bad patterns inside @durable_execution for testing."""

import os
import random
import time
import uuid
from datetime import datetime

import requests

from aws_durable_execution_sdk_python import (
    DurableContext,
    StepContext,
    durable_execution,
    durable_step,
)


# This function is NOT decorated - should NOT trigger any warnings
def normal_function():
    value = random.random()  # OK - not in durable context
    now = datetime.now()  # OK
    return value


@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    # GOOD - inside ctx.step(), result is checkpointed
    safe_random = context.step(lambda: random.random())
    safe_time = context.step(lambda: datetime.now().isoformat())
    safe_uuid = context.step(lambda: str(uuid.uuid4()))

    # DAR001: random calls OUTSIDE step
    value = random.random()
    num = random.randint(1, 100)

    # DAR002: datetime calls OUTSIDE step
    now = datetime.now()
    today = datetime.today()

    # DAR003: uuid generation OUTSIDE step
    id1 = uuid.uuid4()
    id2 = uuid.uuid1()

    # DAR004: time calls OUTSIDE step
    ts = time.time()
    mono = time.monotonic()

    # DAR005: os.environ access OUTSIDE step
    api_key = os.environ["API_KEY"]

    # DAR006: network calls OUTSIDE step
    response = requests.get("https://api.example.com")
    data = requests.post("https://api.example.com", json={})

    # DAR008: closure mutation
    count = 0
    count += 1
    items = {}
    items["key"] = "value"

    return {"value": value}


# DAR007: durable ops inside @durable_step
@durable_step
def bad_step(step_ctx: StepContext) -> dict:
    # This is wrong - can't call durable ops inside a step
    result = step_ctx.step(lambda: 1)
    step_ctx.wait(duration=5)
    return {"result": result}
