from __future__ import annotations

import os

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from pydantic_ai import Agent, RunContext
from pydantic_ai.models.bedrock import BedrockConverseModel

try:
    from lambda_durability import LambdaDurability, run_durable
except ImportError:
    from src.pydantic_ai_agent.lambda_durability import LambdaDurability, run_durable

MODEL_ID = os.environ.get("PYDANTIC_AI_MODEL", "us.amazon.nova-micro-v1:0")

agent = Agent(BedrockConverseModel(MODEL_ID))


@agent.tool
def get_weather(ctx: RunContext, city: str) -> str:
    return f"It is sunny and 22C in {city}."


@durable_execution
def handler(event: dict, context: DurableContext) -> str:
    prompt = event.get("prompt", "What is the weather in Vancouver?")
    capability = LambdaDurability(context)
    result = run_durable(
        lambda: agent.run(prompt, capabilities=[capability]),
        capability,
    )
    return result.output
