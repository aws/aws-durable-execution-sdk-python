"""Tests for the Pydantic AI durable agent example."""

from __future__ import annotations

import asyncio
import threading

import pytest

from aws_durable_execution_sdk_python.execution import InvocationStatus
from src.pydantic_ai_agent import lambda_durability
from test.conftest import deserialize_operation_payload


class FakeStepContext:
    pass


class FakeDurableContext:
    """Records the durable steps run against it and the thread they run on."""

    def __init__(self) -> None:
        self.steps: list[str] = []
        self.step_thread_ids: list[int] = []

    def step(self, func, name=None, config=None):
        self.step_thread_ids.append(threading.get_ident())
        self.steps.append(name)
        return func(FakeStepContext())


@pytest.mark.example
def test_run_durable_runs_steps_on_the_handler_thread():
    context = FakeDurableContext()
    handler_thread_id = threading.get_ident()
    agent_thread_ids: list[int] = []

    capability = lambda_durability.LambdaDurability(context)

    async def call_model():
        agent_thread_ids.append(threading.get_ident())
        return {"answer": 42}

    async def run_agent():
        # Exercise a single durable step through the queue bridge.
        return await capability._durable_step(call_model, "model.request", None)

    result = lambda_durability.run_durable(run_agent, capability)

    assert result == {"answer": 42}
    # The durable step ran on the handler (main) thread.
    assert context.step_thread_ids == [handler_thread_id]
    assert context.steps == ["model.request"]
    # The model call ran on the background agent thread, not the handler thread.
    assert agent_thread_ids and agent_thread_ids[0] != handler_thread_id


@pytest.mark.example
def test_agent_loop_thread_is_reused_across_invocations():
    def loop_identity() -> int:
        return id(lambda_durability._agent_loop.get())

    assert loop_identity() == loop_identity()


@pytest.mark.example
@pytest.mark.durable_execution(
    lambda_function_name="Pydantic AI Durable Agent",
)
def test_pydantic_ai_agent_runs_in_cloud(durable_runner):
    if durable_runner.mode != "cloud":
        pytest.skip("Pydantic AI Bedrock example only runs in cloud mode")

    with durable_runner:
        result = durable_runner.run(
            input={"prompt": "What is the weather in Vancouver?"},
            timeout=120,
        )

    assert result.status is InvocationStatus.SUCCEEDED
    assert deserialize_operation_payload(result.result)
    operation_names = {operation.name for operation in result.operations}
    assert any(name and name.startswith("model.request") for name in operation_names)
    assert any(name and name.startswith("tool.") for name in operation_names)
