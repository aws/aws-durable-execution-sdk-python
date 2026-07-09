"""Tests for the Pydantic AI durable capability prototype."""

from __future__ import annotations

import asyncio
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import pytest

from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.execution import InvocationStatus
from src.pydantic_ai_agent import pydantic_ai_agent
from test.conftest import deserialize_operation_payload


@dataclass
class FakeStepContext:
    value: str = "step-context"


@dataclass
class FakeRunContext:
    run_step: int = 3


@dataclass
class FakeToolCall:
    tool_name: str = "lookup_customer_tier"
    tool_call_id: str = "call_123"


@dataclass
class FakeToolDefinition:
    name: str = "lookup_customer_tier"


class FakeDurableContext:
    def __init__(self) -> None:
        self.steps: list[dict[str, Any]] = []
        self.step_thread_ids: list[int] = []

    def step(self, func, name: str | None = None, config: Any | None = None):
        self.step_thread_ids.append(threading.get_ident())
        self.steps.append({"name": name, "config": config})
        return func(FakeStepContext())


@pytest.mark.example
def test_model_requests_run_inside_named_durable_step():
    context = FakeDurableContext()
    user_handler_thread_id = threading.get_ident()
    handler_loop: asyncio.AbstractEventLoop | None = None

    async def handler(request_context: dict[str, str]) -> dict[str, str]:
        nonlocal handler_loop
        handler_loop = asyncio.get_running_loop()
        await asyncio.sleep(0)
        return {"response": request_context["prompt"]}

    async def run_hook() -> tuple[
        dict[str, str],
        asyncio.AbstractEventLoop,
        int,
        StepConfig,
    ]:
        capability = pydantic_ai_agent.LambdaDurability(
            context,
            agent_name="support",
        )
        original_loop = asyncio.get_running_loop()
        agent_loop_thread_id = threading.get_ident()
        result = await capability.wrap_model_request(
            FakeRunContext(),
            request_context={"prompt": "hello"},
            handler=handler,
        )
        return (
            result,
            original_loop,
            agent_loop_thread_id,
            capability.model_step_config,
        )

    result, original_loop, agent_loop_thread_id, model_step_config = (
        pydantic_ai_agent.run_pydantic_ai_from_sync(run_hook)
    )

    assert result == {"response": "hello"}
    assert handler_loop is original_loop
    assert agent_loop_thread_id != user_handler_thread_id
    assert context.step_thread_ids == [user_handler_thread_id]
    assert context.steps == [
        {
            "name": "support.model.request.3",
            "config": model_step_config,
        }
    ]


@pytest.mark.example
def test_tool_execution_uses_tool_specific_step_config():
    context = FakeDurableContext()
    user_handler_thread_id = threading.get_ident()
    lookup_config = StepConfig()

    async def handler(args: dict[str, str]) -> str:
        await asyncio.sleep(0)
        return f"tier:{args['customer_id']}"

    async def run_hook() -> str:
        capability = pydantic_ai_agent.LambdaDurability(
            context,
            agent_name="support",
            tool_step_configs={"lookup_customer_tier": lookup_config},
        )
        return await capability.wrap_tool_execute(
            FakeRunContext(),
            call=FakeToolCall(),
            tool_def=FakeToolDefinition(),
            args={"customer_id": "customer-123"},
            handler=handler,
        )

    assert pydantic_ai_agent.run_pydantic_ai_from_sync(run_hook) == "tier:customer-123"
    assert context.step_thread_ids == [user_handler_thread_id]
    assert context.steps == [
        {
            "name": "support.tool.lookup_customer_tier.3.call_123",
            "config": lookup_config,
        }
    ]


@pytest.mark.example
def test_agent_loop_thread_is_reused_across_invocations():
    async def get_loop_identity() -> tuple[int, int]:
        return id(asyncio.get_running_loop()), threading.get_ident()

    first = pydantic_ai_agent.run_pydantic_ai_from_sync(get_loop_identity)
    second = pydantic_ai_agent.run_pydantic_ai_from_sync(get_loop_identity)

    assert second == first


@pytest.mark.example
def test_handler_thread_stops_from_agent_completion_queue():
    step_runner = pydantic_ai_agent._HandlerThreadStepRunner()
    step_runner.completion_queue.put(
        pydantic_ai_agent._AgentLoopCompletion(result="done")
    )

    assert step_runner.run_until_agent_done() == "done"


@pytest.mark.example
def test_wrap_run_sets_sequential_tool_execution_mode(monkeypatch):
    context = FakeDurableContext()
    capability = pydantic_ai_agent.LambdaDurability(context)
    modes: list[str] = []

    @contextmanager
    def fake_execution_mode(mode: str):
        modes.append(f"enter:{mode}")
        try:
            yield
        finally:
            modes.append(f"exit:{mode}")

    monkeypatch.setattr(
        pydantic_ai_agent,
        "_parallel_tool_call_execution_mode",
        fake_execution_mode,
    )

    async def handler() -> str:
        modes.append("handler")
        return "done"

    assert asyncio.run(capability.wrap_run(FakeRunContext(), handler=handler)) == "done"
    assert modes == ["enter:sequential", "handler", "exit:sequential"]


@pytest.mark.example
def test_async_bridge_also_works_when_a_loop_is_already_running():
    async def call_bridge() -> str:
        return pydantic_ai_agent.run_async_from_sync(_return_from_async)

    assert asyncio.run(call_bridge()) == "bridged"


async def _return_from_async() -> str:
    await asyncio.sleep(0)
    return "bridged"


@pytest.mark.example
@pytest.mark.durable_execution(
    handler=pydantic_ai_agent.handler,
    lambda_function_name="Pydantic AI Durable Capability Prototype",
)
def test_pydantic_ai_agent_runs_in_cloud(durable_runner):
    if durable_runner.mode != "cloud":
        pytest.skip("Pydantic AI Bedrock example only runs in cloud mode")

    with durable_runner:
        result = durable_runner.run(
            input={
                "customer_id": "enterprise-123",
                "prompt": (
                    "Use the customer tier tool, then answer with concise support "
                    "advice for a customer asking whether to escalate a production "
                    "incident."
                ),
            },
            timeout=120,
        )

    assert result.status is InvocationStatus.SUCCEEDED
    result_data = deserialize_operation_payload(result.result)
    assert result_data["customer_id"] == "enterprise-123"
    assert "answer" in result_data
    operation_names = {operation.name for operation in result.operations}
    assert any(
        name and name.startswith("lambda_support_agent.model.request")
        for name in operation_names
    )
    assert any(
        name and name.startswith("lambda_support_agent.tool.lookup_customer_tier")
        for name in operation_names
    )
