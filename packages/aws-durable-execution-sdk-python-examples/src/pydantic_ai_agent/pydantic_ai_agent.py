"""Prototype durable execution capability for Pydantic AI agents.

This mirrors the shape of Pydantic AI's workflow-engine integrations:
- The Pydantic AI agent loop runs as the durable Lambda workflow.
- Model requests are checkpointed as durable steps.
- Tool executions are checkpointed as durable steps.

The adapter is intentionally kept in examples. It demonstrates the integration
shape without committing this SDK to Pydantic AI's evolving extension surface.
"""

from __future__ import annotations

import asyncio
import contextvars
import os
import re
import threading
from collections.abc import Awaitable, Callable, Generator
from concurrent.futures import CancelledError as FutureCancelledError
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from queue import Empty, Queue
from typing import Any, Generic, TypeVar, cast

from aws_durable_execution_sdk_python.config import StepConfig
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.serdes import SerDes, SerDesContext


T = TypeVar("T")

try:
    from pydantic_ai.capabilities import AbstractCapability
except ImportError:

    class AbstractCapability(Generic[T]):  # type: ignore[no-redef]
        """Fallback base so this example module remains importable in tests."""


@dataclass(frozen=True)
class SupportDependencies:
    customer_id: str


@dataclass(frozen=True)
class SupportAnswer:
    support_advice: str
    escalate: bool
    confidence: float


def _run_coro(coro_factory: Callable[[], Awaitable[T]]) -> T:
    with asyncio.Runner() as runner:
        return runner.run(coro_factory())


def run_async_from_sync(coro_factory: Callable[[], Awaitable[T]]) -> T:
    """Run async code from this SDK's synchronous durable function surface.

    If called from a running event loop, this uses a fresh loop in a worker
    thread. That is fine for the Lambda handler entry point, but not for
    loop-bound async resources. Capability hooks use
    ``_run_async_in_durable_step`` so Pydantic AI work stays on its agent loop.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return _run_coro(coro_factory)

    with ThreadPoolExecutor(
        max_workers=1,
        thread_name_prefix="pydantic-ai-durable",
    ) as executor:
        return executor.submit(_run_coro, coro_factory).result()


@dataclass(frozen=True)
class _DurableStepRequest(Generic[T]):
    context: DurableContext
    step_body: Callable[[Any], T]
    name: str
    config: StepConfig | None
    loop: asyncio.AbstractEventLoop
    future: asyncio.Future[T]


@dataclass(frozen=True)
class _AgentLoopCompletion(Generic[T]):
    result: T | None = None
    error: BaseException | None = None
    cancelled: bool = False


_HandlerThreadQueueItem = _DurableStepRequest[Any] | _AgentLoopCompletion[Any]


@dataclass(frozen=True)
class _AgentLoopSubmission(Generic[T]):
    coro_factory: Callable[[], Awaitable[T]]
    task_context: contextvars.Context
    completion_queue: Queue[_HandlerThreadQueueItem]


class _HandlerThreadStepRunner:
    def __init__(self) -> None:
        self._queue: Queue[_HandlerThreadQueueItem] = Queue()

    async def run_step(
        self,
        context: DurableContext,
        *,
        step_body: Callable[[Any], T],
        name: str,
        config: StepConfig | None,
    ) -> T:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[T] = loop.create_future()
        self._queue.put(
            _DurableStepRequest(
                context=context,
                step_body=step_body,
                name=name,
                config=config,
                loop=loop,
                future=future,
            )
        )
        return await _await_cross_thread_future(future)

    def run_until_agent_done(self) -> T:
        while True:
            item = self._queue.get()
            if isinstance(item, _DurableStepRequest):
                self._process_step_request(item)
                continue

            if item.cancelled:
                raise FutureCancelledError()

            if item.error is not None:
                raise item.error

            return cast(T, item.result)

    @property
    def completion_queue(self) -> Queue[_HandlerThreadQueueItem]:
        return self._queue

    @staticmethod
    def _process_step_request(request: _DurableStepRequest[Any]) -> None:
        try:
            result = request.context.step(
                request.step_body,
                name=request.name,
                config=request.config,
            )
        except BaseException as exc:
            _set_async_exception(request.loop, request.future, exc)
        else:
            _set_async_result(request.loop, request.future, result)


class _PersistentAgentLoop:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready: threading.Event | None = None
        self._submissions: Queue[_AgentLoopSubmission[Any]] = Queue()

    def submit(
        self,
        coro_factory: Callable[[], Awaitable[T]],
        *,
        task_context: contextvars.Context,
        completion_queue: Queue[_HandlerThreadQueueItem],
    ) -> None:
        self._get_loop()
        self._submissions.put(
            _AgentLoopSubmission(
                coro_factory=coro_factory,
                task_context=task_context,
                completion_queue=completion_queue,
            )
        )

    def _drain_submissions(self, loop: asyncio.AbstractEventLoop) -> None:
        while True:
            try:
                submission = self._submissions.get_nowait()
            except Empty:
                break

            self._start_task(loop, submission)

        loop.call_later(0.001, self._drain_submissions, loop)

    @staticmethod
    def _start_task(
        loop: asyncio.AbstractEventLoop,
        submission: _AgentLoopSubmission[T],
    ) -> None:
        try:
            coro = submission.task_context.run(submission.coro_factory)
            task = loop.create_task(coro, context=submission.task_context)
        except BaseException as exc:
            submission.completion_queue.put(_AgentLoopCompletion(error=exc))
            return

        def complete(completed: asyncio.Task[T]) -> None:
            if completed.cancelled():
                submission.completion_queue.put(_AgentLoopCompletion(cancelled=True))
                return

            error = completed.exception()
            if error is not None:
                submission.completion_queue.put(_AgentLoopCompletion(error=error))
                return

            submission.completion_queue.put(
                _AgentLoopCompletion(result=completed.result())
            )

        task.add_done_callback(complete)

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        while True:
            with self._lock:
                if self._loop is not None and self._thread is not None:
                    if self._thread.is_alive() and not self._loop.is_closed():
                        return self._loop

                if self._thread is not None and self._thread.is_alive():
                    ready = self._ready
                else:
                    ready = threading.Event()
                    self._ready = ready

                    def run_loop() -> None:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        self._loop = loop
                        ready.set()
                        loop.call_soon(self._drain_submissions, loop)
                        loop.run_forever()

                    self._thread = threading.Thread(
                        target=run_loop,
                        name="pydantic-ai-agent-loop",
                        daemon=True,
                    )
                    self._thread.start()

            if ready is not None:
                ready.wait()


_CURRENT_STEP_RUNNER = contextvars.ContextVar(
    "pydantic_ai_durable_step_runner",
    default=None,
)
_AGENT_LOOP = _PersistentAgentLoop()


def _set_async_result(
    loop: asyncio.AbstractEventLoop,
    future: asyncio.Future[T],
    result: T,
) -> None:
    def resolve() -> None:
        if not future.cancelled():
            future.set_result(result)

    loop.call_soon_threadsafe(resolve)


def _set_async_exception(
    loop: asyncio.AbstractEventLoop,
    future: asyncio.Future[Any],
    error: BaseException,
) -> None:
    def resolve() -> None:
        if not future.cancelled():
            future.set_exception(error)

    loop.call_soon_threadsafe(resolve)


async def _await_cross_thread_future(future: asyncio.Future[T]) -> T:
    # Keep the agent loop ticking while the handler thread is inside context.step.
    while not future.done():
        await asyncio.sleep(0.001)
    return future.result()


def run_pydantic_ai_from_sync(coro_factory: Callable[[], Awaitable[T]]) -> T:
    """Run Pydantic AI on a shared loop thread while this thread performs steps."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        msg = "run_pydantic_ai_from_sync must be called from synchronous code"
        raise RuntimeError(msg)

    step_runner = _HandlerThreadStepRunner()
    task_context = contextvars.copy_context()
    task_context.run(_CURRENT_STEP_RUNNER.set, step_runner)
    _AGENT_LOOP.submit(
        coro_factory,
        task_context=task_context,
        completion_queue=step_runner.completion_queue,
    )
    return step_runner.run_until_agent_done()


async def _run_async_in_durable_step(
    context: DurableContext,
    *,
    step_runner: _HandlerThreadStepRunner,
    coro_factory: Callable[[], Awaitable[T]],
    name: str,
    config: StepConfig | None,
) -> T:
    loop = asyncio.get_running_loop()
    task_context = contextvars.copy_context()

    def run_on_original_loop() -> T:
        future: Future[T] = Future()

        def schedule() -> None:
            try:
                task = loop.create_task(coro_factory(), context=task_context)
            except BaseException as exc:
                future.set_exception(exc)
                return

            def complete(completed: asyncio.Task[T]) -> None:
                if completed.cancelled():
                    future.cancel()
                    return

                error = completed.exception()
                if error is not None:
                    future.set_exception(error)
                    return

                future.set_result(completed.result())

            task.add_done_callback(complete)

        loop.call_soon_threadsafe(schedule)
        return future.result()

    def step_body(_step_context: Any) -> T:
        return run_on_original_loop()

    return await step_runner.run_step(
        context,
        step_body=step_body,
        name=name,
        config=config,
    )


class PydanticAIModelResponseSerDes(SerDes[Any]):
    """Serialize Pydantic AI ``ModelResponse`` values for step checkpoints."""

    def serialize(
        self,
        value: Any,
        _context: SerDesContext | None = None,
    ) -> str:
        adapter = self._response_adapter()
        return adapter.dump_json(value).decode("utf-8")

    def deserialize(
        self,
        data: str,
        _context: SerDesContext | None = None,
    ) -> Any:
        adapter = self._response_adapter()
        return adapter.validate_json(data)

    @staticmethod
    def _response_adapter() -> Any:
        try:
            import pydantic
            from pydantic_ai.messages import ModelResponse
        except ImportError as exc:
            msg = (
                "The pydantic-ai package is required to deserialize model "
                "request checkpoints for this example."
            )
            raise RuntimeError(msg) from exc

        return pydantic.TypeAdapter(ModelResponse)


@contextmanager
def _parallel_tool_call_execution_mode(mode: str) -> Generator[None]:
    try:
        from pydantic_ai.tool_manager import ToolManager
    except ImportError as exc:
        msg = (
            "The pydantic-ai package is required to run the durable Pydantic AI "
            "capability."
        )
        raise RuntimeError(msg) from exc

    with ToolManager.parallel_execution_mode(mode):
        yield


def _step_name_part(value: Any, *, default: str = "unknown") -> str:
    if value is None:
        return default
    name = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value)).strip("-")
    return name or default


class LambdaDurability(AbstractCapability[Any]):
    """Pydantic AI capability that checkpoints model and tool calls as steps.

    Pydantic AI is async, while this durable SDK exposes synchronous operations.
    The Lambda handler thread services durable step calls while Pydantic AI runs
    on a shared event-loop thread that can survive warm Lambda invocations.
    Step bodies schedule async work back to the loop that owns the agent's
    async resources.
    """

    def __init__(
        self,
        context: DurableContext,
        *,
        agent_name: str = "pydantic-ai-agent",
        model_step_config: StepConfig | None = None,
        tool_step_config: StepConfig | None = None,
        tool_step_configs: dict[str, StepConfig | None] | None = None,
        step_runner: _HandlerThreadStepRunner | None = None,
        sequential_tool_calls: bool = True,
    ) -> None:
        self.context = context
        self.agent_name = agent_name
        self.model_step_config = model_step_config or StepConfig(
            serdes=PydanticAIModelResponseSerDes()
        )
        self.tool_step_config = tool_step_config
        self.tool_step_configs = tool_step_configs or {}
        self.step_runner = step_runner or _CURRENT_STEP_RUNNER.get()
        self.sequential_tool_calls = sequential_tool_calls

        self.id = f"{_step_name_part(agent_name)}-lambda-durability"
        self.description = "Checkpoint Pydantic AI model requests and tool calls"
        self.defer_loading = False

    @classmethod
    def get_serialization_name(cls) -> str | None:
        return None

    async def wrap_run(self, ctx: Any, *, handler: Callable[[], Awaitable[Any]]) -> Any:
        if not self.sequential_tool_calls:
            return await handler()

        # Durable operation ids are allocated by call order, so tool steps must
        # not race each other within one model response.
        with _parallel_tool_call_execution_mode("sequential"):
            return await handler()

    async def wrap_model_request(
        self,
        ctx: Any,
        *,
        request_context: Any,
        handler: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        if self.step_runner is None:
            msg = (
                "LambdaDurability must run under run_pydantic_ai_from_sync, "
                "or be constructed with a step_runner."
            )
            raise RuntimeError(msg)

        return await _run_async_in_durable_step(
            self.context,
            step_runner=self.step_runner,
            coro_factory=lambda: handler(request_context),
            name=self._model_step_name(ctx),
            config=self.model_step_config,
        )

    async def wrap_tool_execute(
        self,
        ctx: Any,
        *,
        call: Any,
        tool_def: Any,
        args: dict[str, Any],
        handler: Callable[[dict[str, Any]], Awaitable[Any]],
    ) -> Any:
        if self.step_runner is None:
            msg = (
                "LambdaDurability must run under run_pydantic_ai_from_sync, "
                "or be constructed with a step_runner."
            )
            raise RuntimeError(msg)

        tool_name = self._tool_name(call=call, tool_def=tool_def)

        return await _run_async_in_durable_step(
            self.context,
            step_runner=self.step_runner,
            coro_factory=lambda: handler(args),
            name=self._tool_step_name(ctx=ctx, call=call, tool_name=tool_name),
            config=self.tool_step_configs.get(tool_name, self.tool_step_config),
        )

    def _model_step_name(self, ctx: Any) -> str:
        run_step = _step_name_part(getattr(ctx, "run_step", None), default="0")
        return f"{self.agent_name}.model.request.{run_step}"

    def _tool_step_name(self, *, ctx: Any, call: Any, tool_name: str) -> str:
        run_step = _step_name_part(getattr(ctx, "run_step", None), default="0")
        call_id = _step_name_part(getattr(call, "tool_call_id", None), default="call")
        tool_name_part = _step_name_part(tool_name, default="tool")
        return f"{self.agent_name}.tool.{tool_name_part}.{run_step}.{call_id}"

    @staticmethod
    def _tool_name(*, call: Any, tool_def: Any) -> str:
        return str(
            getattr(tool_def, "name", None)
            or getattr(call, "tool_name", None)
            or "tool"
        )


LambdaDurableExecutionCapability = LambdaDurability


async def _lookup_customer_tier(customer_id: str) -> str:
    # This stands in for a database or service call. Because the capability wraps
    # tool execution, a real implementation would be checkpointed.
    return "enterprise" if customer_id.startswith("enterprise-") else "standard"


def create_support_agent(context: DurableContext) -> Any:
    try:
        from pydantic_ai import Agent, RunContext
        from pydantic_ai.models.bedrock import BedrockConverseModel
    except ImportError as exc:
        msg = (
            "The pydantic-ai package is required for this example. "
            "Install the examples package with the pydantic-ai extra, or add "
            "pydantic-ai to the Lambda deployment package, and configure "
            "Bedrock access for PYDANTIC_AI_MODEL."
        )
        raise RuntimeError(msg) from exc

    model_name = os.environ.get("PYDANTIC_AI_MODEL", "us.amazon.nova-micro-v1:0")
    if model_name.startswith("bedrock:"):
        model_name = model_name.removeprefix("bedrock:")

    agent = Agent(
        BedrockConverseModel(model_name),
        name="lambda_support_agent",
        deps_type=SupportDependencies,
        output_type=SupportAnswer,
        instructions=(
            "You are a concise first-tier support agent. Use customer tier "
            "information when it is relevant, decide whether to escalate to a "
            "human, and provide a confidence score from 0 to 1."
        ),
        capabilities=[
            LambdaDurability(context, agent_name="lambda_support_agent"),
        ],
    )

    @agent.tool
    async def lookup_customer_tier(ctx: RunContext[SupportDependencies]) -> str:
        """Return the support tier for the current customer."""
        return await _lookup_customer_tier(ctx.deps.customer_id)

    return agent


def _parse_event(event: Any) -> tuple[str, SupportDependencies]:
    if isinstance(event, dict):
        prompt = event.get("prompt")
        customer_id = event.get("customer_id", "anonymous")
    else:
        prompt = str(event)
        customer_id = "anonymous"

    if not isinstance(prompt, str) or not prompt.strip():
        msg = "event must include a non-empty 'prompt' string"
        raise ValueError(msg)

    if not isinstance(customer_id, str) or not customer_id.strip():
        msg = "event 'customer_id' must be a non-empty string when provided"
        raise ValueError(msg)

    return prompt, SupportDependencies(customer_id=customer_id)


@durable_execution
def handler(event: Any, context: DurableContext) -> dict[str, Any]:
    """Durable Lambda handler using the Pydantic AI capability prototype."""
    prompt, deps = _parse_event(event)
    result = run_pydantic_ai_from_sync(
        lambda: create_support_agent(context).run(prompt, deps=deps)
    )
    output = result.output

    return {
        "customer_id": deps.customer_id,
        "answer": asdict(output) if hasattr(output, "__dataclass_fields__") else output,
    }
