# The capability bridges the sync Lambda durable functions API with the async
# agent, using two threads connected by a queue:
#
#   - Main thread: runs the durable handler and its steps.
#   - Background thread: runs the async agent and its model/tool calls.
#
# On each model/tool call the agent enqueues a step request. The main thread
# consumes the queue, runs the durable step, and dispatches the actual async
# call back to the background loop.

import asyncio
import contextvars
import threading
from concurrent.futures import Future
from queue import Queue
from typing import Any

from aws_durable_execution_sdk_python import DurableContext
from aws_durable_execution_sdk_python.config import StepConfig

try:
    from pydantic_ai.capabilities import AbstractCapability
except ImportError:

    class AbstractCapability:  # type: ignore[no-redef]
        pass


# Durable steps checkpoint JSON data by default.
# Use these to convert model responses and retry prompts from/to JSON.
def _model_response_adapter() -> Any:
    from pydantic import TypeAdapter
    from pydantic_ai.messages import ModelResponse

    return TypeAdapter(ModelResponse)


def _retry_prompt_adapter() -> Any:
    from pydantic import TypeAdapter
    from pydantic_ai.messages import RetryPromptPart

    return TypeAdapter(RetryPromptPart)


# Persistent background-thread event loop that runs the agent and its
# model/tool calls, reused across warm invocations so loop-bound async
# resources (e.g. a cached HTTP client) stay valid.
class _AgentLoop:
    def __init__(self):
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None

    def get(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            if self._loop is None or self._loop.is_closed():
                self._loop = asyncio.new_event_loop()
                threading.Thread(target=self._loop.run_forever, daemon=True).start()
            return self._loop


_agent_loop = _AgentLoop()


# Queue-based bridge: the agent runs on the background thread and sends step
# requests to the queue. The main handler thread consumes the queue and runs
# context.step(), so every durable step is created on the same thread as the
# handler (one continuous context).
class _StepQueue:
    def __init__(self):
        self._queue: Queue = Queue()

    # Run on background thread: add step to queue, await result from main thread
    async def run_step(self, step_body, name, config, context):
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        def reply(success, value):
            fn = future.set_result if success else future.set_exception
            loop.call_soon_threadsafe(fn, value)

        # Copy the agent's contextvars to the durable step.
        ctx = contextvars.copy_context()
        self._queue.put(("step", step_body, name, config, context, reply, ctx))
        return await future

    def finish(self, result=None, error=None):
        self._queue.put(("done", result, error))

    # Run on main thread: execute queued durable steps until finish()
    def consume(self):
        while True:
            item = self._queue.get()
            if item[0] == "done":
                _, result, error = item
                if error is not None:
                    raise error
                return result
            _, step_body, name, config, context, reply, ctx = item
            try:
                reply(True, ctx.run(context.step, step_body, name=name, config=config))
            except BaseException as e:
                reply(False, e)


class LambdaDurability(AbstractCapability):
    def __init__(
        self,
        context: DurableContext,
        step_config: StepConfig | None = None,
        tool_configs: dict[str, StepConfig] | None = None,
    ):
        self.context = context
        self.step_config = step_config
        self.tool_configs = tool_configs or {}
        self.step_queue: _StepQueue | None = None

    async def wrap_run(self, ctx, *, handler):
        from pydantic_ai.tool_manager import ToolManager

        # Run tool calls one at a time. Durable step IDs are assigned by
        # call order, so concurrent step() calls could race and break replay.
        with ToolManager.parallel_execution_mode("sequential"):
            return await handler()

    async def _durable_step(self, coro_factory, name, config):
        loop = asyncio.get_running_loop()

        # step_body runs on the main thread. It schedules the async model/tool
        # call back onto the background thread (which stays free while we await).
        def step_body(_step_ctx):
            future: Future = Future()

            # Copy the durable step's contextvars to model/tool calls.
            step_ctx = contextvars.copy_context()

            def on_done(task):
                if task.exception():
                    future.set_exception(task.exception())
                else:
                    future.set_result(task.result())

            def schedule():
                try:
                    task = loop.create_task(coro_factory(), context=step_ctx)
                    task.add_done_callback(on_done)
                except BaseException as e:
                    future.set_exception(e)

            loop.call_soon_threadsafe(schedule)
            return future.result()

        return await self.step_queue.run_step(step_body, name, config, self.context)

    async def wrap_model_request(self, ctx, *, request_context, handler):
        adapter = _model_response_adapter()

        async def call_model():
            response = await handler(request_context)
            return adapter.dump_python(response, mode="json")

        checkpointed = await self._durable_step(
            call_model, "model.request", self.step_config
        )
        return adapter.validate_python(checkpointed)

    async def wrap_tool_execute(self, ctx, *, call, tool_def, args, handler):
        from pydantic_ai.exceptions import ToolRetryError

        adapter = _retry_prompt_adapter()

        # A ToolRetryError is a control signal, not a failure. Checkpoint it as
        # a normal result inside the step, then re-raise it outside so the step
        # is not recorded as failed.
        async def call_tool():
            try:
                return {"retry": False, "value": await handler(args)}
            except ToolRetryError as e:
                retry_prompt = adapter.dump_python(e.tool_retry, mode="json")
                return {"retry": True, "retry_prompt": retry_prompt}

        config = self.tool_configs.get(tool_def.name, self.step_config)
        result = await self._durable_step(call_tool, f"tool.{tool_def.name}", config)
        if result["retry"]:
            raise ToolRetryError(adapter.validate_python(result["retry_prompt"]))
        return result["value"]


# Run the agent on the background thread while the main thread consumes
# and executes its durable steps from the queue.
def run_durable(coro_factory, capability: LambdaDurability):
    step_queue = _StepQueue()
    capability.step_queue = step_queue
    loop = _agent_loop.get()

    async def run_agent():
        try:
            step_queue.finish(result=await coro_factory())
        except BaseException as e:
            step_queue.finish(error=e)

    # Copy the handler's contextvars to the agent. Fresh copy each invocation
    # so warm starts do not reuse a stale context.
    ctx = contextvars.copy_context()

    def schedule():
        loop.create_task(run_agent(), context=ctx)

    loop.call_soon_threadsafe(schedule)
    return step_queue.consume()   # blocks on the main thread until the agent finishes
