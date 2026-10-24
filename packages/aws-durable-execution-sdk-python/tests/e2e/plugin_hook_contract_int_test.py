"""The plugin hook contract, run through the real engine.

The unit tests in ``plugin_hook_registry_test.py`` drive ``PluginExecutor``
and the checkpoint loop directly. These cases drive a handler through
``DurableFunctionTestRunner``, so the hooks run on the threads the SDK really
uses: an operation's end hook on the thread that observes its result, the
change hook on the invocation's plugin change thread.
"""

from __future__ import annotations

import threading
import time
from typing import Any

from aws_durable_execution_sdk_python.config import (
    CompletionConfig,
    Duration,
    NestingType,
    ParallelConfig,
)
from aws_durable_execution_sdk_python.context import DurableContext
from aws_durable_execution_sdk_python.execution import durable_execution
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    OperationChangeInfo,
    OperationEndInfo,
    OperationStartInfo,
    OperationType,
)
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from tests.test_helpers import plugin_factory


class _TimelinePlugin(DurableInstrumentationPlugin):
    """Records when each hook was entered and left, by operation name, and on which thread."""

    def __init__(self, end_hook_delay: dict[str, float] | None = None) -> None:
        self.end_hook_delay = end_hook_delay or {}
        self.events: list[tuple[float, str, str]] = []
        self._lock = threading.Lock()

    def _record(self, label: str) -> None:
        with self._lock:
            self.events.append(
                (time.monotonic(), label, threading.current_thread().name)
            )

    def labels(self) -> list[str]:
        with self._lock:
            return [label for _, label, _ in self.events]

    def at(self, label: str) -> float:
        with self._lock:
            return next(stamp for stamp, found, _ in self.events if found == label)

    def thread_of(self, label: str) -> str:
        with self._lock:
            return next(thread for _, found, thread in self.events if found == label)

    def on_operation_start(self, info: OperationStartInfo) -> None:
        self._record(f"start:{info.name}")

    def on_operation_end(self, info: OperationEndInfo) -> None:
        if info.operation_type is OperationType.EXECUTION:
            return
        self._record(f"end-enter:{info.name}")
        delay = self.end_hook_delay.get(info.name or "")
        if delay:
            time.sleep(delay)
        self._record(f"end-leave:{info.name}")

    def on_operation_change(self, info: OperationChangeInfo) -> None:  # noqa: ARG002
        self._record("change")

    def on_invocation_end(self, info: InvocationEndInfo) -> None:  # noqa: ARG002
        self._record("invocation_end")


def _run(handler, plugin: _TimelinePlugin):
    wrapped = durable_execution(handler, plugins=[plugin_factory(plugin)])
    with DurableFunctionTestRunner(handler=wrapped) as runner:
        return runner.run(input="{}", execution_timeout=60)


def test_an_end_hook_completes_before_its_operation_returns() -> None:
    """A step's end hook, even a slow one, has returned when the step returns.

    So on one thread the end hook of one operation precedes the start hook of
    the next, which is the order the OpenTelemetry plugins and the shared
    conformance suite rely on.
    """
    plugin = _TimelinePlugin(end_hook_delay={"first": 0.3})
    step_returned: dict[str, float] = {}

    def handler(_event: Any, context: DurableContext) -> str:
        context.step(lambda _c: "first", name="first")
        step_returned["first"] = time.monotonic()
        context.step(lambda _c: "second", name="second")
        return "done"

    result = _run(handler, plugin)

    assert result.result == '"done"'
    assert plugin.at("end-leave:first") <= step_returned["first"]
    labels = plugin.labels()
    assert labels.index("end-leave:first") < labels.index("start:second")
    assert labels.index("end-leave:second") < labels.index("invocation_end")


def test_an_end_hook_runs_on_the_thread_that_ran_the_operation() -> None:
    """Top-level operations: the handler thread. Branch operations: the branch thread.

    A child context runs on its caller's thread, so a parallel branch is used
    for the branch case; its thread differs from the handler's.
    """
    plugin = _TimelinePlugin()
    threads: dict[str, str] = {}

    def branch(ctx: DurableContext) -> str:
        threads["branch"] = threading.current_thread().name
        return ctx.step(lambda _c: "inner", name="inner")

    def handler(_event: Any, context: DurableContext) -> str:
        threads["handler"] = threading.current_thread().name
        context.step(lambda _c: "top", name="top")
        return context.parallel([branch], name="fan").get_results()[0]

    result = _run(handler, plugin)

    assert result.result == '"inner"'
    assert threads["branch"] != threads["handler"]
    assert plugin.thread_of("end-enter:top") == threads["handler"]
    assert plugin.thread_of("end-enter:inner") == threads["branch"]
    # The change hook runs on the plugin change thread: neither of those, and
    # not the checkpoint thread either.
    assert plugin.thread_of("change") == "plugin-change"


def test_a_slow_end_hook_delays_only_its_own_branch() -> None:
    """Two parallel branches checkpoint through the one checkpoint thread.

    Branch 0's step has a slow end hook. When hooks ran on the checkpoint
    thread, that hook delayed every checkpoint submitted after it, so branch
    1's step could not return until it finished. Now the hook runs on branch
    0's thread. Branch 1 checkpoints its step only once the slow hook has
    entered, so its checkpoint is submitted after the slow one in every
    interleaving, and it still returns while the slow hook is running.
    """
    slow_hook_entered = threading.Event()
    release_slow_hook = threading.Event()

    class _GatedPlugin(_TimelinePlugin):
        def on_operation_end(self, info: OperationEndInfo) -> None:
            if info.name == "slow":
                self._record("end-enter:slow")
                slow_hook_entered.set()
                # An assertion here would be contained by the SDK; a label is
                # asserted on instead.
                if not release_slow_hook.wait(10):
                    self._record("timed-out")
                self._record("end-leave:slow")
                return
            super().on_operation_end(info)

    plugin = _GatedPlugin()
    returned: dict[str, float] = {}

    def slow_branch(ctx: DurableContext) -> str:
        ctx.step(lambda _c: "slow", name="slow")
        returned["slow"] = time.monotonic()
        return "slow"

    def quick_branch(ctx: DurableContext) -> str:
        assert slow_hook_entered.wait(10)
        ctx.step(lambda _c: "quick", name="quick")
        returned["quick"] = time.monotonic()
        # The quick branch returned while the slow hook was still blocked.
        release_slow_hook.set()
        return "quick"

    def handler(_event: Any, context: DurableContext) -> list[str]:
        return context.parallel([slow_branch, quick_branch], name="both").get_results()

    result = _run(handler, plugin)

    assert result.result == '["slow", "quick"]'
    assert "timed-out" not in plugin.labels()
    assert returned["quick"] < plugin.at("end-leave:slow")
    assert plugin.at("end-leave:slow") <= returned["slow"]


def test_nested_end_hooks_complete_in_order_child_before_parent() -> None:
    """parent.start, child.start, child.end, parent.end.

    The child's terminal checkpoint is synchronous, and its end hook runs on
    the child's thread before the child returns. So the child's hook has
    returned before the parent can complete and run its own.
    """
    plugin = _TimelinePlugin(end_hook_delay={"inner": 0.2})

    def child(ctx: DurableContext) -> str:
        return ctx.step(lambda _c: "stepped", name="inner")

    def handler(_event: Any, context: DurableContext) -> str:
        return context.run_in_child_context(child, name="outer")

    result = _run(handler, plugin)

    assert result.result == '"stepped"'
    labels = plugin.labels()
    assert labels.index("start:outer") < labels.index("start:inner")
    assert labels.index("end-leave:inner") < labels.index("end-enter:outer")
    assert labels.index("end-leave:outer") < labels.index("invocation_end")


def test_map_item_end_hooks_complete_before_the_map_end_hook() -> None:
    plugin = _TimelinePlugin(end_hook_delay={"doubles": 0.05})

    def handler(_event: Any, context: DurableContext) -> list[int]:
        return context.map(
            [1, 2, 3], lambda _ctx, item, _index, _items: item * 2, name="doubles"
        ).get_results()

    result = _run(handler, plugin)

    assert result.result == "[2, 4, 6]"
    labels = plugin.labels()
    map_end = labels.index("end-enter:doubles")
    item_leaves = [
        index
        for index, label in enumerate(labels)
        if label.startswith("end-leave:") and label != "end-leave:doubles"
    ]
    assert len(item_leaves) == 3
    assert all(leave < map_end for leave in item_leaves)
    assert labels.index("end-leave:doubles") < labels.index("invocation_end")


def test_early_completion_lets_a_branch_end_after_its_parent() -> None:
    """The documented exception to child-before-parent.

    With ``first_successful`` the parallel completes when the first branch
    does, while the second branch is still running. The parent's end hook
    runs then, on the handler thread. The second branch is a FLAT, virtual
    context: it checkpoints nothing, so when it returns it runs its own end
    hook on its thread, after the parent's. The SDK joins every branch thread
    before ``on_invocation_end``, so that hook still precedes it.

    The quick branch returns only once the slow branch is running. On early
    completion the SDK cancels the branches the pool has not started yet, and
    a branch that never ran has no end hook; so without this, a slow machine
    would have no hook to find.
    """
    slow_started = threading.Event()
    parent_ended = threading.Event()

    class _ReleasingPlugin(_TimelinePlugin):
        def on_operation_end(self, info: OperationEndInfo) -> None:
            super().on_operation_end(info)
            if info.name == "race":
                parent_ended.set()

    plugin = _ReleasingPlugin()

    def quick(_ctx: DurableContext) -> str:
        assert slow_started.wait(10)
        return "quick"

    def slow(_ctx: DurableContext) -> str:
        slow_started.set()
        assert parent_ended.wait(10)
        return "slow"

    def handler(_event: Any, context: DurableContext) -> str:
        results = context.parallel(
            [quick, slow],
            config=ParallelConfig(
                completion_config=CompletionConfig.first_successful(),
                nesting_type=NestingType.FLAT,
            ),
            name="race",
        )
        return results.get_results()[0]

    result = _run(handler, plugin)

    assert result.result == '"quick"'
    labels = plugin.labels()
    assert labels.index("end-enter:parallel-branch-0") < labels.index("end-enter:race")
    assert labels.index("end-enter:race") < labels.index("end-enter:parallel-branch-1")
    assert labels.index("end-leave:parallel-branch-1") < labels.index("invocation_end")


def test_a_wait_in_a_branch_resumed_by_a_refresh_ends_before_the_branches_next_step() -> (
    None
):
    """The wait completes in a refresh response, which no operation thread waits for.

    The other branch stays in a step until the first branch is past its wait,
    so the coordinator is still running when the wait comes due: it waits for
    the refresh and resumes the branch in this invocation. The resumed branch
    thread re-traverses the wait and reads its result; that read runs the end
    hook, on that thread, before the branch starts its next step. So the
    wait's end hook precedes the step's start hook on one thread, which is the
    order the OpenTelemetry suite asserts, and the end of the invocation finds
    nothing left to run for it.
    """
    plugin = _TimelinePlugin()
    step_threads: dict[str, str] = {}
    first_branch_done = threading.Event()

    def resumed(ctx: DurableContext) -> str:
        ctx.step(lambda _c: "before", name="before-wait")
        ctx.wait(Duration.from_seconds(1), name="branch-wait")
        step_threads["after-wait"] = threading.current_thread().name
        after = ctx.step(lambda _c: "after", name="after-wait")
        first_branch_done.set()
        return after

    def hold(_c: Any) -> str:
        # Keeps the coordinator in the invocation until the first branch has
        # resumed; the timeout only bounds a failing test.
        first_branch_done.wait(30)
        return "other"

    def other(ctx: DurableContext) -> str:
        return ctx.step(hold, name="hold-step")

    def handler(_event: Any, context: DurableContext) -> list[str]:
        return context.parallel([resumed, other], name="both").get_results()

    result = _run(handler, plugin)

    assert result.result == '["after", "other"]'
    labels = plugin.labels()
    assert labels.count("invocation_end") == 1, (
        "the invocation suspended instead of resuming the branch in-process"
    )
    assert labels.count("end-enter:branch-wait") == 1
    assert labels.index("end-leave:branch-wait") < labels.index("start:after-wait")
    assert plugin.thread_of("end-enter:branch-wait") == step_threads["after-wait"]


def test_a_callback_result_read_ends_the_callback_before_the_next_step() -> None:
    """The submitter completes the callback, so its result arrives in a response
    no operation thread waits for; the thread that reads the result runs the
    end hook first. When the completion arrives only after this invocation
    suspended, the replaying thread runs it instead. Either way the callback's
    end hook precedes the next step's start hook on one thread, once.
    """

    class _CallbackAware(_TimelinePlugin):
        def on_operation_end(self, info: OperationEndInfo) -> None:
            if info.operation_type is OperationType.CALLBACK:
                self._record("end-enter:the-callback")
                self._record("end-leave:the-callback")
                return
            super().on_operation_end(info)

    plugin = _CallbackAware()
    step_threads: dict[str, str] = {}

    def handler(_event: Any, context: DurableContext) -> str:
        context.wait_for_callback(
            lambda callback_id, _ctx: runner.send_callback_success(
                callback_id, b'"completed"'
            ),
            name="await-external",
        )
        step_threads["after-callback"] = threading.current_thread().name
        return context.step(lambda _c: "after", name="after-callback")

    wrapped = durable_execution(handler, plugins=[plugin_factory(plugin)])
    with DurableFunctionTestRunner(handler=wrapped) as runner:
        result = runner.run(input="{}", execution_timeout=60)

    assert result.result == '"after"'
    labels = plugin.labels()
    assert labels.count("end-enter:the-callback") == 1
    assert labels.index("end-leave:the-callback") < labels.index("start:after-callback")
    assert plugin.thread_of("end-enter:the-callback") == step_threads["after-callback"]
