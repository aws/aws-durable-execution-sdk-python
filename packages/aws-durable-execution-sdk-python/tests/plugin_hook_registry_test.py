"""Where plugin hooks run once the checkpoint thread stops running them.

The checkpoint thread records the hooks of one checkpoint response and runs
none. It records each terminal operation's end hook before it publishes the
response's operations, so the thread that then observes a terminal result,
the waiter of a synchronous checkpoint or a later reader, claims the hook and
runs it before it acts on the result. End hooks no thread claims run on the
handler thread before ``on_invocation_end``. The change hook goes to the
invocation's plugin change thread, which merges the notifications that arrive
while a plugin is still in the hook.

A failing case here means a change breaks that placement or order; the class
docstring of ``DurableInstrumentationPlugin`` states it as the public contract.
"""

from __future__ import annotations

import datetime
import functools
import logging
import threading
import time
from collections.abc import Callable, Collection
from types import TracebackType
from unittest.mock import Mock

import pytest

from aws_durable_execution_sdk_python import plugin as plugin_module
from aws_durable_execution_sdk_python.exceptions import GetExecutionStateError
from aws_durable_execution_sdk_python.lambda_service import (
    CheckpointOutput,
    CheckpointUpdatedExecutionState,
    DurableExecutionInvocationOutput,
    InvocationStatus as ServiceInvocationStatus,
    LambdaClient,
    Operation,
    OperationAction,
    OperationStatus,
    OperationSubType,
    OperationType as ServiceOperationType,
    OperationUpdate,
)
from aws_durable_execution_sdk_python.plugin import (
    DurableInstrumentationPlugin,
    InvocationEndInfo,
    InvocationStartInfo,
    OperationChangeInfo,
    OperationEndInfo,
    OperationInfo,
    PluginExecutor,
    PropagationInput,
    PropagationMetadata,
    _merge_change_infos,
)
from aws_durable_execution_sdk_python import state as state_module
from aws_durable_execution_sdk_python.context import DurableContext, ExecutionContext
from aws_durable_execution_sdk_python.identifier import OperationIdentifier
from aws_durable_execution_sdk_python.state import ExecutionState, ReplayStatus
from aws_durable_execution_sdk_python.threading import CompletionEvent
from tests.test_helpers import plugin_factory

START_TS = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
SUCCEEDED = DurableExecutionInvocationOutput(status=ServiceInvocationStatus.SUCCEEDED)
TIMEOUT = 10.0


def _operation(
    operation_id: str,
    status: OperationStatus = OperationStatus.SUCCEEDED,
    parent_id: str | None = None,
) -> Operation:
    return Operation(
        operation_id=operation_id,
        operation_type=ServiceOperationType.STEP,
        status=status,
        name=operation_id,
        sub_type=OperationSubType.STEP,
        parent_id=parent_id,
        start_timestamp=START_TS,
        end_timestamp=START_TS,
    )


def _update(
    operation_id: str, action: OperationAction = OperationAction.SUCCEED
) -> OperationUpdate:
    return OperationUpdate(
        operation_id=operation_id,
        operation_type=ServiceOperationType.STEP,
        action=action,
        name=operation_id,
    )


def _map(*operations: Operation) -> dict[str, Operation]:
    return {operation.operation_id: operation for operation in operations}


class _Recorder(DurableInstrumentationPlugin):
    """Records every hook, and any marker the test adds, with the thread it ran on."""

    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []
        self.changes: list[OperationChangeInfo] = []
        self.ends: list[OperationEndInfo] = []
        self._lock = threading.Lock()

    def record(self, label: str) -> None:
        with self._lock:
            self.events.append((label, threading.current_thread().name))

    def block_on(self, event: threading.Event) -> None:
        """Wait inside a hook. An assertion there would be contained by the SDK."""
        if not event.wait(TIMEOUT):
            self.record("timed-out")

    def labels(self) -> list[str]:
        with self._lock:
            return [label for label, _ in self.events]

    def thread_of(self, label: str) -> str:
        with self._lock:
            return next(name for found, name in self.events if found == label)

    def on_invocation_start(self, info: InvocationStartInfo) -> None:  # noqa: ARG002
        self.record("invocation_start")

    def on_invocation_end(self, info: InvocationEndInfo) -> None:  # noqa: ARG002
        self.record("invocation_end")

    def on_operation_end(self, info: OperationEndInfo) -> None:
        with self._lock:
            self.ends.append(info)
        self.record(f"operation_end:{info.operation_id}")

    def on_operation_change(self, info: OperationChangeInfo) -> None:
        with self._lock:
            self.changes.append(info)
        self.record("operation_change:" + ",".join(sorted(info.updated_operations)))


def _started_executor(plugin: DurableInstrumentationPlugin) -> PluginExecutor:
    executor = PluginExecutor([plugin_factory(plugin)])
    executor.on_invocation_start(
        execution_arn="arn:exec",
        lambda_context=None,
        execution_start_time=START_TS,
        is_first_invocation=True,
    )
    return executor


def _thread(target: Callable[[], object], name: str) -> threading.Thread:
    """A started daemon thread: a thread a failed test leaves blocked cannot hold pytest open."""
    thread = threading.Thread(target=target, name=name, daemon=True)
    thread.start()
    return thread


def _join(thread: threading.Thread) -> None:
    thread.join(TIMEOUT)
    assert not thread.is_alive()


def _wait_until(condition: Callable[[], bool], timeout: float = TIMEOUT) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.001)
    return condition()


def _end_in_thread(executor: PluginExecutor, name: str = "handler") -> threading.Thread:
    return _thread(lambda: executor.on_invocation_end(SUCCEEDED), name)


# region End hook registry


def test_record_runs_no_hook_and_drain_runs_one_per_terminal_operation() -> None:
    recorder = _Recorder()
    executor = _started_executor(recorder)

    executor.record_end_hooks(
        [
            _operation("a"),
            _operation("started", OperationStatus.STARTED),
            _operation("b", OperationStatus.FAILED),
        ]
    )
    assert recorder.labels() == ["invocation_start"]

    executor.drain_end_hooks()

    assert recorder.labels() == [
        "invocation_start",
        "operation_end:a",
        "operation_end:b",
    ]
    assert recorder.thread_of("operation_end:a") == threading.current_thread().name
    assert [info.status for info in recorder.ends] == [
        OperationStatus.SUCCEEDED,
        OperationStatus.FAILED,
    ]


def test_claim_runs_the_hook_on_the_claiming_thread_and_only_once() -> None:
    recorder = _Recorder()
    executor = _started_executor(recorder)
    executor.record_end_hooks([_operation("a")])

    claimer = _thread(lambda: executor.claim_end_hook("a"), "claimer")
    _join(claimer)
    executor.claim_end_hook("a")
    executor.drain_end_hooks()

    assert recorder.labels() == ["invocation_start", "operation_end:a"]
    assert recorder.thread_of("operation_end:a") == "claimer"


def test_concurrent_claims_of_one_operation_run_the_hook_exactly_once() -> None:
    recorder = _Recorder()
    executor = _started_executor(recorder)
    executor.record_end_hooks([_operation("a")])
    go = threading.Event()

    def claim() -> None:
        go.wait(TIMEOUT)
        executor.claim_end_hook("a")

    claimers = [_thread(claim, f"claimer-{i}") for i in range(8)]
    go.set()
    for claimer in claimers:
        _join(claimer)

    assert recorder.labels().count("operation_end:a") == 1


def test_claiming_an_unrecorded_operation_does_nothing() -> None:
    recorder = _Recorder()
    executor = _started_executor(recorder)
    executor.record_end_hooks([_operation("a")])

    executor.claim_end_hook("not-recorded")

    assert recorder.labels() == ["invocation_start"]


def test_the_registry_is_inert_without_plugins() -> None:
    executor = PluginExecutor(None)

    executor.record_end_hooks([_operation("a")])
    executor.claim_end_hook("a")
    executor.drain_end_hooks()
    executor.on_invocation_end(SUCCEEDED)


def test_drain_runs_the_unclaimed_hooks_in_recording_order() -> None:
    recorder = _Recorder()
    executor = _started_executor(recorder)
    executor.record_end_hooks([_operation("a")])
    executor.record_end_hooks([_operation("b"), _operation("c")])
    executor.claim_end_hook("b")

    executor.drain_end_hooks()

    assert recorder.labels() == [
        "invocation_start",
        "operation_end:b",
        "operation_end:a",
        "operation_end:c",
    ]


def test_an_operation_reported_again_before_its_hook_ran_keeps_one_entry() -> None:
    """The repeated report replaces the entry's state; the entry keeps its position."""
    recorder = _Recorder()
    executor = _started_executor(recorder)
    executor.record_end_hooks([_operation("a"), _operation("b")])
    executor.record_end_hooks([_operation("a", OperationStatus.FAILED)])

    executor.drain_end_hooks()

    assert recorder.labels() == [
        "invocation_start",
        "operation_end:a",
        "operation_end:b",
    ]
    assert recorder.ends[0].status is OperationStatus.FAILED


def test_an_operation_the_replay_path_reported_is_not_recorded_again() -> None:
    """The invocation input carried the operation; a response lists it again."""
    recorder = _Recorder()
    executor = _started_executor(recorder)

    executor.on_operation_update(_operation("a"))
    executor.record_end_hooks([_operation("a")])
    executor.drain_end_hooks()

    assert recorder.labels() == ["invocation_start", "operation_end:a"]


def test_a_claimed_operation_is_not_reported_again_by_the_replay_path() -> None:
    recorder = _Recorder()
    executor = _started_executor(recorder)
    executor.record_end_hooks([_operation("a")])

    executor.claim_end_hook("a")
    executor.on_operation_update(_operation("a"))

    assert recorder.labels() == ["invocation_start", "operation_end:a"]


def test_unclaimed_end_hooks_run_on_the_handler_thread_before_invocation_end() -> None:
    recorder = _Recorder()
    executor = _started_executor(recorder)
    executor.record_end_hooks([_operation("a")])

    _join(_end_in_thread(executor))

    assert recorder.labels() == [
        "invocation_start",
        "operation_end:a",
        "invocation_end",
    ]
    assert recorder.thread_of("operation_end:a") == "handler"


class _ObservedEvent(threading.Event):
    """An event whose wait() reports that a thread has entered the wait."""

    def __init__(self, entered: threading.Event) -> None:
        super().__init__()
        self._entered = entered

    def wait(self, timeout: float | None = None) -> bool:
        self._entered.set()
        return super().wait(timeout)


def test_a_second_observer_waits_for_a_hook_another_thread_is_running() -> None:
    """Two threads observe one result: one runs the hook, the other waits for it.

    Without the wait the second thread would act on the result while the end
    hook still runs. The readiness signal comes from inside the wait itself:
    the hook's completion event is replaced by one that reports entry into
    its wait, so the negative assertion is made while the second thread is in
    that wait, and an implementation that does not wait never signals.
    """
    hook_entered = threading.Event()
    release = threading.Event()
    wait_entered = threading.Event()

    class _Slow(_Recorder):
        def on_operation_end(self, info: OperationEndInfo) -> None:
            super().on_operation_end(info)
            hook_entered.set()
            self.block_on(release)
            self.record("end-left")

    recorder = _Slow()
    executor = _started_executor(recorder)
    executor.record_end_hooks([_operation("a")])
    first = _thread(lambda: executor.claim_end_hook("a"), "first")
    assert hook_entered.wait(TIMEOUT)
    with executor.claim_lock:
        entry = executor._end_hooks["a"]  # noqa: SLF001
        assert entry.done is not None
        entry.done = _ObservedEvent(wait_entered)

    def second_observer() -> None:
        executor.claim_end_hook("a")
        recorder.record("second-proceeded")

    second = _thread(second_observer, "second")
    assert wait_entered.wait(TIMEOUT), "the second observer did not wait"
    assert "second-proceeded" not in recorder.labels()
    release.set()
    _join(first)
    _join(second)

    labels = recorder.labels()
    assert "timed-out" not in labels
    assert labels.count("operation_end:a") == 1
    assert labels.index("end-left") < labels.index("second-proceeded")


def test_a_control_exception_in_one_hook_of_a_tree_retires_the_rest() -> None:
    """The entries not reached are retired, so no thread waits on them forever."""

    class _Interrupting(_Recorder):
        def on_operation_end(self, info: OperationEndInfo) -> None:
            super().on_operation_end(info)
            if info.operation_id == "child":
                raise KeyboardInterrupt

    recorder = _Interrupting()
    executor = _started_executor(recorder)
    executor.record_end_hooks(
        [_operation("parent"), _operation("child", parent_id="parent")]
    )

    with pytest.raises(KeyboardInterrupt):
        executor.claim_end_hook("parent")

    # Nothing is left owned-but-unfinished: a later observer returns at once,
    # and the drain finds nothing.
    _join(_thread(lambda: executor.claim_end_hook("parent"), "late-observer"))
    executor.drain_end_hooks()
    assert recorder.labels() == ["invocation_start", "operation_end:child"]


def test_a_hook_that_reads_its_own_operation_does_not_wait_for_itself() -> None:
    class _Reentrant(_Recorder):
        def on_operation_end(self, info: OperationEndInfo) -> None:
            super().on_operation_end(info)
            executor.claim_end_hook(info.operation_id)
            self.record("reentered")

    recorder = _Reentrant()
    executor = _started_executor(recorder)
    executor.record_end_hooks([_operation("a")])

    _join(_thread(lambda: executor.claim_end_hook("a"), "claimer"))

    assert recorder.labels() == ["invocation_start", "operation_end:a", "reentered"]


def test_a_hook_reserved_for_a_waiter_is_run_by_the_waiter_and_awaited_by_a_reader() -> (
    None
):
    """The response wakes the operation's synchronous waiter: the hook is its.

    A reader that observes the result first waits for the waiter's hook, and
    an ancestor's claim leaves the reserved hook alone.
    """
    recorder = _Recorder()
    executor = _started_executor(recorder)
    wait_entered = threading.Event()
    executor.record_end_hooks(
        [_operation("parent"), _operation("child", parent_id="parent")],
        waited_for={"child"},
    )
    with executor.claim_lock:
        entry = executor._end_hooks["child"]  # noqa: SLF001
        assert entry.done is not None
        entry.done = _ObservedEvent(wait_entered)

    executor.claim_end_hook("parent")
    assert recorder.labels() == ["invocation_start", "operation_end:parent"]

    def reader() -> None:
        executor.claim_end_hook("child")
        recorder.record("reader-proceeded")

    reading = _thread(reader, "reader")
    assert wait_entered.wait(TIMEOUT), "the reader did not wait for the waiter"
    assert "reader-proceeded" not in recorder.labels()

    waiting = _thread(lambda: executor.claim_end_hook("child", waiter=True), "waiter")
    _join(waiting)
    _join(reading)

    assert recorder.labels() == [
        "invocation_start",
        "operation_end:parent",
        "operation_end:child",
        "reader-proceeded",
    ]
    assert recorder.thread_of("operation_end:child") == "waiter"


def test_a_reader_keeps_its_descendant_hook_from_an_ancestors_waiter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The reader decided under the claim lock; the ancestor's waiter claims next.

    The reader is held inside the claim lock, after it read the child's
    terminal state and took the child's hook. The waiter's claim is made
    only when the lock is released, so it finds the child owned and leaves
    it: the child's hook runs on the reader, the parent's on the waiter.
    """
    recorder = _Recorder()
    state = _state(recorder, [])
    executor = state._plugin_executor  # noqa: SLF001
    state.fetch_paginated_operations(
        [_operation("parent"), _operation("child", parent_id="parent")],
        "token",
        None,
        before_publish=functools.partial(
            executor.record_end_hooks, waited_for={"parent"}
        ),
    )
    decided = threading.Event()
    proceed = threading.Event()
    original_take = executor.take_end_hook

    def held_take(operation_id: str, *, waiter: bool = False) -> object:
        claim = original_take(operation_id, waiter=waiter)
        if threading.current_thread().name == "reader":
            decided.set()
            proceed.wait(TIMEOUT)
        return claim

    monkeypatch.setattr(executor, "take_end_hook", held_take)

    def reader() -> None:
        state.get_checkpoint_result("child")
        recorder.record("reader-proceeded")

    reading = _thread(reader, "reader")
    assert decided.wait(TIMEOUT)
    waiting = _thread(lambda: executor.claim_end_hook("parent", waiter=True), "waiter")
    proceed.set()
    _join(reading)
    _join(waiting)

    assert recorder.thread_of("operation_end:child") == "reader"
    assert recorder.thread_of("operation_end:parent") == "waiter"
    labels = recorder.labels()
    assert labels.index("operation_end:child") < labels.index("reader-proceeded")
    assert labels.count("operation_end:child") == 1
    executor.on_invocation_end(SUCCEEDED)


class _ObservedLock:
    """Stands in for the claim lock; reports that a thread is acquiring it."""

    def __init__(self, lock: threading.Lock, acquiring: threading.Event) -> None:
        self._lock = lock
        self._acquiring = acquiring

    def __enter__(self) -> bool:
        self._acquiring.set()
        return self._lock.__enter__()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._lock.__exit__(exc_type, exc, tb)


def test_a_reader_takes_the_claim_lock_before_it_reads_the_state() -> None:
    """What makes the handoff above hold: the read itself is under the lock.

    With the lock held by the test, the reader must reach the lock without
    having read the state. A read made before the lock would let another
    thread's claim come between the read and the decision.
    """
    recorder = _Recorder()
    state = _state(recorder, [])
    executor = state._plugin_executor  # noqa: SLF001
    state.fetch_paginated_operations(
        [_operation("a")], "token", None, before_publish=executor.record_end_hooks
    )
    readers: list[str] = []

    class _ObservedOperations(dict):  # type: ignore[type-arg]
        def get(self, key: str, default: object = None) -> object:
            readers.append(threading.current_thread().name)
            return super().get(key, default)

    state._operations = _ObservedOperations(state._operations)  # noqa: SLF001
    acquiring = threading.Event()
    real_lock = executor.claim_lock
    executor.claim_lock = _ObservedLock(real_lock, acquiring)  # type: ignore[assignment]

    with real_lock:
        reading = _thread(lambda: state.get_checkpoint_result("a"), "reader")
        assert acquiring.wait(TIMEOUT)
        assert "reader" not in readers, "the state was read before the claim lock"
    _join(reading)

    assert readers == ["reader"]
    assert recorder.thread_of("operation_end:a") == "reader"
    executor.claim_lock = real_lock
    executor.on_invocation_end(SUCCEEDED)


def test_a_waiters_claim_is_decided_under_the_claim_lock() -> None:
    """The other half of the handoff: the waiter's claim takes the same lock.

    With the lock held by the test, the waiter reaches the lock without having
    run any hook. A claim decided without the lock could take a descendant
    between its reader's read and that reader's decision.
    """
    recorder = _Recorder()
    executor = _started_executor(recorder)
    executor.record_end_hooks(
        [_operation("parent"), _operation("child", parent_id="parent")],
        waited_for={"parent"},
    )
    acquiring = threading.Event()
    real_lock = executor.claim_lock
    executor.claim_lock = _ObservedLock(real_lock, acquiring)  # type: ignore[assignment]

    with real_lock:
        waiting = _thread(
            lambda: executor.claim_end_hook("parent", waiter=True), "waiter"
        )
        assert acquiring.wait(TIMEOUT)
        assert recorder.labels() == ["invocation_start"], "a hook ran without the lock"
    _join(waiting)

    assert recorder.labels() == [
        "invocation_start",
        "operation_end:child",
        "operation_end:parent",
    ]


class _OwnerTrackingLock:
    """Stands in for the claim lock; remembers which thread holds it, and numbers
    its acquisitions so two steps can be shown to share one."""

    def __init__(self, lock: threading.Lock) -> None:
        self._lock = lock
        self.owner: int | None = None
        self.acquisition = 0

    def __enter__(self) -> bool:
        held = self._lock.__enter__()
        self.owner = threading.get_ident()
        self.acquisition += 1
        return held

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.owner = None
        self._lock.__exit__(exc_type, exc, tb)


def test_every_claim_decision_is_made_while_the_deciding_thread_holds_the_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reader, waiter, replay path and drain: each decides inside the lock.

    Taking and releasing the lock before the decision would satisfy a test
    that only observes acquisition; this one observes, at the decision
    itself, that the deciding thread holds the lock. The reader's read of
    the state and its decision must also fall in the same acquisition: two
    acquisitions, one per step, would let another claim come between them.
    """
    recorder = _Recorder()
    state = _state(recorder, [])
    executor = state._plugin_executor  # noqa: SLF001
    lock = _OwnerTrackingLock(executor.claim_lock)
    executor.claim_lock = lock  # type: ignore[assignment]
    decisions: list[tuple[str, bool, int]] = []
    reads: list[tuple[str, bool, int]] = []
    original_take = executor.take_end_hook
    original_tree = executor._claim_tree_locked  # noqa: SLF001

    def observed_take(operation_id: str, *, waiter: bool = False) -> object:
        decisions.append(
            (operation_id, lock.owner == threading.get_ident(), lock.acquisition)
        )
        return original_take(operation_id, waiter=waiter)

    def observed_tree(root: plugin_module._RecordedEndHook) -> object:  # noqa: SLF001
        decisions.append(
            (
                root.info.operation_id,
                lock.owner == threading.get_ident(),
                lock.acquisition,
            )
        )
        return original_tree(root)

    class _ObservedOperations(dict):  # type: ignore[type-arg]
        def get(self, key: str, default: object = None) -> object:
            reads.append((key, lock.owner == threading.get_ident(), lock.acquisition))
            return super().get(key, default)

    monkeypatch.setattr(executor, "take_end_hook", observed_take)
    monkeypatch.setattr(executor, "_claim_tree_locked", observed_tree)
    state.fetch_paginated_operations(
        [_operation("waited"), _operation("read"), _operation("drained")],
        "token",
        None,
        before_publish=functools.partial(
            executor.record_end_hooks, waited_for={"waited"}
        ),
    )

    state._operations = _ObservedOperations(state._operations)  # noqa: SLF001

    _join(_thread(lambda: state.get_checkpoint_result("read"), "reader"))
    _join(_thread(lambda: executor.claim_end_hook("waited", waiter=True), "waiter"))
    executor.on_operation_update(_operation("replayed"))
    executor.drain_end_hooks()

    assert {operation_id for operation_id, _, _ in decisions} == {
        "read",
        "waited",
        "replayed",
        "drained",
    }
    assert all(under_lock for _, under_lock, _ in decisions), decisions
    # The reader read the state under the lock, in the same acquisition as
    # its decision.
    assert len(reads) == 1, reads
    read_key, read_under_lock, read_acquisition = reads[0]
    assert (read_key, read_under_lock) == ("read", True)
    assert {acq for op, _, acq in decisions if op == "read"} == {read_acquisition}
    assert sorted(label for label in recorder.labels() if ":" in label) == [
        "operation_end:drained",
        "operation_end:read",
        "operation_end:replayed",
        "operation_end:waited",
    ]


def test_hooks_that_read_each_others_operations_do_not_wait_on_each_other() -> None:
    """Two threads run the end hooks of a and b at the same time.

    From inside each hook the plugin reads the other operation, as a plugin
    that calls a callback's ``result()`` from a hook would. Each hook's
    completion event is set only when the hook returns, so were an in-hook
    read to wait for the other hook, the two would wait on each other
    forever. A read from inside a hook waits for nothing: both hooks return,
    both reads see the terminal result, and each hook runs once.
    """
    both_in_hook = threading.Barrier(2, timeout=TIMEOUT)
    seen_from_hook: dict[str, bool] = {}
    holder: dict[str, ExecutionState] = {}

    class _CrossReading(_Recorder):
        def on_operation_end(self, info: OperationEndInfo) -> None:
            super().on_operation_end(info)
            if info.operation_id in ("a", "b"):
                both_in_hook.wait()
                other = "b" if info.operation_id == "a" else "a"
                seen_from_hook[info.operation_id] = (
                    holder["state"].get_checkpoint_result(other).is_succeeded()
                )
                self.record("hook-returned:" + info.operation_id)

    recorder = _CrossReading()
    state = _state(recorder, [])
    holder["state"] = state
    state._plugin_executor.record_end_hooks([_operation("a"), _operation("b")])  # noqa: SLF001
    state._operations.update(_map(_operation("a"), _operation("b")))  # noqa: SLF001

    reader_a = _thread(lambda: state.get_checkpoint_result("a"), "reader-a")
    reader_b = _thread(lambda: state.get_checkpoint_result("b"), "reader-b")
    _join(reader_a)
    _join(reader_b)

    assert seen_from_hook == {"a": True, "b": True}
    labels = recorder.labels()
    assert labels.count("operation_end:a") == 1
    assert labels.count("operation_end:b") == 1
    assert recorder.thread_of("operation_end:a") == "reader-a"
    assert recorder.thread_of("operation_end:b") == "reader-b"
    state._plugin_executor.on_invocation_end(SUCCEEDED)  # noqa: SLF001
    assert recorder.labels().count("operation_end:a") == 1


def test_a_change_hook_that_holds_a_lock_and_reads_a_result_does_not_deadlock() -> None:
    """A plugin guards its hooks with one lock, as the contract tells it to.

    From on_operation_change, holding the lock, it reads a callback's result
    through the call ``Callback.result()`` makes. The callback's end hook is
    recorded and no thread has taken it. Were the read to claim the hook, the
    end hook would run nested in the change hook, on the change thread, and
    block on the lock that thread holds: a deadlock, and the read would
    never complete. A read from inside any hook claims nothing, so the read
    completes and the change hook returns; the end hook runs on the handler
    thread at the drain, once, before the invocation-end hook.

    The invocation ends only after the read has happened, so the drain
    cannot run the end hook first and leave the read nothing to claim.
    """
    lock = threading.Lock()
    read_done = threading.Event()
    holder: dict[str, ExecutionState] = {}
    seen: dict[str, bool] = {}

    class _Locking(_Recorder):
        def on_operation_change(self, info: OperationChangeInfo) -> None:
            super().on_operation_change(info)
            with lock:
                seen["cb"] = holder["state"].get_checkpoint_result("cb").is_succeeded()
                read_done.set()

        def on_operation_end(self, info: OperationEndInfo) -> None:
            with lock:
                super().on_operation_end(info)

    recorder = _Locking()
    state = _state(recorder, [])
    holder["state"] = state
    cb = _operation("cb")
    executor = state._plugin_executor  # noqa: SLF001
    executor.record_end_hooks([cb])
    state._operations.update(_map(cb))  # noqa: SLF001
    executor.record_change([cb], _map(cb), {})

    assert read_done.wait(TIMEOUT), "the read from inside the change hook hung"
    assert seen == {"cb": True}
    assert "operation_end:cb" not in recorder.labels()

    _join(_end_in_thread(executor))

    labels = recorder.labels()
    assert labels.count("operation_end:cb") == 1
    assert labels.index("operation_end:cb") < labels.index("invocation_end")
    assert recorder.thread_of("operation_end:cb") == "handler"


def test_a_propagation_metadata_provider_that_reads_a_result_runs_no_hook() -> None:
    """``provide_propagation_metadata`` is the other entry into plugin code.

    A provider that reads a terminal operation whose hook is recorded and
    untaken runs no hook, like a read from any hook; the hook stays for the
    consuming read.
    """
    holder: dict[str, ExecutionState] = {}

    class _Reading(_Recorder):
        def provide_propagation_metadata(
            self, info: PropagationInput
        ) -> PropagationMetadata | None:
            assert holder["state"].get_checkpoint_result("a").is_succeeded()
            self.record("read-a-from-provider")
            return None

    recorder = _Reading()
    state = _state(recorder, [])
    holder["state"] = state
    executor = state._plugin_executor  # noqa: SLF001
    executor.record_end_hooks([_operation("a")])
    state._operations.update(_map(_operation("a")))  # noqa: SLF001

    executor.provide_propagation_metadata(
        PropagationInput(
            execution_arn="arn:exec", operation_id="invoke", target_function_name="f"
        )
    )
    labels = recorder.labels()
    assert "read-a-from-provider" in labels
    assert "operation_end:a" not in labels

    assert state.get_checkpoint_result("a").is_succeeded()
    assert recorder.labels().count("operation_end:a") == 1
    executor.on_invocation_end(SUCCEEDED)
    assert recorder.labels().count("operation_end:a") == 1


def test_a_read_from_inside_an_end_hook_runs_no_hook() -> None:
    """A hook reads an operation whose recorded hook no thread has taken.

    The read inspects the result and runs nothing: nesting one hook in
    another is not done. The hook stays recorded, and the thread that later
    consumes the result runs it, before it acts on the result.
    """
    holder: dict[str, ExecutionState] = {}

    class _ReadingOther(_Recorder):
        def on_operation_end(self, info: OperationEndInfo) -> None:
            super().on_operation_end(info)
            if info.operation_id == "a":
                assert holder["state"].get_checkpoint_result("c").is_succeeded()
                self.record("read-c-from-hook-a")

    recorder = _ReadingOther()
    state = _state(recorder, [])
    holder["state"] = state
    state._plugin_executor.record_end_hooks([_operation("a"), _operation("c")])  # noqa: SLF001
    state._operations.update(_map(_operation("a"), _operation("c")))  # noqa: SLF001

    _join(_thread(lambda: state.get_checkpoint_result("a"), "reader-a"))
    labels = recorder.labels()
    assert "operation_end:c" not in labels
    assert labels.index("read-c-from-hook-a") > labels.index("operation_end:a")

    def consumer() -> None:
        assert state.get_checkpoint_result("c").is_succeeded()
        recorder.record("consumed:c")

    _join(_thread(consumer, "reader-c"))
    labels = recorder.labels()
    assert labels.index("operation_end:c") < labels.index("consumed:c")
    assert recorder.thread_of("operation_end:c") == "reader-c"
    state._plugin_executor.on_invocation_end(SUCCEEDED)  # noqa: SLF001
    assert recorder.labels().count("operation_end:c") == 1


def test_a_virtual_child_contexts_end_hook_reads_without_waiting() -> None:
    """The end hook of a context that wrote no checkpoint is an end hook too.

    It runs on the context's thread as its body returns, through a different
    dispatch than the registry's. A read from inside it of an operation whose
    hook another thread is running returns at once, like any read from
    inside an end hook; otherwise that hook could wait on this one.
    """
    hook_a_running = threading.Event()
    release_hook_a = threading.Event()
    holder: dict[str, ExecutionState] = {}

    class _ReadingFromChildEnd(_Recorder):
        def on_operation_end(self, info: OperationEndInfo) -> None:
            super().on_operation_end(info)
            if info.operation_id == "a":
                hook_a_running.set()
                assert release_hook_a.wait(TIMEOUT)
            elif info.operation_id == "virtual":
                assert holder["state"].get_checkpoint_result("a").is_succeeded()
                self.record("read-a-from-virtual-end-hook")

    recorder = _ReadingFromChildEnd()
    state = _state(recorder, [])
    holder["state"] = state
    state._plugin_executor.record_end_hooks([_operation("a")])  # noqa: SLF001
    state._operations.update(_map(_operation("a")))  # noqa: SLF001

    reader_a = _thread(lambda: state.get_checkpoint_result("a"), "reader-a")
    assert hook_a_running.wait(TIMEOUT)
    virtual = OperationIdentifier(
        operation_id="virtual",
        sub_type=OperationSubType.RUN_IN_CHILD_CONTEXT,
        parent_id=None,
        name="virtual",
        operation_type=ServiceOperationType.CONTEXT,
    )
    _join(
        _thread(
            lambda: state.emit_child_context_end_hook(
                virtual, OperationStatus.SUCCEEDED
            ),
            "virtual",
        )
    )
    # The virtual context's hook returned while hook a is still running.
    assert "read-a-from-virtual-end-hook" in recorder.labels()
    assert reader_a.is_alive()
    release_hook_a.set()
    _join(reader_a)
    state._plugin_executor.on_invocation_end(SUCCEEDED)  # noqa: SLF001
    assert recorder.labels().count("operation_end:a") == 1


def test_a_hook_may_read_an_operation_whose_hook_its_thread_runs_next() -> None:
    """Within one taken tree, the running thread owns every entry: a read of a
    sibling or ancestor whose hook is about to run on this thread returns at
    once instead of waiting for itself."""

    class _ReadsParent(_Recorder):
        def on_operation_end(self, info: OperationEndInfo) -> None:
            super().on_operation_end(info)
            if info.operation_id == "child":
                executor.claim_end_hook("parent")
                self.record("read-parent-from-child-hook")

    recorder = _ReadsParent()
    executor = _started_executor(recorder)
    executor.record_end_hooks(
        [_operation("parent"), _operation("child", parent_id="parent")]
    )

    _join(_thread(lambda: executor.claim_end_hook("parent"), "claimer"))

    assert recorder.labels() == [
        "invocation_start",
        "operation_end:child",
        "read-parent-from-child-hook",
        "operation_end:parent",
    ]


def test_a_reserved_hook_whose_waiter_never_came_is_drained() -> None:
    recorder = _Recorder()
    executor = _started_executor(recorder)
    executor.record_end_hooks([_operation("a")], waited_for={"a"})

    executor.drain_end_hooks()

    assert recorder.labels() == ["invocation_start", "operation_end:a"]


def test_a_claim_runs_unread_descendants_first() -> None:
    """A response completes a context and a callback inside it nobody awaited.

    The thread that observes the context's result runs the callback's end
    hook first, then the context's: a child ends before its parent, as it
    would have on the thread that read the callback.
    """
    recorder = _Recorder()
    executor = _started_executor(recorder)
    executor.record_end_hooks(
        [
            _operation("context"),
            _operation("callback", parent_id="context"),
            _operation("grandchild", parent_id="callback"),
            _operation("unrelated"),
        ]
    )

    executor.claim_end_hook("context")

    assert recorder.labels() == [
        "invocation_start",
        "operation_end:grandchild",
        "operation_end:callback",
        "operation_end:context",
    ]
    executor.drain_end_hooks()
    assert recorder.labels()[-1] == "operation_end:unrelated"


def test_drain_runs_descendants_before_ancestors() -> None:
    recorder = _Recorder()
    executor = _started_executor(recorder)
    executor.record_end_hooks(
        [_operation("parent"), _operation("child", parent_id="parent")]
    )

    executor.drain_end_hooks()

    assert recorder.labels() == [
        "invocation_start",
        "operation_end:child",
        "operation_end:parent",
    ]


# endregion

# region Change dispatcher


def test_change_notifications_run_in_order_on_the_change_thread() -> None:
    recorder = _Recorder()
    executor = _started_executor(recorder)
    a, b, c = _operation("a"), _operation("b"), _operation("c")

    executor.record_change([a], _map(a), {})
    executor.record_change([b], _map(a, b), _map(a))
    executor.record_change([c], _map(a, b, c), _map(a, b))
    _join(_end_in_thread(executor))

    changes = recorder.changes
    # Each response reports a different operation, so with or without merging
    # every id is delivered once, and in response order.
    delivered = [key for change in changes for key in change.updated_operations]
    assert delivered == ["a", "b", "c"]
    assert changes[-1].operations.keys() == {"a", "b", "c"}
    for label in recorder.labels():
        if label.startswith("operation_change"):
            assert recorder.thread_of(label) == "plugin-change"
    assert recorder.labels()[-1] == "invocation_end"


def test_notifications_arriving_during_a_slow_hook_merge_into_one() -> None:
    """Two responses arrive while the plugin is in the hook: one delivery covers both.

    The second response reports b (started) and d; the third reports c, and
    its map holds b succeeded. The merged delivery reports b, c and d, with b
    in the newest state, and carries the newest map. Replacing the pending
    notification would lose d; keeping the pending states would report b as
    started.
    """
    entered = threading.Event()
    release = threading.Event()

    class _SlowOnFirst(_Recorder):
        def on_operation_change(self, info: OperationChangeInfo) -> None:
            super().on_operation_change(info)
            if len(self.changes) == 1:
                entered.set()
                self.block_on(release)

    recorder = _SlowOnFirst()
    executor = _started_executor(recorder)
    a, c, d = _operation("a"), _operation("c"), _operation("d")
    b_started = _operation("b", OperationStatus.STARTED)
    b_done = _operation("b")

    executor.record_change([a], _map(a), {})
    assert entered.wait(TIMEOUT)
    executor.record_change([b_started, d], _map(a, b_started, d), _map(a))
    executor.record_change([c], _map(a, b_done, c, d), _map(a, b_started, d))
    release.set()
    _join(_end_in_thread(executor))

    assert "timed-out" not in recorder.labels()
    assert [sorted(change.updated_operations) for change in recorder.changes] == [
        ["a"],
        ["b", "c", "d"],
    ]
    merged = recorder.changes[1]
    assert merged.updated_operations["b"].status is OperationStatus.SUCCEEDED
    assert merged.operations.keys() == {"a", "b", "c", "d"}
    assert merged.operations["b"].status is OperationStatus.SUCCEEDED


def test_merge_takes_the_union_of_ids_in_the_newest_state() -> None:
    def info(operation: Operation) -> OperationInfo:
        return OperationInfo.from_operation(operation)

    a, b_old, b_new, c = (
        _operation("a"),
        _operation("b", OperationStatus.STARTED),
        _operation("b"),
        _operation("c"),
    )
    pending = OperationChangeInfo(
        execution_arn="arn:exec",
        updated_operations={"a": info(a), "b": info(b_old)},
        operations={"a": info(a), "b": info(b_old)},
    )
    latest = OperationChangeInfo(
        execution_arn="arn:exec",
        updated_operations={"c": info(c)},
        operations={"a": info(a), "b": info(b_new), "c": info(c)},
    )

    merged = _merge_change_infos(pending, latest)

    assert list(merged.updated_operations) == ["a", "b", "c"]
    assert merged.updated_operations["b"].status is OperationStatus.SUCCEEDED
    assert merged.operations is latest.operations
    assert merged.execution_arn == "arn:exec"


class _ObservedThread:
    """Stands in for the dispatcher thread; reports that join() was entered."""

    def __init__(self, thread: threading.Thread, entered: threading.Event) -> None:
        self._thread = thread
        self._entered = entered

    def join(self, timeout: float | None = None) -> None:
        self._entered.set()
        self._thread.join(timeout)


def test_on_invocation_end_waits_for_a_notification_still_in_the_hook() -> None:
    """The readiness signal comes from inside the dispatcher thread's join, so
    the negative assertion is made while the ending thread is in that join,
    and an implementation that does not join never signals.
    """
    entered = threading.Event()
    release = threading.Event()
    join_entered = threading.Event()

    class _Slow(_Recorder):
        def on_operation_change(self, info: OperationChangeInfo) -> None:
            super().on_operation_change(info)
            entered.set()
            self.block_on(release)
            self.record("change-left")

    recorder = _Slow()
    executor = _started_executor(recorder)
    a = _operation("a")
    executor.record_change([a], _map(a), {})
    assert entered.wait(TIMEOUT)
    dispatcher = executor._dispatcher  # noqa: SLF001
    assert dispatcher is not None
    dispatcher._thread = _ObservedThread(dispatcher._thread, join_entered)  # type: ignore[assignment]  # noqa: SLF001

    ending = _end_in_thread(executor)
    assert join_entered.wait(TIMEOUT), "on_invocation_end did not join the dispatcher"
    assert "invocation_end" not in recorder.labels()
    release.set()
    _join(ending)

    assert recorder.labels() == [
        "invocation_start",
        "operation_change:a",
        "change-left",
        "invocation_end",
    ]


def test_no_change_thread_starts_without_a_notification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    starts: list[str] = []
    original = plugin_module._ChangeDispatcher.start  # noqa: SLF001

    def counting_start(executor: PluginExecutor) -> plugin_module._ChangeDispatcher:  # noqa: SLF001
        starts.append("started")
        return original(executor)

    monkeypatch.setattr(plugin_module._ChangeDispatcher, "start", counting_start)  # noqa: SLF001
    recorder = _Recorder()
    executor = _started_executor(recorder)
    a = _operation("a")

    executor.record_end_hooks([a])
    # No status changed between the maps: no notification, no thread.
    executor.record_change([a], _map(a), _map(a))
    executor.on_invocation_end(SUCCEEDED)

    assert starts == []
    assert recorder.labels() == [
        "invocation_start",
        "operation_end:a",
        "invocation_end",
    ]


def test_notifications_run_once_on_the_handler_thread_when_no_thread_can_start(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def cannot_start(executor: PluginExecutor) -> plugin_module._ChangeDispatcher:  # noqa: ARG001, SLF001
        msg = "can't start new thread"
        raise RuntimeError(msg)

    monkeypatch.setattr(plugin_module._ChangeDispatcher, "start", cannot_start)  # noqa: SLF001
    recorder = _Recorder()
    executor = _started_executor(recorder)
    a, b = _operation("a"), _operation("b")

    with caplog.at_level(logging.WARNING, logger=plugin_module.__name__):
        executor.record_change([a], _map(a), {})
        executor.record_change([b], _map(a, b), _map(a))
    assert recorder.labels() == ["invocation_start"]
    _join(_end_in_thread(executor))

    assert recorder.labels() == [
        "invocation_start",
        "operation_change:a,b",
        "invocation_end",
    ]
    assert recorder.thread_of("operation_change:a,b") == "handler"
    assert "could not be started" in caplog.text


def test_a_control_exception_on_the_change_thread_is_logged_and_the_next_runs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _Interrupting(_Recorder):
        def on_operation_change(self, info: OperationChangeInfo) -> None:
            super().on_operation_change(info)
            if "a" in info.updated_operations:
                raise KeyboardInterrupt

    recorder = _Interrupting()
    executor = _started_executor(recorder)
    a, b = _operation("a"), _operation("b")

    with caplog.at_level(logging.ERROR, logger=plugin_module.__name__):
        executor.record_change([a], _map(a), {})
        assert _wait_until(lambda: "operation_change:a" in recorder.labels())
        executor.record_change([b], _map(a, b), _map(a))
        executor.on_invocation_end(SUCCEEDED)

    assert recorder.labels() == [
        "invocation_start",
        "operation_change:a",
        "operation_change:b",
        "invocation_end",
    ]
    assert "plugin change thread; ignored" in caplog.text


def test_a_control_exception_from_one_plugin_does_not_cost_the_others_the_notification() -> (
    None
):
    class _Interrupting(_Recorder):
        def on_operation_change(self, info: OperationChangeInfo) -> None:
            super().on_operation_change(info)
            raise KeyboardInterrupt

    interrupting = _Interrupting()
    healthy = _Recorder()
    executor = PluginExecutor([plugin_factory(interrupting), plugin_factory(healthy)])
    executor.on_invocation_start(
        execution_arn="arn:exec",
        lambda_context=None,
        execution_start_time=START_TS,
        is_first_invocation=True,
    )
    a = _operation("a")

    executor.record_change([a], _map(a), {})
    executor.on_invocation_end(SUCCEEDED)

    assert healthy.labels() == [
        "invocation_start",
        "operation_change:a",
        "invocation_end",
    ]
    assert interrupting.labels()[1] == "operation_change:a"


def test_a_control_exception_from_a_drained_end_hook_is_raised_after_the_end_hook() -> (
    None
):
    class _Interrupting(_Recorder):
        def on_operation_end(self, info: OperationEndInfo) -> None:
            super().on_operation_end(info)
            raise KeyboardInterrupt

    recorder = _Interrupting()
    executor = _started_executor(recorder)
    executor.record_end_hooks([_operation("a")])

    with pytest.raises(KeyboardInterrupt):
        executor.on_invocation_end(SUCCEEDED)

    assert recorder.labels() == [
        "invocation_start",
        "operation_end:a",
        "invocation_end",
    ]


def test_a_control_exception_from_one_drained_tree_does_not_stop_the_others() -> None:
    """The other trees still run before the end hook; the exception follows it.

    Were the drain to stop at the first, the executor's scope exit would
    drain the rest after on_invocation_end.
    """

    class _Interrupting(_Recorder):
        def on_operation_end(self, info: OperationEndInfo) -> None:
            super().on_operation_end(info)
            if info.operation_id == "a":
                raise KeyboardInterrupt

    recorder = _Interrupting()
    executor = PluginExecutor([plugin_factory(recorder)])
    with pytest.raises(KeyboardInterrupt), executor.run():
        executor.on_invocation_start(
            execution_arn="arn:exec",
            lambda_context=None,
            execution_start_time=START_TS,
            is_first_invocation=True,
        )
        executor.record_end_hooks([_operation("a"), _operation("b")])
        executor.on_invocation_end(SUCCEEDED)

    assert recorder.labels() == [
        "invocation_start",
        "operation_end:a",
        "operation_end:b",
        "invocation_end",
    ]


def test_an_ordinary_exception_in_a_claimed_hook_is_contained(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _Failing(_Recorder):
        def on_operation_end(self, info: OperationEndInfo) -> None:
            super().on_operation_end(info)
            msg = "plugin bug"
            raise ValueError(msg)

    recorder = _Failing()
    executor = _started_executor(recorder)
    executor.record_end_hooks([_operation("a")])

    with caplog.at_level(logging.ERROR, logger=plugin_module.__name__):
        executor.claim_end_hook("a")

    assert recorder.labels() == ["invocation_start", "operation_end:a"]
    assert "exception ignored" in caplog.text


# endregion

# region ExecutionState: record before publish, claim on observe


def _state(
    recorder: _Recorder,
    response: list[Operation],
    failure: Exception | None = None,
) -> ExecutionState:
    client = Mock(spec=LambdaClient)
    if failure is not None:
        client.checkpoint.side_effect = failure
    else:
        client.checkpoint.return_value = CheckpointOutput(
            checkpoint_token="next",  # noqa: S106
            new_execution_state=CheckpointUpdatedExecutionState(
                operations=response, next_marker=None
            ),
        )
    return ExecutionState(
        durable_execution_arn="arn:exec",
        initial_checkpoint_token="token",  # noqa: S106
        operations={},
        service_client=client,
        plugin_executor=_started_executor(recorder),
    )


def test_fetch_records_before_it_publishes_the_operations() -> None:
    recorder = _Recorder()
    state = _state(recorder, [])
    seen: list[tuple[list[str], bool, bool]] = []

    def before_publish(operations: list[Operation]) -> None:
        # Called before the operations lock is taken: the map is read directly,
        # and the lock is free. Recording takes the plugin executor's claim
        # lock, and a reader takes the claim lock before the operations lock,
        # so recording under the operations lock would invert that order.
        lock_free = state._operations_lock.acquire(blocking=False)  # noqa: SLF001
        if lock_free:
            state._operations_lock.release()  # noqa: SLF001
        seen.append(
            (
                [op.operation_id for op in operations],
                "a" in state._operations,
                lock_free,
            )  # noqa: SLF001
        )

    state.fetch_paginated_operations(
        [_operation("a")], "token", None, before_publish=before_publish
    )

    assert seen == [(["a"], False, True)]
    assert state.get_checkpoint_result("a").is_succeeded()


def _run_loop(state: ExecutionState) -> threading.Thread:
    return _thread(state.checkpoint_batches_forever, "checkpoint")


def test_a_synchronous_waiter_runs_its_end_hook_before_create_checkpoint_returns() -> (
    None
):
    """The checkpoint thread records, wakes the waiter, and runs nothing itself."""
    recorder = _Recorder()
    state = _state(recorder, [_operation("a")])
    loop = _run_loop(state)

    def waiter() -> None:
        state.create_checkpoint(_update("a"), is_sync=True)
        recorder.record("returned:a")

    _join(_thread(waiter, "waiter"))
    state.stop_checkpointing()
    _join(loop)
    state._plugin_executor.on_invocation_end(SUCCEEDED)  # noqa: SLF001

    labels = recorder.labels()
    assert labels.index("operation_end:a") < labels.index("returned:a")
    assert recorder.thread_of("operation_end:a") == "waiter"
    assert recorder.thread_of("operation_change:a") == "plugin-change"
    assert all(thread != "checkpoint" for _, thread in recorder.events)
    assert labels.count("operation_end:a") == 1


def test_the_end_hook_is_recorded_before_the_waiter_is_signalled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Observed on the checkpoint thread, at the moment it sets the waiter's event.

    A waiter woken before the record would claim nothing, and its hook would
    run late, at the end of the invocation; the timing makes that rare, so the
    order is asserted at the signal itself rather than through the waiter.
    """
    recorder = _Recorder()
    state = _state(recorder, [_operation("a")])
    executor = state._plugin_executor  # noqa: SLF001
    recorded_at_signal: list[bool] = []

    class _ObservingEvent(CompletionEvent):
        def set(self, error: BaseException | None = None) -> None:
            with executor.claim_lock:
                recorded_at_signal.append("a" in executor._end_hooks)  # noqa: SLF001
            super().set(error)

    monkeypatch.setattr(state_module, "CompletionEvent", _ObservingEvent)
    loop = _run_loop(state)
    _join(_thread(lambda: state.create_checkpoint(_update("a"), True), "waiter"))
    state.stop_checkpointing()
    _join(loop)

    assert recorded_at_signal == [True]
    state._plugin_executor.on_invocation_end(SUCCEEDED)  # noqa: SLF001


def test_a_reader_of_a_result_another_response_completed_runs_its_end_hook() -> None:
    """A wait that elapsed, reported in another operation's response.

    The thread that later reads the result claims the hook in
    get_checkpoint_result, so the hook precedes anything that thread does
    with the result, and the end of the invocation finds nothing to drain.
    """
    recorder = _Recorder()
    state = _state(recorder, [_operation("a"), _operation("elapsed")])
    loop = _run_loop(state)

    _join(_thread(lambda: state.create_checkpoint(_update("a"), True), "waiter"))

    def reader() -> None:
        assert state.get_checkpoint_result("elapsed").is_succeeded()
        recorder.record("read:elapsed")

    _join(_thread(reader, "reader"))
    state.stop_checkpointing()
    _join(loop)
    state._plugin_executor.on_invocation_end(SUCCEEDED)  # noqa: SLF001

    labels = recorder.labels()
    assert labels.index("operation_end:elapsed") < labels.index("read:elapsed")
    assert recorder.thread_of("operation_end:elapsed") == "reader"
    assert recorder.thread_of("operation_end:a") == "waiter"
    assert labels.count("operation_end:elapsed") == 1


def test_the_replay_boundary_reads_take_no_end_hook() -> None:
    """DurableContext reads a checkpoint twice without consuming its result.

    While replaying, it reads the next operation's checkpoint to place the
    replay boundary, and an operation's own checkpoint to validate its
    identity before the operation runs. Neither read runs the end hook a
    checkpoint response recorded for the operation; the operation's own
    read, which consumes the result, does. So the hook neither delays the
    operation before it nor runs before the identity check.
    """
    recorder = _Recorder()
    context = DurableContext(
        state=_state(recorder, []),
        execution_context=ExecutionContext(durable_execution_arn="arn:exec"),
        replay_status=ReplayStatus.REPLAY,
    )
    state = context.state
    # The context's logical steps 1 and 2. Step 1 completed in a prior
    # invocation; step 2 completed during this one, reported by a response.
    first_id = context._create_step_id()  # noqa: SLF001
    second_id = context._peek_next_operation_id()  # noqa: SLF001
    state._operations[first_id] = _operation(first_id)  # noqa: SLF001
    state._plugin_executor.record_end_hooks([_operation(second_id)])  # noqa: SLF001
    state._operations[second_id] = _operation(second_id)  # noqa: SLF001

    # The boundary read at step 1's exit sees step 2 complete, and runs nothing.
    assert context._next_operation_exists()  # noqa: SLF001
    assert "operation_end:" + second_id not in recorder.labels()

    # Step 2's identity read runs nothing either.
    second = OperationIdentifier(
        operation_id=context._create_step_id(),  # noqa: SLF001
        sub_type=OperationSubType.STEP,
        parent_id=None,
        name=second_id,
        operation_type=ServiceOperationType.STEP,
    )
    assert second.operation_id == second_id
    with context._replay_aware(second):  # noqa: SLF001
        assert "operation_end:" + second_id not in recorder.labels()
    assert "operation_end:" + second_id not in recorder.labels()

    # The read that consumes the result runs the hook, on this thread.
    assert state.get_checkpoint_result(second_id).is_succeeded()
    assert recorder.labels().count("operation_end:" + second_id) == 1
    assert (
        recorder.thread_of("operation_end:" + second_id)
        == threading.current_thread().name
    )
    state._plugin_executor.on_invocation_end(SUCCEEDED)  # noqa: SLF001
    assert recorder.labels().count("operation_end:" + second_id) == 1


def test_a_result_no_thread_reads_gets_its_end_hook_at_invocation_end() -> None:
    recorder = _Recorder()
    state = _state(recorder, [_operation("a"), _operation("unread")])
    loop = _run_loop(state)

    _join(_thread(lambda: state.create_checkpoint(_update("a"), True), "waiter"))
    state.stop_checkpointing()
    _join(loop)
    _join(_end_in_thread(state._plugin_executor))  # noqa: SLF001

    labels = recorder.labels()
    assert labels.index("operation_end:unread") < labels.index("invocation_end")
    assert recorder.thread_of("operation_end:unread") == "handler"
    assert labels.count("operation_end:unread") == 1


def test_the_loop_records_end_hooks_before_it_publishes_the_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The loop's own wiring: record_end_hooks runs while the operation is not yet readable."""
    recorder = _Recorder()
    state = _state(recorder, [_operation("a")])
    executor = state._plugin_executor  # noqa: SLF001
    published_at_record: list[bool] = []
    waited: list[set[str]] = []
    original = executor.record_end_hooks

    def observing_record(
        operations: list[Operation], waited_for: Collection[str] = ()
    ) -> None:
        # Called before the operations lock is taken, so the map is read directly.
        published_at_record.append("a" in state._operations)  # noqa: SLF001
        waited.append(set(waited_for))
        original(operations, waited_for=waited_for)

    monkeypatch.setattr(executor, "record_end_hooks", observing_record)
    loop = _run_loop(state)
    _join(_thread(lambda: state.create_checkpoint(_update("a"), True), "waiter"))
    state.stop_checkpointing()
    _join(loop)

    assert published_at_record == [False]
    assert recorder.thread_of("operation_end:a") == "waiter"
    assert waited == [{"a"}]
    state._plugin_executor.on_invocation_end(SUCCEEDED)  # noqa: SLF001


def test_a_read_of_the_older_non_terminal_state_runs_no_hook() -> None:
    """The hook of a completion the reader did not see stays for the reader that does."""
    recorder = _Recorder()
    state = _state(recorder, [])
    state.fetch_paginated_operations(
        [_operation("a", OperationStatus.STARTED)], "token", None
    )
    executor = state._plugin_executor  # noqa: SLF001
    # The response completing "a" was recorded but not yet published.
    executor.record_end_hooks([_operation("a")])

    assert not state.get_checkpoint_result("a").is_terminal()
    assert recorder.labels() == ["invocation_start"]

    state.fetch_paginated_operations([_operation("a")], "token", None)
    assert state.get_checkpoint_result("a").is_succeeded()
    assert recorder.labels() == ["invocation_start", "operation_end:a"]


def test_a_waiter_woken_by_a_failed_later_page_still_runs_its_recorded_hook() -> None:
    """The first page completes the operation; fetching the next page fails.

    The pages already fetched are recorded and published, then the waiter is
    woken with BackgroundThreadError. The claim is in a finally, so the hook
    runs on the waiter thread before the error propagates, in the order a
    successful batch would have given it.
    """
    recorder = _Recorder()
    client = Mock(spec=LambdaClient)
    client.checkpoint.return_value = CheckpointOutput(
        checkpoint_token="next",  # noqa: S106
        new_execution_state=CheckpointUpdatedExecutionState(
            operations=[_operation("a")], next_marker="page-2"
        ),
    )
    client.get_execution_state.side_effect = GetExecutionStateError("page fetch failed")
    state = ExecutionState(
        durable_execution_arn="arn:exec",
        initial_checkpoint_token="token",  # noqa: S106
        operations={},
        service_client=client,
        plugin_executor=_started_executor(recorder),
    )
    loop = _run_loop(state)
    outcome: list[str] = []

    def waiter() -> None:
        try:
            state.create_checkpoint(_update("a"), is_sync=True)
        except BaseException as error:  # noqa: BLE001 - BackgroundThreadError is a BaseException
            outcome.append(type(error).__name__)
        recorder.record("raised:a")

    _join(_thread(waiter, "waiter"))
    _join(loop)

    assert outcome == ["BackgroundThreadError"]
    labels = recorder.labels()
    assert labels.index("operation_end:a") < labels.index("raised:a")
    assert recorder.thread_of("operation_end:a") == "waiter"
    state._plugin_executor.on_invocation_end(SUCCEEDED)  # noqa: SLF001


def test_a_waiter_whose_checkpoint_failed_runs_no_hook() -> None:
    recorder = _Recorder()
    state = _state(recorder, [], failure=RuntimeError("service down"))
    loop = _run_loop(state)
    outcome: list[str] = []

    def waiter() -> None:
        try:
            state.create_checkpoint(_update("a"), is_sync=True)
        except BaseException as error:  # noqa: BLE001 - BackgroundThreadError is a BaseException
            outcome.append(type(error).__name__)

    _join(_thread(waiter, "waiter"))
    _join(loop)

    assert outcome == ["BackgroundThreadError"]
    assert recorder.labels() == ["invocation_start"]


# endregion
