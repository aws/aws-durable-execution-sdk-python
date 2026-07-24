"""DagExecutor: a dedicated topological scheduler for the DAG operation.

Reuses the SDK's worker-thread primitives (``ThreadPoolExecutor``, the
``SuspendExecution`` protocol) but is a *separate* component from
``ConcurrentExecutor`` (which is hard-wired for the flat map/parallel shape).
It gates task submission on dependency readiness, evaluates trigger rules and
``run_if`` predicates, drains on failure by default (failure is a terminal
state, not an abort), and computes DAG-global success/failure/skip counts,
feeding only success+failure into the reused threshold ``CompletionConfig``.

.. warning::
   **Experimental.** Internal implementation of the DAG scheduler.
"""

from __future__ import annotations

import itertools
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.concurrency import TimerScheduler
from aws_durable_execution_sdk_python.dag import (
    DagCompletionReason,
    DepsMap,
    SkipReason,
    TaskExecution,
    TaskStatus,
)
from aws_durable_execution_sdk_python.exceptions import (
    SuspendExecution,
    TimedSuspendExecution,
    ValidationError,
)
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.operation.dag_result import DagResultImpl

if TYPE_CHECKING:
    from concurrent.futures import Future

    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.dag import DagConfig
    from aws_durable_execution_sdk_python.operation.dag_context import TaskDef

logger = logging.getLogger(__name__)

_TERMINAL = (TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.SKIPPED)

# task scheduling decisions
_RUN = "RUN"
_SKIP = "SKIP"
_FAIL = "FAIL"


def _trigger_passes(rule, statuses: list[TaskStatus]) -> bool:
    """Trigger-rule truth table over upstream terminal statuses.

    Ports the JS truth table verbatim, incl. the empty-upstream rows and the
    ``ALL_FAILED`` ``len > 0`` guard.
    """
    from aws_durable_execution_sdk_python.dag import TriggerRule

    has_failed = any(s is TaskStatus.FAILED for s in statuses)
    has_succeeded = any(s is TaskStatus.SUCCEEDED for s in statuses)
    if rule is TriggerRule.ALL_SUCCESS:
        return all(s is TaskStatus.SUCCEEDED for s in statuses)
    if rule is TriggerRule.ALL_FAILED:
        return len(statuses) > 0 and all(s is TaskStatus.FAILED for s in statuses)
    if rule is TriggerRule.ALL_DONE:
        return True
    if rule is TriggerRule.ANY_SUCCESS:
        return has_succeeded
    if rule is TriggerRule.ANY_FAILED:
        return has_failed
    if rule is TriggerRule.NONE_FAILED:
        return not has_failed
    msg = f"Unknown trigger rule: {rule}"  # pragma: no cover
    raise ValidationError(msg)  # pragma: no cover


_resume_seq = itertools.count()


class _TimedResume:
    """Adapter so the DAG can reuse the base ``TimerScheduler`` unchanged.

    ``TimerScheduler`` (from the concurrency module) drives
    ``ExecutableWithState``-shaped objects: on fire it checks ``can_resume``,
    calls ``reset_to_pending()`` then hands the object to its resubmit callback.
    The DAG tracks task state by *name* rather than by an ``ExecutableWithState``
    instance, so this thin adapter carries only the task name and satisfies that
    interface. ``__lt__`` (via a monotonic sequence) keeps heap ties in the
    scheduler total-orderable when two resumes share a timestamp.
    """

    __slots__ = ("_seq", "name")

    def __init__(self, name: str) -> None:
        self.name = name
        self._seq = next(_resume_seq)

    @property
    def can_resume(self) -> bool:
        return True

    def reset_to_pending(self) -> None:
        """No-op: the DAG resets its own task bookkeeping in ``_resubmit``."""

    def __lt__(self, other: _TimedResume) -> bool:
        return self._seq < other._seq


class DagExecutor:
    """Topological scheduler for a validated DAG."""

    def __init__(
        self,
        ctx: DurableContext,
        tasks: dict[str, TaskDef],
        config: DagConfig,
    ) -> None:
        if config.max_concurrency is not None and config.max_concurrency <= 0:
            msg = f"Invalid max_concurrency: {config.max_concurrency}"
            raise ValidationError(msg)
        self._ctx = ctx
        self._tasks = tasks
        self._config = config
        self._lock = threading.Lock()
        self._completion_event = threading.Event()
        self._results: dict[str, TaskExecution] = {}
        self._scheduled: set[str] = set()
        self._in_flight: set[str] = set()
        self._success = 0
        self._failure = 0
        self._skip = 0
        # All suspends raised by tasks this run. We do NOT re-raise the first
        # one captured; when stopping we resolve which suspend to surface with
        # the same precedence as ConcurrentExecutor.should_execution_suspend
        # (earliest timed wins over indefinite) so a concurrent short timer is
        # never dropped behind an indefinite wait_for_callback.
        self._pending_suspends: list[SuspendExecution] = []
        # In-process timed-resume bookkeeping (parity with ConcurrentExecutor's
        # TimerScheduler): a timed suspend does NOT stop the DAG. While other
        # tasks make progress the base TimerScheduler re-runs the timed task in
        # this same invocation at its scheduled timestamp. Only an *indefinite*
        # (callback) suspend forces leaving the invocation for platform replay.
        self._scheduler: TimerScheduler | None = None
        self._pending_timers: set[str] = set()
        self._timed_suspend_by_name: dict[str, TimedSuspendExecution] = {}
        self._scheduler_exception: Exception | None = None
        self._early_reason: DagCompletionReason | None = None
        self._pool: ThreadPoolExecutor | None = None

    # region public
    def run(self) -> DagResultImpl:
        """Schedule and run the DAG; return a DagResult (may raise to suspend)."""
        total = len(self._tasks)
        if total == 0:
            return DagResultImpl({}, DagCompletionReason.ALL_COMPLETED)

        max_workers = self._config.max_concurrency or total
        # Mirror ConcurrentExecutor.execute: scheduler OUTER, pool INNER, so the
        # pool drains (joins in-flight tasks) before the timer thread is torn
        # down. Any suspend is raised inside the pool ``with`` (as before).
        with (
            TimerScheduler(self._resubmit) as scheduler,
            ThreadPoolExecutor(max_workers=max_workers) as pool,
        ):
            self._scheduler = scheduler
            self._pool = pool
            self._pump()
            self._completion_event.wait()
            if self._scheduler_exception is not None:
                raise self._scheduler_exception
            suspend = self._resolve_suspend()
            if suspend is not None:
                raise suspend
        return self._build_result()

    # endregion public

    # region scheduling
    def _pump(self) -> None:
        """Resolve newly-ready tasks (skip or submit); set completion if done."""
        to_submit: list[tuple[str, TaskDef]] = []
        with self._lock:
            progressed = True
            while progressed:
                progressed = False
                if self._stopping_locked():
                    break
                for name, task in self._tasks.items():
                    if name in self._scheduled or not self._deps_terminal_locked(name):
                        continue
                    decision, payload = self._evaluate_locked(task)
                    self._scheduled.add(name)
                    if decision == _RUN:
                        self._in_flight.add(name)
                        to_submit.append((name, task))
                    elif decision == _SKIP:
                        self._results[name] = TaskExecution(
                            name=name, status=TaskStatus.SKIPPED, skip_reason=payload
                        )
                        self._skip += 1
                    else:  # _FAIL: run_if raised — treat as task failure (drain)
                        self._results[name] = TaskExecution(
                            name=name, status=TaskStatus.FAILED, error=payload
                        )
                        self._failure += 1
                    progressed = True
            done = self._is_done_locked()

        for name, task in to_submit:
            future = self._pool.submit(self._run_task, name, task)  # type: ignore[union-attr]

            def _done(f: Future, n: str = name) -> None:
                self._on_done(n, f)

            future.add_done_callback(_done)

        if done:
            self._completion_event.set()

    def _run_task(self, name: str, task: TaskDef) -> Any:
        # Snapshot deps under the lock: this runs on a worker thread and
        # _build_deps_map reads self._results, which the scheduler mutates
        # concurrently (the run_if path already builds deps under the lock).
        with self._lock:
            deps_map = self._build_deps_map(task)
        logger.debug("▶️ DAG task %s starting", name)
        return task.executor(self._ctx, deps_map)

    def _on_done(self, name: str, future: Future) -> None:
        try:
            result = future.result()
            with self._lock:
                self._results[name] = TaskExecution(
                    name=name, status=TaskStatus.SUCCEEDED, result=result
                )
                self._success += 1
                self._in_flight.discard(name)
        except SuspendExecution as se:  # includes TimedSuspendExecution
            schedule_ts: float | None = None
            with self._lock:
                # Record every suspend (timed + indefinite); precedence is
                # resolved in _resolve_suspend when the DAG stops.
                self._pending_suspends.append(se)
                self._results[name] = TaskExecution(
                    name=name, status=TaskStatus.STARTED
                )
                self._in_flight.discard(name)
                # Timed suspend: register an in-process resume so the base timer
                # thread re-runs this task at its timestamp WITHOUT leaving the
                # invocation. Indefinite (callback) suspends get no timer and
                # fall through to platform replay via _resolve_suspend.
                if isinstance(se, TimedSuspendExecution):
                    self._timed_suspend_by_name[name] = se
                    self._pending_timers.add(name)
                    schedule_ts = se.scheduled_timestamp
            if schedule_ts is not None and self._scheduler is not None:
                self._scheduler.schedule_resume(_TimedResume(name), schedule_ts)
        except Exception as e:  # noqa: BLE001
            with self._lock:
                self._results[name] = TaskExecution(
                    name=name,
                    status=TaskStatus.FAILED,
                    error=ErrorObject.from_exception(e),
                )
                self._failure += 1
                self._in_flight.discard(name)
        self._safe_pump()

    def _safe_pump(self) -> None:
        """Run ``_pump`` from a worker-thread completion callback.

        ``concurrent.futures`` swallows exceptions raised inside
        ``add_done_callback``. If ``_pump`` ever raised there (e.g. an
        unexpected scheduler bug) the completion event would never be set and
        ``run()`` would block forever. Capture any escaping exception and set
        the event so ``run()`` re-raises it instead of hanging.
        """
        try:
            self._pump()
        except Exception as e:  # noqa: BLE001
            with self._lock:
                if self._scheduler_exception is None:
                    self._scheduler_exception = e
            self._completion_event.set()
    # endregion scheduling

    def _resubmit(self, resume: _TimedResume) -> None:
        """Base-TimerScheduler callback: re-run a timed task in-process.

        Fires on the scheduler's timer thread once a task's scheduled timestamp
        elapses. Mirrors ``ConcurrentExecutor``'s resubmitter (checkpoint, then
        re-submit): it clears the task's timed-suspend bookkeeping and its
        STARTED placeholder so ``_pump`` sees it as a fresh, ready task and
        re-runs it within the same invocation. If the task already left the
        timer set (e.g. the DAG bubbled to the platform and the scheduler was
        torn down), this is a no-op.
        """
        name = resume.name
        with self._lock:
            if name not in self._pending_timers:
                return
            self._pending_timers.discard(name)
            se = self._timed_suspend_by_name.pop(name, None)
            if se is not None:
                try:
                    self._pending_suspends.remove(se)
                except ValueError:  # pragma: no cover - defensive
                    pass
            # Make the task schedulable again: drop its STARTED placeholder and
            # its scheduled mark so _pump re-evaluates and re-runs it.
            self._scheduled.discard(name)
            self._results.pop(name, None)
        # Checkpoint before re-running, matching ConcurrentExecutor.resubmitter.
        self._ctx.state.create_checkpoint()
        self._safe_pump()

    def _resolve_suspend(self) -> SuspendExecution | None:
        """Pick which suspend to surface, matching the base executor's contract.

        Ports ``ConcurrentExecutor.should_execution_suspend`` precedence: if any
        timed suspend is pending, raise a ``TimedSuspendExecution`` with the
        EARLIEST ``scheduled_timestamp`` (timed wins over indefinite so the
        platform resumes at the soonest timer); otherwise raise the indefinite
        ``SuspendExecution``. Returns ``None`` when nothing suspended. Called
        after the completion event fires, so no lock is needed.
        """
        earliest_timestamp = float("inf")
        indefinite: SuspendExecution | None = None
        for se in self._pending_suspends:
            if isinstance(se, TimedSuspendExecution):
                if se.scheduled_timestamp < earliest_timestamp:
                    earliest_timestamp = se.scheduled_timestamp
            else:
                indefinite = se
        if earliest_timestamp != float("inf"):
            return TimedSuspendExecution(
                "DAG suspended; resuming at the earliest pending timer.",
                earliest_timestamp,
            )
        return indefinite

    # region helpers (lock held)
    def _deps_terminal_locked(self, name: str) -> bool:
        task = self._tasks[name]
        for dep in task.all_deps:
            te = self._results.get(dep.name)
            if te is None or te.status not in _TERMINAL:
                return False
        return True

    def _evaluate_locked(self, task: TaskDef) -> tuple[str, Any]:
        """Decide a ready task's fate: ``(_RUN, None)``, ``(_SKIP, SkipReason)``
        or ``(_FAIL, ErrorObject)``.

        The trigger rule is a pure function of upstream enum statuses. ``run_if``
        is user code: if it raises, we treat the task as FAILED (spec §5.5's
        "raise ⇒ FAILED", drain model) rather than letting the exception escape
        the scheduler. This keeps root and non-root ``run_if`` behaviour
        identical and never hangs the scheduler.
        """
        statuses = [self._results[dep.name].status for dep in task.all_deps]
        if not _trigger_passes(task.trigger_rule, statuses):
            return (_SKIP, SkipReason.TRIGGER_RULE)
        if task.run_if is not None:
            deps_map = self._build_deps_map(task)
            try:
                should_run = task.run_if(deps_map)
            except Exception as e:  # noqa: BLE001
                return (_FAIL, ErrorObject.from_exception(e))
            if not should_run:
                return (_SKIP, SkipReason.RUN_IF_PREDICATE)
        return (_RUN, None)

    def _build_deps_map(self, task: TaskDef) -> DepsMap:
        by_name: dict[str, Any] = {}
        for dep in task.inline_deps:
            te = self._results.get(dep.name)
            by_name[dep.name] = te.result if te else None
        return DepsMap(by_name)

    def _threshold_reason_locked(self) -> DagCompletionReason | None:
        """Early-completion reason, mirroring ``ExecutionCounters.should_complete``.

        Order matches the reused batch logic: success threshold first, then the
        failure-tolerance conditions, then the impossible-to-succeed early stop
        (which batch reports as ``FAILURE_TOLERANCE_EXCEEDED`` — see
        ``ConcurrentExecutor._create_result``). The failure-percentage
        denominator excludes SKIPPED tasks (they neither succeed nor fail) so
        skips do not dilute the ratio.
        """
        cc = self._config.completion_config
        if cc is None:
            return None
        min_successful = cc.min_successful
        # Success condition (checked before failure, matching batch semantics).
        if min_successful is not None and self._success >= min_successful:
            return DagCompletionReason.MIN_SUCCESSFUL_REACHED
        # Failure-tolerance conditions (count, then percentage).
        if (
            cc.tolerated_failure_count is not None
            and self._failure > cc.tolerated_failure_count
        ):
            return DagCompletionReason.FAILURE_TOLERANCE_EXCEEDED
        if cc.tolerated_failure_percentage is not None:
            denom = len(self._tasks) - self._skip
            if denom > 0:
                pct = (self._failure / denom) * 100
                if pct > cc.tolerated_failure_percentage:
                    return DagCompletionReason.FAILURE_TOLERANCE_EXCEEDED
        # Impossible-to-succeed early stop: max reachable successes is every task
        # that has not already failed or been skipped.
        if min_successful is not None:
            reachable = len(self._tasks) - self._failure - self._skip
            if reachable < min_successful:
                return DagCompletionReason.FAILURE_TOLERANCE_EXCEEDED
        return None

    def _has_indefinite_locked(self) -> bool:
        """True if any *indefinite* (non-timed) suspend is outstanding.

        Only indefinite suspends (e.g. ``wait_for_callback``) force leaving the
        invocation for platform replay; timed suspends are resumed in-process by
        the base ``TimerScheduler``.
        """
        return any(
            not isinstance(se, TimedSuspendExecution) for se in self._pending_suspends
        )

    def _stopping_locked(self) -> bool:
        # An indefinite suspend can only be resolved by the platform, so we stop
        # scheduling new work and drain (unchanged pre-timer behaviour). Timed
        # suspends do NOT stop the DAG: they are resumed in-process while other
        # tasks keep making progress (parity with ConcurrentExecutor).
        if self._has_indefinite_locked():
            return True
        reason = self._threshold_reason_locked()
        if reason is not None:
            self._early_reason = reason
            return True
        return False

    def _has_schedulable_locked(self) -> bool:
        for name in self._tasks:
            if name not in self._scheduled and self._deps_terminal_locked(name):
                return True
        return False

    def _is_done_locked(self) -> bool:
        if self._stopping_locked():
            return len(self._in_flight) == 0
        if self._in_flight:
            return False
        return not self._has_schedulable_locked()

    # endregion helpers

    def _build_result(self) -> DagResultImpl:
        if self._early_reason is not None:
            reason = self._early_reason
        elif self._failure == 0:
            reason = DagCompletionReason.ALL_COMPLETED
        else:
            reason = DagCompletionReason.COMPLETED_WITH_FAILURES
        task_kinds = {name: task.kind for name, task in self._tasks.items()}
        return DagResultImpl(
            dict(self._results), reason, task_kinds, total_count=len(self._tasks)
        )
