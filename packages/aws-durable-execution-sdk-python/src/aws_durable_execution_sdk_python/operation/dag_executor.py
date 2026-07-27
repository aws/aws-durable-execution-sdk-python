"""DagExecutor: a dedicated topological scheduler for the DAG operation.

Reuses the SDK's worker-thread primitives (``ThreadPoolExecutor``, the
``SuspendExecution`` protocol) but is a *separate* component from
``ConcurrentExecutor`` (which is hard-wired for the flat map/parallel shape).
It gates task submission on dependency readiness, evaluates trigger rules and
``run_if`` predicates, drains on task *failure* by default (a task body that
raises is a terminal FAILED state, not an abort — spec §5.5), and computes
DAG-global success/failure/skip counts, feeding only success+failure into the
reused threshold ``CompletionConfig``. A ``run_if`` predicate that *raises* is
different: it is a defect in deterministic code, so it aborts the DAG with
``DagPredicateError`` rather than being recorded as a task failure.

.. warning::
   **Experimental.** Internal implementation of the DAG scheduler.
"""

from __future__ import annotations

import heapq
import datetime
import itertools
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from aws_durable_execution_sdk_python.dag import (
    DagCompletionReason,
    DepsMap,
    SkipReason,
    TaskExecution,
    TaskStatus,
)
from aws_durable_execution_sdk_python.exceptions import (
    DagPredicateError,
    SuspendExecution,
    TimedSuspendExecution,
    ValidationError,
)
from aws_durable_execution_sdk_python.lambda_service import ErrorObject
from aws_durable_execution_sdk_python.operation.dag_result import DagResultImpl

if TYPE_CHECKING:
    from collections.abc import Callable
    from concurrent.futures import Future
    from typing import Self

    from aws_durable_execution_sdk_python.context import DurableContext
    from aws_durable_execution_sdk_python.dag import DagConfig
    from aws_durable_execution_sdk_python.operation.dag_context import TaskDef

logger = logging.getLogger(__name__)

_TERMINAL = (TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.SKIPPED)

# Default cap on how many top-level DAG tasks the scheduler runs concurrently
# when the config leaves ``max_concurrency`` unset. Previously the DAG was
# unbounded: ``max_workers`` fell back to the task count, so an N-task DAG
# spawned N OS threads inside the Lambda sandbox (a 500-task DAG -> 500 threads).
# 40 is a pragmatic bound -- high enough that realistic graphs are unaffected,
# low enough to keep thread/socket usage sane in the smallest Lambda configs. It
# governs the DAG SCHEDULER ONLY (top-level tasks of THIS DAG); it is not
# inherited by a task's own map/parallel fan-out, and a nested dag task resolves
# its own independent default of 40. An explicit ``max_concurrency`` always wins,
# including a value above 40. See dag-review/DEFAULT_CONCURRENCY_CONTRACT.md.
DEFAULT_DAG_MAX_CONCURRENCY = 40

# task scheduling decisions
_RUN = "RUN"
_SKIP = "SKIP"


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
    """A name-keyed timed-resume record driven by the DAG's ``TimerScheduler``.

    The DAG-owned :class:`TimerScheduler` fires resume records on a background
    timer thread: on fire it checks ``can_resume``, calls ``reset_to_pending()``
    then hands the record to its resubmit callback. The DAG tracks task state by
    *name* rather than by an executable instance, so this record carries only
    the task name. ``__lt__`` (via a monotonic sequence) keeps heap ties in the
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


class TimerScheduler:
    """DAG-owned timer for in-process timed resumes.

    Manages timed suspend records with a background timer thread. This is a
    self-contained copy of the mechanism the core map/parallel executor used
    before that logic was inlined into ``ConcurrentExecutor``; the DAG owns it
    so timed waits resume in-process (earliest-timed wins) while indefinite and
    callback suspends still bubble to the platform. It drives :class:`_TimedResume`
    records: on fire it transitions each resumable record to pending under the
    lock, then hands the ready wave to ``resubmit_callback`` off the lock.
    """

    def __init__(
        self, resubmit_callback: Callable[[list[_TimedResume]], None]
    ) -> None:
        self.resubmit_callback = resubmit_callback
        self._pending_resumes: list[tuple[float, int, _TimedResume]] = []
        self._lock = threading.Lock()
        self._schedule_counter = 0
        self._shutdown = threading.Event()
        self._timer_thread = threading.Thread(target=self._timer_loop, daemon=True)
        self._timer_thread.start()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.shutdown()

    def schedule_resume(self, exe_state: _TimedResume, resume_time: float) -> None:
        """Schedule a record to resume at the specified time.

        Uses a counter as a tie-breaker to ensure FIFO ordering when multiple
        records share a ``resume_time``.
        """
        with self._lock:
            heapq.heappush(
                self._pending_resumes,
                (resume_time, self._schedule_counter, exe_state),
            )
            self._schedule_counter += 1

    def shutdown(self) -> None:
        """Shutdown the timer thread and cancel all pending resumes."""
        self._shutdown.set()
        self._timer_thread.join(timeout=1.0)
        with self._lock:
            self._pending_resumes.clear()

    def _timer_loop(self) -> None:
        """Background thread that processes timed resumes."""
        while not self._shutdown.is_set():
            next_resume_time = None

            with self._lock:
                if self._pending_resumes:
                    next_resume_time = self._pending_resumes[0][0]

            if next_resume_time is None:
                # No pending resumes, wait a bit and check again
                self._shutdown.wait(timeout=0.1)
                continue

            current_time = time.time()
            if current_time >= next_resume_time:
                # Drain every due resume under the lock, transitioning each to
                # PENDING atomically with the pop, then resubmit off the lock so
                # timed resumes don't serialize behind the resubmit callback and
                # the timer thread can't re-enter this non-reentrant lock when a
                # resubmit schedules another resume inline.
                ready: list[_TimedResume] = []
                with self._lock:
                    while (
                        self._pending_resumes
                        and self._pending_resumes[0][0] <= current_time
                    ):
                        _, _, exe_state = heapq.heappop(self._pending_resumes)
                        if exe_state.can_resume:
                            exe_state.reset_to_pending()
                            ready.append(exe_state)
                if ready:
                    self.resubmit_callback(ready)
            else:
                # Wait until next resume time
                wait_time = min(next_resume_time - current_time, 0.1)
                self._shutdown.wait(timeout=wait_time)


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
        # First-observed wall-clock start per task name, stamped when the body is
        # about to run. Terminal records copy it into TaskExecution.started_at so
        # the envelope's startedAt/completedAt are populated (Python omitted them
        # entirely before). Set once per name so a timed re-run keeps the first
        # start.
        self._started_at: dict[str, datetime.datetime] = {}
        self._success = 0
        self._failure = 0
        self._skip = 0
        # All suspends raised by tasks this run. We do NOT re-raise the first
        # one captured; when stopping we resolve which suspend to surface with
        # the same precedence as ConcurrentExecutor.should_execution_suspend
        # (earliest timed wins over indefinite) so a concurrent short timer is
        # never dropped behind an indefinite wait_for_callback.
        self._pending_suspends: list[SuspendExecution] = []
        # In-process timed-resume bookkeeping: a timed suspend does NOT stop the
        # DAG. While other tasks make progress the DAG-owned TimerScheduler
        # re-runs the timed task in this same invocation at its scheduled
        # timestamp. Only an *indefinite* (callback) suspend forces leaving the
        # invocation for platform replay.
        self._scheduler: TimerScheduler | None = None
        self._pending_timers: set[str] = set()
        self._timed_suspend_by_name: dict[str, TimedSuspendExecution] = {}
        self._scheduler_exception: Exception | None = None
        self._early_reason: DagCompletionReason | None = None
        self._pool: ThreadPoolExecutor | None = None

    # region public
    def run(
        self,
        *,
        reconstruct_started: set[str] | None = None,
        reconstruct_reason: DagCompletionReason | None = None,
        reconstruct_total: int | None = None,
    ) -> DagResultImpl:
        """Schedule and run the DAG; return a DagResult (may raise to suspend).

        Offloaded-replay reconstruct (contract "replay rule"): when the container
        payload had ``tasks`` dropped, the caller passes the envelope's
        ``startedTaskNames`` as ``reconstruct_started`` and its
        ``completionReason``/``totalCount`` as ``reconstruct_reason``/
        ``reconstruct_total``. Reconstruct then re-runs this deterministic
        register graph exactly as a first run would -- each task fast-paths from
        its own retained child checkpoint, so bodies never re-execute -- except
        that a task named in ``reconstruct_started`` is seeded STARTED and never
        scheduled. That is the fix for the documented STARTED-set loss: an
        in-flight task recorded STARTED in the offloaded envelope is reproduced
        as STARTED instead of being restarted. The completion reason and total
        are taken from the envelope rather than re-derived.
        """
        total = len(self._tasks)
        if total == 0:
            reason = reconstruct_reason or DagCompletionReason.ALL_COMPLETED
            return DagResultImpl(
                {}, reason, total_count=reconstruct_total
            )

        if reconstruct_started:
            # Seed the started set before pumping: mark each as STARTED and
            # already-scheduled so _pump never submits it (no body run) and
            # downstream deps see it as non-terminal (stay unscheduled), exactly
            # reproducing the live in-flight snapshot.
            with self._lock:
                for name in reconstruct_started:
                    if name in self._tasks and name not in self._results:
                        self._results[name] = TaskExecution(
                            name=name, status=TaskStatus.STARTED
                        )
                        self._scheduled.add(name)

        # Resolve the single effective concurrency bound: an explicit
        # max_concurrency always wins (including a value above the default);
        # otherwise cap at DEFAULT_DAG_MAX_CONCURRENCY. This is BOTH the
        # scheduler's in-flight bound and the pool's max_workers -- the pool is
        # the resource that made an unbounded DAG spawn one OS thread per task.
        max_workers = self._config.max_concurrency or min(
            total, DEFAULT_DAG_MAX_CONCURRENCY
        )
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
        return self._build_result(
            reconstruct_reason=reconstruct_reason,
            reconstruct_total=reconstruct_total,
        )

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
                    else:  # _SKIP: trigger rule or run_if predicate returned False
                        self._results[name] = TaskExecution(
                            name=name, status=TaskStatus.SKIPPED, skip_reason=payload
                        )
                        self._skip += 1
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
            # Stamp the first-observed start for this task name (kept across a
            # timed re-run). Copied into the terminal/STARTED record so the
            # envelope carries startedAt.
            self._started_at.setdefault(name, datetime.datetime.now(datetime.UTC))
        logger.debug("DAG task %s starting", name)
        return task.executor(self._ctx, deps_map)

    def _on_done(self, name: str, future: Future) -> None:
        completed_at = datetime.datetime.now(datetime.UTC)
        try:
            result = future.result()
            with self._lock:
                self._results[name] = TaskExecution(
                    name=name,
                    status=TaskStatus.SUCCEEDED,
                    result=result,
                    started_at=self._started_at.get(name),
                    completed_at=completed_at,
                )
                self._success += 1
                self._in_flight.discard(name)
        except SuspendExecution as se:  # includes TimedSuspendExecution
            schedule_ts: float | None = None
            with self._lock:
                # Record every suspend (timed + indefinite); precedence is
                # resolved in _resolve_suspend when the DAG stops. A STARTED task
                # has begun but not completed, so it carries startedAt but no
                # completedAt.
                self._pending_suspends.append(se)
                self._results[name] = TaskExecution(
                    name=name,
                    status=TaskStatus.STARTED,
                    started_at=self._started_at.get(name),
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
                    started_at=self._started_at.get(name),
                    completed_at=completed_at,
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

    def _resubmit(self, resumes: list[_TimedResume]) -> None:
        """DAG-owned TimerScheduler callback: re-run a wave of timed tasks in-process.

        Fires on the scheduler's timer thread once tasks' scheduled timestamps
        elapse. The DAG-owned ``TimerScheduler`` batches all due resumes into one
        callback invocation (one checkpoint refresh serves the whole wave), so
        this accepts a list. It clears each task's timed-suspend bookkeeping and its STARTED placeholder so ``_pump`` sees them as fresh,
        ready tasks and re-runs them within the same invocation. Tasks that
        already left the timer set (e.g. the DAG bubbled to the platform and the
        scheduler was torn down) are skipped.
        """
        with self._lock:
            # Abort guard: once the DAG has decided to abort (a run_if predicate
            # raised, so _scheduler_exception is set), no further checkpoint may
            # be written and no task may re-run. The scheduler is the OUTER
            # context manager and the pool the INNER one, so the timer thread is
            # still alive during the pool-drain window; a resume whose timestamp
            # elapsed in that window would otherwise fire create_checkpoint()
            # AFTER the abort decision — a stray flush that contradicts the abort
            # contract (run() re-raises _scheduler_exception once the pool
            # drains). Bail before mutating state or checkpointing. This is the
            # only scheduler entry point that checkpoints; _on_done/_pump are
            # already gated by _stopping_locked (which checks _scheduler_exception
            # first), so no other teardown-window checkpoint exists.
            if self._scheduler_exception is not None:
                return
            for resume in resumes:
                name = resume.name
                if name not in self._pending_timers:
                    continue
                self._pending_timers.discard(name)
                se = self._timed_suspend_by_name.pop(name, None)
                if se is not None:
                    try:
                        self._pending_suspends.remove(se)
                    except ValueError:  # pragma: no cover - defensive
                        pass
                # Make the task schedulable again: drop its STARTED placeholder
                # and its scheduled mark so _pump re-evaluates and re-runs it.
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
        """Decide a ready task's fate: ``(_RUN, None)`` or ``(_SKIP, SkipReason)``.

        The trigger rule is a pure function of upstream enum statuses. ``run_if``
        is user-supplied code, but it is specified as a synchronous,
        deterministic, pure predicate over resolved upstream results, not a
        checkpointed operation. A ``run_if`` that raises is therefore a defect in
        deterministic code, not a business outcome: we neither record the task
        ``FAILED`` (which would silently drive every downstream ``ALL_FAILED`` /
        ``ANY_FAILED`` / ``ALL_DONE`` compensation path off a scheduler defect)
        nor ``SKIPPED``. Instead we raise :class:`DagPredicateError`, chaining the
        original exception, so the whole DAG aborts and ``dag()`` fails loudly.
        The offending task is left with no terminal state (it is never added to
        ``self._results``). This is identical for root and non-root tasks. The
        raise propagates out of ``_pump``: on the caller thread (first pump) it
        leaves ``run()`` directly; on a worker/timer thread it is captured by
        ``_safe_pump`` into ``self._scheduler_exception`` and re-raised by
        ``run()`` after the pool drains.
        """
        statuses = [self._results[dep.name].status for dep in task.all_deps]
        if not _trigger_passes(task.trigger_rule, statuses):
            return (_SKIP, SkipReason.TRIGGER_RULE)
        if task.run_if is not None:
            deps_map = self._build_deps_map(task)
            try:
                should_run = task.run_if(deps_map)
            except Exception as e:
                msg = (
                    f"run_if predicate for DAG task {task.name!r} raised "
                    f"{type(e).__name__}: {e}"
                )
                raise DagPredicateError(msg, task_name=task.name) from e
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
        the DAG-owned ``TimerScheduler``.
        """
        return any(
            not isinstance(se, TimedSuspendExecution) for se in self._pending_suspends
        )

    def _stopping_locked(self) -> bool:
        # A captured scheduler exception (e.g. a run_if predicate raised and the
        # DAG is aborting with DagPredicateError) stops all further scheduling:
        # no new tasks start while the pool drains any in-flight work. Checked
        # first so an abort is never downgraded by a threshold reason.
        if self._scheduler_exception is not None:
            return True
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

    def _build_result(
        self,
        *,
        reconstruct_reason: DagCompletionReason | None = None,
        reconstruct_total: int | None = None,
    ) -> DagResultImpl:
        if reconstruct_reason is not None:
            # Offloaded reconstruct: the completion reason is authoritative from
            # the envelope, not re-derived (a re-derivation over fast-pathed
            # results could disagree at an early-completion boundary).
            reason = reconstruct_reason
        elif self._early_reason is not None:
            reason = self._early_reason
        elif self._failure == 0:
            reason = DagCompletionReason.ALL_COMPLETED
        else:
            reason = DagCompletionReason.COMPLETED_WITH_FAILURES
        task_kinds = {name: task.kind for name, task in self._tasks.items()}
        total = (
            reconstruct_total if reconstruct_total is not None else len(self._tasks)
        )
        return DagResultImpl(
            dict(self._results), reason, task_kinds, total_count=total
        )
