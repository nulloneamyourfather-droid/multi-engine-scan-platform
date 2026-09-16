"""TaskManager: task creation, transitions, retry and heartbeat enforcement.

TaskManager is the single authority on a task's lifecycle. Every state change
is validated by the TaskStateMachine before it is persisted; Storage only
stores what TaskManager tells it to. This is what keeps the platform safe to
run with many agents.

Attempt semantics
-----------------
``task.attempts`` counts *executions that actually started*: it increments only
when a task moves ASSIGNED -> RUNNING (``start``). This is the retry budget.

- RUNNING task goes stale (timeout / agent crash): the *current* attempt is
  closed as failed, but ``attempts`` is NOT incremented again — the execution
  was already counted when it started.
- ASSIGNED task goes stale (agent died before ``start``): nothing was executed,
  so no ScanAttempt is created and ``attempts`` is untouched.

Repeated claim-before-start abuse is bounded separately by ``reclaim_count``,
which is *not* the execution budget: it only guards against a broken node
grabbing tasks it can never start. ``reclaim_count`` and ``attempts`` are never
mixed.

Lease model: claiming gives an agent an exclusive lease. ``start`` confirms the
lease (rejects if the agent is no longer the holder). A stale task is reclaimed
by the scheduler; stale workers' late results are rejected (lease fencing), so
execution is at-least-once while *accepted* results are idempotent.
"""
from __future__ import annotations

import logging
import time

from models.attempt import ScanAttempt
from models.state_machine import InvalidTransition, TaskStateMachine
from models.task import ScanResult, ScanTask, TaskStatus
from storage.sqlite import SQLiteStore

logger = logging.getLogger(__name__)

HEARTBEAT_TIMEOUT_S = 60.0
MAX_RECLAIMS_BEFORE_START = 5  # guards claim-without-start abuse


class TaskManager:
    def __init__(
        self,
        store: SQLiteStore,
        heartbeat_timeout: float = HEARTBEAT_TIMEOUT_S,
        max_reclaims_before_start: int = MAX_RECLAIMS_BEFORE_START,
    ) -> None:
        self.store = store
        self.heartbeat_timeout = heartbeat_timeout
        self.max_reclaims_before_start = max_reclaims_before_start
        self.sm = TaskStateMachine()

    # ---- submission -------------------------------------------------------

    def submit(
        self,
        artifact_sha256: str,
        engines: list[str],
        priority: int = 0,
        max_retries: int = 3,
        execution_timeout_s: float = 300.0,
    ) -> list[ScanTask]:
        """Submit one artifact to one or more engines; returns created tasks."""
        tasks = [
            ScanTask(
                artifact_sha256=artifact_sha256,
                engine=engine,
                priority=priority,
                max_retries=max_retries,
                execution_timeout_s=execution_timeout_s,
            )
            for engine in engines
        ]
        for task in tasks:
            self.store.insert_task(task)
            logger.info("submitted task %s engine=%s", task.task_id, task.engine)
        return tasks

    # ---- claiming / reporting ----------------------------------------------

    def claim(self, agent_id: str, capabilities: list[str] | None = None) -> ScanTask | None:
        """Hand a queued task this agent can run; moves it queued -> assigned."""
        task = self.store.claim_next_task(agent_id, capabilities)
        if task is not None:
            self.sm.transition(task, TaskStatus.ASSIGNED)
            logger.info("task %s claimed by %s", task.task_id, agent_id)
        return task

    def start(self, task_id: str, agent_id: str) -> ScanTask | None:
        """Agent confirms it will execute: assigned -> running (lease check).

        Returns None if the agent is not the current lease holder, or the task
        is not in ASSIGNED — the caller must then NOT execute the scan, to avoid
        two workers scanning the same artifact.

        This is the ONLY place ``attempts`` is incremented: an attempt is a
        scan execution that actually started.
        """
        task = self.store.get_task(task_id)
        if task is None or task.agent_id != agent_id:
            return None
        try:
            self.sm.transition(task, TaskStatus.RUNNING)
        except InvalidTransition:
            logger.warning("task %s cannot start from %s", task_id, task.status)
            return None
        task.attempts += 1
        self._record_attempt(task, status=TaskStatus.RUNNING, error=None)
        self.store.update_task(task)
        return task

    def report(self, task_id: str, agent_id: str, result: ScanResult) -> ScanTask | None:
        """Finalize a task from a result: running -> succeeded | failed.

        Lease fencing: only the agent that holds the task's lease may report; a
        stale agent's late result is rejected (returns None) and its duplicate
        execution is discarded.

        True idempotency: the first accepted terminal result becomes the
        canonical result. A duplicate report on an already-terminal task is
        ACKed WITHOUT overwriting the stored result.
        """
        task = self.store.get_task(task_id)
        if task is None or task.agent_id != agent_id:
            logger.warning("result rejected for %s from %s", task_id, agent_id)
            return None

        # Idempotency: terminal is final. ACK and return the existing task; do
        # NOT re-store, so a conflicting duplicate can never overwrite the
        # canonical result (first accepted terminal result wins).
        if task.status in (TaskStatus.SUCCEEDED, TaskStatus.FAILED):
            logger.info("task %s already terminal; acking duplicate result (canonical kept)", task_id)
            return task

        try:
            self.sm.transition(task, result.status)
        except InvalidTransition:
            logger.warning("task %s cannot finish as %s", task_id, result.status)
            return None

        self._record_attempt(
            task,
            status=result.status,
            error=result.error,
            duration_ms=result.scan_duration_ms,
            verdict=result.verdict,
        )
        self.store.save_result(result)

        if result.status == TaskStatus.SUCCEEDED:
            self.store.update_task(task)
            logger.info("task %s succeeded in %d ms", task_id, result.scan_duration_ms)
            return task

        # failure path: requeue if retries remain, else terminal FAILED
        if task.attempts < task.max_retries:
            logger.warning(
                "task %s failed (attempt %d/%d); requeueing",
                task_id,
                task.attempts,
                task.max_retries,
            )
            self._requeue(task)
        else:
            logger.error("task %s failed permanently after %d attempts", task_id, task.attempts)
            self.store.update_task(task)
        return task

    def heartbeat(self, agent_id: str) -> None:
        self.store.heartbeat(agent_id)

    # ---- reclaim / timeout ---------------------------------------------------

    def requeue_stale(self, now: float | None = None) -> int:
        """Reclaim tasks whose lease has expired.

        A task is stale when its agent stopped heartbeating, or it has been
        RUNNING longer than ``execution_timeout_s``.

        - RUNNING stale: close the current attempt as failed, then requeue if
          retries remain. ``attempts`` is NOT incremented (already counted at
          start).
        - ASSIGNED stale (agent died before start): nothing was executed, so no
          ScanAttempt is created and ``attempts`` is untouched. ``reclaim_count``
          is incremented to bound claim-without-start abuse.
        - Budget exhausted -> terminal FAILED (valid from both RUNNING and
          ASSIGNED via the state machine).

        Returns the number of tasks requeued (not failed).
        """
        now = now or time.time()
        requeued = 0
        agents = {a["agent_id"]: a for a in self.store.list_agents()}
        for task in self.store.list_tasks():
            if task.status not in (TaskStatus.ASSIGNED, TaskStatus.RUNNING):
                continue
            if task.agent_id is None:
                continue
            agent = agents.get(task.agent_id)
            heartbeat_dead = agent is None or (now - float(agent["last_seen"])) > self.heartbeat_timeout
            lease_dead = task.status == TaskStatus.RUNNING and (
                now - task.updated_at
            ) > task.execution_timeout_s
            if not (heartbeat_dead or lease_dead):
                continue

            if task.status == TaskStatus.RUNNING:
                # Execution started; close the current attempt as failed.
                self._close_attempt(task, error="stale lease (timeout)")
                logger.warning(
                    "task %s stale while running (agent %s); attempt %d/%d closed",
                    task.task_id,
                    task.agent_id,
                    task.attempts,
                    task.max_retries,
                )
            else:
                # ASSIGNED but never started: no execution happened.
                task.reclaim_count += 1
                logger.warning(
                    "task %s stale before start (agent %s); reclaim %d/%d",
                    task.task_id,
                    task.agent_id,
                    task.reclaim_count,
                    self.max_reclaims_before_start,
                )
                if task.reclaim_count >= self.max_reclaims_before_start:
                    logger.error(
                        "task %s failed: claimed but never started %d times",
                        task.task_id,
                        task.reclaim_count,
                    )
                    self._fail_permanently(task)
                    continue

            if task.attempts >= task.max_retries and task.status == TaskStatus.RUNNING:
                # Execution budget exhausted after a failed attempt.
                self._fail_permanently(task)
                continue
            if task.status in (TaskStatus.FAILED,):
                continue
            self._requeue(task)
            requeued += 1
        return requeued

    # ---- internal -----------------------------------------------------------

    def _requeue(self, task: ScanTask) -> None:
        """Validate the transition, then have Storage persist the new state."""
        self.sm.transition(task, TaskStatus.QUEUED)
        self.store.requeue_task(task)

    def _fail_permanently(self, task: ScanTask) -> None:
        """Mark a task terminal FAILED, valid from RUNNING or ASSIGNED."""
        self.sm.transition(task, TaskStatus.FAILED)
        task.agent_id = None
        self.store.update_task(task)
        logger.error("task %s failed permanently (budget exhausted)", task.task_id)

    def _record_attempt(
        self,
        task: ScanTask,
        status: TaskStatus,
        error: str | None,
        duration_ms: int = 0,
        verdict=None,
    ) -> None:
        attempt = ScanAttempt(
            task_id=task.task_id,
            attempt_no=task.attempts,
            agent_id=task.agent_id or "",
            status=status,
            started_at=task.updated_at,
            finished_at=time.time() if status in (TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.QUEUED) else None,
            error=error,
            scan_duration_ms=duration_ms,
            verdict=verdict,
        )
        self.store.insert_attempt(attempt)

    def _close_attempt(self, task: ScanTask, error: str) -> None:
        """Close the in-flight attempt (the one recorded at start) as failed."""
        self._record_attempt(
            task,
            status=TaskStatus.FAILED,
            error=error,
        )
