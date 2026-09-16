"""TaskManager: task creation, transitions, retry and heartbeat enforcement.

All task state changes go through the TaskStateMachine; the manager is the
single authority on a task's lifecycle, which is what makes the platform safe
to run with many agents.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from models.state_machine import InvalidTransition, TaskStateMachine
from models.task import ScanResult, ScanTask, TaskStatus
from storage.sqlite import SQLiteStore

logger = logging.getLogger(__name__)

HEARTBEAT_TIMEOUT_S = 60.0


class TaskManager:
    def __init__(self, store: SQLiteStore, heartbeat_timeout: float = HEARTBEAT_TIMEOUT_S) -> None:
        self.store = store
        self.heartbeat_timeout = heartbeat_timeout
        self.sm = TaskStateMachine()

    # ---- submission -------------------------------------------------------

    def submit(
        self,
        artifact_sha256: str,
        engines: list[str],
        priority: int = 0,
        max_retries: int = 3,
    ) -> list[ScanTask]:
        """Submit one artifact to one or more engines; returns created tasks."""
        tasks = [
            ScanTask(
                artifact_sha256=artifact_sha256,
                engine=engine,
                priority=priority,
                max_retries=max_retries,
            )
            for engine in engines
        ]
        for task in tasks:
            self.store.insert_task(task)
            logger.info("submitted task %s engine=%s", task.task_id, task.engine)
        return tasks

    # ---- claiming / reporting ----------------------------------------------

    def claim(self, agent_id: str) -> Optional[ScanTask]:
        """Hand a queued task to an agent; moves it queued -> assigned."""
        task = self.store.claim_next_task(agent_id)
        if task is not None:
            self.sm.transition(task, TaskStatus.ASSIGNED)
            logger.info("task %s claimed by %s", task.task_id, agent_id)
        return task

    def start(self, task_id: str, agent_id: str) -> Optional[ScanTask]:
        """Agent confirms it started executing: assigned -> running."""
        task = self.store.get_task(task_id)
        if task is None or task.agent_id != agent_id:
            return None
        try:
            self.sm.transition(task, TaskStatus.RUNNING)
        except InvalidTransition:
            logger.warning("task %s cannot start from %s", task_id, task.status)
            return None
        self.store.update_task(task)
        return task

    def report(
        self,
        task_id: str,
        agent_id: str,
        result: ScanResult,
    ) -> Optional[ScanTask]:
        """Finalize a task from a result: running -> succeeded | failed.

        On failure with retries remaining the task is requeued; otherwise it
        stays FAILED. The result is always persisted.
        """
        task = self.store.get_task(task_id)
        if task is None or task.agent_id != agent_id:
            return None
        try:
            self.sm.transition(task, result.status)
        except InvalidTransition:
            logger.warning("task %s cannot finish as %s", task_id, result.status)
            return None

        task.attempts += 1
        self.store.save_result(result)

        if result.status == TaskStatus.SUCCEEDED:
            self.store.update_task(task)
            logger.info("task %s succeeded in %d ms", task_id, result.scan_duration_ms)
            return task

        # failure path
        if task.attempts < task.max_retries:
            logger.warning(
                "task %s failed (attempt %d/%d); requeueing",
                task_id,
                task.attempts,
                task.max_retries,
            )
            self.store.requeue_task(task)
        else:
            logger.error("task %s failed permanently after %d attempts", task_id, task.attempts)
            self.store.update_task(task)
        return task

    # ---- heartbeat / timeout -----------------------------------------------

    def heartbeat(self, agent_id: str) -> None:
        self.store.heartbeat(agent_id)

    def requeue_stale(self, now: float | None = None) -> int:
        """Requeue ASSIGNED/RUNNING tasks whose agent heartbeat has expired.

        Returns the number of tasks requeued. Called periodically by the
        scheduler; this is the fault-tolerance path for crashed agents.
        """
        now = now or time.time()
        requeued = 0
        for task in self.store.list_tasks():
            if task.status not in (TaskStatus.ASSIGNED, TaskStatus.RUNNING):
                continue
            if task.agent_id is None:
                continue
            agent = next(
                (a for a in self.store.list_agents() if a["agent_id"] == task.agent_id),
                None,
            )
            if agent is None or (now - float(agent["last_seen"])) > self.heartbeat_timeout:
                logger.warning("task %s stale (agent %s); requeueing", task.task_id, task.agent_id)
                self.store.requeue_task(task)
                requeued += 1
        return requeued
