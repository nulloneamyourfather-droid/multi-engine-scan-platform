"""SQLite-backed storage for tasks, results and agent registrations.

A single connection is used with ``check_same_thread=False`` guarded by a lock;
the workload here is small (single coordinator process), so a threading.Lock is
sufficient and keeps the code dependency-free.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from typing import Any, Optional

from models.task import ScanResult, ScanTask, TaskStatus

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id         TEXT PRIMARY KEY,
    artifact_sha256 TEXT NOT NULL,
    engine          TEXT NOT NULL,
    status          TEXT NOT NULL,
    priority        INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 3,
    attempts        INTEGER NOT NULL DEFAULT 0,
    agent_id        TEXT,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS results (
    task_id         TEXT PRIMARY KEY,
    artifact_sha256 TEXT NOT NULL,
    engine          TEXT NOT NULL,
    status          TEXT NOT NULL,
    verdict         TEXT NOT NULL,
    submitted_at    REAL NOT NULL,
    scan_duration_ms INTEGER NOT NULL,
    error           TEXT,
    details         TEXT
);

CREATE TABLE IF NOT EXISTS agents (
    agent_id     TEXT PRIMARY KEY,
    hostname     TEXT,
    last_seen    REAL NOT NULL,
    status       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created_at);
"""


class SQLiteStore:
    def __init__(self, path: str = ":memory:") -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    # ---- tasks -----------------------------------------------------------

    def insert_task(self, task: ScanTask) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    task.task_id,
                    task.artifact_sha256,
                    task.engine,
                    task.status.value,
                    task.priority,
                    task.max_retries,
                    task.attempts,
                    task.agent_id,
                    task.created_at,
                    task.updated_at,
                ),
            )
            self._conn.commit()

    def get_task(self, task_id: str) -> Optional[ScanTask]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE task_id=?", (task_id,)
            ).fetchone()
        return self._row_to_task(row) if row else None

    def update_task(self, task: ScanTask) -> None:
        task.updated_at = time.time()
        self.insert_task(task)

    def claim_next_task(self, agent_id: str) -> Optional[ScanTask]:
        """Atomically claim the highest-priority queued task for an agent.

        Returns the task already moved to ASSIGNED, or None if the queue is
        empty. ``BEGIN IMMEDIATE`` gives us a real read-then-write lock so two
        agents can never claim the same task.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT * FROM tasks WHERE status=? ORDER BY priority DESC, created_at ASC LIMIT 1",
                    (TaskStatus.QUEUED.value,),
                ).fetchone()
                if row is None:
                    self._conn.execute("ROLLBACK")
                    return None
                task = self._row_to_task(row)
                assert task is not None
                task.agent_id = agent_id
                task.updated_at = time.time()
                self._conn.execute(
                    "UPDATE tasks SET status=?, agent_id=?, updated_at=? WHERE task_id=?",
                    (TaskStatus.ASSIGNED.value, agent_id, task.updated_at, task.task_id),
                )
                self._conn.commit()
                return task
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def requeue_task(self, task: ScanTask) -> None:
        """Return an ASSIGNED/RUNNING task to the queue (heartbeat loss, timeout)."""
        task.agent_id = None
        self._transition(task, TaskStatus.QUEUED)
        self.update_task(task)

    def list_tasks(
        self,
        status: Optional[TaskStatus] = None,
        limit: int = 100,
    ) -> list[ScanTask]:
        with self._lock:
            if status is not None:
                rows = self._conn.execute(
                    "SELECT * FROM tasks WHERE status=? ORDER BY created_at DESC LIMIT ?",
                    (status.value, limit),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM tasks ORDER BY created_at DESC LIMIT ?", (limit,)
                ).fetchall()
        return [t for t in (self._row_to_task(r) for r in rows) if t is not None]

    # ---- results ---------------------------------------------------------

    def save_result(self, result: ScanResult) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO results VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    result.task_id,
                    result.artifact_sha256,
                    result.engine,
                    result.status.value,
                    result.verdict.value,
                    result.submitted_at,
                    result.scan_duration_ms,
                    result.error,
                    json.dumps(result.details, ensure_ascii=False),
                ),
            )
            self._conn.commit()

    def get_result(self, task_id: str) -> Optional[ScanResult]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM results WHERE task_id=?", (task_id,)
            ).fetchone()
        if row is None:
            return None
        return ScanResult(
            task_id=row["task_id"],
            artifact_sha256=row["artifact_sha256"],
            engine=row["engine"],
            status=TaskStatus(row["status"]),
            verdict=self._verdict(row["verdict"]),
            submitted_at=row["submitted_at"],
            scan_duration_ms=row["scan_duration_ms"],
            error=row["error"],
            details=json.loads(row["details"]) if row["details"] else {},
        )

    def list_results(self, limit: int = 100) -> list[ScanResult]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM results ORDER BY submitted_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [self.get_result(r["task_id"]) for r in rows if self.get_result(r["task_id"]) is not None]

    # ---- agents ----------------------------------------------------------

    def register_agent(self, agent_id: str, hostname: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO agents (agent_id, hostname, last_seen, status) VALUES (?,?,?,?)",
                (agent_id, hostname, time.time(), "online"),
            )
            self._conn.commit()

    def heartbeat(self, agent_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE agents SET last_seen=?, status=? WHERE agent_id=?",
                (time.time(), "online", agent_id),
            )
            self._conn.commit()

    def mark_agent_offline(self, agent_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE agents SET status=? WHERE agent_id=?",
                ("offline", agent_id),
            )
            self._conn.commit()

    def list_agents(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM agents ORDER BY last_seen DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    # ---- internal helpers --------------------------------------------------

    def _transition(self, task: ScanTask, target: TaskStatus) -> None:
        # Direct DB-level status change used by requeue paths; validation is
        # done at the API/manager layer with the TaskStateMachine.
        task.status = target

    @staticmethod
    def _row_to_task(row: Optional[sqlite3.Row]) -> Optional[ScanTask]:
        if row is None:
            return None
        return ScanTask(
            task_id=row["task_id"],
            artifact_sha256=row["artifact_sha256"],
            engine=row["engine"],
            status=TaskStatus(row["status"]),
            priority=row["priority"],
            max_retries=row["max_retries"],
            attempts=row["attempts"],
            agent_id=row["agent_id"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _verdict(value: str):
        from models.task import ScanVerdict

        return ScanVerdict(value)

    def close(self) -> None:
        with self._lock:
            self._conn.close()
