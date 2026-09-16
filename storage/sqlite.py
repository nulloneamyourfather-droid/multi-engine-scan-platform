"""SQLite-backed storage for tasks, attempts, results and agent registrations.

Storage is deliberately dumb: it only persists state. All state-transition
rules live in TaskManager / TaskStateMachine, so the invariant "all task state
changes go through the state machine" holds at the domain layer and Storage
cannot silently bypass it.

A single connection is used with ``check_same_thread=False`` guarded by a lock;
the workload here is small (single coordinator process), so a threading.Lock is
sufficient and keeps the code dependency-free.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from typing import Any

from models.attempt import ScanAttempt
from models.task import ScanResult, ScanTask, TaskStatus

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id             TEXT PRIMARY KEY,
    artifact_sha256     TEXT NOT NULL,
    engine              TEXT NOT NULL,
    status              TEXT NOT NULL,
    priority            INTEGER NOT NULL DEFAULT 0,
    max_retries         INTEGER NOT NULL DEFAULT 3,
    attempts            INTEGER NOT NULL DEFAULT 0,
    agent_id            TEXT,
    execution_timeout_s REAL NOT NULL DEFAULT 300,
    created_at          REAL NOT NULL,
    updated_at          REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS scan_attempts (
    task_id            TEXT NOT NULL,
    attempt_no         INTEGER NOT NULL,
    attempt_id         TEXT NOT NULL,
    agent_id           TEXT NOT NULL,
    status             TEXT NOT NULL,
    started_at         REAL NOT NULL,
    finished_at        REAL,
    error              TEXT,
    scan_duration_ms   INTEGER NOT NULL DEFAULT 0,
    verdict            TEXT,
    PRIMARY KEY (task_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS results (
    task_id          TEXT PRIMARY KEY,
    artifact_sha256  TEXT NOT NULL,
    engine           TEXT NOT NULL,
    status           TEXT NOT NULL,
    verdict          TEXT NOT NULL,
    submitted_at     REAL NOT NULL,
    scan_duration_ms INTEGER NOT NULL,
    error            TEXT,
    details          TEXT
);

CREATE TABLE IF NOT EXISTS agents (
    agent_id     TEXT PRIMARY KEY,
    hostname     TEXT,
    capabilities TEXT NOT NULL DEFAULT '[]',
    last_seen    REAL NOT NULL,
    status       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created_at);
CREATE INDEX IF NOT EXISTS idx_attempts_task ON scan_attempts(task_id);
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
                "INSERT OR REPLACE INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    task.task_id,
                    task.artifact_sha256,
                    task.engine,
                    task.status.value,
                    task.priority,
                    task.max_retries,
                    task.attempts,
                    task.agent_id,
                    task.execution_timeout_s,
                    task.created_at,
                    task.updated_at,
                ),
            )
            self._conn.commit()

    def get_task(self, task_id: str) -> ScanTask | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE task_id=?", (task_id,)
            ).fetchone()
        return self._row_to_task(row) if row else None

    def update_task(self, task: ScanTask) -> None:
        task.updated_at = time.time()
        self.insert_task(task)

    def claim_next_task(
        self,
        agent_id: str,
        capabilities: list[str] | None = None,
    ) -> ScanTask | None:
        """Atomically claim the best queued task an agent can run.

        When ``capabilities`` is provided (a non-empty list of engine names the
        agent supports), only tasks whose engine is in that set are eligible —
        this is what lets heterogeneous worker nodes coexist (agent with
        mock_engine_a never claims a mock_engine_b task). Returns the task
        already moved to ASSIGNED, or None if nothing is claimable.
        ``BEGIN IMMEDIATE`` gives a real read-then-write lock so two agents can
        never claim the same task.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                # capabilities=None  -> no engine filter (claim anything)
                # capabilities=[]    -> agent can run nothing
                # capabilities=[...] -> engine IN (...)
                if capabilities == []:
                    self._conn.execute("ROLLBACK")
                    return None
                sql = (
                    "SELECT * FROM tasks WHERE status=? "
                    "ORDER BY priority DESC, created_at ASC LIMIT 1"
                )
                params: list[Any] = [TaskStatus.QUEUED.value]
                if capabilities:
                    placeholders = ",".join("?" for _ in capabilities)
                    sql = (
                        "SELECT * FROM tasks WHERE status=? AND engine IN ("
                        + placeholders
                        + ") ORDER BY priority DESC, created_at ASC LIMIT 1"
                    )
                    params = [TaskStatus.QUEUED.value, *capabilities]
                row = self._conn.execute(sql, params).fetchone()
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
        """Return an ASSIGNED/RUNNING task to the queue.

        ``task.status`` is set by the caller (TaskManager) after validating the
        transition; Storage only persists it.
        """
        task.agent_id = None
        task.updated_at = time.time()
        self.insert_task(task)

    def list_tasks(
        self,
        status: TaskStatus | None = None,
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

    # ---- scan attempts ------------------------------------------------------

    def insert_attempt(self, attempt: ScanAttempt) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO scan_attempts "
                "(task_id, attempt_no, attempt_id, agent_id, status, started_at,"
                " finished_at, error, scan_duration_ms, verdict) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    attempt.task_id,
                    attempt.attempt_no,
                    attempt.attempt_id,
                    attempt.agent_id,
                    attempt.status.value,
                    attempt.started_at,
                    attempt.finished_at,
                    attempt.error,
                    attempt.scan_duration_ms,
                    attempt.verdict.value if attempt.verdict else None,
                ),
            )
            self._conn.commit()

    def list_attempts(self, task_id: str) -> list[ScanAttempt]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM scan_attempts WHERE task_id=? ORDER BY attempt_no ASC",
                (task_id,),
            ).fetchall()
        return [self._row_to_attempt(r) for r in rows]

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

    def get_result(self, task_id: str) -> ScanResult | None:
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
        return [
            r
            for r in (self.get_result(row["task_id"]) for row in rows)
            if r is not None
        ]

    # ---- agents ----------------------------------------------------------

    def register_agent(self, agent_id: str, hostname: str, capabilities: list[str] | None = None) -> None:
        caps = json.dumps(capabilities or [])
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO agents (agent_id, hostname, capabilities, last_seen, status) VALUES (?,?,?,?,?)",
                (agent_id, hostname, caps, time.time(), "online"),
            )
            self._conn.commit()

    def get_agent(self, agent_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM agents WHERE agent_id=?", (agent_id,)
            ).fetchone()
        if row is None:
            return None
        d = dict(row)
        d["capabilities"] = json.loads(d["capabilities"] or "[]")
        return d

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
        agents = []
        for r in rows:
            d = dict(r)
            d["capabilities"] = json.loads(d["capabilities"] or "[]")
            agents.append(d)
        return agents

    # ---- internal helpers --------------------------------------------------

    @staticmethod
    def _row_to_task(row: sqlite3.Row | None) -> ScanTask | None:
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
            execution_timeout_s=row["execution_timeout_s"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row_to_attempt(row: sqlite3.Row) -> ScanAttempt:
        from models.task import ScanVerdict

        return ScanAttempt(
            task_id=row["task_id"],
            attempt_no=row["attempt_no"],
            attempt_id=row["attempt_id"],
            agent_id=row["agent_id"],
            status=TaskStatus(row["status"]),
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            error=row["error"],
            scan_duration_ms=row["scan_duration_ms"],
            verdict=ScanVerdict(row["verdict"]) if row["verdict"] else None,
        )

    @staticmethod
    def _verdict(value: str):
        from models.task import ScanVerdict

        return ScanVerdict(value)

    def close(self) -> None:
        with self._lock:
            self._conn.close()
