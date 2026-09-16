"""Scan attempt: one actual execution of a task.

A task is the business lifecycle of a scan request (queued -> ... -> done); an
attempt is a single execution on an agent. Keeping them separate means retries
keep their history: attempt 1 may time out, attempt 2 fail with an engine
error, attempt 3 succeed — all recorded, and the task stores only the aggregate
status. This is the standard way to reason about retries in distributed
systems (cf. job/attempt models in workflow engines).
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from models.task import ScanVerdict, TaskStatus


@dataclass
class ScanAttempt:
    task_id: str
    attempt_no: int
    agent_id: str
    status: TaskStatus = TaskStatus.ASSIGNED
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    error: str | None = None
    scan_duration_ms: int = 0
    verdict: ScanVerdict | None = None
    attempt_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "attempt_no": self.attempt_no,
            "attempt_id": self.attempt_id,
            "agent_id": self.agent_id,
            "status": self.status.value,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "error": self.error,
            "scan_duration_ms": self.scan_duration_ms,
            "verdict": self.verdict.value if self.verdict else None,
        }
