"""Task and result domain models.

Task lifecycle (see ``models.state_machine``):
    queued -> assigned -> running -> succeeded | failed -> (retry) -> queued
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class TaskStatus(str, Enum):
    QUEUED = "queued"
    ASSIGNED = "assigned"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ScanVerdict(str, Enum):
    MALICIOUS = "malicious"
    BENIGN = "benign"
    UNKNOWN = "unknown"


@dataclass
class ScanTask:
    """A unit of work: scan one artifact with one engine.

    ``task`` is the business lifecycle of a scan request; each actual execution
    is a separate ``ScanAttempt`` (see models/attempt.py). A task that keeps
    failing is retried up to ``max_retries`` attempts, then stays FAILED.
    """

    artifact_sha256: str
    engine: str
    task_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    status: TaskStatus = TaskStatus.QUEUED
    priority: int = 0
    max_retries: int = 3
    attempts: int = 0
    agent_id: str | None = None
    execution_timeout_s: float = 300.0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "artifact_sha256": self.artifact_sha256,
            "engine": self.engine,
            "status": self.status.value,
            "priority": self.priority,
            "max_retries": self.max_retries,
            "attempts": self.attempts,
            "agent_id": self.agent_id,
            "execution_timeout_s": self.execution_timeout_s,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass
class ScanResult:
    """Normalized scan output shared across all engines."""

    task_id: str
    artifact_sha256: str
    engine: str
    status: TaskStatus
    verdict: ScanVerdict
    submitted_at: float
    scan_duration_ms: int
    error: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "sha256": self.artifact_sha256,
            "engine": self.engine,
            "status": self.status.value,
            "verdict": self.verdict.value,
            "submitted_at": self.submitted_at,
            "scan_duration_ms": self.scan_duration_ms,
            "error": self.error,
            "details": self.details,
        }
