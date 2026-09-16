"""Agent executor: runs a claimed task through its engine adapter.

The executor is engine-agnostic: it looks up the adapter by name, invokes it,
and produces a normalized ScanResult. Exceptions from the adapter (engine
unavailable, bad artifact) become FAILED results eligible for retry.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from adapters import create_adapter
from models.task import ScanResult, ScanTask, ScanVerdict, TaskStatus

logger = logging.getLogger(__name__)


class Executor:
    def __init__(self, adapter_configs: dict[str, dict[str, Any]] | None = None) -> None:
        self.adapter_configs = adapter_configs or {}

    def execute(self, task: ScanTask) -> ScanResult:
        start = time.monotonic()
        adapter = create_adapter(task.engine, self.adapter_configs.get(task.engine, {}))
        try:
            result = adapter.scan(task)
            # Adapters may leave status/verdict unset; enforce the contract.
            result.status = TaskStatus.SUCCEEDED
            if result.verdict is None:
                result.verdict = ScanVerdict.UNKNOWN
            return result
        except Exception as exc:  # infrastructure failure -> retryable
            logger.warning("engine %s failed on %s: %s", task.engine, task.task_id, exc)
            return ScanResult(
                task_id=task.task_id,
                artifact_sha256=task.artifact_sha256,
                engine=task.engine,
                status=TaskStatus.FAILED,
                verdict=ScanVerdict.UNKNOWN,
                submitted_at=time.time(),
                scan_duration_ms=int((time.monotonic() - start) * 1000),
                error=f"{type(exc).__name__}: {exc}",
            )
