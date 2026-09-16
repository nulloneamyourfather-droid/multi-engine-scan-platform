"""Mock scanner engine B.

Simulates a heuristic/behavioral engine that agrees with engine A most of the
time but disagrees on a small share of samples (``disagree_rate``). This models
real multi-engine disagreement, which the coordinator's aggregation layer can
later use to compute a consensus verdict.
"""
from __future__ import annotations

import time
from typing import Any

from adapters.base import ScannerAdapter
from models.task import ScanResult, ScanTask, ScanVerdict


class MockScannerB(ScannerAdapter):
    name = "mock_engine_b"

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        super().__init__(config)
        self.disagree_rate = float(self.config.get("disagree_rate", 0.2))

    def scan(self, task: ScanTask) -> ScanResult:
        start = time.monotonic()
        h = task.artifact_sha256
        seed = int(h[:8], 16) if len(h) >= 8 else len(h)
        base_malicious = h[0] in "0f"
        # Flip the verdict for a deterministic subset of samples.
        disagree = seed % 100 < self.disagree_rate * 100
        malicious = not base_malicious if disagree else base_malicious
        verdict = ScanVerdict.MALICIOUS if malicious else ScanVerdict.BENIGN
        return ScanResult(
            task_id=task.task_id,
            artifact_sha256=task.artifact_sha256,
            engine=self.name,
            status=task.status,
            verdict=verdict,
            submitted_at=time.time(),
            scan_duration_ms=int((time.monotonic() - start) * 1000),
            details={"heuristic_score": 80 if malicious else 10, "disagreed": disagree},
        )
