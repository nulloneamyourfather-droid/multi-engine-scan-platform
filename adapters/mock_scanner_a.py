"""Mock scanner engine A.

Simulates a signature-based engine:
- artifacts whose sha256 hex starts with "0" or "f" are found malicious
  (mock of a signature hit);
- scan time is deterministic per sha256 so results are reproducible in tests;
- raises when the configured ``fail_rate`` fires, to exercise the retry path.
"""
from __future__ import annotations

import time
from typing import Any

from adapters.base import ScannerAdapter
from models.task import ScanResult, ScanTask, ScanVerdict


class MockScannerA(ScannerAdapter):
    name = "mock_engine_a"

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        super().__init__(config)
        self.fail_rate = float(self.config.get("fail_rate", 0.0))

    def scan(self, task: ScanTask) -> ScanResult:
        start = time.monotonic()
        h = task.artifact_sha256
        # Deterministic pseudo-randomness derived from the artifact hash, so the
        # fail path is stable for a given hash instead of flaky.
        seed = int(h[:8], 16) if len(h) >= 8 else len(h)
        if self.fail_rate > 0 and seed % 100 < self.fail_rate * 100:
            raise RuntimeError("mock_engine_a temporarily unavailable")

        malicious = h[0] in "0f"
        verdict = ScanVerdict.MALICIOUS if malicious else ScanVerdict.BENIGN
        return ScanResult(
            task_id=task.task_id,
            artifact_sha256=task.artifact_sha256,
            engine=self.name,
            status=task.status,  # coordinator sets final status
            verdict=verdict,
            submitted_at=time.time(),
            scan_duration_ms=int((time.monotonic() - start) * 1000),
            details={"matched_rule": "MockSig.{}".format(h[:8])} if malicious else {},
        )
