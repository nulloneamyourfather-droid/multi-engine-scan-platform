"""Scanner adapter interface.

A scanner adapter wraps a concrete AV/analysis engine and exposes a single
``scan`` method. The coordinator never talks to engines directly: agents run
adapters and feed normalized results back. This keeps the platform
vendor-neutral and makes engines pluggable.
"""
from __future__ import annotations

import abc
from typing import Any

from models.task import ScanResult, ScanTask, ScanVerdict


class ScannerAdapter(abc.ABC):
    """Interface every scanner engine adapter must implement."""

    name: str = "base"

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = config or {}

    @abc.abstractmethod
    def scan(self, task: ScanTask) -> ScanResult:
        """Scan the artifact referenced by ``task`` and return a normalized result.

        Implementations must not raise on a scan finding: report the outcome
        through the returned ScanResult. Raise only for infrastructure failures
        (engine unavailable, invalid artifact), which the agent turns into a
        failed task eligible for retry.
        """
        raise NotImplementedError

    def describe(self) -> dict[str, Any]:
        """Return adapter metadata (shown by the coordinator's /engines API)."""
        return {"name": self.name}
