"""Coordinator scheduler: background loop that reclaims stale tasks.

Runs as an asyncio task inside the FastAPI app so there is no extra process
to deploy; the interval is configurable. This is the coordinator's only
proactive job — everything else is pull-based from agents, which keeps the
agent/coordinator contract simple.
"""
from __future__ import annotations

import asyncio
import logging

from coordinator.task_manager import TaskManager

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL_S = 15.0


class Scheduler:
    def __init__(self, task_manager: TaskManager, interval: float = DEFAULT_INTERVAL_S) -> None:
        self.tm = task_manager
        self.interval = interval
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="coordinator-scheduler")
        logger.info("scheduler started (interval=%ss)", self.interval)

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                requeued = self.tm.requeue_stale()
                if requeued:
                    logger.info("scheduler requeued %d stale task(s)", requeued)
            except Exception:  # never let the loop die
                logger.exception("scheduler tick failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
            except asyncio.TimeoutError:
                pass
