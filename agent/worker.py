"""Agent: pull-based worker that claims tasks from the coordinator.

Loop:
    register              -> POST /agents/register
    heartbeat every N sec -> POST /agents/heartbeat
    claim                 -> POST /tasks/{id}/start after GET-ish claim
    execute               -> local Executor (engine adapter)
    report                -> POST /tasks/{id}/result

A single agent runs one task at a time (configurable concurrency can be added
later); pull-based claiming means any number of agents can join the cluster
without the coordinator knowing them in advance.
"""
from __future__ import annotations

import argparse
import logging
import socket
import threading
import time
import uuid
from typing import Any, Optional

import requests

from agent.executor import Executor
from models.task import ScanTask

logger = logging.getLogger(__name__)

POLL_INTERVAL_S = 1.0
HEARTBEAT_INTERVAL_S = 5.0


class Agent:
    def __init__(
        self,
        coordinator_url: str,
        agent_id: str | None = None,
        adapter_configs: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.coordinator = coordinator_url.rstrip("/")
        self.agent_id = agent_id or f"agent-{uuid.uuid4().hex[:8]}"
        self.hostname = socket.gethostname()
        self.executor = Executor(adapter_configs)
        self._stop = threading.Event()

    # ---- lifecycle ----------------------------------------------------------

    def register(self) -> None:
        resp = requests.post(
            f"{self.coordinator}/agents/register",
            json={"agent_id": self.agent_id, "hostname": self.hostname},
            timeout=10,
        )
        resp.raise_for_status()
        logger.info("agent %s registered", self.agent_id)

    def heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            try:
                requests.post(
                    f"{self.coordinator}/agents/heartbeat",
                    json={"agent_id": self.agent_id},
                    timeout=10,
                )
            except requests.RequestException:
                logger.warning("heartbeat failed (coordinator unreachable?)")
            self._stop.wait(HEARTBEAT_INTERVAL_S)

    def run(self) -> None:
        self.register()
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        logger.info("agent %s polling %s", self.agent_id, self.coordinator)
        while not self._stop.is_set():
            task = self._claim()
            if task is None:
                self._stop.wait(POLL_INTERVAL_S)
                continue
            self._process(task)

    # ---- task flow ----------------------------------------------------------

    def _claim(self) -> Optional[ScanTask]:
        try:
            resp = requests.post(f"{self.coordinator}/tasks/claim", json={"agent_id": self.agent_id}, timeout=10)
        except requests.RequestException:
            return None
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()
        return ScanTask(**data)

    def _process(self, task: ScanTask) -> None:
        # Confirm start so the coordinator can track running tasks.
        try:
            requests.post(
                f"{self.coordinator}/tasks/{task.task_id}/start",
                json={"agent_id": self.agent_id},
                timeout=10,
            )
        except requests.RequestException:
            logger.warning("start ack failed for %s", task.task_id)
        result = self.executor.execute(task)
        try:
            resp = requests.post(
                f"{self.coordinator}/tasks/{task.task_id}/result",
                json={
                    "agent_id": self.agent_id,
                    "verdict": result.verdict.value if result.verdict else "unknown",
                    "error": result.error,
                    "details": result.details,
                    "scan_duration_ms": result.scan_duration_ms,
                },
                timeout=10,
            )
            resp.raise_for_status()
            logger.info(
                "task %s -> %s verdict=%s",
                task.task_id,
                result.status.value,
                result.verdict.value if result.verdict else "unknown",
            )
        except requests.RequestException as exc:
            logger.error("result report failed for %s: %s", task.task_id, exc)

    def stop(self) -> None:
        self._stop.set()


def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-Engine Scan Platform agent")
    parser.add_argument("--coordinator", default="http://127.0.0.1:8000")
    parser.add_argument("--agent-id", default=None)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    agent = Agent(args.coordinator, agent_id=args.agent_id)
    try:
        agent.run()
    except KeyboardInterrupt:
        agent.stop()


if __name__ == "__main__":
    main()
