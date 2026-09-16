"""Agent: pull-based worker that claims tasks from the coordinator.

Loop:
    register              -> POST /agents/register
    heartbeat every N sec -> POST /agents/heartbeat
    claim                 -> POST /tasks/claim
    start                 -> POST /tasks/{id}/start   (lease; 409 => do NOT run)
    execute               -> local Executor (engine adapter)
    report                -> POST /tasks/{id}/result  (retried with backoff)

A single agent runs one task at a time (configurable concurrency can be added
later); pull-based claiming means any number of agents can join the cluster
without the coordinator knowing them in advance.
"""
from __future__ import annotations

import argparse
import logging
import socket
import threading
import uuid
from typing import Any

import requests

from agent.executor import Executor
from models.task import ScanTask

logger = logging.getLogger(__name__)

POLL_INTERVAL_S = 1.0
HEARTBEAT_INTERVAL_S = 5.0
REPORT_MAX_ATTEMPTS = 5
REPORT_BACKOFF_BASE_S = 1.0
REPORT_BACKOFF_MAX_S = 30.0


class Agent:
    def __init__(
        self,
        coordinator_url: str,
        agent_id: str | None = None,
        capabilities: list[str] | None = None,
        adapter_configs: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.coordinator = coordinator_url.rstrip("/")
        self.agent_id = agent_id or f"agent-{uuid.uuid4().hex[:8]}"
        self.hostname = socket.gethostname()
        self.capabilities = capabilities or []
        self.executor = Executor(adapter_configs)
        self._stop = threading.Event()

    # ---- lifecycle ----------------------------------------------------------

    def register(self) -> None:
        resp = requests.post(
            f"{self.coordinator}/agents/register",
            json={
                "agent_id": self.agent_id,
                "hostname": self.hostname,
                "capabilities": self.capabilities,
            },
            timeout=10,
        )
        resp.raise_for_status()
        logger.info("agent %s registered (capabilities=%s)", self.agent_id, self.capabilities)

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

    def _claim(self) -> ScanTask | None:
        try:
            resp = requests.post(
                f"{self.coordinator}/tasks/claim",
                json={"agent_id": self.agent_id},
                timeout=10,
            )
        except requests.RequestException:
            return None
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return ScanTask(**resp.json())

    def _process(self, task: ScanTask) -> None:
        # Confirm start. If the coordinator rejects it (409), our lease has
        # expired (e.g. the task was reclaimed after a long pause) — running
        # it would double-execute, so we must NOT scan.
        try:
            resp = requests.post(
                f"{self.coordinator}/tasks/{task.task_id}/start",
                json={"agent_id": self.agent_id},
                timeout=10,
            )
            if resp.status_code == 409:
                logger.warning("task %s lease expired; skipping execution", task.task_id)
                return
            resp.raise_for_status()
        except requests.RequestException:
            logger.warning("start ack failed for %s; not executing", task.task_id)
            return

        result = self.executor.execute(task)
        self._report_with_retry(task, result)

    def _report_with_retry(self, task: ScanTask, result) -> None:
        """Report a result with exponential backoff so a transient network
        blip cannot strand a finished task in ``running``.

        The coordinator's result endpoint is idempotent (INSERT OR REPLACE),
        so retries are safe: at-least-once delivery with idempotent handling
        gives us logically exactly-once results.
        """
        delay = REPORT_BACKOFF_BASE_S
        for attempt in range(1, REPORT_MAX_ATTEMPTS + 1):
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
                return
            except requests.RequestException as exc:
                if attempt == REPORT_MAX_ATTEMPTS:
                    logger.error(
                        "result report failed after %d attempts for %s: %s",
                        REPORT_MAX_ATTEMPTS,
                        task.task_id,
                        exc,
                    )
                else:
                    logger.warning(
                        "result report attempt %d/%d failed for %s: %s; retrying in %.1fs",
                        attempt,
                        REPORT_MAX_ATTEMPTS,
                        task.task_id,
                        exc,
                        delay,
                    )
                    self._stop.wait(delay)
                    delay = min(delay * 2, REPORT_BACKOFF_MAX_S)

    def stop(self) -> None:
        self._stop.set()


def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-Engine Scan Platform agent")
    parser.add_argument("--coordinator", default="http://127.0.0.1:8000")
    parser.add_argument("--agent-id", default=None)
    parser.add_argument(
        "--capabilities",
        nargs="*",
        default=None,
        help="engine names this agent can run, e.g. --capabilities mock_engine_a",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    agent = Agent(args.coordinator, agent_id=args.agent_id, capabilities=args.capabilities)
    try:
        agent.run()
    except KeyboardInterrupt:
        agent.stop()


if __name__ == "__main__":
    main()
