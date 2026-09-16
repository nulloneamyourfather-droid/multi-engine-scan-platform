"""Tests for the distributed-systems invariants the Review called out:

- concurrent claims: exactly one agent wins
- capability-aware scheduling: an agent never claims an engine it can't run
- stale/duplicate worker: a reclaimed task rejects its old agent's start/report
- result idempotency: duplicate reports don't corrupt state
- execution lease/timeout: running tasks are reclaimed and consume retry budget
- attempt history: every execution is recorded in scan_attempts
"""
from __future__ import annotations

import threading
import time

from coordinator.task_manager import TaskManager
from models.task import ScanResult, ScanVerdict, TaskStatus
from storage.sqlite import SQLiteStore


def _make_manager(**kwargs):
    return TaskManager(SQLiteStore(":memory:"), **kwargs)


# ---- concurrent claims ------------------------------------------------------


def test_concurrent_claim_exactly_one_wins():
    tm = _make_manager()
    (_task,) = tm.submit("a" * 64, ["mock_engine_a"])
    n_agents = 20
    winners: list[str] = []
    lock = threading.Lock()

    def claim(agent_id: str):
        t = tm.claim(agent_id, ["mock_engine_a"])
        if t is not None:
            with lock:
                winners.append(agent_id)

    threads = [threading.Thread(target=claim, args=(f"agent-{i}",)) for i in range(n_agents)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(winners) == 1
    assert winners[0].startswith("agent-")
    # exactly one task created, and it was claimed exactly once
    assert tm.store.get_task(_task.task_id).agent_id == winners[0]


# ---- capability-aware scheduling --------------------------------------------


def test_agent_capability_restricts_claims():
    tm = _make_manager()
    tm.submit("a" * 64, ["mock_engine_a"])
    tm.submit("b" * 64, ["mock_engine_b"])
    # Engine-B-only agent must NOT get the engine-a task.
    claimed = tm.claim("agent-b", ["mock_engine_b"])
    assert claimed is not None
    assert claimed.engine == "mock_engine_b"
    # Engine-A-only agent must NOT get the engine-b task.
    claimed = tm.claim("agent-a", ["mock_engine_a"])
    assert claimed is not None
    assert claimed.engine == "mock_engine_a"


def test_agent_without_capabilities_claims_nothing():
    tm = _make_manager()
    tm.submit("a" * 64, ["mock_engine_a"])
    assert tm.claim("agent-unknown", []) is None


# ---- stale / duplicate worker ------------------------------------------------


def test_reclaimed_task_rejects_old_agent_start():
    tm = _make_manager(heartbeat_timeout=1.0)
    tm.store.register_agent("agent-1", "h1")
    (_task,) = tm.submit("a" * 64, ["mock_engine_a"])
    claimed = tm.claim("agent-1", ["mock_engine_a"])
    assert claimed is not None
    tm.start(claimed.task_id, "agent-1")

    # simulate lease expiry (no heartbeat) and reclaim
    n = tm.requeue_stale(now=time.time() + tm.heartbeat_timeout + 10)
    assert n == 1

    # old agent tries to start again -> rejected (returns None)
    assert tm.start(claimed.task_id, "agent-1") is None
    # a fresh agent can claim and run it
    fresh = tm.claim("agent-2", ["mock_engine_a"])
    assert fresh is not None and fresh.task_id == claimed.task_id
    assert tm.start(fresh.task_id, "agent-2") is not None


def test_reclaimed_task_rejects_old_agent_report():
    tm = _make_manager(heartbeat_timeout=1.0)
    tm.store.register_agent("agent-1", "h1")
    (_task,) = tm.submit("a" * 64, ["mock_engine_a"])
    claimed = tm.claim("agent-1", ["mock_engine_a"])
    assert claimed is not None
    tm.start(claimed.task_id, "agent-1")

    tm.requeue_stale(now=time.time() + tm.heartbeat_timeout + 10)
    fresh = tm.claim("agent-2", ["mock_engine_a"])
    assert fresh is not None
    tm.start(fresh.task_id, "agent-2")

    # stale agent's late result is rejected (duplicate execution discarded)
    stale_result = ScanResult(
        task_id=claimed.task_id,
        artifact_sha256="a" * 64,
        engine="mock_engine_a",
        status=TaskStatus.SUCCEEDED,
        verdict=ScanVerdict.MALICIOUS,
        submitted_at=time.time(),
        scan_duration_ms=1,
    )
    assert tm.report(claimed.task_id, "agent-1", stale_result) is None

    # fresh agent's result is accepted
    fresh_result = ScanResult(
        task_id=fresh.task_id,
        artifact_sha256="a" * 64,
        engine="mock_engine_a",
        status=TaskStatus.SUCCEEDED,
        verdict=ScanVerdict.BENIGN,
        submitted_at=time.time(),
        scan_duration_ms=2,
    )
    assert tm.report(fresh.task_id, "agent-2", fresh_result) is not None


# ---- result idempotency -----------------------------------------------------


def test_duplicate_report_is_idempotent():
    tm = _make_manager()
    (_task,) = tm.submit("a" * 64, ["mock_engine_a"])
    claimed = tm.claim("agent-1", ["mock_engine_a"])
    assert claimed is not None
    tm.start(claimed.task_id, "agent-1")

    def result():
        return ScanResult(
            task_id=claimed.task_id,
            artifact_sha256="a" * 64,
            engine="mock_engine_a",
            status=TaskStatus.SUCCEEDED,
            verdict=ScanVerdict.BENIGN,
            submitted_at=time.time(),
            scan_duration_ms=5,
        )

    first = tm.report(claimed.task_id, "agent-1", result())
    assert first is not None and first.status == TaskStatus.SUCCEEDED
    # duplicate report: same task/agent -> still accepted, no corruption
    dup = tm.report(claimed.task_id, "agent-1", result())
    assert dup is not None and dup.status == TaskStatus.SUCCEEDED
    stored = tm.store.get_result(claimed.task_id)
    assert stored is not None and stored.verdict == ScanVerdict.BENIGN
    # exactly one result row, one final status
    assert len(tm.store.list_results()) == 1
    assert tm.store.get_task(claimed.task_id).status == TaskStatus.SUCCEEDED


# ---- lease / execution timeout ----------------------------------------------


def test_execution_timeout_reclaims_running_task():
    tm = _make_manager()
    tm.store.register_agent("agent-1", "h1")
    (task,) = tm.submit("a" * 64, ["mock_engine_a"], execution_timeout_s=1.0)
    claimed = tm.claim("agent-1", ["mock_engine_a"])
    assert claimed is not None
    tm.start(claimed.task_id, "agent-1")

    # agent heartbeats fine but execution exceeds the timeout
    tm.heartbeat("agent-1")
    n = tm.requeue_stale(now=time.time() + 5.0)
    assert n == 1
    assert tm.store.get_task(task.task_id).status == TaskStatus.QUEUED


def test_running_task_within_timeout_is_not_reclaimed():
    tm = _make_manager()
    tm.store.register_agent("agent-1", "h1")
    (task,) = tm.submit("a" * 64, ["mock_engine_a"], execution_timeout_s=300.0)
    claimed = tm.claim("agent-1", ["mock_engine_a"])
    assert claimed is not None
    tm.start(claimed.task_id, "agent-1")
    tm.heartbeat("agent-1")
    n = tm.requeue_stale(now=time.time() + 10.0)
    assert n == 0
    assert tm.store.get_task(task.task_id).status == TaskStatus.RUNNING


def test_repeated_crash_consumes_retry_budget():
    tm = _make_manager()
    (task,) = tm.submit("a" * 64, ["mock_engine_a"], max_retries=2)
    for i in range(1, 4):
        agent = f"agent-{i}"
        claimed = tm.claim(agent, ["mock_engine_a"])
        if claimed is None:
            break
        tm.start(claimed.task_id, agent)
        tm.requeue_stale(now=time.time() + tm.heartbeat_timeout + 10)
    final = tm.store.get_task(task.task_id)
    assert final.status == TaskStatus.FAILED  # budget exhausted, no infinite loop
    assert final.attempts == 2  # max_retries attempts were consumed


# ---- attempt history ---------------------------------------------------------


def test_attempts_recorded_with_retry_history():
    tm = _make_manager()
    (task,) = tm.submit("a" * 64, ["mock_engine_a"], max_retries=3)
    claimed = tm.claim("agent-1", ["mock_engine_a"])
    assert claimed is not None
    tm.start(claimed.task_id, "agent-1")
    fail = ScanResult(
        task_id=claimed.task_id,
        artifact_sha256="a" * 64,
        engine="mock_engine_a",
        status=TaskStatus.FAILED,
        verdict=ScanVerdict.UNKNOWN,
        submitted_at=time.time(),
        scan_duration_ms=1,
        error="engine error",
    )
    tm.report(claimed.task_id, "agent-1", fail)
    claimed = tm.claim("agent-2", ["mock_engine_a"])
    assert claimed is not None
    tm.start(claimed.task_id, "agent-2")
    ok = ScanResult(
        task_id=claimed.task_id,
        artifact_sha256="a" * 64,
        engine="mock_engine_a",
        status=TaskStatus.SUCCEEDED,
        verdict=ScanVerdict.BENIGN,
        submitted_at=time.time(),
        scan_duration_ms=2,
    )
    tm.report(claimed.task_id, "agent-2", ok)

    attempts = tm.store.list_attempts(task.task_id)
    assert len(attempts) == 2
    assert attempts[0].attempt_no == 1 and attempts[0].status == TaskStatus.FAILED
    assert attempts[1].attempt_no == 2 and attempts[1].status == TaskStatus.SUCCEEDED
    assert attempts[1].verdict == ScanVerdict.BENIGN
    # task aggregate reflects the final success
    assert tm.store.get_task(task.task_id).status == TaskStatus.SUCCEEDED
