"""Tests for TaskManager: submit, claim, report, retry, heartbeat reclaim."""
from __future__ import annotations

import time

from coordinator.task_manager import TaskManager
from models.task import ScanResult, ScanVerdict, TaskStatus
from storage.sqlite import SQLiteStore


def _make_manager():
    return TaskManager(SQLiteStore(":memory:"))


def test_submit_creates_one_task_per_engine():
    tm = _make_manager()
    tasks = tm.submit("a" * 64, ["mock_engine_a", "mock_engine_b"])
    assert len(tasks) == 2
    assert {t.engine for t in tasks} == {"mock_engine_a", "mock_engine_b"}
    assert all(t.status == TaskStatus.QUEUED for t in tasks)


def test_claim_moves_queued_to_assigned_and_binds_agent():
    tm = _make_manager()
    (_task,) = tm.submit("a" * 64, ["mock_engine_a"])
    claimed = tm.claim("agent-1")
    assert claimed is not None
    assert claimed.task_id == _task.task_id
    assert claimed.status == TaskStatus.ASSIGNED
    assert claimed.agent_id == "agent-1"
    # Second claim must not get the same task.
    assert tm.claim("agent-2") is None


def test_report_success_finalizes_task_and_saves_result():
    tm = _make_manager()
    (_task,) = tm.submit("a" * 64, ["mock_engine_a"])
    claimed = tm.claim("agent-1")
    assert claimed is not None
    tm.start(claimed.task_id, "agent-1")

    result = ScanResult(
        task_id=claimed.task_id,
        artifact_sha256="a" * 64,
        engine="mock_engine_a",
        status=TaskStatus.SUCCEEDED,
        verdict=ScanVerdict.BENIGN,
        submitted_at=time.time(),
        scan_duration_ms=42,
    )
    done = tm.report(claimed.task_id, "agent-1", result)
    assert done is not None
    assert done.status == TaskStatus.SUCCEEDED
    saved = tm.store.get_result(claimed.task_id)
    assert saved is not None and saved.verdict == ScanVerdict.BENIGN


def test_report_failure_requeues_until_max_retries():
    tm = _make_manager()
    (task,) = tm.submit("a" * 64, ["mock_engine_a"], max_retries=2)
    claimed = tm.claim("agent-1")
    assert claimed is not None
    tm.start(claimed.task_id, "agent-1")

    current_agent = "agent-1"
    for attempt in range(1, 3):  # two failures: one requeue, one terminal
        result = ScanResult(
            task_id=claimed.task_id,
            artifact_sha256="a" * 64,
            engine="mock_engine_a",
            status=TaskStatus.FAILED,
            verdict=ScanVerdict.UNKNOWN,
            submitted_at=time.time(),
            scan_duration_ms=1,
            error="boom",
        )
        back = tm.report(claimed.task_id, current_agent, result)
        assert back is not None
        if attempt == 1:
            assert back.status == TaskStatus.QUEUED  # requeued
            claimed = tm.claim("agent-2")  # another agent picks it up
            current_agent = "agent-2"
            assert claimed is not None
            tm.start(claimed.task_id, "agent-2")
        else:
            assert back.status == TaskStatus.FAILED  # terminal
    assert tm.store.get_task(task.task_id).status == TaskStatus.FAILED


def test_heartbeat_keeps_agent_online_and_stale_task_is_requeued():
    tm = _make_manager()
    tm.store.register_agent("agent-1", "host1")
    tm.heartbeat("agent-1")

    (_task,) = tm.submit("a" * 64, ["mock_engine_a"])
    claimed = tm.claim("agent-1")
    assert claimed is not None
    tm.start(claimed.task_id, "agent-1")

    # No heartbeat for longer than timeout -> scheduler should requeue.
    n = tm.requeue_stale(now=time.time() + tm.heartbeat_timeout + 10)
    assert n == 1
    requeued = tm.store.get_task(_task.task_id)
    assert requeued.status == TaskStatus.QUEUED
    assert requeued.agent_id is None

    # A fresh agent can now claim it.
    assert tm.claim("agent-2") is not None
