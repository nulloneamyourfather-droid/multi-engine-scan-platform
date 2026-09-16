"""Tests for the task state machine."""
from __future__ import annotations

import pytest

from models.state_machine import InvalidTransition, TaskStateMachine
from models.task import ScanTask, TaskStatus


@pytest.fixture
def task():
    return ScanTask(artifact_sha256="a" * 64, engine="mock_engine_a")


def test_happy_path_transitions(task):
    sm = TaskStateMachine()
    assert sm.can_transition(TaskStatus.QUEUED, TaskStatus.ASSIGNED)
    sm.transition(task, TaskStatus.ASSIGNED)
    sm.transition(task, TaskStatus.RUNNING)
    sm.transition(task, TaskStatus.SUCCEEDED)
    assert task.status == TaskStatus.SUCCEEDED


def test_retry_path(task):
    sm = TaskStateMachine()
    sm.transition(task, TaskStatus.ASSIGNED)
    sm.transition(task, TaskStatus.RUNNING)
    sm.transition(task, TaskStatus.FAILED)
    sm.transition(task, TaskStatus.QUEUED)
    assert task.status == TaskStatus.QUEUED


def test_succeeded_is_terminal(task):
    sm = TaskStateMachine()
    sm.transition(task, TaskStatus.ASSIGNED)
    sm.transition(task, TaskStatus.RUNNING)
    sm.transition(task, TaskStatus.SUCCEEDED)
    with pytest.raises(InvalidTransition):
        sm.transition(task, TaskStatus.QUEUED)


@pytest.mark.parametrize(
    "current,target",
    [
        (TaskStatus.QUEUED, TaskStatus.RUNNING),  # cannot skip assigned
        (TaskStatus.QUEUED, TaskStatus.SUCCEEDED),
        # ASSIGNED -> FAILED is legal: it's the "claimed but never started,
        # budget exhausted" terminal path (see TaskManager.requeue_stale).
        (TaskStatus.FAILED, TaskStatus.RUNNING),   # failed goes back to queued
        (TaskStatus.RUNNING, TaskStatus.ASSIGNED),
    ],
)
def test_invalid_transitions_are_rejected(current, target):
    sm = TaskStateMachine()
    task = ScanTask(artifact_sha256="a" * 64, engine="e")
    task.status = current
    with pytest.raises(InvalidTransition):
        sm.transition(task, target)


def test_assigned_can_fail_permanently():
    """Budget exhausted while ASSIGNED (agent died before start) -> FAILED."""
    sm = TaskStateMachine()
    task = ScanTask(artifact_sha256="a" * 64, engine="e")
    task.status = TaskStatus.ASSIGNED
    sm.transition(task, TaskStatus.FAILED)
    assert task.status == TaskStatus.FAILED
