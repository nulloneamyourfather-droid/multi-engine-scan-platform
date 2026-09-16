"""Task state machine.

Transition rules:

    queued   -> assigned          (coordinator assigns to an agent)
    assigned -> running           (agent picks the task up)
    running  -> succeeded         (agent reports a result)
    running  -> failed            (agent reports failure)
    failed   -> queued            (coordinator schedules a retry, if attempts remain)
    running  -> queued            (agent heartbeat lost / task timed out; reschedule)

Any other transition is rejected with ``InvalidTransition``.
"""
from __future__ import annotations

from collections.abc import Callable

from models.task import TaskStatus


class InvalidTransition(Exception):
    def __init__(self, current: TaskStatus, target: TaskStatus) -> None:
        super().__init__(f"invalid transition: {current.value} -> {target.value}")
        self.current = current
        self.target = target


_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.QUEUED: frozenset({TaskStatus.ASSIGNED}),
    TaskStatus.ASSIGNED: frozenset({TaskStatus.RUNNING, TaskStatus.QUEUED}),
    TaskStatus.RUNNING: frozenset(
        {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.QUEUED}
    ),
    TaskStatus.FAILED: frozenset({TaskStatus.QUEUED}),
    TaskStatus.SUCCEEDED: frozenset(),  # terminal
}


class TaskStateMachine:
    """Validates task transitions and applies them to a task object."""

    def __init__(
        self,
        on_transition: Callable[[TaskStatus, TaskStatus], None] | None = None,
    ) -> None:
        self._on_transition = on_transition

    def can_transition(self, current: TaskStatus, target: TaskStatus) -> bool:
        return target in _TRANSITIONS.get(current, frozenset())

    def transition(self, task, target: TaskStatus) -> TaskStatus:
        """Validate and apply a transition in-place; returns the new status."""
        if not self.can_transition(task.status, target):
            raise InvalidTransition(task.status, target)
        if self._on_transition is not None:
            self._on_transition(task.status, target)
        task.status = target
        return target
