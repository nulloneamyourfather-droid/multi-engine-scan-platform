"""FastAPI coordinator API.

Endpoints:
    POST /tasks                 submit an artifact to one or more engines
    GET  /tasks                 list tasks (optional ?status=)
    GET  /tasks/{id}            task detail
    GET  /tasks/{id}/result     normalized result
    POST /tasks/{id}/start      agent start (assigned -> running)
    POST /tasks/{id}/result     agent result (running -> succeeded|failed)
    POST /agents/register       agent registration
    POST /agents/heartbeat      agent heartbeat
    GET  /agents                list agents
    GET  /engines               registered scanner adapters
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from adapters import create_adapter, list_adapters
from coordinator.scheduler import Scheduler
from coordinator.task_manager import TaskManager
from models.task import ScanResult, ScanTask, ScanVerdict, TaskStatus
from storage.sqlite import SQLiteStore

logger = logging.getLogger(__name__)


class ClaimRequest(BaseModel):
    agent_id: str


class SubmitRequest(BaseModel):
    artifact_sha256: str = Field(min_length=6, max_length=128)
    engines: list[str] = Field(min_length=1)
    priority: int = 0
    max_retries: int = 3


class AgentRegister(BaseModel):
    agent_id: str
    hostname: str = ""


class StartRequest(BaseModel):
    agent_id: str


class ResultRequest(BaseModel):
    agent_id: str
    verdict: ScanVerdict
    error: Optional[str] = None
    details: dict[str, Any] = Field(default_factory=dict)
    scan_duration_ms: int = 0


def build_app(store: SQLiteStore, scheduler_interval: float = 15.0) -> FastAPI:
    tm = TaskManager(store)
    scheduler = Scheduler(tm, interval=scheduler_interval)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await scheduler.start()
        yield
        await scheduler.stop()
        store.close()

    app = FastAPI(
        title="Multi-Engine Scan Platform",
        version="1.0.0",
        description="Vendor-neutral distributed scanning platform: Coordinator, "
        "Agent, pluggable Scanner Adapters, task queue, retry, state machine "
        "and normalized results.",
        lifespan=lifespan,
    )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    # ---- tasks --------------------------------------------------------------

    @app.post("/tasks", status_code=201)
    def submit(req: SubmitRequest) -> dict[str, Any]:
        unknown = [e for e in req.engines if e not in list_adapters()]
        if unknown:
            raise HTTPException(400, f"unknown engines: {unknown}")
        tasks = tm.submit(req.artifact_sha256, req.engines, req.priority, req.max_retries)
        return {"submitted": [t.to_dict() for t in tasks]}

    @app.get("/tasks")
    def list_tasks(status: Optional[str] = None) -> list[dict[str, Any]]:
        st = TaskStatus(status) if status else None
        return [t.to_dict() for t in tm.store.list_tasks(status=st)]

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str) -> dict[str, Any]:
        task = tm.store.get_task(task_id)
        if task is None:
            raise HTTPException(404, "task not found")
        return task.to_dict()

    @app.get("/tasks/{task_id}/result")
    def get_result(task_id: str) -> dict[str, Any]:
        result = tm.store.get_result(task_id)
        if result is None:
            raise HTTPException(404, "no result yet")
        return result.to_dict()

    @app.post("/tasks/{task_id}/start")
    def start_task(task_id: str, req: StartRequest) -> dict[str, Any]:
        task = tm.start(task_id, req.agent_id)
        if task is None:
            raise HTTPException(409, "task not claimable by this agent")
        return task.to_dict()

    @app.post("/tasks/claim", status_code=200)
    def claim_task(req: ClaimRequest) -> dict[str, Any]:
        task = tm.claim(req.agent_id)
        if task is None:
            raise HTTPException(404, "no task available")
        return task.to_dict()

    @app.post("/tasks/{task_id}/result")
    def report_result(task_id: str, req: ResultRequest) -> dict[str, Any]:
        task = tm.store.get_task(task_id)
        if task is None:
            raise HTTPException(404, "task not found")
        result = ScanResult(
            task_id=task_id,
            artifact_sha256=task.artifact_sha256,
            engine=task.engine,
            status=TaskStatus.SUCCEEDED if req.error is None else TaskStatus.FAILED,
            verdict=req.verdict,
            submitted_at=__import__("time").time(),
            scan_duration_ms=req.scan_duration_ms,
            error=req.error,
            details=req.details,
        )
        task = tm.report(task_id, req.agent_id, result)
        if task is None:
            raise HTTPException(409, "result rejected")
        return {"task": task.to_dict(), "result": result.to_dict()}

    # ---- agents ---------------------------------------------------------------

    @app.post("/agents/register")
    def register(req: AgentRegister) -> dict[str, str]:
        store.register_agent(req.agent_id, req.hostname)
        return {"status": "registered"}

    @app.post("/agents/heartbeat")
    def heartbeat(req: AgentRegister) -> dict[str, str]:
        store.heartbeat(req.agent_id)
        return {"status": "ok"}

    @app.get("/agents")
    def list_agents() -> list[dict[str, Any]]:
        return store.list_agents()

    @app.get("/engines")
    def engines() -> list[dict[str, Any]]:
        return [create_adapter(name).describe() for name in list_adapters()]

    app.state.task_manager = tm
    return app
