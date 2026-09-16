"""End-to-end API tests: submit -> claim -> start -> report -> result."""
from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from coordinator.api import build_app
from models.task import ScanVerdict
from storage.sqlite import SQLiteStore


@pytest.fixture
def client():
    store = SQLiteStore(":memory:")
    app = build_app(store, scheduler_interval=9999)  # disable scheduler ticking
    with TestClient(app) as c:
        yield c


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_engines_listed(client):
    engines = client.get("/engines").json()
    names = {e["name"] for e in engines}
    assert names == {"mock_engine_a", "mock_engine_b"}


def test_submit_and_query(client):
    resp = client.post(
        "/tasks",
        json={"artifact_sha256": "a" * 64, "engines": ["mock_engine_a", "mock_engine_b"]},
    )
    assert resp.status_code == 201
    submitted = resp.json()["submitted"]
    assert len(submitted) == 2
    task_id = submitted[0]["task_id"]

    detail = client.get(f"/tasks/{task_id}").json()
    assert detail["status"] == "queued"

    tasks = client.get("/tasks").json()
    assert len(tasks) == 2
    queued = client.get("/tasks?status=queued").json()
    assert len(queued) == 2


def test_submit_unknown_engine_rejected(client):
    resp = client.post(
        "/tasks",
        json={"artifact_sha256": "a" * 64, "engines": ["nope"]},
    )
    assert resp.status_code == 400


def test_full_agent_flow(client):
    # agent registers
    assert client.post("/agents/register", json={"agent_id": "agent-1", "hostname": "h1"}).status_code == 200

    # submit one task
    (task,) = client.post(
        "/tasks", json={"artifact_sha256": "a" * 64, "engines": ["mock_engine_a"]}
    ).json()["submitted"]
    task_id = task["task_id"]

    # claim
    claim = client.post("/tasks/claim", json={"agent_id": "agent-1"}).json()
    assert claim["task_id"] == task_id
    assert claim["status"] == "assigned"
    assert claim["agent_id"] == "agent-1"

    # start
    start = client.post(f"/tasks/{task_id}/start", json={"agent_id": "agent-1"}).json()
    assert start["status"] == "running"

    # report result
    resp = client.post(
        f"/tasks/{task_id}/result",
        json={
            "agent_id": "agent-1",
            "verdict": ScanVerdict.BENIGN.value,
            "scan_duration_ms": 7,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["task"]["status"] == "succeeded"

    # normalized result is retrievable
    result = client.get(f"/tasks/{task_id}/result").json()
    assert result["sha256"] == "a" * 64
    assert result["engine"] == "mock_engine_a"
    assert result["status"] == "succeeded"
    assert result["verdict"] == "benign"
    assert result["scan_duration_ms"] == 7


def test_agent_heartbeat_and_listing(client):
    client.post("/agents/register", json={"agent_id": "agent-1", "hostname": "h1"})
    client.post("/agents/heartbeat", json={"agent_id": "agent-1"})
    agents = client.get("/agents").json()
    assert len(agents) == 1
    assert agents[0]["status"] == "online"
