# Multi-Engine Scan Platform

> A vendor-neutral distributed scanning platform designed for **batch file
> analysis across heterogeneous scanning engines** — with **Coordinator**,
> **Agent**, pluggable **Scanner Adapters**, task queue, **retry**, **state
> machine** and **normalized results**.

```
Distributed Task Scheduling · Agent Architecture · Pluggable Adapter ·
Retry & Fault Tolerance · State Machine · REST API · Result Normalization ·
Automated Testing
```

## Problem

Running malware scans at scale with multiple engines (signature, heuristic,
sandbox, internal tools) hits the same wall every time:

- every engine has its own CLI, output format and failure modes;
- engines live on different machines, some are licensed per-host;
- a scan can die half-way and nothing reschedules it;
- results can't be compared because every engine speaks a different schema.

This platform separates the **scheduling/routing** (coordinator) from the
**execution** (agents) from the **engines** (adapters), and normalizes every
outcome into one envelope — so adding a new engine is a day's work, not a
rewrite.

## Architecture

```
Coordinator (FastAPI + SQLite)
    │  REST: claim / start / report / heartbeat
    ▼
Agents × N  ──►  Executor  ──►  ScannerAdapter (mock_engine_a / b / ...)
```

- **Coordinator** — REST API, task queue, state machine, retry budget, and a
  scheduler that reclaims tasks from dead agents.
- **Agent** — pull-based worker: registers, heartbeats, claims tasks, runs them
  through an adapter, reports normalized results. Join any number of agents to a
  running coordinator with zero coordinator-side config.
- **Adapter** — one interface (`scan(task) -> ScanResult`); the mock engines
  demonstrate the contract and the retry path. Real engines plug in the same
  way.

See [docs/architecture.md](docs/architecture.md) for the full design (lifecycle
diagram, failure-handling table, trade-offs).

## Core design

**Task lifecycle** — enforced by a whitelist state machine:

```
queued → assigned → running → succeeded | failed → (retry) → queued
```

**Normalized result** — whatever the engine returns, the platform stores this:

```json
{
  "task_id": "213d0bd2...",
  "sha256": "0badc0de...",
  "engine": "mock_engine_a",
  "status": "succeeded",
  "verdict": "malicious",
  "submitted_at": 1789558313.33,
  "scan_duration_ms": 1320,
  "error": null
}
```

**Fault tolerance**

- engine crash → adapter raises → agent reports `failed` → coordinator requeues
  until `max_retries`;
- agent dies → heartbeat goes silent → scheduler reclaims its tasks;
- coordinator restarts → tasks persist in SQLite, state machine resumes;
- claim race → `BEGIN IMMEDIATE` transaction guarantees one agent per task.

## Quick start

```bash
git clone https://github.com/nulloneamyourfather-droid/multi-engine-scan-platform.git
cd multi-engine-scan-platform
pip install -r requirements.txt
```

**1. Start the coordinator**

```bash
python -m uvicorn coordinator.main:app --host 0.0.0.0 --port 8000
```

Interactive API docs: http://127.0.0.1:8000/docs

**2. Start one or more agents**

```bash
python -m agent.worker --coordinator http://127.0.0.1:8000 --agent-id agent-1
```

**3. Submit a scan task**

```bash
curl -X POST http://127.0.0.1:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"artifact_sha256":"0badc0de0000000000000000000000000000000000000000000000000000000",
       "engines":["mock_engine_a","mock_engine_b"],"max_retries":3}'
```

**4. Watch it complete and read the normalized result**

```bash
curl http://127.0.0.1:8000/tasks
curl http://127.0.0.1:8000/tasks/<task_id>/result
```

### Docker

```bash
docker compose up --build     # coordinator + 2 agents
```

### Tests

```bash
python -m pytest
```

## Demo (real run)

Coordinator + one agent running locally; one artifact submitted to both engines:

```
agent demo-agent-1 registered
task 213d0bd2... -> succeeded verdict=malicious   (mock_engine_a)
task e6645d45... -> succeeded verdict=malicious   (mock_engine_b)
```

Normalized result for `mock_engine_a`:

```json
{
  "task_id": "213d0bd295444cfc836eb920fb1fe019",
  "sha256": "0badc0de0000000000000000000000000000000000000000000000000000000",
  "engine": "mock_engine_a",
  "status": "succeeded",
  "verdict": "malicious",
  "submitted_at": 1789558313.3315306,
  "scan_duration_ms": 0,
  "error": null,
  "details": { "matched_rule": "MockSig.0badc0de" }
}
```

## Test results

```
29 passed, 1 warning in 0.49s
```

Coverage: state machine transitions (valid + invalid), task manager
submit/claim/report/retry/heartbeat-reclaim, adapter verdicts & failure path,
end-to-end API flow (submit → claim → start → report → result).

## Roadmap

- [ ] Consensus verdict endpoint (fuse multiple engines: vote / weighted)
- [ ] Per-engine SLA metrics and accuracy tracking
- [ ] Agent concurrency (thread pool per agent)
- [ ] Real adapters (ClamAV / YARA / VirusTotal API)
- [ ] Auth (coordinator API token) and TLS
