# Architecture

`multi-engine-scan-platform` is a vendor-neutral distributed scanning platform:
a **Coordinator** schedules scan tasks, **Agents** on any number of machines pull
tasks and run them through pluggable **Scanner Adapters**, and all results are
**normalized** into one schema so downstream aggregation is engine-agnostic.

## Why this shape

Scanning workloads have a property that makes a pull-based, task-queue design
the right fit:

- engines are heterogeneous (a VM sandbox takes minutes, a signature check takes
  milliseconds) and some are licensed to a fixed number of hosts;
- agents must be able to join/leave without the coordinator knowing them in
  advance (horizontal scaling);
- a single scan result is useless without provenance: which engine, how long it
  took, whether it errored.

The pull model keeps the agent/coordinator contract tiny and lets any process
with an adapter become a worker.

## Components

```
┌──────────────────────────────────────────────────────────────────┐
│                        Coordinator                               │
│                                                                  │
│   FastAPI (REST)   ──►   TaskManager   ──►   SQLiteStore         │
│      ▲                        │                                      │
│      │                        ▼                                      │
│      │                   StateMachine  ◄── Scheduler (reclaim stale) │
└──────┼──────────────────────────────────────────────────────────┘
       │ HTTP (poll claim / heartbeat / report)
┌──────▼──────────────────────────────────────────────────────────┐
│                        Agent (x N)                              │
│   register → claim → start → Executor → report                  │
│                                   │                             │
│                                   ▼                             │
│                        ScannerAdapter (pluggable)               │
│                        mock_engine_a / mock_engine_b / ...      │
└──────────────────────────────────────────────────────────────────┘
```

### Coordinator
- **REST API** (`coordinator/api.py`): task submission, task/result queries,
  agent registration & heartbeat, engine metadata.
- **TaskManager** (`coordinator/task_manager.py`): the single authority on task
  lifecycle — claiming, start ack, result finalization, retry budgeting.
- **StateMachine** (`models/state_machine.py`): a whitelist of legal transitions;
  anything else raises `InvalidTransition`. See the lifecycle below.
- **Scheduler** (`coordinator/scheduler.py`): an asyncio loop that reclaims
  tasks whose agent heartbeat has expired — the fault-tolerance path for a
  crashed or wedged agent.

### Agent
- Pull-based worker (`agent/worker.py`): registers once, then loops
  `claim → start → execute → report`, heartbeating in a background thread.
- **Executor** (`agent/executor.py`): engine-agnostic — looks up the adapter by
  name and turns adapter exceptions into retryable `failed` results.

### Scanner Adapters (`adapters/`)
- `base.py` defines the interface: one `scan(task) -> ScanResult`.
- `mock_engine_a` simulates a signature engine (deterministic verdicts; optional
  `fail_rate` to exercise retries).
- `mock_engine_b` simulates a heuristic engine (optional `disagree_rate` to
  model multi-engine disagreement).
- Adding a real engine (ClamAV, YARA, VirusTotal, a custom binary, ...) means
  implementing one class and registering it in `adapters/__init__.py`.

### Storage (`storage/sqlite.py`)
SQLite with a locking wrapper. `claim_next_task` uses `BEGIN IMMEDIATE` so two
agents can never claim the same task — the correctness point of a distributed
queue.

## Task lifecycle

```
queued
  ↓  (coordinator assigns to an agent)
assigned
  ↓  (agent acks start)
running
  ↓  ┌──────────────────────────┐
succeeded                    failed
                              ↓  (attempts < max_retries)
                           queued
```

A failed task with retries remaining returns to `queued` and can be claimed by a
different agent; after `max_retries` it stays `failed`. A task whose agent stops
heartbeating is reclaimed by the scheduler and requeued.

## Result normalization

Every engine returns the same envelope, whatever its internals:

```json
{
  "task_id": "213d0bd2...",
  "sha256": "0badc0de...",
  "engine": "mock_engine_a",
  "status": "succeeded",
  "verdict": "malicious",
  "submitted_at": 1789558313.33,
  "scan_duration_ms": 1320,
  "error": null,
  "details": { "matched_rule": "MockSig.0badc0de" }
}
```

`status` and `verdict` are enums (`TaskStatus`, `ScanVerdict`); `details` is a
free-form engine-specific payload. Aggregation (consensus verdicts, SLA metrics,
per-engine accuracy) can be layered on top without knowing any engine internals.

## Failure handling

| Failure | Detection | Recovery |
|---|---|---|
| Engine raises during scan | Executor catches, emits `failed` result | Coordinator requeues if attempts remain |
| Agent process dies | Heartbeat goes silent | Scheduler reclaims task after timeout |
| Coordinator restart | — | Tasks persist in SQLite; state machine resumes |
| Two agents race a claim | `BEGIN IMMEDIATE` transaction | Only one gets the task |

## Trade-offs / next steps

- **No push transport**: pull-based polling (1s) adds latency vs WebSocket/gRPC;
  acceptable here and much simpler to reason about. Swap `claim` for a stream if
  latency becomes a concern.
- **Single coordinator**: the store is the bottleneck and a SPOF; the design
  keeps it swappable for Postgres if you need HA.
- **Concurrency**: each agent runs one task at a time; a per-agent thread pool
  is the natural next increment.
- **Aggregation layer**: a `/tasks/{id}/verdict` endpoint that fuses multiple
  engine results (vote, weighted, first-disagreement) is the obvious next
  feature.
