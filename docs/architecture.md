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
- **REST API** (`coordinator/api.py`): task submission, task/result/attempt
  queries, agent registration (with capabilities) & heartbeat, engine metadata.
- **TaskManager** (`coordinator/task_manager.py`): the single authority on task
  lifecycle — capability-aware claiming, lease start ack, idempotent result
  finalization, retry budgeting, stale reclaim.
- **StateMachine** (`models/state_machine.py`): a whitelist of legal transitions;
  anything else raises `InvalidTransition`. Every state change passes through it;
  Storage never mutates status directly.
- **Scheduler** (`coordinator/scheduler.py`): an asyncio loop that reclaims
  stale tasks (heartbeat lost **or** execution lease timed out) — the
  fault-tolerance path for a crashed or wedged agent.

### Agent
- Pull-based worker (`agent/worker.py`): registers once (advertising its
  `capabilities`), then loops `claim → start → execute → report`, heartbeating
  in a background thread.
- **Lease start ack**: `start` returning `409` means the coordinator has
  reclaimed the task (lease expired); the agent then **must not execute**, to
  avoid two workers scanning the same artifact.
- **Result report retry**: results are reported with exponential backoff, and
  the coordinator's result endpoint is idempotent — so a transient network blip
  cannot strand a finished task in `running` (at-least-once + idempotency ⇒
  logically exactly-once results).
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
queue. Storage is deliberately dumb: it only persists; it never decides state
transitions.

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
different agent; after `max_retries` it stays `failed`.

### Leases & execution timeout

Claiming gives an agent an exclusive **lease** on a task. The lease expires when:

- the agent's heartbeat goes silent (agent crashed), or
- the task stays `RUNNING` longer than `execution_timeout_s` (the engine hung).

Either way the scheduler reclaims the task. A reclaim **consumes one retry
budget**, so a repeatedly-crashing agent cannot reschedule the same task
forever. A reclaimed task is also un-leased: the old agent's `start` gets a
`409` and must not run, and its late `report` is rejected.

### Task vs attempt

A task is the **business lifecycle** of a scan request; each actual execution
on an agent is a **`ScanAttempt`** (task_id + attempt_no + agent_id + status +
timing + error + verdict). Retries keep their history:

```
attempt 1: agent-1  timeout       (reclaimed by scheduler)
attempt 2: agent-2  engine error  (reported failed)
attempt 3: agent-2  succeeded     (final)
```

`GET /tasks/{id}/attempts` exposes the full history; the task row stores only
the aggregate status.

### Heterogeneous nodes & capability-aware scheduling

Agents register the engines they can actually run:

```json
{ "agent_id": "agent-win-01", "capabilities": ["mock_engine_a"] }
{ "agent_id": "agent-linux-01", "capabilities": ["mock_engine_b"] }
```

Claiming filters by capability (`WHERE engine IN (...)`), so an engine-B-only
agent never gets an engine-A task. This is what makes the platform genuinely
"heterogeneous" rather than a single-engine demo.

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
| Agent process dies | Heartbeat goes silent | Scheduler reclaims task after timeout; reclaim consumes retry budget |
| Agent wedged / engine hangs | Task `RUNNING` past `execution_timeout_s` | Scheduler reclaims (lease expiry) |
| Result report lost (network blip) | Report HTTP error | Agent retries with backoff; endpoint is idempotent |
| Stale worker double-executes | Task reclaimed, lease revoked | `start` → `409` (must not run); late `report` rejected |
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
