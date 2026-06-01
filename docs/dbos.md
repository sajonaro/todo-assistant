# DBOS concepts in this codebase

[DBOS](https://docs.dbos.dev) is a **durable execution** framework. We write
ordinary Python functions; DBOS persists their progress to a database so that if
the process crashes or restarts mid-execution, each function resumes exactly where
it left off — without re-running already-completed steps. This app uses it to drive
long-lived "nudge" workflows that wait hours between deadline reminders and survive
restarts.

There are two databases (see `app/main.py:90`):

- **Application database** (`APP_DATABASE_URL`) — the app's data: `todo.tasks`.
- **System database** (`DBOS_SYSTEM_DATABASE_URL`) — DBOS's bookkeeping: workflow
  state, step results, pending messages. It is never written to directly.

---

## 1. `@ds.transaction()` — atomic database functions

`ds` is a SQLAlchemy datasource (`app/db.py:8`). Decorating a function with
`@ds.transaction()` makes DBOS run it inside a single database transaction:

```python
# app/db.py
ds = SQLAlchemyDatasource.create(os.environ["APP_DATABASE_URL"])

@ds.transaction()
def insert_task(id: str, text: str, horizon: str, deadline: datetime) -> None:
    ds.sql_session().execute(
        sa_text("INSERT INTO todo.tasks (id, text, horizon, deadline) "
                "VALUES (:id, :text, :horizon, :deadline)"),
        {"id": id, "text": text, "horizon": horizon, "deadline": deadline},
    )
```

The decorator:

- opens a transaction and exposes its session via `ds.sql_session()`,
- **commits** when the function returns, **rolls back** if it raises,
- records the result so a workflow retry re-uses it instead of re-executing.

No explicit `commit()` / `rollback()` is needed. Every DB accessor in
`app/db.py` (`get_task`, `set_status`, `list_tasks`, …) follows this pattern.

> Note: `apply_migration()` (`app/db.py:17`) deliberately does **not** use this
> decorator — it runs before `DBOS.launch()`, so it uses a throwaway plain
> SQLAlchemy engine instead.

---

## 2. `@DBOS.step()` — a retriable unit of work

A **step** is a function whose result DBOS records once it completes. If the
surrounding workflow is interrupted and resumed, a completed step is **not**
re-run — its saved result is returned. Steps are for side effects (I/O, network,
notifications) that should happen *exactly once*.

```python
# app/notifications.py
@DBOS.step()
def send_notification(task_id: str, message: str, urgent: bool = False) -> None:
    tag = "URGENT" if urgent else "NUDGE"
    print(f"[{tag}] task={task_id} :: {message}", flush=True)
```

Because this is a step, a notification won't be sent twice if the workflow recovers
after sending it.

---

## 3. `@DBOS.workflow()` — durable orchestration

A **workflow** orchestrates steps and transactions. DBOS checkpoints its progress,
so it can sleep for hours, survive a restart, and continue. `nudge_workflow`
(`app/workflows.py:24`) is the heart of the app — it loops through deadline phases:

```python
@DBOS.workflow()
def nudge_workflow(task_id: str) -> None:
    while True:
        task = db.get_task(task_id)                 # transaction
        if task is None or task["status"] != "pending":
            return
        deadline = task["deadline"]

        # durable, interruptible wait until T-1h
        delay = max(0.0, (deadline - timedelta(hours=1) - db.utcnow()).total_seconds())
        event = DBOS.recv(ACTION_TOPIC, timeout_seconds=delay)
        if event is not None:
            ...
        send_notification(task_id, "Heads up: deadline in 1h")   # step
        ...
```

Even though this function may "live" for days, its position is durable — a crash
and restart resumes the same workflow at the same point.

> The free names here are not ad-hoc globals: `db` is a module import (`import
> app.db as db`), `send_notification` is an imported function, and `ACTION_TOPIC` is
> a module constant — calling `db.get_task(...)` is just "call another module's
> function". The one true process-wide singleton is the DBOS runtime itself (and the
> datasource `ds` behind `db`), which `@DBOS.workflow` and `DBOS.recv` bind to
> implicitly. That singleton model is inherent to DBOS — it's the reason the suite
> tests against a real Postgres rather than an injected fake.

---

## 4. Durable messaging — `DBOS.recv()` / `DBOS.send_async()` / topics

A running workflow can **wait for external events** durably. The workflow blocks on
`DBOS.recv(topic, timeout_seconds=…)`; another part of the app (an HTTP route)
delivers an event with `DBOS.send_async(workflow_id, payload, topic=…)`. The
message is persisted, so it's delivered even across restarts.

```python
# workflow side — app/workflows.py
ACTION_TOPIC = "action"
event = DBOS.recv(ACTION_TOPIC, timeout_seconds=delay)   # wakes on event OR timeout

# sender side — app/main.py:57
await DBOS.send_async(task["workflow_id"], payload, topic=ACTION_TOPIC)
```

This is how `POST /tasks/{id}/done|drop|snooze` reach the running nudge loop. Note
the async variant: DBOS forbids the sync `send()` inside FastAPI's event loop
(`app/main.py:63`).

---

## 5. `SetWorkflowID` + `start_workflow` — idempotent launches

Every workflow has an ID. Choosing a **deterministic** ID makes starting the "same"
workflow twice a no-op — DBOS sees the ID already exists and won't spawn a
duplicate. The task-creation route builds the ID from the task ID:

```python
# app/main.py:44
wf_id = f"nudge-{task_id}"
with SetWorkflowID(wf_id):
    DBOS.start_workflow(nudge_workflow, task_id)   # idempotent via deterministic id
db.set_workflow_id(task_id, wf_id)
```

`start_workflow` launches the workflow in the background (the HTTP request returns
immediately while the nudge loop runs durably).

---

## 6. `@DBOS.scheduled()` — cron workflows

Stacking `@DBOS.scheduled(cron)` on top of `@DBOS.workflow()` runs it on a schedule.
DBOS guarantees each scheduled slot fires once (and backfills missed slots after
downtime). The function receives the scheduled and actual times:

```python
# app/workflows.py:70
@DBOS.scheduled("0 8 * * *")   # daily 8 AM
@DBOS.workflow()
def daily_morning(scheduled_time: datetime, actual_time: datetime) -> None:
    ...
```

The app has three: `daily_morning`, `weekly_review`, `monthly_checkin`.

---

## 7. Initialization & registration

DBOS must be configured and launched before workflows run (`app/main.py:90`):

```python
config: DBOSConfig = {
    "name": "todo-assistant",
    "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
    "application_database_url": os.environ["APP_DATABASE_URL"],
}
db.apply_migration()   # plain SQLAlchemy, runs before launch
DBOS(config=config)
DBOS.launch()          # recovers in-flight workflows from the system DB
```

Workflows and steps are registered simply by **importing** the module that defines
them — that's why `app/main.py:16` does `import app.workflows  # noqa: F401`. At
`DBOS.launch()`, any workflows that were mid-flight when the process last died are
automatically recovered and resumed.

---

## 8. How DBOS compares to other durable/orchestration options

The problem DBOS solves — *keep a multi-step process alive across crashes and long
waits* — is also addressed by AWS Step Functions, Temporal, and (partially) plain
task queues like Celery. They differ mainly in **where the orchestration logic
lives** and **what infrastructure must be operated**.

| | **DBOS** (this app) | **AWS Step Functions** | **Temporal** | **Celery + Redis/RabbitMQ** |
|---|---|---|---|---|
| Workflow definition | Plain Python in-process | JSON/ASL state machine + Lambdas | Code-first (worker SDKs) | Tasks + hand-rolled glue |
| Durable state store | Postgres (the app's own DB) | Managed (AWS-internal) | Dedicated Temporal cluster + DB | None built-in (queue only) |
| Extra infra to run | None — it's a library | None (fully managed, AWS-only) | Server cluster + workers | Broker + result backend + workers |
| Durable multi-step resume | Yes | Yes | Yes | No (manual) |
| App-DB transactions in the flow | **Same transaction/DB** | Separate calls (no shared txn) | Separate (activities) | Separate |
| Local dev / testing | Run the process + Postgres | Emulators / cloud round-trips | Run a local cluster | Run broker + workers |
| Lock-in | Postgres | AWS | Temporal | Broker-specific |
| Latency per step | In-process function call | Per-transition (network + billing) | Worker RPC round-trip | Queue hop |

### Why DBOS "goes the extra mile"

- **One codebase, one mental model.** The nudge loop reads like ordinary Python
  (`app/workflows.py:24`) — control flow, loops, and waits are just code. Step
  Functions splits the same logic across an ASL state-machine document *and* a set
  of Lambdas; the orchestration lives in a different language and tool than the
  steps it calls.
- **Workflow state shares the app's database.** Transactions
  (`@ds.transaction()`) and workflow checkpoints both live in Postgres, so a step's
  data write and its "this step completed" record can be consistent. Step Functions
  and Temporal keep orchestration state in a *separate* system from the business
  database, so exactly-once-against-the-data requires extra care.
- **No orchestrator to operate.** DBOS is a library inside the existing process —
  there is no Temporal cluster to run/upgrade and no AWS-managed service to wire up.
  For this app, "add DBOS" meant adding a dependency and a second Postgres database,
  not standing up new infrastructure.
- **Trivial local development.** The whole stack is `docker compose up` (Postgres +
  the app). There is no local cluster, no broker, and no cloud round-trips to
  exercise a workflow end-to-end.

### Where the others win (the "when scale/volume permits" caveat)

DBOS's durability is bounded by **Postgres throughput** and the **single-process
model** — every workflow step is a database write, and workflows run inside the app
process. That is the right trade for low-to-moderate volume (this app: a handful of
nudges per user, mostly *sleeping*). Past that ceiling, the dedicated systems pull
ahead:

- **AWS Step Functions** scales transitions elastically with no servers to manage,
  and integrates natively with the broader AWS event ecosystem; *Express* workflows
  handle very high-volume, short-lived executions cheaply.
- **Temporal** is built for very large fleets of concurrent workflows, scales
  workers independently of the orchestration store, and offers mature
  multi-language SDKs and tooling.
- **Celery** remains the simplest fit when the need is fan-out task throughput
  rather than durable, resumable *workflows*.

In short: when the volume fits comfortably on a Postgres instance, DBOS delivers the
same durability guarantees with far less moving infrastructure and far simpler code.
When volume outgrows a single database, a purpose-built orchestrator earns its
operational cost.

---

## Mental model

| Decorator | Unit | Guarantee |
|---|---|---|
| `@ds.transaction()` | DB transaction | atomic commit/rollback; result memoized on retry |
| `@DBOS.step()` | side effect | runs **exactly once**; result memoized |
| `@DBOS.workflow()` | orchestration | **durable** — survives crashes, resumes in place |
| `@DBOS.scheduled(cron)` | timer | fires per cron slot, backfills missed slots |
| `DBOS.recv` / `send_async` | messaging | durable, cross-process event delivery |
