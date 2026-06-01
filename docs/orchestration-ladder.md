# The Workflow Orchestration Ladder

Six rungs, lowest to highest. Use the lowest that honestly solves your problem.

## Table of contents

- [The table](#the-table)
- [What "higher" actually means](#what-higher-actually-means)
- Tier-by-tier details:
  - [Level 0 — Sync function calls](#level-0--sync-function-calls)
  - [Level 1 — Job queue](#level-1--job-queue)
  - [Level 2 — Cluster-aware scheduler](#level-2--cluster-aware-scheduler)
  - [Level 3 — DAG orchestrator](#level-3--dag-orchestrator)
  - [Level 3.5 — Stream processing](#level-35--stream-processing)
  - [Level 4 — Durable execution engine](#level-4--durable-execution-engine)
  - [Level 5 — Distributed transactions](#level-5--distributed-transactions)
- [DAG runs vs workflow instances: the deepest distinction in the ladder](#dag-runs-vs-workflow-instances-the-deepest-distinction-in-the-ladder)
  - [Two mental models](#two-mental-models)
  - [Where this shows up in code](#where-this-shows-up-in-code)
  - [Practical consequences](#practical-consequences)
  - [Both can be "dynamic" — but in different ways](#both-can-be-dynamic--but-in-different-ways)
  - [Demonstration: workflows subsume DAGs](#demonstration-workflows-subsume-dags)
  - [The formal claim](#the-formal-claim)
  - [Why this doesn't make Level 3 obsolete](#why-this-doesnt-make-level-3-obsolete)
  - [Why this is the deepest distinction](#why-this-is-the-deepest-distinction)
  - [How to tell which one you actually have](#how-to-tell-which-one-you-actually-have)
- [Higher tiers can simulate lower ones — but usually shouldn't](#higher-tiers-can-simulate-lower-ones--but-usually-shouldnt)
  - [Worked example: DAG on streams](#worked-example-dag-on-streams)
  - [What you've actually built](#what-youve-actually-built)
  - [The cost-benefit pattern](#the-cost-benefit-pattern)
  - [The general principle](#the-general-principle)
- [Orchestration vs choreography: the other axis](#orchestration-vs-choreography-the-other-axis)
- [Picking a rung](#picking-a-rung)
- [Glossary](#glossary)

---

## The table

| Tier | Sub | What it is | What it adds vs previous | Example tools | Where it breaks |
|---|---|---|---|---|---|
| **0 — Synchronous** | | Sync function calls | — (baseline); exactly-once on success, lost on failure | regular code | head-of-line blocking, no crash recovery, p99 tail latency, no horizontal scale |
| **1 — Background jobs** | 1a | In-memory background work | caller no longer blocks; **at-most-once** (lost on process death) | `asyncio.create_task`, threadpool, fire-and-forget | lost on process death, no visibility, no retry |
| | 1b | Durable job queue | work survives crashes; retries; **at-least-once delivery** (idempotency = your problem) | Sidekiq, Celery, Hangfire, BullMQ, Solid Queue, Kafka (as queue), RabbitMQ, SQS | at-least-once duplicates, no multi-step coordination, lost context on retry, poison messages, no DAG semantics, idempotency burden on caller |
| **2 — Scheduling** | 2a | Single-node scheduler | recurring execution (cron); still at-least-once with **double-fire risk across replicas** | cron, Spring `@Scheduled`, APScheduler | double-execution across replicas, no failover, single point of scheduling |
| | 2b | Cluster-aware scheduler | **exactly-once *firing*** of scheduled jobs across N replicas; failover; priorities (per-job execution still at-least-once) | Quartz, Oban, Hatchet, ShedLock, pg_cron | clock skew on missed-fire detection, thundering herd on cron tick, stuck locks after worker death, no workflow-level state, hard to express step dependencies |
| **3 — DAG orchestration** | 3a | Static DAG orchestrator | dependency graph; parallelism; lineage; backfill; **exactly-once DAG-run scheduling**, per-task at-least-once with retries | Airflow, Luigi, classic Argo, Azure Data Factory, Fabric Data Pipelines | DAG structure baked at parse time, versioning hell on long-running DAGs, scheduler SPOF, heavy operational footprint, batch-oriented mental model, executor tier becomes the bottleneck, task-level idempotency still your problem |
| | 3b | Dynamic / asset-aware DAG orchestrator | runtime-defined graph; asset/data awareness; partial reruns; same delivery semantics as 3a | Prefect, Dagster, pgflow, modern Argo | better than 3a but still no native long-running stateful workflows, real-time fit is awkward, asset-graph paradigm has its own learning curve, lineage tracking adds overhead |
| **3.5 — Stream processing** | | Continuous queries over unbounded event streams | unbounded data; sub-second latency; windowing & joins; **exactly-once via offset commits + transactional sinks** (Kafka transactions, Flink checkpoints) | Kafka Streams, ksqlDB, Flink, Spark Structured Streaming, Materialize, RisingWave | requires Kafka-shaped event infrastructure, stateful operators need careful checkpointing, late-arriving data and watermark semantics are tricky, no human gates, awkward for request/response, exactly-once only holds if sink is transactional |
| **4 — Durable execution** | 4a | Separate state store | code-as-workflow; per-entity long-lived instances; deterministic replay; **effectively-once per step** via record-and-replay (not atomic with business writes) | Temporal, Restate, Step Functions, Durable Functions, Inngest | no transactional coupling — race between business write and checkpoint requires outbox or idempotency keys, non-determinism breaks replay, versioning hell, history bloat, vendor lock-in, separate cluster to operate |
| | 4b | Transactionally coupled to app DB | **truly exactly-once per `@transaction` step** (business write + checkpoint commit atomically); SQL-queryable workflow state; no separate cluster | DBOS (currently the only one) | younger ecosystem, smaller SDK surface than Temporal, Postgres becomes hot path under high throughput, same determinism constraint as 4a; external (non-DB) side effects still need idempotency keys |
| **5 — Distributed transactions** | 5a | True atomicity (2PC / XA) | real ACID across systems; **exactly-once across participants** if all complete the protocol | XA-aware JMS, some legacy financial systems | coordinator SPOF, blocking on prepare, heuristic outcomes (exactly-once breaks when coordinator dies), doesn't compose over HTTP, mostly avoided in modern stacks |
| | 5b-orch | **Orchestrated** saga | central coordinator drives the saga; calls each participant, handles failures, triggers compensation | Seata, dtm, Temporal/DBOS workflows acting as saga coordinator | coordinator is a SPOF (mitigated by the underlying durable execution); centralization can become a bottleneck; participants are tightly coupled to the orchestrator's contract |
| | 5b-chor | **Choreographed** saga | no central coordinator; each participant listens for events and emits new ones; compensation events flow the same way | Eventuate Tram, Axon, hand-rolled on Kafka/event bus | hard to reason about end-to-end flow ("where's this saga right now?"), implicit dependencies emerge as services subscribe to events, debugging requires reconstructing the choreography from logs, cycles are easy to introduce by accident |
| | 5c | Transactional outbox / log-driven | local ACID + reliable async propagation; **effectively-once event delivery** (downstream still needs idempotency) | Debezium + Kafka, DBOS's coupling, hand-rolled outbox | eventual consistency only, dual-write trap if outbox isn't atomic with business write, downstream consumer lag, ordering across partitions |

---

## What "higher" actually means

The ladder is a **partial order on vectors**, not a total order. Each tier is a point in a multidimensional space; a tier is "higher" than another only when it dominates on every relevant dimension. Some tiers don't compare cleanly — they're incomparable in the formal sense, and we cluster them at adjacent labels for convenience.

### The dimensions

Each dimension has its own ordering relation. Climbing higher = larger value on that dimension.

| Dimension | Ordering (low → high) |
|---|---|
| **Compositional structure** | one step → declared graph → arbitrary code |
| **Statefulness across steps** | none → per-task transient → per-instance durable |
| **Temporal expressiveness** | immediate → scheduled → durable sleep / wait-for-event |
| **Cycle expressiveness** | acyclic only → bounded loops → unbounded loops & recursion |
| **Crash persistence** | nothing survives → queue survives → workflow state survives |
| **Delivery semantics** | at-most-once → at-least-once → effectively-once → exactly-once-with-DB |
| **Coordination scope** | single process → single system → multi-system |
| **Turing-completeness** | not (tiers 0–3) → yes (tiers 3.5+) |

The T-C boundary sits at tier 3.5. Specifically:

- **Tiers 0, 1, 2, 5a:** ✓ via host language. Each task is arbitrary code; the orchestration just runs single tasks.
- **Tiers 3a, 3b:** ✗ at the orchestration layer. Tasks inside can be T-C code, but the DAG topology itself is bounded and acyclic.
- **Tiers 3.5, 4a, 4b, 5b, 5c:** ✓ at the orchestration layer itself. Loops, recursion, arbitrary control flow.

### Where each tier sits

| Tier | Composition | State | Temporal | Cycles | Persistence | Delivery | Scope |
|---|---|---|---|---|---|---|---|
| 0 | one step | none | immediate | n/a | none | exactly-once-on-success | single proc |
| 1a | one step | none | deferred | n/a | none | at-most-once | single proc |
| 1b | one step | none | deferred | n/a | queue | at-least-once | single system |
| 2a | one step | none | scheduled | n/a | queue | at-least-once (double-fire) | single proc |
| 2b | one step | none | scheduled | n/a | queue + lock | exactly-once *fire* | cluster |
| 3a | static graph | per-task transient | scheduled batch | acyclic only | DAG-run state | exactly-once *schedule*; per-task at-least-once | cluster |
| 3b | dynamic graph | per-task transient | scheduled batch | acyclic only | DAG-run state | same as 3a | cluster |
| 3.5 | streaming graph | stateful operators | continuous | acyclic topology, but processors loop | checkpointed state | effectively-once via offsets | cluster |
| 4a | arbitrary code | per-instance durable | sleep/wait for days | unbounded | workflow log | effectively-once per step | cluster |
| 4b | arbitrary code | per-instance durable + business-DB-coupled | sleep/wait for days | unbounded | workflow log + business writes atomic | exactly-once per `@transaction` | cluster |
| 5a | coordinated multi-system | participant locks | bounded by prepare timeout | n/a | log per participant | exactly-once when protocol completes | multi-system |
| 5b | coordinated multi-system | per-saga durable (on top of 4) | as 4 | as 4 | as 4 | effectively-once with compensation | multi-system |
| 5c | local-state + propagation | local durable + outbox | as 4 | as 4 | as 4 | effectively-once propagation | multi-system |

### The partial-order claim

A tier A is *strictly higher* than tier B only if A dominates B on every dimension. Most adjacent labels in the ladder satisfy this. Examples that do:

- 1b > 1a — durable queue dominates in-memory on persistence, every other dimension equal.
- 2b > 2a — cluster lock adds coordination-scope and delivery, others equal.
- 4b > 4a — adds business-DB coupling on state and delivery dimensions, others equal.

Examples that **don't strictly dominate** — these are honest incomparabilities:

- **3.5 vs 4** — streams beat workflows on continuous temporal model and throughput; workflows beat streams on cycle expressiveness, per-entity statefulness, and human-gate ergonomics. Neither strictly dominates. We labeled streams "3.5" and workflows "4" by convention; the ordering reflects "workflows feel more general for business processes," but you could defend ordering them the other way for stream-shaped problems.
- **3a vs 3b** — they differ only on composition (static vs dynamic graph). 3b dominates trivially, but only on one dimension — they're peers in everything else.
- **5a vs 5b vs 5c** — three different tradeoffs in the distributed-transaction space. 5a has true atomicity but blocks; 5b composes over HTTP but admits intermediate states; 5c is eventually-consistent. None dominates the others.

### Why we present it as a linear ladder anyway

Three reasons:

1. **For most engineering decisions, the dominant dimensions are composition, persistence, and cycle expressiveness — and those *do* increase mostly monotonically up the ladder.** The incomparabilities are at the edges.
2. **The decision flowchart needs a tree, not a lattice.** Presenting the full partial order as a Hasse diagram would be more honest but less actionable.
3. **The historical evolution of tooling followed roughly this order.** Engineers built queues, then schedulers, then DAG runners, then streaming engines, then durable execution. The labels track the genealogy.

If you ever feel like two tiers are "the same level" — they probably are, on the dimensions you care about. Pick by which dimension matters for *your* problem, not by ladder position.

---

## Level 0 — Sync function calls

**What:** Regular function calls. Caller blocks, gets result.

**Properties:** No durability beyond exception handling. Single process. Fast.

**Use for:** Fast operations (<100ms) where the caller can wait. Form validation, simple lookups.

---

## Level 1 — Job queue

**What:** Caller publishes job to durable queue. Worker picks it up later. Caller returns immediately.

**Properties:** Single-step work units. Survives crashes (queue is durable). Retries with backoff. Idempotency is your problem. No multi-step coordination.

**Use for:** Atomic background work. Sending emails, resizing images, generating PDFs, single-record syncs.

**Tools:** Sidekiq, Celery, Hangfire, BullMQ, Solid Queue.

---

## Level 2 — Cluster-aware scheduler

**What:** Job queue + cron + cluster-wide deduplication.

**Properties:** Recurring jobs run exactly once across N replicas. Priorities, throttling, delayed jobs. Still one-job-at-a-time mentally. Visibility dashboard usually included.

**Use for:** Scheduled work in multi-replica deployments. Nightly cleanup, hourly syncs, rate-limited outbound traffic.

**Tools:** Quartz, Oban, Hatchet, ShedLock + Spring `@Scheduled`, pg_cron.

---

## Level 3 — DAG orchestrator

**What:** Workflows as directed acyclic graphs. Nodes are tasks, edges are dependencies. Runtime executes in topological order, parallel where possible.

**Properties:** Strong batch/ETL fit. Native parallelism, lineage, and backfill. Modern DAG engines (Airflow 2.3+, Prefect, Dagster) support branching, sensors/event waits, deferrable long waits, dynamic task mapping, and (Airflow 3.1+) first-class human-in-the-loop operators. The real Level-3/4 line isn't features — it's mental model: DAGs run on a *schedule* and execute *DAG runs* (time-bound batches), not millions of long-lived per-entity workflow instances.

**Use for:** Batch data pipelines, ML training, document processing fan-out, AI agent pipelines with parallel tool calls. Anything where the unit of work is "a run of this DAG."

**Tools:** Airflow, Prefect, Dagster, Luigi, Argo, pgflow, Azure Data Factory, Fabric Data Pipelines.

---

## Level 3.5 — Stream processing

**What:** Continuous queries over unbounded event streams. Sibling to Level 3 — DAG is to *batch* what stream processing is to *continuous*.

**Properties:** Sub-second latency. Stateful operators (running aggregations, joins, windows) with checkpointing. Exactly-once via transactional sinks. Replaying history just means rewinding consumer offsets. Requires Kafka-shaped event infrastructure. Awkward for request/response and human gates.

**Use for:** Real-time analytics, fraud detection, CDC pipelines, IoT telemetry, materialized views over event streams.

**Tools:** Kafka Streams, ksqlDB, Flink, Spark Structured Streaming, Materialize, RisingWave.

---

## Level 4 — Durable execution engine

**What:** Workflows are code that survives crashes, restarts, and redeploys. Runtime checkpoints state; on resume, completed steps return cached results.

**Properties:** Any control flow (loops, branches, recursion). Native `wait for event` and `sleep for days`. Idempotent steps. Some engines (DBOS) couple checkpoints to business writes transactionally. Determinism constraint on workflow body.

**Use for:** Multi-step business processes with branching, waits, human gates. Order processing, approval flows, agent loops, multi-day onboarding, anything stateful that lives longer than one request.

**Tools:** DBOS, Temporal, Restate, Azure Durable Functions, AWS Step Functions, Inngest.

---

## Level 5 — Distributed transactions

**What:** True or simulated cross-system atomicity.

**Three families:**
- **2PC/XA:** Real atomicity, fragile in practice, mostly legacy.
- **Sagas:** Compensation-based. Not truly atomic, but composes. Usually built on top of Level 4.
- **Transactional outbox:** Local ACID + reliable propagation. The modern pragmatic default. DBOS's transactional coupling is this.

**Properties:** Pick one of {true atomicity, availability, composability} — you don't get all three.

**Use for:** Cross-system "all or nothing" guarantees when money, regulated state, or irreversible actions are involved.

---

## DAG runs vs workflow instances: the deepest distinction in the ladder

This is the line between Level 3 and Level 4, and it confuses experienced engineers more than any other distinction in the ladder. Two systems can both have branching, retries, parallel fan-out, human gates, and durable state — and still belong on different rungs because they answer "what is being executed?" differently.

### Two mental models

**DAG run model (Level 3):** the unit of execution is *one execution of the DAG, tied to a logical time window*. The DAG is the noun. A "run" is the verb. Each run has a logical date — `2026-04-23 02:00 UTC` — and processes whatever data belongs to that window.

A DAG named `process_orders` running nightly produces 365 DAG runs per year. If you have a million orders flowing through it, that's still 365 runs, each iterating over the day's worth of orders internally.

**Workflow instance model (Level 4):** the unit of execution is *one workflow per business entity, living as long as the entity needs it to*. The workflow is the noun. An "instance" is a specific entity's lifecycle: this order, this customer onboarding, this approval.

A workflow named `order_lifecycle` doesn't have "runs." It has instances — `wf_01HZ...` is *this specific order*, started when the order was placed, ending when the order is closed weeks later. A million orders = a million instances, each potentially in a different phase.

### Where this shows up in code

**DAG model — Airflow:**
```python
@dag(schedule="0 2 * * *", start_date=...)
def nightly_order_processing():
    pending = fetch_pending_orders()      # all of them, for this DAG run
    validate.expand(order=pending)        # dynamic mapping over the batch
    charge.expand(order=validate.output)
    fulfill.expand(order=charge.output)
```
One DAG runs nightly. It processes "today's pending orders" as a batch.

**Workflow instance model — DBOS:**
```python
@DBOS.workflow()
def order_lifecycle(order_id: str):
    validate(order_id)
    payment = charge(order_id)
    if payment.requires_3ds:
        approval = DBOS.recv(timeout=86400)
        if not approval:
            refund(payment)
            return
    fulfillment = fulfill(order_id)
    delivery = DBOS.recv(timeout=86400 * 14)  # wait up to 2 weeks
    close(order_id, delivery)
```
One workflow per order. Lives until that order closes — could be 2 minutes, could be 2 weeks. A million orders means a million live workflow instances, each at its own phase.

### Practical consequences

| Concern | DAG runs | Workflow instances |
|---|---|---|
| **Identity** | (DAG name, logical date) | (workflow name, UUID per business entity) |
| **Concurrency at scale** | Tens of DAG runs in flight | Millions of workflow instances in flight |
| **Lifetime** | Minutes to hours per run | Seconds to months per instance |
| **State scope** | Per-run, mostly transient | Per-instance, persists for the entity's lifetime |
| **Failure unit** | A DAG run fails — investigate that run | An instance fails — investigate that entity |
| **Backfill** | Re-run the DAG for past dates | Doesn't make sense — instances are entity-bound, not time-bound |
| **"It's stuck"** | "The 02:00 run hasn't finished" | "Customer #4527's onboarding has been waiting for approval for 3 days" |
| **Versioning** | Old runs done; new code applies to next run | Old instances still running old code — versioning hell |

### Both can be "dynamic" — but in different ways

A common confusion: "DAGs are static, workflows are dynamic." Wrong. Both are dynamic. They're dynamic in different ways, and the difference matters.

Three flavors of dynamism, often conflated:

1. **Dynamic parameterization** — same shape, runtime-supplied inputs.
2. **Dynamic structure** — the shape of the work is determined at runtime.
3. **Dynamic control flow** — the sequence of steps depends on results of earlier ones in ways that can't be pre-declared.

How each tier handles them:

| Flavor | Level 3 (DAG) | Level 4 (workflow instance) |
|---|---|---|
| Parameterization | Native (`dag_run.conf`, params, Variables) | Native (workflow inputs) |
| Structure | **Bounded:** expand a declared template via dynamic task mapping (`expand()`, `.map()`). Shape known at run start. | **Arbitrary:** structure is whatever code produces. Loop count, recursion depth, sub-workflow spawning — all runtime. |
| Control flow | **Branching within a pre-declared graph** (`@task.branch`). Can't invent new step types mid-flight; no recursion; no unbounded depth. | **Anything code can express:** while-loops, recursion, conditional sub-workflow spawning, dynamic step generation. |

**In one sentence:** Level 3 dynamism is *"the same declared template, expanded with runtime data."* Level 4 dynamism is *"code that decides its own shape as it runs."*

Where this bites in practice:

- **Discover-then-act flows** (agent loops, investigation tools, anything where next-step depends on what was just learned in ways you can't enumerate): Level 4 is natural; Level 3 fights you.
- **Recursive decomposition** ("if this is hard, split it and run sub-tasks of the same shape"): Level 4 has native recursive workflows; Level 3 needs DAG-triggers-DAG hacks that lose the unified view.
- **Unbounded depth fan-out:** Level 3 can do dynamic fan-out at one level, not arbitrary depth. Level 4 just loops.

Conversely, Level 3's *bounded* dynamism is sometimes a feature:

- **Visualization:** finite declared shape can be drawn as a diagram. Arbitrary-shape workflow code can't be.
- **Cost / capacity prediction:** "this DAG run executes at most N tasks" is answerable; "this workflow might recurse 0 to 1000 times" is not.
- **Static analysis:** the graph is inspectable as data before execution.

Both kinds of dynamism are legitimate. Pick the one whose constraints fit your problem.

### Demonstration: workflows subsume DAGs

A workflow instance can express any DAG, but a DAG can't express every workflow. Concretely:

**Any DAG can be expressed as a workflow.** Take a simple DAG: `fetch → [extract, validate] → merge → publish`. As a workflow:

```python
@DBOS.workflow()
def pipeline(input_id: str):
    raw = fetch(input_id)                                 # @step
    # parallel fan-out
    h_extract  = DBOS.start_workflow(extract, raw)
    h_validate = DBOS.start_workflow(validate, raw)
    extracted = h_extract.get_result()
    validated = h_validate.get_result()
    # join
    merged = merge(extracted, validated)                  # @step
    publish(merged)                                       # @step
```

Topological order, parallel where possible, join before downstream — same semantics as the DAG. The DAG is one specific *shape* a workflow can take.

**The reverse fails.** Take three workflow patterns and try to express them as DAGs:

*Recursion:* "If this subtask is too big, split it and run yourself on each piece."
```python
@DBOS.workflow()
def process(item):
    if is_atomic(item):
        return handle(item)
    pieces = split(item)
    handles = [DBOS.start_workflow(process, p) for p in pieces]
    return combine([h.get_result() for h in handles])
```
DAGs forbid cycles. There's no edge that says "this node calls itself." You can simulate this with DAG-triggers-DAG, but you lose unified state, unified retries, and unified observability — you're outside the DAG runner's view.

*Wait-then-decide-then-wait:* "Place order, await payment confirmation (could be days), then based on the payment method either await fraud review or skip it, then await shipment."
```python
@DBOS.workflow()
def order(order_id):
    place(order_id)
    payment = DBOS.recv(timeout=86400)
    if payment.method == "card" and payment.amount > 10_000:
        review = DBOS.recv(topic="fraud", timeout=259200)
        if not review.approved:
            cancel(order_id); return
    ship(order_id)
    delivery = DBOS.recv(topic="delivery", timeout=86400 * 14)
    close(order_id, delivery)
```
The shape of the work depends on values that aren't known when the workflow starts. A DAG run is a single execution unit — it can't split into "the no-review path takes 3 days, the review path takes 5 days" where which path you're on emerges mid-flight.

*Unbounded loop:* "Agent keeps trying tools until it solves the problem or runs out of budget."
```python
@DBOS.workflow()
def agent(goal, budget):
    findings = []
    while budget > 0 and not solved(findings, goal):
        action = choose_next_action(findings, goal)       # @step (LLM)
        result = execute_action(action)                    # @step
        findings.append(result)
        budget -= action.cost
    return findings
```
DAGs don't have unbounded loops. You'd have to declare a fixed maximum iteration count as separate tasks, which destroys the abstraction.

### The formal claim

Workflows are **Turing-complete** as orchestration primitives: any computation expressible as a program is expressible as a workflow (because workflows are programs). DAGs are a restricted class — they're **decidable, finite, acyclic graphs evaluated topologically**. They can't express recursion, unbounded iteration, or runtime-emergent structure.

DAGs are a *strictly smaller* class of orchestrations than workflows. Every DAG is a workflow; not every workflow is a DAG.

### Why this doesn't make Level 3 obsolete

Generality has costs. DAGs trade expressiveness for things workflows can't easily give back:

- **Drawable.** A finite declared graph renders as a picture. Workflow code doesn't — every instance is potentially shaped differently, and the "diagram" of a workflow is at best a trace of one specific run.
- **Statically analyzable.** You can verify "all paths through this DAG are covered by tests" because the paths are finite. You can't make the same claim about arbitrary code.
- **Predictable cost.** "This DAG run will execute at most N tasks" is computable from the declaration. "This workflow might recurse 0 to 1000 times" is not.
- **Backfillable.** Re-running a DAG for past dates with new logic is well-defined. Workflow instances don't have a natural backfill semantic — they're entity-bound, not date-bound.

So: **workflows are more powerful; DAGs are more constrained, and the constraint is sometimes the feature.** Pick DAGs when the constraints fit your problem (batch data pipelines, reproducible reports, lineage-tracked transformations). Pick workflows when the constraints don't (long-lived business processes, agent loops, recursive work).

This is the standard expressiveness-vs-constraints tradeoff, the same one you make choosing SQL vs general-purpose code, or regex vs full parsers, or HCL vs Python. More general doesn't mean better — it means "use it when you actually need the generality."

### Why this is the deepest distinction

Almost every other feature has migrated across the boundary over time. Airflow has branching, sensors, HITL, dynamic task mapping. DBOS and Temporal can do scheduled work. The feature-checklist comparison is dead.

The line that hasn't moved — and probably won't — is **what does the system fundamentally model as the unit of work**. DAG engines were born from batch data pipelines where "a run" was the natural unit. Workflow engines were born from business processes where "an entity's lifecycle" was the natural unit. The internals reflect those origins.

You can simulate one with the other (and people do — Airflow with one-task-per-entity DAGs, or DBOS with periodic scheduled workflows that fan out over a batch), but the impedance mismatch is real:

- **Airflow forced into per-entity model:** explodes scheduler memory, UI can't render millions of DAG runs, the metadata DB gets hammered, backfill semantics become meaningless.
- **DBOS forced into batch model:** loses most of its advantages, the per-instance state machinery is overhead you're not using, the workflow-instance UI shows one giant batch instance per night.

### How to tell which one you actually have

A useful test: ask "what is the *thing* whose lifecycle this system is tracking?"

- If the answer is a time window (today, this hour, this batch) → DAG runs → Level 3
- If the answer is a business entity (this order, this customer, this approval) → workflow instances → Level 4
- If the answer is both — you have two separate things, and they belong at different tiers. Use both, not one impersonating the other.

Most non-trivial systems end up running at both tiers simultaneously. Level 3 for the nightly batch reports, Level 4 for the order lifecycles. That heterogeneity is correct.

---

## Higher tiers can simulate lower ones — but usually shouldn't

Lower tiers are *less general* than higher ones, which means higher tiers can technically express lower-tier patterns. Examples:

- **Streams → queue:** Kafka topic + consumer group = at-least-once job queue. Transactional sinks make it effectively-once.
- **Streams → scheduler:** events with `fire_at` timestamps + a punctuator/timer scanning state for due items — how Kafka Streams and Flink-based schedulers actually work.
- **Streams → DAG:** see worked example below.
- **Durable execution → everything below:** workflows can drive a DAG, schedule jobs, act as a queue. They subsume the lower tiers entirely.

### Worked example: DAG on streams

Take a simple DAG:
```
fetch ──┬──▶ extract ──┐
        │              ├──▶ merge ──▶ publish
        └──▶ validate ─┘
```

Translation to streaming topology:

```
topic "raw" ─► [fetch] ─► topic "fetched"
                              │
              ┌───────────────┴────────────────┐
              ▼                                 ▼
         [extract]                         [validate]
              │                                 │
              ▼                                 ▼
       topic "extracted"                topic "validated"
              │                                 │
              └────────────────┬────────────────┘
                               ▼
                  [merge — stateful join on correlation_id]
                               │
                               ▼
                       topic "merged" ─► [publish] ─► topic "published"
```

**Topological order** falls out naturally: each processor consumes from upstream topics and writes to downstream topics. A processor can't run before its inputs exist.

**Fan-out** is free: `extract` and `validate` are separate consumer groups on the same `fetched` topic, processing the same input in parallel.

**Fan-in (the join)** is the hard part. Streams have no native "wait for both branches to finish" — nothing ever finishes in a stream. The merge processor needs **state**:

```python
# Pseudocode for the merge processor (Kafka Streams / Flink shape)
state_store = KeyValueStore("pending_merges")  # keyed by correlation_id

def on_message(topic, msg):
    cid = msg.correlation_id
    pending = state_store.get(cid) or {}

    if topic == "extracted":
        pending["extracted"] = msg.payload
    elif topic == "validated":
        pending["validated"] = msg.payload

    # When both halves have arrived, emit and clear state
    if "extracted" in pending and "validated" in pending:
        emit("merged", combine(pending["extracted"], pending["validated"]))
        state_store.delete(cid)
    else:
        state_store.put(cid, pending)
```

This is the standard streaming join pattern. Kafka Streams calls it a "co-partitioned stream-stream join." Flink has explicit `connect()` + `CoProcessFunction`. Same idea: keyed state holds partial completions until both sides arrive.

### What you've actually built

A DAG, executed continuously, with these properties:

- Per-input parallelism (multiple correlation_ids processed concurrently)
- Crash-safe (state store is durable; checkpoints survive restarts)
- Exactly-once (with transactional sinks)
- Replayable (rewind offsets to re-execute)

And these gaps versus a real DAG engine:

- **No DAG visualization** — the graph exists only in your topology code; there's no UI showing the structure unless you build one.
- **No backfill** — you can rewind to reprocess, but "run this DAG for last week with the new logic" requires offset gymnastics and is rarely clean.
- **No lineage tracking** — Dagster gives you "this asset depends on these inputs"; here you'd build it from message payloads.
- **State store TTL is your problem** — if `extracted` arrives but `validated` never does, the pending entry sits in the state store forever unless you add cleanup logic. A DAG engine would just time out the task.
- **Per-DAG-run identity is lost** — there are no "DAG runs"; there's just a continuous flow. If you need "did the DAG run for 2026-04-23?", you have to derive it from message timestamps and explicit run-id propagation through every payload.

### The cost-benefit pattern

| Approach | Cost | Benefit |
|---|---|---|
| Use Airflow for a DAG | Run Airflow | DAG semantics out of the box |
| DAG-on-streams via Kafka | Build stateful joins, run cleanup, write topology code | Continuous execution, replay, unified with your event infrastructure |

The streaming version is roughly 5–10× the code of the equivalent Airflow DAG. You'd do this only when:

1. You already run heavy stream infrastructure and don't want to add Airflow.
2. The "DAG" is actually closer to a continuous stream of events than a batch job, and Airflow's batch model fights you.
3. You need unified replay/observability across DAG-shaped and stream-shaped work.

### The general principle

**Pick the tier that matches the abstraction you want to think in.** "Jobs to run" → queue. "Events flowing through transformations" → streams. "DAG of dependent tasks on a schedule" → DAG orchestrator. "Workflow per business entity" → durable execution.

The fact that one *can* simulate another is implementation flexibility, not a reason to muddle the conceptual layer. Cron + Redis-backed Sidekiq is ~100 lines of config; a Kafka-based scheduler is ~1000 lines of stateful streaming code. An Airflow DAG is a few dozen lines; the streaming equivalent is a few hundred.

The exception is when you already run heavy infrastructure at a higher tier and adding a lower-tier system is operationally more expensive than implementing it on what's already there. Fintech teams running Kafka often do this — the unified observability and replay across queues, streams, and DAGs justify the extra code.

---

## Orchestration vs choreography: the other axis

Tier is *what* the orchestration machinery does. Coordination style is *how* the work gets driven across participants. They're orthogonal — same tier can be run either way at multiple points above Level 1.

| Axis | Orchestration | Choreography |
|---|---|---|
| Who decides what's next | Central controller | Each participant, reacting to events |
| Shape | Hub-and-spoke | Peer-to-peer |
| End-to-end visibility | Easy — read the orchestrator's state | Hard — must reconstruct from event logs |
| Coupling | Participants couple to the orchestrator's API | Participants couple to event schemas |
| Failure handling | Centralized retry/compensation logic | Each participant handles its own; saga emerges |
| Org fit | One team owns the workflow | Each participant owned by its own team |
| Natural at tier | 3, 4 (workflow engines are orchestrators by design) | 3.5, 5b-chor (streams and event-driven sagas are choreography by nature) |

The classical place this matters is **Level 5b — sagas**. There are two saga flavors:

- **Orchestrated saga:** a workflow engine (Temporal, DBOS) drives the saga. Calls service A, awaits result, calls service B, on failure calls A's compensator. The saga lives as code in one place.
- **Choreographed saga:** services publish events; other services subscribe; the saga emerges from the pattern. No central coordinator. Compensation is itself an event other services react to.

Both work. Orchestration is easier to debug and reason about; choreography scales better organizationally because services don't need to know about the orchestrator. Pick orchestration when you have <10 services and need clarity; pick choreography when you have many teams and the loose coupling pays off.

**Rule of thumb:** start with orchestration. Move to choreography only when the orchestrator becomes a bottleneck — either technical (it's a SPOF you can't tolerate) or organizational (too many teams routing through one engine). Premature choreography produces "where the hell does this saga end?" pain that's worse than the centralization it was meant to escape.

---

## Picking a rung

- Single unit, caller waits → Level 0
- Single unit, async → Level 1
- Scheduled or cluster-wide → Level 2
- Scheduled batch with task dependencies → Level 3
- Continuous processing over unbounded streams → Level 3.5
- Per-entity workflow instances that live long, evolve, branch, wait → Level 4
- Cross-system atomicity → Level 5 (usually as sagas on top of Level 4)

**Rules of thumb:**
1. Most non-trivial business processes are Level 4. Teams underuse it because the tooling is newer.
2. The Level 3/4 line isn't "features" — it's whether you think in **DAG runs** (time-bound batches) or **workflow instances** (one per business entity, long-lived). Both can have branching, waits, human gates today. They differ in unit of execution.
3. Mature systems run at multiple rungs simultaneously. Background email at Level 1, order processing at Level 4, nightly batch at Level 3.

---

## Glossary

### A

- **ACID** — Atomicity, Consistency, Isolation, Durability. The classical database transaction guarantees.
- **At-most-once / at-least-once / effectively-once** — Delivery semantics.
  - **At-most-once**: may be lost, never duplicated (e.g., in-memory work that dies with the process).
  - **At-least-once**: guaranteed to be delivered, may be duplicated. The default for durable queues. Idempotency is the caller's burden.
  - **Effectively-once** (sometimes marketed as "exactly-once"): same observable outcome as if processed once. Achieved via at-least-once + dedup/idempotency or via transactional sinks. **True exactly-once delivery is a myth** in the general distributed case — what's actually possible is exactly-once *effect* via idempotency.

### B

- **Backfill** — Re-running scheduled jobs for past time periods (e.g., "process last month's data with the new logic"). Standard in DAG orchestrators.

### C

- **Choreography** — Coordination style where there's no central controller. Each participant listens for events and reacts independently, emitting new events. The workflow emerges from peer-to-peer reactions. Opposite of orchestration.
- **Compensation** — A "reverse" operation that undoes a previously committed step. The core mechanism of saga patterns.
- **Coordinator SPOF** — In 2PC, the coordinator decides commit/abort for all participants. If it dies mid-protocol, participants are stuck holding locks. Single point of failure.

### D

- **DAG** — Directed Acyclic Graph. The data structure for declaring task dependencies in orchestrators.
- **Deferrable operator** — Airflow concept: a task that releases its worker slot while waiting (for a sensor, an event, a time), resumed by the Triggerer component. Enables long waits without holding resources.
- **Determinism constraint** — Durable execution engines replay workflow code on resume. If the code is non-deterministic (random, time, network), replay diverges. Non-deterministic operations must live inside steps, not in workflow body.
- **Dual-write trap** — Writing to two systems (e.g., DB + Kafka) outside a single transaction. Either can fail independently, producing inconsistent state. Outbox pattern is the standard fix.

### E

- **Eventual consistency** — Systems will converge to a consistent state, but readers may see stale or inconsistent views in the meantime.
- **Exactly-once via offsets** — In stream processing, "exactly-once" is achieved by committing the consumer's read offset and any downstream writes atomically. Not magic — depends on transactional sinks (e.g., Kafka transactions, Flink checkpoints).

### H

- **Head-of-line blocking** — A slow item at the front of a queue/pipeline blocks faster items behind it.
- **Heuristic outcomes** — In 2PC, when the coordinator can't be reached, participants may guess (commit or abort) and document the guess. Recovery is manual.
- **History bloat** — Durable execution engines (esp. Temporal) store every workflow event. Long-running workflows accumulate large histories, slowing replay and consuming storage.

### I

- **Idempotency** — Property where running an operation twice has the same effect as running it once. Critical for safe retries.

### K

- **Kafka / event log** — A durable, partitioned, append-only log. Acts as queue, event-source store, and stream-processing substrate depending on how you use it. Itself not a workflow engine; tools like Kafka Streams, Flink, Materialize layer processing semantics on top.

### L

- **Lineage** — Tracking which outputs were produced by which inputs through which transformations. Standard in DAG orchestrators (Dagster especially).

### O

- **Orchestration** — Coordination style where a central controller drives the sequence: it calls participants, gets results, decides what's next. Hub-and-spoke. Opposite of choreography.
- **Outbox pattern** — Atomically write business state + an outgoing event to the same DB transaction; a separate process drains events to a bus. Fixes the dual-write trap.

### P

- **Poison message** — A message that consistently fails processing, jamming the queue if not isolated to a dead-letter queue (DLQ).

### R

- **Replay** — Re-executing workflow code on resume after a crash, using checkpointed step results to skip work already done.

### S

- **Saga** — A multi-step workflow where each step has a compensating action. If a later step fails, earlier steps are compensated in reverse order. Pragmatic alternative to 2PC.
- **Sensor** — Airflow concept: a task that polls or waits for some external condition (file arrived, time passed, signal received).
- **SPOF** — Single Point of Failure.

### T

- **Tail latency / p99** — The 99th-percentile response time. A small fraction of requests can dominate user perception of system performance.
- **Thundering herd** — Many workers/clients waking simultaneously (e.g., cron tick) and all hitting the same resource, causing overload.
- **Transactional coupling** — The ability to commit business writes and workflow-engine state in a single database transaction. Unique to DBOS in current Level-4 tools.
- **Two-phase commit (2PC / XA)** — Coordinator asks all participants to "prepare" (lock resources); if all succeed, asks all to "commit." Classical distributed-transaction protocol. Blocking and fragile.

### V

- **Versioning hell** — Changing a long-running workflow's code while old workflow instances are still executing the old version. Each engine has its own approach (and limitations).

### W

- **Watermark** — In stream processing, a heuristic for "all events up to time T have probably arrived." Drives when to close windows. Late-arriving data past the watermark is dropped or handled specially.
- **Windowing** — In stream processing, grouping unbounded data into bounded chunks (tumbling, sliding, session windows) so you can aggregate over them.