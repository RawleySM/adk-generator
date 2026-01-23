ROLE
You are “Planning-Agent” (systems + data + runtime integration). Your goal is to produce an implementable design + step plan (NO CODE) for a custom Google ADK SessionService backend named `DeltaSessionService` that persists Sessions and Events into Databricks Unity Catalog Delta tables via Spark SQL / DataFrame writes.

CONTEXT (WHY THIS EXISTS)
- Google ADK’s built-in `DatabaseSessionService` targets relational DBs (SQLite/Postgres/MySQL) via an async DB driver; that does NOT map to Unity Catalog Delta tables by “connection string”.
- We want Unity Catalog / Delta to be the source of truth for session persistence, so we need custom plumbing.

AUTHORITATIVE REFERENCES (READ THESE; CITE THEM IN YOUR PLAN)
ADK docs:
- Session overview + lifecycle + built-in SessionService implementations (incl. DatabaseSessionService + runner lifecycle notes):
  https://google.github.io/adk-docs/sessions/session/
- State behavior (state is updated via events / event actions, not “mutated in place” magically):
  https://google.github.io/adk-docs/sessions/state/
- ADK Python API reference (BaseSessionService / Session / Event types):
  https://google.github.io/adk-docs/api-reference/python/
  (In particular, locate `google.adk.sessions.BaseSessionService` and its abstract async methods.)
Databricks docs:
- Delta Lake MERGE INTO (for idempotent upserts):
  https://docs.databricks.com/sql/language-manual/delta-merge-into.html
- Delta Lake concurrency / optimistic concurrency concepts:
  https://docs.databricks.com/delta/concurrency-control.html
- Unity Catalog managed tables overview (naming, governance assumptions):
  https://docs.databricks.com/data-governance/unity-catalog/index.html

SCOPE (STRICT)
Implement ONLY `DeltaSessionService` (session persistence). DO NOT implement telemetry plugins, MLflow tracing, or extra observability in this task.
However, your plan must ensure the resulting service is compatible with the ADK Runner calling pattern (Runner loads session, then calls `append_event(session, event)` each turn).

NON-GOALS
- Do not propose using an external RDBMS, Lakehouse Federation, or “just use DatabaseSessionService with a driver”.
- Do not propose a plugin-only approach as a replacement for session persistence. Plugins may be used later for telemetry, but this task is the SessionService.

DELIVERABLE FORMAT (WHAT YOU MUST OUTPUT)
Produce a structured plan with these sections:
1) Compatibility contract with ADK (methods + semantics)
2) Delta table schema design (sessions table + events table; columns + types + keys + partitions)
3) Serialization spec (JSON encoding rules; canonicalization; versioning)
4) Method-by-method implementation plan (create/get/list/delete session; append/list events; rewind)
5) Concurrency + idempotency strategy (exactly how you avoid duplicates and handle concurrent writers)
6) Performance strategy (partitioning, clustering, avoiding hot partitions, query patterns)
7) Test plan (unit + integration in Databricks; edge cases)
8) Rollout plan (table migration, backfill, safe deployment steps)

REQUIREMENTS (HARD)
A) ADK INTERFACE / SEMANTICS
- Your plan must map to `google.adk.sessions.BaseSessionService` abstract async methods (verify exact names/signatures in the API reference):
  - create_session(app_name, user_id, state, session_id?)
  - get_session(app_name, user_id, session_id)
  - list_sessions(app_name, user_id)
  - delete_session(app_name, user_id, session_id)
  - append_event(session, event)
  - list_events(app_name, user_id, session_id, since?, limit?, etc if applicable)
  - rewind (if required/available per ADK docs; check “Rewind sessions” page linked from Session docs nav)
- Emphasize that state updates are applied as part of event persistence (Runner → append_event), per ADK docs.

B) STORAGE BACKEND MUST BE UC DELTA
- Use fully-qualified Unity Catalog table names (catalog.schema.table).
- Use Spark SQL / DataFrame writes (no JDBC to some other DB).
- Store complex payloads as STRING columns containing JSON (avoid BLOB/pickle).
- You must propose a stable schema that supports schema evolution (additive) without breaking old sessions.

C) TWO-TABLE MINIMUM (RECOMMENDED)
- sessions table: one row per (app_name, user_id, session_id)
- events table: many rows per (app_name, user_id, session_id) ordered by event time and/or monotonic sequence
If you propose more tables, justify why.

D) IDENTITY + KEYS
- Partition key strategy must consider:
  - high cardinality on (app_name, user_id, session_id)
  - efficient retrieval of “latest sessions” and “events for a session”
- Define a deterministic primary key for events:
  - prefer event.id if ADK provides one; else generate one deterministically at append time
  - ensure idempotency: re-appending the same event must not create duplicates

E) CONCURRENCY + CONSISTENCY
- You MUST propose an “optimistic concurrency” approach that works in Delta:
  - Example: session row includes last_update_time and/or version counter
  - append_event writes event row then updates session row in a single logical transaction pattern
- Address race conditions when two workers append to the same session concurrently.
- Your plan must include how you detect/handle “stale session object” updates (session.last_update_time drift) because ADK’s DB session service enforces last_update_time expectations (see docs + common errors).
- Use Delta MERGE patterns where appropriate; be explicit.

F) REWIND
- If ADK expects rewind support, implement it. If optional, state that clearly and propose an implementation:
  - Option 1: logical rewind pointer in sessions table (don’t delete events; filter by pointer)
  - Option 2: soft-delete flags on events past rewind point
  - Option 3: physical delete of events after rewind point (least preferred if audit matters)
- State tradeoffs.

DATA MODEL (STARTING POINT YOU MAY ADAPT)
Propose schemas like:

sessions_uc:
- app_name STRING (PK part)
- user_id STRING (PK part)
- session_id STRING (PK part)
- state_json STRING (JSON object)
- last_update_time TIMESTAMP or DOUBLE (consistent with ADK semantics)
- created_time TIMESTAMP
- updated_time TIMESTAMP
- version BIGINT (optional monotonic)
- is_deleted BOOLEAN (optional)

events_uc:
- app_name STRING
- user_id STRING
- session_id STRING
- event_id STRING (PK part)
- event_time TIMESTAMP
- seq BIGINT (optional ordering)
- author STRING (optional)
- event_json STRING (full Event serialized)
- actions_json STRING (EventActions serialized; includes state_delta)
- state_delta_json STRING (extracted subset for faster state application)
- created_time TIMESTAMP
- is_deleted BOOLEAN (optional)

You MUST decide:
- whether “session.state_json” is reconstructed from events each read, or incrementally maintained on append_event.
- the exact JSON shape stored for Event and Session (canonical format).

IMPLEMENTATION PLANNING DETAILS (YOU MUST INCLUDE)
- Initialization: how DeltaSessionService ensures tables exist (CREATE TABLE IF NOT EXISTS) and enforces schema.
- append_event algorithm:
  1) validate session identity matches event target
  2) write event row idempotently (MERGE on (app_name,user_id,session_id,event_id))
  3) compute next session.state_json by applying event.actions.state_delta (per ADK docs) to prior state_json
  4) update sessions row with new state_json + last_update_time (+ bump version) using optimistic check
  5) if optimistic check fails, reload latest session row and retry merge (bounded retries)
- get_session algorithm:
  - load sessions row
  - decide whether to also load recent events (ADK Session object includes events list per docs; but you can bound it for performance if ADK allows)
- list_sessions algorithm:
  - query sessions_uc by app_name,user_id, filter is_deleted, order by updated_time desc
- delete_session algorithm:
  - soft delete vs hard delete; address cleanup of events

TEST PLAN MUST COVER
- idempotent append (same event twice)
- concurrent append from two workers (simulate race)
- stale session object (last_update_time mismatch)
- schema evolution (adding a column)
- rewind correctness (events visible vs not; state matches)
- large session (many events) performance tests (read patterns)

OUTPUT QUALITY BAR
- No code, but the plan must be implementable by a coding agent without further clarification.
- Use the links above as “sources of truth” and explicitly reference which doc section informed which decision.

NOW PRODUCE THE PLAN.
