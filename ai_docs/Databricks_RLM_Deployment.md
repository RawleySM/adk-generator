---

## Secrets Management for databricks_rlm_agent

This section documents the required secrets configuration for deploying the `databricks_rlm_agent` wheel as a Databricks Job (spark_python_task).

### Overview

API keys and credentials are loaded from **Databricks Secrets** at runtime via the `databricks_rlm_agent/secrets.py` module. This replaces hardcoded keys and provides secure, auditable credential management.

### Required Secrets

| Secret Key | Environment Variable | Required | Description |
|------------|---------------------|----------|-------------|
| `google-api-key` | `GOOGLE_API_KEY` | **Yes** | Google/Gemini API key for ADK |
| `openai-api-key` | `OPENAI_API_KEY` | No | OpenAI API key (if using OpenAI models) |
| `anthropic-api-key` | `ANTHROPIC_API_KEY` | No | Anthropic API key (if using Claude models) |
| `databricks-host` | `DATABRICKS_HOST` | No | Databricks workspace URL (for Jobs API calls) |
| `databricks-token` | `DATABRICKS_TOKEN` | No | Databricks PAT (for Jobs API calls) |

### Setup: Create Secret Scope and Add Secrets

```bash
# Create secret scope (run once per workspace/environment)
databricks secrets create-scope adk-secrets

# Add required secrets
databricks secrets put-secret adk-secrets google-api-key --string-value "<your-google-api-key>"

# Add optional secrets as needed
databricks secrets put-secret adk-secrets databricks-host --string-value "https://<workspace>.cloud.databricks.com"
databricks secrets put-secret adk-secrets databricks-token --string-value "<your-pat>"
```

### Databricks Job Configuration (Recommended Method)

The recommended way to inject secrets is via **environment variables in the Job task definition**. This centralizes secret access in job config and keeps the wheel code simple.

**Example spark_python_task configuration:**

```json
{
  "name": "databricks-rlm-agent-job",
  "tasks": [
    {
      "task_key": "run_agent",
      "spark_python_task": {
        "python_file": "/Volumes/silo_dev_rs/adk/wheels/run_agent.py",
        "parameters": []
      },
      "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "num_workers": 0,
        "node_type_id": "Standard_DS3_v2"
      },
      "libraries": [
        {"whl": "/Volumes/silo_dev_rs/adk/wheels/databricks_rlm_agent-0.1.0-py3-none-any.whl"}
      ]
    }
  ],
  "job_clusters": [],
  "parameters": [],
  "run_as": {
    "user_name": "service-principal@example.com"
  },
  "environments": [
    {
      "environment_key": "default",
      "spec": {
        "client": "1",
        "dependencies": []
      }
    }
  ],
  "task_environment_vars": {
    "run_agent": {
      "GOOGLE_API_KEY": "{{secrets/adk-secrets/google-api-key}}",
      "DATABRICKS_HOST": "{{secrets/adk-secrets/databricks-host}}",
      "DATABRICKS_TOKEN": "{{secrets/adk-secrets/databricks-token}}",
      "ADK_DELTA_CATALOG": "silo_dev_rs",
      "ADK_DELTA_SCHEMA": "adk"
    }
  }
}
```

**Alternative: Using spark_conf for environment variables:**

```json
{
  "new_cluster": {
    "spark_conf": {
      "spark.databricks.passthrough.enabled": "false"
    },
    "spark_env_vars": {
      "GOOGLE_API_KEY": "{{secrets/adk-secrets/google-api-key}}",
      "DATABRICKS_HOST": "{{secrets/adk-secrets/databricks-host}}",
      "DATABRICKS_TOKEN": "{{secrets/adk-secrets/databricks-token}}"
    }
  }
}
```

### Fallback: dbutils.secrets.get()

If environment variables are not configured in the job definition, the `secrets.py` module will attempt to retrieve secrets using `dbutils.secrets.get()` at runtime. This requires:

1. The secret scope name is set via `ADK_SECRET_SCOPE` env var (default: `adk-secrets`)
2. The job's run identity (user or service principal) has **READ** permission on the secret scope

```python
# This happens automatically in run.py at startup:
from databricks_rlm_agent.secrets import load_secrets
load_secrets(spark)  # Loads secrets into os.environ
```

### Permissions

Grant READ access to the secret scope for the job's run identity:

```bash
# For a service principal
databricks secrets put-acl adk-secrets <service-principal-id> READ

# For a user
databricks secrets put-acl adk-secrets user@example.com READ

# For a group
databricks secrets put-acl adk-secrets data-engineers READ
```

### Security Best Practices

1. **Use scopes per environment**: Create separate scopes like `adk-dev`, `adk-staging`, `adk-prod`
2. **Minimize permissions**: Only grant READ to identities that need it
3. **Prefer env var injection**: Use job config secret references over dbutils fallback
4. **Avoid PATs when possible**: For `DATABRICKS_HOST`/`DATABRICKS_TOKEN`, consider using OAuth or managed identity if your org supports it
5. **Never log secret values**: The secrets module avoids logging actual values

### Troubleshooting

**Error: "Required secrets not found"**
- Ensure secrets exist in the scope: `databricks secrets list adk-secrets`
- Check job env var configuration uses correct syntax: `{{secrets/scope/key}}`
- Verify run identity has READ permission on the scope

**Error: "Could not initialize DBUtils"**
- This is expected when running outside Databricks Runtime
- Ensure env vars are set if running locally for testing

---

### 15. Assistant · Dr. Evelyn Reed
*Timestamp:* 2026-01-20T02:45:22.328Z

You want an explicit, Databricks-native deployment pattern where the **RLM orchestrator (wheel task)** is the component most affected by execution-model decisions, and where notebook/script execution is done via the **Databricks Jobs API** (not in-process), with durable state in Unity Catalog + artifacts in Volumes. Below is the pattern I recommend for production robustness and operational clarity.

---

## Recommended Databricks deployment pattern: **“Orchestrator-as-Wheel + Run-Submit Executor Job”**

### Why this pattern (and why it most impacts the RLM orchestrator)
The orchestrator’s job is to (1) decide next action, (2) materialize an executable artifact, (3) submit execution, (4) ingest results into durable state, and (5) iterate safely. The key decision is **how execution happens**. If you bind execution to in-process notebook calls, you conflate thinking and doing, lose clean audit boundaries, and make retries / cancellation / traceability messy. Using the Jobs API gives you:

- **Hard isolation** between orchestrator and data-plane compute
- **First-class run tracking** (run_id, run_url, lifecycle_state, repair history)
- **Replayability** and audit via persisted notebooks + run metadata
- **Clean idempotency boundaries**: orchestrator decides; execution job performs; both are recordable

In other words: the orchestrator becomes a **control-plane state machine** that issues Jobs API commands and records the resulting run references.

---

## Concrete topology (two-job pattern)

### Job A — **Control Plane (Continuous)**
**Name:** `autodatascidev-orchestrator`  
**Type:** Databricks Workflow job, **continuous** (or high-frequency schedule)  
**Primary task:** Python wheel task running the ADK Runner and your `RLMOrchestratorAgent`.

Responsibilities:
1. Poll `jira_board_clone` (or CDF/watermark)
2. Acquire/renew lease (UC Delta `work_leases`)
3. Load `run_state` + `session_capsule`
4. Decide DEV vs PROD
5. In DEV: run one RLM iteration (bounded)
6. Submit execution to Job B using Jobs API (`runs/submit`)
7. Persist iteration refs + capsule updates
8. Optionally: if you want non-blocking orchestration, record “pending execution” and return quickly

Key design choice:
- **One iteration per poll-cycle per run** (recommended). Avoid keeping the wheel alive waiting on long notebook runs. This yields better fault tolerance and avoids long-lived driver fragility.

---

### Job B — **Execution Plane (On-Demand)**
**Name:** `autodatascidev-executor`  
**Type:** Standard Databricks Job used as a reusable execution harness  
**Invocation:** called via Jobs API from Job A (`runs/submit`) with parameters that point to the generated artifact(s)

Responsibilities:
1. Checkout/load code from Volumes / Repo snapshot references (if applicable)
2. Execute the generated notebook/script on an appropriate cluster
3. Produce:
   - structured `result.json` (success/failure, metrics, produced artifacts)
   - log pointers (stdout/stderr locations if you capture)
   - output table refs / DQ report refs
4. Write results to governed storage (UC Volumes + Delta tables, ideally append-only “execution_events”)

This creates a clean separation:
- **Thinking plane** = Job A (wheel + ADK)
- **Data plane** = Job B (Spark-heavy notebook/script)

---

## Jobs API workflow (explicit)

### 1) Orchestrator submits an execution run
Use `POST /api/2.1/jobs/runs/submit` with a **single-run, parameterized** payload.

You have two good options for Job B tasks:

#### Option B1 (most Databricks-native): **Notebook task**
- Orchestrator writes a generated notebook to a **Workspace path or Repo path**.
- Submit it as a notebook task with `notebook_params`.

Pros: native notebook UX, easy debugging.  
Cons: if you insist on UC Volumes-only storage, you may need a sync step (Volumes aren’t Workspace).

#### Option B2 (often best for Volumes): **Spark Python task**
- Orchestrator writes `entrypoint.py` (and optionally a wheel) into UC Volumes.
- Job B runs a `spark_python_task` where the script loads the generated code from Volumes and executes it.

Pros: Volumes-first, no Workspace coupling, very deterministic packaging.  
Cons: less “notebook-y” debugging unless you also emit a notebook artifact.

For robustness with Volumes + repos, I generally recommend **B2** for execution and still optionally emitting a notebook for human inspection.

---

## The RLM orchestrator’s “one-iteration” contract (what it must persist)

The orchestrator should treat every iteration as a transaction with durable references:

**Inputs it records**
- `run_id`, `iter_n`, `plan_digest`, `artifact_refs` (generated code paths + hashes)
- submitted `databricks_job_run_id` + `run_url`
- the exact parameter set used (for replay)

**Outputs it records**
- `execution_result_ref` (path to `result.json`)
- `parsed_summary` (from ephemeral agents)
- `capsule_delta` (what changed in the SessionCapsule)

This is how you keep the Base agent context small and make the system replayable.

---

## Minimal Jobs API objects you should standardize

### A) `execution_request` (written by orchestrator)
Delta table row, append-only:

- `run_id`, `iter_n`
- `artifact_path`, `artifact_hash`
- `job_submit_payload` (JSON)
- `submitted_at`, `submitted_by_worker`

### B) `execution_result` (written by executor)
Delta table row, append-only:

- `run_id`, `iter_n`
- `databricks_run_id`, `run_url`
- `status` (SUCCESS/FAILED)
- `error_type`, `error_summary`
- `metrics` (rows read/written, durations, DQ scores)
- `produced_artifacts` (paths/hashes)
- `produced_tables`

### C) `run_state` (authoritative, upsert)
- `current_mode` DEV/PROD
- `fsm_state` (or `dev_phase`)
- `lease_owner`, `lease_expiry`
- `capsule_struct`
- pointers to latest execution request/result

This split (append-only event tables + authoritative state) is a well-established reliability pattern (event sourcing + materialized state).

---

## Clusters/compute: align with isolation and governance

- **Job A (orchestrator)**: small driver-friendly cluster; minimal libraries; no broad data permissions beyond UC reads/writes for state tables and artifact storage.
- **Job B (executor)**: compute-appropriate cluster(s) with:
  - UC permissions for ERP hospital datasets (read), and controlled write targets
  - policy-enforced cluster configurations
  - optional photon / autoscaling tuned for workload class

If you have multiple workload types (ETL vs model training vs app builds), make Job B a **multi-task workflow** and select task keys based on the orchestrator’s planned action (routing).

---

## What I would implement first (Databricks-native MVP)
1. **Job A** continuous wheel task:
   - poll + lease + run_state load
   - generate artifact to Volumes
   - submit Job B via Jobs API
   - record run_id + run_url + refs
2. **Job B** spark_python_task:
   - read artifact from Volumes
   - execute with Spark
   - write `result.json` + append `execution_result`
3. Add ephemeral summarizers in Job A that read `result.json` and compress into capsule updates.

This yields a robust REPL-like loop without fragile in-process execution.

---

## Clarifying questions (so I can give you an exact Jobs API payload template)
1. Do you require **Workspace notebooks** as first-class artifacts, or is a **Volumes-first script/wheel** acceptable (with optional notebook emission)?
2. Is DEV iteration allowed to run on **shared interactive clusters**, or must everything be Jobs clusters?
3. Do you want the orchestrator to **wait synchronously** for Job B completion (simpler) or run **async** (more scalable, more state-machine-like)?

If you answer these three, I’ll provide:
- an exact `runs/submit` JSON payload,
- a reference implementation of the orchestrator’s `submit_execution()` + `poll_run()` functions,
- and the executor harness contract (`result.json` schema + Delta append schema) consistent with your `AutoDataSciDev_synth.md` tables and plugins.

---
Both council members agree on the central architectural move: treat the **RLM orchestrator as a control-plane state machine** and push all “doing” (Spark/notebook execution) into a **Databricks-native execution boundary** with durable state and artifacts in **Unity Catalog (Delta + Volumes)**. Where they differ is *how* to invoke execution:

- **Reed**: strongly recommends a **two-job pattern** using the **Databricks Jobs API (`runs/submit`)** for isolation, auditability, retries/repair, and clean run tracking.  
- **Rossi**: proposes a **single-job / same-cluster** approach using `dbutils.notebook.run()` to execute a generic executor notebook that loads code from Volumes—lower latency, but less robust isolation and not the Jobs API-centric workflow you asked for.

Given your explicit requirement to “utilize the Databricks Jobs API in a specific workflow,” the balanced recommendation is: **adopt Reed’s two-job pattern as the production default**, while optionally keeping Rossi’s same-cluster executor as a **DEV-only fast path** (if you truly need sub-second iteration). The component most impacted—your **RLM orchestrator**—becomes cleaner and more reliable when it **submits, monitors, and records Jobs runs** rather than executing code in-process.

---

## Recommended explicit deployment pattern (Databricks-native): **Orchestrator-as-Wheel + Jobs API Executor Harness**

### High-level topology
- **Job A: RLM Orchestrator (Control Plane)**
  - Databricks Workflow Job running a **Python wheel task**
  - Responsibilities: plan → materialize artifact → submit execution via Jobs API → ingest results → update state → iterate
  - Designed to run **continuous or on a tight schedule**, but performing **one bounded iteration per invocation** (or per loop tick) to avoid long-lived drivers.

- **Job B: Executor Harness (Data Plane)**
  - Databricks Workflow Job (or reusable “run-submit” payload) that runs the actual generated work on the right compute
  - Responsibilities: load artifact from UC Volumes, run it, write structured results to Delta/Volumes, exit with status.

This cleanly separates:
- **Thinking** (RLM + governance-aware decisioning + state machine) from
- **Doing** (Spark-heavy execution with first-class job tracking)

---

## Concrete “Jobs API workflow” (what the orchestrator does each iteration)

### 0) Preconditions (UC-native foundations)
- **Volumes**
  - `/Volumes/<catalog>/<schema>/scratchpad/...` (RW): generated code, run manifests, result.json
  - `/Volumes/<catalog>/<schema>/company_codebase/...` (RO): cloned repos / libraries for pipelines & apps
- **Delta tables in UC**
  - `rlm.run_state` (upsert/materialized state): authoritative session capsule + FSM state
  - `rlm.execution_requests` (append-only): what was submitted, with hashes and payload
  - `rlm.execution_results` (append-only): what happened, with metrics/errors and produced artifacts

### 1) Orchestrator loads state + acquires a lease
- Read `rlm.run_state` for `run_id`
- Acquire/renew a **lease** row (prevents duplicate concurrent controllers)
- Decide next action based on capsule + policy (DEV/PROD, allowed tools, data scopes)

### 2) Orchestrator writes an executable artifact to Volumes (the REPL “input”)
- Generate a script entrypoint (recommended):  
  `.../scratchpad/run_<run_id>/iter_<n>/entrypoint.py`
- Also write a **manifest** with:
  - inputs (tables queried, parameters)
  - hashes of generated files
  - intended executor “task type” (etl, analytics, app-db update, etc.)

### 3) Orchestrator submits execution via **Jobs API `runs/submit`**
- Use **`POST /api/2.1/jobs/runs/submit`** to launch Job B (or a one-off run) with parameters:
  - `run_id`, `iter_n`
  - `artifact_path` (Volumes path)
  - `codebase_path` (Volumes path)
  - optional policy flags (write targets, timeouts, max shuffle, etc.)

**Why this is the key decision for the RLM orchestrator:** it turns the orchestrator into a durable controller that only needs to track **run_ids** and results, rather than surviving as a long-lived process tied to execution.

### 4) Orchestrator records the submission durably
Append a row to `rlm.execution_requests` including:
- `run_id`, `iter_n`
- `artifact_path`, `artifact_hash`
- the exact Jobs API payload (JSON)
- returned `databricks_run_id` and `run_url`

### 5) Execution Job runs (Job B)
Job B should be a stable, human-reviewed harness with a strict contract:
- Reads `artifact_path` from Volumes
- Adds `/Volumes/.../company_codebase` to `sys.path` (so generated code composes with your real pipelines)
- Executes within guardrails (timeouts, exception capture, optional allowlist of modules)
- Writes:
  - `result.json` to Volumes (small, structured)
  - `execution_results` Delta append (status, metrics, table outputs, error summary)
  - any produced tables to governed UC locations

### 6) Orchestrator ingests results + updates capsule
- Poll Jobs API for lifecycle state (or run async and pick up later)
- Read `result.json` / `execution_results`
- Use the ephemeral summarizer to compress noisy logs into capsule deltas
- Upsert `rlm.run_state` to advance the FSM and iteration counter

---

## Implementation choice: Notebook task vs Spark Python task for Job B

Both are viable, but given your “Volumes + cloned repos in Volumes” posture:

- **Preferred for production**: **Spark Python task** (or wheel task) that loads the entrypoint from Volumes  
  - Best alignment with Volumes-first artifacts
  - Deterministic, less Workspace coupling

- **Optional for visibility**: also emit a notebook artifact for humans to inspect, but don’t make notebooks the execution substrate unless your organization strongly prefers that debugging model.

Rossi’s “Generic Executor Notebook” idea still fits—just reposition it:
- Use it as the implementation of **Job B** (executor harness), *invoked by Jobs API*, not via `dbutils.notebook.run()` from inside Job A.

---

## Where the council disagreed (and the practical resolution)

- **Single-cluster / `dbutils.notebook.run()` (Rossi)**  
  - Pros: fast, simple, same-cluster context reuse  
  - Cons: weaker isolation, harder audit boundaries, less “jobs-native” traceability, controller tied to execution runtime

- **Two-job / Jobs API boundary (Reed)**  
  - Pros: strongest operational model—clear run tracking, retries/repair, cancellation, replay, least coupling, best audit  
  - Cons: slightly more moving parts; may add seconds of overhead per iteration

**Resolution:** Use **Jobs API two-job pattern as the standard**, and only use same-cluster notebook chaining as a **DEV-mode optimization** when latency matters more than isolation.

---

## Actionable “next steps” blueprint
- Define **Job A** (wheel task) and **Job B** (executor harness) in a Databricks Workflow.
- Standardize three UC tables: `run_state`, `execution_requests`, `execution_results`.
- Standardize the Volumes layout: `scratchpad/run_id/iter_n/{entrypoint.py, manifest.json, result.json}`.
- Implement orchestrator functions:
  - `submit_execution_via_jobs_api(payload) -> databricks_run_id, run_url`
  - `poll_run(databricks_run_id) -> terminal_status`
  - `ingest_result(run_id, iter_n) -> capsule_delta`
- Enforce compute separation:
  - small/cheap cluster for Job A
  - policy-controlled job clusters for Job B (possibly multiple executor variants by workload type)

If you tell me (1) whether you want synchronous wait vs async, (2) whether Job B should be a single generic executor or multiple routed executor tasks, and (3) whether Workspace notebooks are mandatory artifacts, I can provide an explicit `runs/submit` JSON template and the exact result contract schema.