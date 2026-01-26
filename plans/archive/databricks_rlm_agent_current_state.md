# Databricks RLM Agent: Intended Flow vs Current Reality

This document compares the intended “RLM on Databricks Jobs” design in `plans/RLM_overview.md` to what is currently implemented under `databricks_rlm_agent/`. It is deliberately blunt because the current system behavior (especially around `llm_query`) does **not** match the architectural promise.

---

## Intended flow (per `plans/RLM_overview.md`)

The vision describes a **two-job RLM loop** that behaves like a secure “REPL”:

- **Job_A (Orchestrator / control plane)**: A long-lived “Base agent” that plans the next step, generates code, and decides what to run next. It should be careful with its context window and avoid ingesting long outputs directly.
- **Job_B (Executor / data plane)**: Runs generated code close to the data (Spark/SQL), producing stdout/stderr and persisted artifacts (tables/files).
- **SubLM agents (downstream)**: Disposable large-context agents that read/condense long outputs, chunk work, and return compact summaries to the Base agent.

The conceptual loop in the vision:

1. Base agent decides next step and **generates Python code**.
2. Code is persisted after a safety scan.
3. Job_B runs the code.
4. Outputs (especially long ones) are routed to downstream agents for summarization/extraction.
5. A compact “session capsule” / state summary is persisted and reloaded each iteration.
6. Repeat until termination.

Critically, `llm_query()` is described as a *tool that invokes a downstream LLM agent*, i.e. it should **return analysis**, not just write a file.

---

## Current state (what the code actually does)

What exists today is closer to:

- A **single ADK LoopAgent** (“orchestrator_loop”) that runs in Job_A, with two ADK agents registered (`databricks_analyst` and `llm_query_agent`).
- A **Python artifact runner** in Job_B that executes a `.py` file via `exec()` and captures stdout/stderr.
- A **CDF polling ingestor** (Job_C) that triggers Job_A runs for new Jira tasks via the Jobs API.

The “two-job loop” exists in skeleton form (Job_A submits Job_B and optionally iterates), but the **core RLM behavior** (REPL semantics, downstream-agent output digestion, and a working `llm_query` that returns LLM output) is not actually implemented.

---

## Jira task ingestion (`databricks_rlm_agent/ingestor.py`)

### What it does

`IngestorService` is a real CDF poller:

- Tracks a watermark in a Delta **state table** (`{catalog}.{schema}.ingestor_state`).
- Uses `table_changes(trigger_table, start_version, end_version)` and filters to `_change_type = 'insert'`.
- Attempts to locate an assignee column via a small heuristic list (e.g. `ASSIGNEE__assignee`, `assignee`, etc.).
- Constructs `JiraTask` objects with best-effort field extraction (`KEY`, `SUMMARY`, `DESCRIPTION`, `STATUS`, etc.).
- Triggers the orchestrator job via the Jobs API (`jobs.run_now`) with parameters like:
  - `ADK_SESSION_ID = jira_{issue_key}_{commit_version}`
  - `ADK_PROMPT = "Process JIRA task ..."`
  - `ADK_DELTA_CATALOG`, `ADK_DELTA_SCHEMA`, `ADK_TASK_ISSUE_KEY`

### Where it’s solid

- Watermarking and polling shape is reasonable for a CDF-driven trigger plane.
- The “trigger orchestrator with prompt + session id” concept matches the high-level intent.

### What’s missing / fragile

- **Schema drift**: The assignee/field extraction is heuristic and will quietly degrade (or process everything) when Jira table schema changes.
- **Deduplication / idempotency**: There is no strong “one Jira insert = one orchestrator run” guarantee beyond watermarking. If multiple inserts appear for the same issue, they will each trigger runs.
- **No feedback loop to Jira**: The ingestion creates work, but there is no code here to update Jira status/comments or write back outcomes.

---

## Agent orchestration (`databricks_rlm_agent/agent.py` + `prompts.py` + `run.py`)

### What it does

`agent.py` defines:

- `llm_query_agent`: an ADK `LlmAgent` intended to be a “specialist sub-agent”.
- `databricks_analyst`: the primary ADK `LlmAgent` with tools:
  - `save_python_code`
  - `save_artifact_to_volumes`
  - `llm_query`
  - `exit_loop`
- `root_agent`: an ADK `LoopAgent` with `max_iterations=10`, sub_agents `[databricks_analyst, llm_query_agent]`.

`prompts.py` defines a system prompt (`RLM_SYSTEM_PROMPT`) that claims a REPL-like environment exists:

- A `context` variable exists (catalog metadata / schemas / loaded data).
- `llm_query` “queries an LLM inside your REPL environment”.
- The agent can execute Python code by emitting ```repl blocks.

`run.py` wires ADK `Runner` + `DeltaSessionService` and streams events with timeouts.

### The harsh truth

The prompt describes a world that does not exist in this codebase.

- There is **no REPL execution tool** implemented that consumes ```repl blocks and executes them.
- There is **no `context` injection** mechanism shown here that populates the agent with Spark metadata or any “context variable”.
- The current `llm_query` tool is **not** an LLM query. It is a file write (see below).

In other words: the system prompt is effectively lying to the model. When the model follows it, it runs head-first into missing capabilities and starts thrashing.

### Looping behavior: why it happens

The LoopAgent will continue iterating unless:

- The agent chooses to call `exit_loop()` (which sets `tool_context.actions.escalate = True`), or
- The escalation plugin forces termination, or
- The orchestrator job ends (outside the ADK loop) due to lack of artifact / max iterations.

When the model believes `llm_query()` will return analysis text (as the prompt claims), and it instead gets back a **status dict**, it typically interprets that as “tool failed / didn’t answer” and tries again.

---

## Job transfer / orchestration loop (`databricks_rlm_agent/cli.py` + `jobs_api.py`)

### What it does

`cli.py` implements three entrypoints:

- `rlm-orchestrator` (Job_A)
- `rlm-executor` (Job_B)
- `rlm-ingestor` (Job_C)

The orchestrator path (`orchestrator_main` → `_run_orchestrator`) implements a basic loop:

1. Create/resume an ADK session (`DeltaSessionService`).
2. Call `run_conversation()` with the current prompt.
3. If the file at `save_python_code.AGENT_CODE_PATH` exists, submit Job_B via `jobs_api.submit_and_wait(...)`.
4. Load `result_{run_id}_iter{iteration}.json` or fall back to `jobs.get_run_output()` logs.
5. If execution failed, format feedback and run another iteration (up to `ADK_MAX_ITERATIONS`).

`jobs_api.py` is a straightforward wrapper around `databricks-sdk`:

- Tries default auth (run identity / default credentials), falls back to `DATABRICKS_HOST` + `DATABRICKS_TOKEN`.
- Submits Job_B with parameters `ARTIFACT_PATH`, `RUN_ID`, `ITERATION`, and optionally `ADK_DELTA_CATALOG/SCHEMA`.
- Polls until terminal state; fetches run output/logs.

### Where it aligns with the vision

- The “Job_A submits Job_B and waits” loop is real.
- Capturing stdout/stderr and feeding back to the next iteration is real (in a basic form).

### Where it diverges hard

- **No downstream summarization / SubLM pipeline**: output is fed back to the same agent prompt (`format_execution_feedback(...)`). The promised “downstream agents digest long outputs and return compact summaries” is not implemented.
- **No persisted “session capsule”**: there is session persistence (`DeltaSessionService`) and telemetry tables, but no deliberate compact state summary artifact that replaces long conversational accumulation.
- **Control-plane vs data-plane separation is weak**: because the prompt encourages massive output + sub-LLM usage, but the implementation doesn’t support that, Job_A ends up bloated and/or stuck.

---

## Job execution (`databricks_rlm_agent/executor.py`)

### What it does

Job_B is a “run a Python file” harness:

- Reads `artifact_path` from UC Volumes.
- Executes it via `exec(code, exec_globals)` with `spark`, `catalog`, `schema`, `run_id`, `iteration` injected.
- Captures stdout/stderr via `redirect_stdout` / `redirect_stderr`.
- Writes a JSON result file: `result_{run_id}_iter{iteration}.json` (either in artifact dir or `ADK_ARTIFACTS_PATH`).

### Hard constraints / risks

- **No real timeout enforcement**: there is a `timeout_seconds` parameter but it’s explicitly “not enforced in this version”.
- **No sandboxing**: it runs arbitrary Python code. The “safety” posture depends on upstream checks (and those checks are currently not consistently wired into the runner/plugins list).

---

## The `llm_query` failure: what’s actually broken

This is the root of the “everything gets stuck” behavior you called out.

### 1) `llm_query` is not an LLM query

`databricks_rlm_agent/tools/llm_query.py`:

- Writes the supplied `code` string to `AGENT_CODE_PATH`
- Returns `{ "status": "success", "file_path": ... }` (or `"error"`)

That is it.

There is **no call** to:

- `llm_query_agent`, or
- `google.genai` / `google.adk` model APIs to get an answer, or
- any “executor job” to run a special subagent script, or
- any artifact ingestion/parsing flow that turns outputs into structured LLM responses.

So when the prompt tells the model “use `llm_query` to query a sub-LLM with 500K context”, the model does exactly that and gets… a file write confirmation. This is a semantic failure even if the tool returns `"status": "success"`.

### 2) `llm_query` and `save_python_code` write to the same path

Both `save_python_code` and `llm_query` write to:

- `AGENT_CODE_PATH` (default `/Volumes/silo_dev_rs/adk/agent_code/agent_code_raw.py`)

This is catastrophic for the intended separation:

- In the vision, the artifact path should contain **executable code** meant for Job_B.
- In the prompt narrative, `llm_query` is for **analysis prompts / context dumps** to a sub-LLM.

In the current implementation, both concepts are fighting over the same file.

Practical consequences:

- If the agent uses `llm_query` as described (writing big analysis “code”), Job_B may later execute that file as Python and fail.
- If Job_B fails, the orchestrator feeds stderr back into the agent, which often leads to more tool calls, more overwrites, and more chaos.

### 3) The “llm_query escalation plugin” is a band-aid for a missing mechanism

`LlmQueryEscalationPlugin` tries to prevent runaway loops by escalating if `databricks_analyst` calls `llm_query` more than N times without `llm_query_agent` completing.

But there is no implemented bridge that makes “calling the `llm_query` tool” actually cause “the `llm_query_agent` sub-agent to run”.

So the plugin is essentially admitting the truth in code comments:

- “Prevents infinite loops where databricks_analyst keeps calling llm_query() without the sub-agent ever being invoked”

That’s exactly what’s happening, because the system currently has no working “sub-agent invocation pipeline” tied to that tool call.

---

## Summary: vision vs reality (no sugar-coating)

### What matches the vision (partially)

- **Multi-job topology exists**: there is an orchestrator job, executor job, and ingestor job.
- **Session persistence exists**: `DeltaSessionService` stores ADK sessions/events in Delta tables.
- **Telemetry exists**: both callback-level telemetry (`UcDeltaTelemetryPlugin`) and a simpler orchestrator/executor event table (`telemetry.py`) exist.
- **A basic “generate code → run code → feed back stdout/stderr” loop exists** in `cli.py`.

### What does not match the vision (the critical gaps)

- **The REPL described in the prompt does not exist.** There is no implementation of a `context` variable, no execution of ```repl blocks, and no tool that behaves like an interactive notebook cell runner.
- **`llm_query` is fundamentally misnamed/misimplemented.** It does not query an LLM. It writes a file. The prompt tells the model it returns LLM analysis, which is false.
- **Downstream/SubLM agents are not wired into the loop.** `llm_query_agent` exists as an ADK agent object but is not actually used as the “digest long outputs” worker described in `plans/RLM_overview.md`.
- **The system has no robust “long output routing” strategy.** The orchestrator just jams execution feedback back into the same agent prompt, which is the exact “context window rot” problem the design claims to solve.
- **The file path collision (`save_python_code` vs `llm_query`) breaks the core loop** by mixing “executable artifact” and “analysis prompt payload” into one mutable file.

### Why you’re stuck right now

Because the model is instructed to rely heavily on `llm_query`, but `llm_query` does not (and cannot, as currently written) return the analysis the model expects. The agent thrashes: repeated `llm_query` calls, overwriting the artifact file, and either escalating or failing execution downstream.

---

## Immediate, concrete next fixes (if you want this to behave like the vision)

Not implementing here (you didn’t ask), but to unblock the architecture:

- **Rename and split responsibilities**:
  - `save_python_code` stays as “write executable artifact for Job_B”
  - `llm_query` must either:
    - actually call an LLM directly and return analysis text, **or**
    - write a distinct “subagent request” artifact to a different path and have a real mechanism to run a downstream agent and return its output.
- **Stop writing both tool outputs to the same `AGENT_CODE_PATH`.** Use separate paths (`ADK_AGENT_CODE_PATH` vs `ADK_LLM_QUERY_PATH`) or per-iteration filenames.
- **Implement the “downstream output digestion” pipeline** (the core promise of the RLM overview):
  - route long stdout/stderr and large artifacts to the downstream agent(s)
  - persist condensed summaries and load them as the base agent’s iteration context

