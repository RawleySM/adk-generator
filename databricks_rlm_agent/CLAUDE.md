# Databricks RLM Agent - Agent Primer

This document provides context for AI agents working with the Databricks RLM Agent codebase.

---

## Overview

The Databricks RLM Agent is a **Recursive Language Model (RLM) workflow** orchestrator implementing a three-job pattern for AI-driven data discovery and code execution:

| Job | Name | Purpose |
|-----|------|---------|
| **Job_A** | Orchestrator | Control plane - runs the agent loop, generates code, delegates execution |
| **Job_B** | Executor | Execution plane - runs generated artifacts, captures output |
| **Job_C** | Ingestor | Trigger plane - polls Jira tasks, triggers Job_A via Jobs API |

This architecture separates agent control flow from code execution, enabling **durable session state**, **artifact persistence**, and **iterative refinement**.

---

## Two Execution Modes

### Databricks Mode (`ADK_RUN_MODE=databricks`)

Production mode that runs as Databricks Jobs:
- **SessionService**: `DeltaSessionService` (persists to UC Delta tables)
- **Backend**: `DatabricksBackend` (submits Job_B via Jobs API)
- **Data Access**: PySpark `spark` session
- **Telemetry**: `UcDeltaTelemetryPlugin` (writes to Delta)
- **Deployment**: Via `scripts/deploy_rlm_two_job_bundle.sh`

### Local Mode (`ADK_RUN_MODE=local`)

Development mode that runs locally:
- **SessionService**: `LocalSessionService` (DuckDB backend)
- **Backend**: `LocalBackend` (subprocess execution)
- **Data Access**: `execute_sql()` via SQL Warehouse API
- **Telemetry**: `LocalTelemetryPlugin` (DuckDB + stdout)
- **Execution**: Via `scripts/run_local_rlm.sh`

---

## Key Entry Points

### CLI Commands (`cli.py`)

```bash
rlm-orchestrator    # Job_A - main agent loop
rlm-executor        # Job_B - artifact execution harness
rlm-ingestor        # Job_C - CDF polling + triggering
rlm-test            # Direct test runner (bypasses ingestor)
```

### Main Flow (`run.py`)

1. Load secrets via `load_secrets()` (API keys from Databricks Secrets)
2. Determine run mode from `ADK_RUN_MODE`
3. Create appropriate session service (Delta or Local)
4. Create ADK `Runner` with plugins
5. Execute `run_conversation()` with event streaming + timeout handling
6. Return `ConversationResult` with status, response, delegation count

---

## Agent Architecture

### Agent Graph (LoopAgent)

```
LoopAgent (orchestrator_loop)
├── databricks_analyst (LlmAgent)
│   └── calls delegate_code_results() → transfers to job_builder
├── job_builder (BaseAgent - deterministic)
│   └── submits Job_B, waits, writes results → transfers to results_processor
└── results_processor (LlmAgent)
    └── analyzes stdout/stderr → loop continues or exit_loop
```

### Plugins (Ordered Execution)

| # | Plugin | Purpose |
|---|--------|---------|
| 1 | `UcToolExecutionSafetyPlugin` | Block destructive SQL/shell patterns |
| 2 | `FormattingCheckPlugin` | Validate delegation blob format |
| 3 | `CodeLintingPlugin` | Validate Python syntax before execution |
| 4 | `UcDeltaTelemetryPlugin` | Record events to Delta table |
| 5 | `GlobalInstructionPlugin` | Inject system prompts |
| 6 | `RlmContextInjectionPlugin` | Inject execution results + stage tracking |

---

## File Structure

```
databricks_rlm_agent/
├── agent.py              # Root agent graph + plugin setup
├── run.py                # Main execution flow + ConversationResult
├── cli.py                # CLI entry points (orchestrator/executor/ingestor)
├── executor.py           # Job_B execution harness
├── ingestor.py           # Job_C CDF polling + Jira integration
├── execution_backend.py  # DatabricksBackend + LocalBackend
├── prompts.py            # System prompts for all modes
├── secrets.py            # Databricks Secrets loading
├── telemetry.py          # Telemetry event definitions
│
├── sessions/
│   ├── delta_session_service.py  # UC Delta persistence
│   └── local_session_service.py  # DuckDB persistence
│
├── artifact_registry.py       # Delta artifact tracking
├── artifact_registry_local.py # DuckDB artifact tracking
│
├── tools/
│   ├── delegate_code_results.py   # Core RLM delegation tool
│   ├── exit_loop.py               # LoopAgent exit tool
│   ├── metadata_keyword_search.py # UC metadata discovery
│   ├── repo_filename_search.py    # GitHub repo search
│   ├── get_repo_file.py           # GitHub file download
│   └── save_artifact_to_volumes.py # Non-executable artifact storage
│
├── agents/
│   ├── job_builder.py        # Deterministic Job_B submission
│   └── results_processor.py  # Execution output analysis
│
├── plugins/
│   ├── rlm_context_injection_plugin.py # Results injection
│   ├── uc_tool_execution_safety_plugin.py # Safety checks
│   ├── global_instruction_plugin.py # Prompt injection
│   ├── formatting_check_plugin.py # Delegation format validation
│   └── code_linting_plugin.py # Python syntax validation
│
├── modeling/
│   └── model_factory.py   # Gemini/LiteLLM model selection
│
└── utils/
    └── state_helpers.py   # State key helpers
```

---

## RLM Workflow: Delegation Pattern

### delegate_code_results Tool

**Input Format:**
```python
'''Instruction for results_processor_agent (triple-quoted string)'''
<python code to execute>
```

**What It Does:**
1. Parses delegation blob → extract instruction + code
2. Saves code to ADK ArtifactService
3. Creates artifact registry entry
4. Sets invocation-scoped state keys:
   - `temp:rlm:artifact_id` - for job_builder to read
   - `temp:rlm:sublm_instruction` - for results_processor
   - `temp:rlm:stage` = "delegated" - stage gating
5. Transfers to `job_builder` agent

### Stage Tracking State Machine

```
Initial: (no stage)
   ↓
delegate_code_results: stage = "delegated"
   ↓
job_builder: stage = "executed"
   ↓
RlmContextInjectionPlugin checks stage
   ↓
results_processor runs, then: stage = "processed"
   ↓
LoopAgent continues or exit_loop
```

---

## Session State Scoping

### State Key Prefixes

| Prefix | Scope | Persistence |
|--------|-------|-------------|
| `temp:rlm:*` | Invocation | Auto-discarded after invocation |
| `app:*` | Application | Persists app-wide |
| `user:*` | User | Persists per-user |
| *(no prefix)* | Session | Persists per-session |

**Critical:** `temp:*` keys are NOT persisted. Use for inter-agent communication within a single invocation.

---

## Environment Variables

### Core Configuration

```bash
ADK_RUN_MODE=databricks          # or "local"
ADK_DELTA_CATALOG=silo_dev_rs
ADK_DELTA_SCHEMA=adk
ADK_MAX_ITERATIONS=10
ADK_EXECUTOR_JOB_ID=12345        # Job_B ID (for Job_A)
ADK_ORCHESTRATOR_JOB_ID=67890    # Job_A ID (for Job_C)
```

### Model Selection

```bash
ADK_MODEL_PROVIDER=gemini        # or "litellm"
ADK_GEMINI_MODEL=gemini-3-pro-preview
ADK_LITELLM_MODEL=openai/gpt-4o
ADK_FALLBACK_ON_BLOCKED=true     # Fallback on content policy errors
```

### Local Mode

```bash
ADK_LOCAL_DB_PATH=.adk_local/adk.duckdb
ADK_LOCAL_ARTIFACTS_PATH=.adk_local/artifacts
ADK_SQL_WAREHOUSE_ID=<warehouse-id>  # Optional, auto-discovers
```

### Secrets

```bash
ADK_SECRET_SCOPE=adk-secrets     # Databricks secret scope
# Secrets loaded: google-api-key, openai-api-key, github-token
```

---

## Deployment Scripts

### Deploy to Databricks (`scripts/deploy_rlm_two_job_bundle.sh`)

**Workflow:**
1. Load `.env` configuration
2. Check and start cluster (10 min timeout)
3. Clear cached wheel versions on cluster
4. Bump wheel version (cache-busting)
5. Clear build caches
6. Validate and deploy bundle
7. Resolve all three job IDs
8. Ensure secret scope and secrets exist
9. Store job IDs in secret scope
10. Wire executor job ID into orchestrator job parameters
11. Wire orchestrator job ID into ingestor job parameters
12. Optionally trigger a run

**Usage:**
```bash
./scripts/deploy_rlm_two_job_bundle.sh [OPTIONS]

Options:
  --skip-deploy           Skip deployment, just run the job
  --skip-cluster-check    Skip cluster check/start
  --skip-cache-clear      Skip clearing cached wheel versions
  --run                   Trigger orchestrator job after deploy
  --test-level <N>        Pass TEST_LEVEL=<N> to the job (requires --run)
  --force-update-secrets  Always overwrite job ID secrets
```

### Run Locally (`scripts/run_local_rlm.sh`)

**Workflow:**
1. Load `.env` configuration
2. Check dependencies (Python, uv, databricks CLI)
3. Verify environment variables (GOOGLE_API_KEY required)
4. Authenticate to Databricks
5. Health check SQL Warehouse (auto-discover if not set)
6. Resolve prompt (--test-level, --prompt-file, --prompt)
7. Generate session ID
8. Execute agent with `ADK_RUN_MODE=local`

**Usage:**
```bash
./scripts/run_local_rlm.sh [OPTIONS]

Options:
  --test-level <N>        Load prompt from test_tasks.py level N (1-17)
  --prompt-file <path>    Read prompt from a local file
  --prompt <string>       Use literal prompt string
  --session-id <id>       Session ID (default: auto-generated)
  --max-iterations <N>    Maximum loop iterations
  --profile <name>        Databricks CLI profile (default: rstanhope)
  --warehouse-id <id>     SQL Warehouse ID (default: auto-discover)
  --checks-only           Run checks only, don't start the agent
  --trace                 Enable bash tracing
```

---

## Critical Patterns & Gotchas

### Pattern 1: Invocation Scoping
```python
# CORRECT - Invocation-scoped (auto-discarded)
context.state["temp:rlm:artifact_id"] = artifact_id

# WRONG - Session-scoped (persists unwanted)
context.state["rlm:artifact_id"] = artifact_id
```

### Pattern 2: Stage Gating
```python
# Always check stage before acting
if state.get("temp:rlm:stage") == "executed":
    # Load execution results
```

### Pattern 3: Agent Transfer vs Escalate
```python
# CORRECT - Transfer control to next sub-agent (continues LoopAgent)
context.actions.transfer_to_agent = "job_builder"

# WRONG - Don't use escalate for delegation (terminates LoopAgent)
# context.actions.escalate = True
```

### Pattern 4: Job Parameters to Env Vars
```python
# In orchestrator_main(), materialize BEFORE importing agent
os.environ["ADK_DELTA_CATALOG"] = args.catalog
os.environ["ADK_EXECUTOR_JOB_ID"] = executor_job_id

# Then import agent (reads os.environ at import time)
from .agent import root_agent
```

### Pattern 5: Spark.stop() Prevention
```python
# In executor, patch spark.stop() to no-op
original_stop = spark.stop
spark.stop = lambda: logger.warning("ignoring spark.stop()")
```

---

## Testing & Debugging

### Test Runner

```bash
rlm-test --level 3                    # Run test task level 3
rlm-test --level 5 --max-iterations 5 # With custom iterations
rlm-test --list                       # List all test tasks
```

### Debug Queries

**Check session state:**
```sql
SELECT * FROM silo_dev_rs.adk.sessions
WHERE session_id = 'session_001'
ORDER BY created_time DESC LIMIT 1
```

**Review artifact registry:**
```sql
SELECT artifact_id, status, sublm_instruction, created_time
FROM silo_dev_rs.adk.artifact_registry
WHERE session_id = 'session_001'
```

**Check telemetry:**
```sql
SELECT event_type, component, metadata, timestamp
FROM silo_dev_rs.adk.adk_telemetry
WHERE run_id = 'session_001'
ORDER BY timestamp DESC
```

---

## Extension Points

### Add a New Tool

1. Create `tools/my_new_tool.py`
2. Register in `agent.py` under `databricks_analyst` tools

### Add a New Plugin

1. Create `plugins/my_plugin.py` extending `BasePlugin`
2. Register in `agent.py` plugins list

### Add a New Sub-Agent

1. Create `agents/my_agent.py` extending `BaseAgent`
2. Register in `agent.py` under `root_agent` sub_agents

---

## Quick Reference

| What | Where |
|------|-------|
| Agent graph definition | `agent.py` |
| Main execution flow | `run.py` |
| CLI entry points | `cli.py` |
| Delegation tool | `tools/delegate_code_results.py` |
| Job_B execution | `executor.py` |
| Session persistence (Databricks) | `sessions/delta_session_service.py` |
| Session persistence (Local) | `sessions/local_session_service.py` |
| Execution backends | `execution_backend.py` |
| System prompts | `prompts.py` |
| Model factory | `modeling/model_factory.py` |
| Results injection | `plugins/rlm_context_injection_plugin.py` |
