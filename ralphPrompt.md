
## Test Databricks_Rlm_Agent end-to-end for Real Task 

```
ralph:loop --max-iterations 15 --completion-promise "All deliverables created in silo_dev_rs.test schema and task DATA-792 marked complete"
```



```markdown
## Objective

Run @scripts/deploy_rlm_two_job_bundle.sh to execute test level 12 (DATA-792) via the RLM Agent and iterate until the task is completed end-to-end without failure.

## Pre-Execution: Locate the Jira Ticket

Before beginning iterations, query the Jira source of truth to confirm task details:

```bash
uv run scripts/execute_sql.py select silo_dev_rs.task.jira_raw_data \
  -w "key = 'DATA-792'" \
  -c "key, summary, description, status"
```

Verify the ticket exists and capture the full description for reference.

---

## Issue Documentation Protocol

⚠️ **MANDATORY**: Document every issue encountered and fixed during iteration.

### Setup

Ensure the issues folder exists:

```bash
mkdir -p ./issues
```

### Documentation Requirements

For **every issue** you encounter and fix, create a markdown document in `./issues/` with:

**Naming Convention**: `NNN_descriptive-title.md`
- `NNN` = Zero-padded sequential number (001, 002, 003, ...)
- `descriptive-title` = Kebab-case summary of the issue

**Examples**:
- `001_executor-timeout-on-large-queries.md`
- `002_missing-column-in-unsubmitted-vendors-view.md`
- `003_api-auth-token-expiration.md`

### Issue Document Template

```markdown
# Issue NNN: [Descriptive Title]

**Date**: YYYY-MM-DD
**Iteration**: N of 5
**Severity**: Critical | High | Medium | Low

## Problem

[Clear description of what went wrong]

## Root Cause

[Analysis of why it happened]

## Symptoms

- [Error message or behavior observed]
- [Relevant log snippets]

## Fix Applied

[Detailed description of the patch/change made]

### Files Modified

- `path/to/file1.py` - [brief change description]
- `path/to/file2.sql` - [brief change description]

### Code Changes

```diff
- old code
+ new code
```

## Verification

[How you confirmed the fix worked]

## Prevention

[Recommendations to prevent recurrence, if applicable]
```

### Issue Tracking Workflow

1. **On error/failure**: Immediately create a new issue document before attempting a fix
2. **Document the fix**: Update the document with your patch details
3. **Verify fix worked**: Add verification evidence to the document
4. **Git commit the patch**: Commit all changes with a descriptive message referencing the issue number
5. **Proceed to next iteration**: Only after documentation and commit are complete

### Git Commit Protocol

⚠️ **MANDATORY**: Every significant issue that warrants a patch markdown file MUST be committed to git.

**Commit immediately after**:
- Creating/updating an issue document in `./issues/`
- Applying code fixes related to the documented issue
- Updating the issue index (`./issues/README.md`)

**Commit Message Format**:
```
fix(issue-NNN): [Brief description of the fix]

Resolves issue documented in ./issues/NNN_descriptive-title.md

- [Summary of changes made]
- [Files affected]
```

**Example**:
```bash
git add ./issues/002_missing-column-in-unsubmitted-vendors-view.md
git add path/to/fixed/file.py
git commit -m "fix(issue-002): Add missing column to unsubmitted vendors view

Resolves issue documented in ./issues/002_missing-column-in-unsubmitted-vendors-view.md

- Added spend_volume_rank column to view definition
- Updated SQL query in executor.py"
```

**Why this matters**:
- Preserves a traceable history of fixes applied during iteration
- Enables rollback if a fix introduces regressions
- Creates atomic changesets that can be cherry-picked or reverted
- Documents the relationship between issue files and code changes

### Issue Index

After each iteration, update `./issues/README.md` with:

```markdown
# Issue Log - DATA-792 (Test Level 12)

| # | Title | Severity | Status | Iteration |
|---|-------|----------|--------|-----------|
| 001 | [Title] | [Sev] | Fixed | 1 |
| 002 | [Title] | [Sev] | Fixed | 2 |
```

---

## Loop Task: Deploy & Run Test Level 12

Execute the canonical deployment + run command:

```bash
./scripts/deploy_rlm_two_job_bundle.sh --run --test-level 12
```

### Task Summary (DATA-792)

**MVM - Create/Modify SM Common submit script**

Create a script and Databricks job that submits unsubmitted vendors from SM Common to Master Vendor Management's submission API with configurable parameters:

| Parameter            | Default Value | Description                                      |
|----------------------|---------------|--------------------------------------------------|
| `min_volume`         | $5000         | Minimum spend threshold                          |
| `spend_volume_rank`  | 95%           | Top percentile of vendor spend                   |
| `is_person`          | false         | Exclude vendors marked as persons                |

### Required Deliverables

1. **Source Table Identification**: Identify correct UC tables/views for:
   - Unsubmitted vendors: `silo_{code}.sm.export_mvm__unsubmitted_vendors`
   - Submitted jobs mapping: `main.mvm.customer_jobs`

2. **Script Logic Specification**: Document the full workflow:
   - Query unsubmitted vendors with filtering logic
   - Submit to MVM API
   - Append customer/job mapping to MVM Submitted Jobs

3. **Databricks Job Configuration Spec**: Define job parameters, cluster config, and scheduling

---

## CRITICAL CONSTRAINTS

### Schema Restrictions (MANDATORY)

⚠️ **ALL deliverables MUST be created in `silo_dev_rs.test` schema**

- ✅ `silo_dev_rs.test.unsubmitted_vendors_view`
- ✅ `silo_dev_rs.test.mvm_submission_staging`
- ✅ `silo_dev_rs.test.customer_jobs`
- ❌ **NO views or tables in production schemas** (e.g., `silo_dev_rs.sm.*`, `main.mvm.*`)

Views will only be promoted to production after human review.

### Success Criteria

The agent MUST:
1. Complete all acceptance criteria from DATA-792
2. Create any required views/tables ONLY in `silo_dev_rs.test`
3. Save artifacts to `/Volumes/silo_dev_rs/adk/artifacts/`
4. Document ALL issues encountered in `./issues/`
5. **Git commit every significant fix** with issue reference in commit message
6. Exit cleanly via `exit_loop` tool with success status

### Failure Handling

If the agent fails or errors:
1. **FIRST**: Document the issue in `./issues/NNN_descriptive-title.md`
2. Review the run output in `runtime_monitor/`
3. Check session state: `silo_dev_rs.adk.sessions`
4. Check event log: `silo_dev_rs.adk.events`
5. Apply fix and document in the issue file
6. **Git commit the fix** with issue reference (see Git Commit Protocol above)
7. Iterate with `./scripts/deploy_rlm_two_job_bundle.sh --run --test-level 12`

---

## Post-Completion Verification

After successful completion, verify deliverables:

```bash
# Check test schema for created views
uv run scripts/execute_sql.py --sql "SHOW VIEWS IN silo_dev_rs.test"

# Verify no production writes occurred
uv run scripts/execute_sql.py history silo_dev_rs.sm.export_mvm__unsubmitted_vendors --limit 1

# Check artifact output
ls -la /Volumes/silo_dev_rs/adk/artifacts/

# Verify issue documentation
ls -la ./issues/

# Verify git commits for issues (should see fix(issue-NNN) commits)
git log --oneline --grep="fix(issue-" -10
```

---

## Exit Condition

Loop terminates when:
- Agent completes DATA-792 with all deliverables in `silo_dev_rs.test`
- No errors in run output
- All encountered issues documented in `./issues/`
- All significant fixes committed to git with issue references
- `exit_loop` called with success status
- OR max iterations (5) reached
```

---

### Key Points (Updated)

| Aspect | Value |
|--------|-------|
| Test Level | 12 |
| Jira Ticket | DATA-792 |
| Target Schema | `silo_dev_rs.test` (NOT production) |
| Max Iterations | 5 |
| Deploy Command | `./scripts/deploy_rlm_two_job_bundle.sh --run --test-level 12` |
| Jira Query | `uv run scripts/execute_sql.py select silo_dev_rs.task.jira_raw_data -w "key = 'DATA-792'"` |
| **Issue Docs** | `./issues/NNN_descriptive-title.md` |
| **Issue Index** | `./issues/README.md` |
| **Git Commit** | `fix(issue-NNN): [description]` for every patch |

⚠️ **This task is not the Jira Ticket.  It is to debug and prove out the databricks_rlm_agent thas is assigned the task**

---

## 🔬 Developer Observability & Evaluation Framework

The `databricks_rlm_agent` is deployed as a **developer agent** handling sensitive data operations. Beyond just making Google ADK agents work in Databricks jobs, you must facilitate **comprehensive observability, persistence, and documentation** to ensure safe, auditable, and reproducible agent behavior.

> 💡 **Build as You Go**: When you encounter unforeseen challenges, **pause and implement improvements** before continuing. If you find yourself repeatedly running the same queries, create a utility script. If debugging is tedious, build a tool to automate it. Your role is not just to run tests—it's to evolve the evaluation infrastructure so future iterations are faster and more reliable. Leave the codebase better than you found it.

### Core Observability Tables (`silo_dev_rs.adk`)

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `sessions` | Session metadata & state | `session_id`, `app_name`, `state_json`, `version` |
| `events` | Event history per session | `event_id`, `session_id`, `event_data_json`, `state_delta_json` |
| `app_states` | Application-level state | `app_name`, `state_json`, `version` |
| `user_states` | User-level state | `app_name`, `user_id`, `state_json` |
| `telemetry` | High-level orchestrator/executor events | `event_type`, `component`, `run_id`, `metadata_json` |
| `adk_telemetry` | Detailed callback-level telemetry | `callback_name`, `tool_name`, `tool_blocked`, `payload_json` |

### Essential Observability Queries

```bash
# Recent sessions
uv run scripts/execute_sql.py select silo_dev_rs.adk.sessions -l 10 -c "session_id, app_name, update_time, version"

# Events for a specific session
uv run scripts/execute_sql.py --sql "SELECT event_id, author, event_timestamp, substring(event_data_json, 1, 200) as preview FROM silo_dev_rs.adk.events WHERE session_id = '<SESSION_ID>' ORDER BY sequence_num ASC LIMIT 50"

# Tool execution telemetry (including blocked tools)
uv run scripts/execute_sql.py select silo_dev_rs.adk.adk_telemetry -l 20 -c "ts, callback_name, tool_name, tool_blocked, blocked_reason" -w "callback_name LIKE '%tool%'"

# High-level telemetry by run
uv run scripts/execute_sql.py --sql "SELECT event_type, component, timestamp, substring(metadata_json, 1, 300) FROM silo_dev_rs.adk.telemetry ORDER BY timestamp DESC LIMIT 20"

# Session state inspection
uv run scripts/execute_sql.py --sql "SELECT session_id, state_json FROM silo_dev_rs.adk.sessions WHERE session_id = '<SESSION_ID>'"
```

---

## 🛠️ Building Utility Scripts for Agent Evaluation

### Recommended Scripts to Create

Create reusable scripts in `scripts/adk_eval/` to streamline evaluation:

#### 1. `scripts/adk_eval/session_inspector.py`
Inspects session state evolution, event timeline, and state deltas.

```python
#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["databricks-sdk>=0.20.0", "click>=8.1.0", "rich>=13.0.0"]
# ///
"""Inspect ADK session state, events, and state evolution."""

import click
import json
from rich.console import Console
from rich.table import Table
from databricks.sdk import WorkspaceClient

@click.command()
@click.argument("session_id")
@click.option("--profile", "-p", default="rstanhope")
@click.option("--show-state-deltas", "-d", is_flag=True, help="Show state changes per event")
def inspect(session_id: str, profile: str, show_state_deltas: bool):
    """Inspect a session's events and state evolution."""
    console = Console()
    client = WorkspaceClient(profile=profile)
    
    # Query session and events
    # ... implementation ...
    
if __name__ == "__main__":
    inspect()
```

#### 2. `scripts/adk_eval/tool_audit.py`
Audit tool executions, blocked tools, and sensitive operations.

```python
#!/usr/bin/env -S uv run
"""Audit tool executions from adk_telemetry for safety analysis."""
# Key features:
# - List all tool calls with arguments
# - Highlight blocked tools and reasons
# - Flag sensitive operations (SQL writes, file operations)
# - Generate safety audit report
```

#### 3. `scripts/adk_eval/run_summary.py`
Generate comprehensive run summaries from telemetry.

```python
#!/usr/bin/env -S uv run
"""Generate a comprehensive summary of an agent run."""
# Key features:
# - Timeline of events
# - Tool call frequency and success rates
# - LLM token usage
# - State mutations summary
# - Error aggregation
```

#### 4. `scripts/adk_eval/compare_runs.py`
Compare two agent runs for regression detection.

```python
#!/usr/bin/env -S uv run
"""Compare telemetry between two runs for regression analysis."""
# Key features:
# - Event count comparison
# - Tool usage diff
# - State diff at completion
# - Token usage comparison
```

---

## 🎯 Agent Skill: ADK Observability

Create a skill at `~/.cursor/skills/adk-observability/SKILL.md`:

```markdown
# ADK Observability Skill

Use this skill when debugging, evaluating, or auditing databricks_rlm_agent runs.

## Capabilities
1. Query session/event/telemetry tables
2. Trace tool execution chains
3. Audit blocked tool attempts
4. Generate run summaries
5. Compare run behavior across iterations

## Quick Commands

### Session Inspection
\`\`\`bash
uv run scripts/execute_sql.py --sql "
  SELECT s.session_id, s.update_time, s.version,
         COUNT(e.event_id) as event_count
  FROM silo_dev_rs.adk.sessions s
  LEFT JOIN silo_dev_rs.adk.events e ON s.session_id = e.session_id
  GROUP BY s.session_id, s.update_time, s.version
  ORDER BY s.update_time DESC
  LIMIT 10
"
\`\`\`

### Tool Execution Audit
\`\`\`bash
uv run scripts/execute_sql.py --sql "
  SELECT tool_name, COUNT(*) as calls,
         SUM(CASE WHEN tool_blocked THEN 1 ELSE 0 END) as blocked_count
  FROM silo_dev_rs.adk.adk_telemetry
  WHERE callback_name = 'after_tool_callback'
  GROUP BY tool_name
  ORDER BY calls DESC
"
\`\`\`

### LLM Usage Analysis
\`\`\`bash
uv run scripts/execute_sql.py --sql "
  SELECT agent_name, model_name, COUNT(*) as requests,
         SUM(CAST(JSON_EXTRACT_SCALAR(payload_json, '$.usage_metadata.prompt_token_count') AS INT)) as input_tokens,
         SUM(CAST(JSON_EXTRACT_SCALAR(payload_json, '$.usage_metadata.candidates_token_count') AS INT)) as output_tokens
  FROM silo_dev_rs.adk.adk_telemetry
  WHERE callback_name = 'after_model_callback'
  GROUP BY agent_name, model_name
"
\`\`\`

### Error Analysis
\`\`\`bash
uv run scripts/execute_sql.py --sql "
  SELECT callback_name, tool_name,
         JSON_EXTRACT_SCALAR(payload_json, '$.error_type') as error_type,
         JSON_EXTRACT_SCALAR(payload_json, '$.error') as error_message,
         ts
  FROM silo_dev_rs.adk.adk_telemetry
  WHERE callback_name LIKE '%error%'
  ORDER BY ts DESC
  LIMIT 20
"
\`\`\`
```

---

## 📊 Automated Evaluation Checklist

Before marking any run as successful, verify:

### 1. Session Integrity
```bash
# Verify session created and persisted
uv run scripts/execute_sql.py --sql "SELECT COUNT(*) FROM silo_dev_rs.adk.sessions WHERE update_time > CURRENT_TIMESTAMP - INTERVAL 1 HOUR"

# Verify events persisted
uv run scripts/execute_sql.py --sql "SELECT COUNT(*) FROM silo_dev_rs.adk.events WHERE created_time > CURRENT_TIMESTAMP - INTERVAL 1 HOUR"
```

### 2. Tool Safety Audit
```bash
# Check for blocked tool attempts (safety plugin)
uv run scripts/execute_sql.py --sql "
  SELECT tool_name, blocked_reason, ts
  FROM silo_dev_rs.adk.adk_telemetry
  WHERE tool_blocked = TRUE
  ORDER BY ts DESC
  LIMIT 20
"

# Verify no unauthorized schema writes
uv run scripts/execute_sql.py --sql "
  SELECT tool_name, JSON_EXTRACT_SCALAR(payload_json, '$.arguments') as args
  FROM silo_dev_rs.adk.adk_telemetry
  WHERE tool_name LIKE '%sql%' OR tool_name LIKE '%execute%'
  ORDER BY ts DESC
  LIMIT 20
"
```

### 3. Artifact Verification
```bash
# Check artifacts saved to volumes
ls -la /Volumes/silo_dev_rs/adk/artifacts/

# Verify artifact metadata in session state
uv run scripts/execute_sql.py --sql "
  SELECT session_id, JSON_EXTRACT_SCALAR(state_json, '$.artifacts') as artifacts
  FROM silo_dev_rs.adk.sessions
  WHERE state_json LIKE '%artifact%'
  ORDER BY update_time DESC
  LIMIT 5
"
```

### 4. Error-Free Completion
```bash
# Check for error events in telemetry
uv run scripts/execute_sql.py --sql "
  SELECT callback_name, COUNT(*) as error_count
  FROM silo_dev_rs.adk.adk_telemetry
  WHERE callback_name LIKE '%error%'
    AND ts > CURRENT_TIMESTAMP - INTERVAL 1 HOUR
  GROUP BY callback_name
"
```

---

## 📝 Documentation Protocol for Agent Behavior

### Runtime Monitor Output

After each run, capture the runtime monitor output:

```bash
# Latest monitor file
ls -lt runtime_monitor/ | head -5

# Read latest monitor output
cat runtime_monitor/$(ls -t runtime_monitor/ | head -1)
```

### Session Event Export

Export session events for detailed analysis:

```bash
uv run scripts/execute_sql.py --sql "
  SELECT event_id, author, event_timestamp, event_data_json
  FROM silo_dev_rs.adk.events
  WHERE session_id = '<SESSION_ID>'
  ORDER BY sequence_num ASC
" > ./issues/session_<SESSION_ID>_events.json
```

### Telemetry Export for Analysis

```bash
uv run scripts/execute_sql.py --sql "
  SELECT * FROM silo_dev_rs.adk.adk_telemetry
  WHERE invocation_id = '<INVOCATION_ID>'
  ORDER BY ts ASC
" > ./issues/telemetry_<INVOCATION_ID>.json
```

---

## 🔄 Iteration Workflow with Observability

### Pre-Run Checklist
1. Note current session count: `SELECT COUNT(*) FROM silo_dev_rs.adk.sessions`
2. Note current event count: `SELECT COUNT(*) FROM silo_dev_rs.adk.events`
3. Clear runtime_monitor if needed: `rm -f runtime_monitor/*.md`

### Deploy & Run
```bash
./scripts/deploy_rlm_two_job_bundle.sh --run --test-level <N>
```

### Post-Run Analysis
1. Check runtime_monitor for output
2. Query new sessions/events
3. Audit tool executions
4. Check for blocked tools
5. Verify artifacts
6. Export telemetry if issues found
7. Document in `./issues/` if problems

### Telemetry-Driven Debugging

When debugging, use telemetry to trace the execution:

```bash
# Get the latest invocation
INVOCATION=$(uv run scripts/execute_sql.py --sql "
  SELECT invocation_id FROM silo_dev_rs.adk.adk_telemetry
  ORDER BY ts DESC LIMIT 1
" | grep -oP '[a-f0-9-]{36}')

# Trace all callbacks for this invocation
uv run scripts/execute_sql.py --sql "
  SELECT ts, callback_name, agent_name, tool_name,
         substring(payload_json, 1, 100) as payload_preview
  FROM silo_dev_rs.adk.adk_telemetry
  WHERE invocation_id = '$INVOCATION'
  ORDER BY ts ASC
"
```

---

## 🚀 Slash Commands to Build

Consider creating these slash commands for rapid evaluation:

### `/adk-status`
Quick health check of the ADK infrastructure.

### `/adk-session <session_id>`
Deep dive into a specific session with events and state.

### `/adk-audit`
Generate a safety audit report for the last N runs.

### `/adk-compare <run1> <run2>`
Compare two runs side-by-side.

---

## ⚠️ Sensitive Data Handling

The agent handles sensitive data. Ensure:

1. **No PII in logs**: Verify telemetry payload truncation
2. **Tool blocking works**: Safety plugin should block unauthorized operations
3. **Schema isolation**: All writes go to `silo_dev_rs.test` not production
4. **Audit trail complete**: Every tool call logged with arguments and results
5. **State sanitization**: Session state doesn't leak credentials