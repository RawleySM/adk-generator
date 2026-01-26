# Issue 004: Remove RlmContextPruningPlugin - Implementation Summary

**Date:** 2026-01-26
**Issue:** `issues/004_remove_RlmContextPruningPlugin.md`
**Status:** Implemented, pending verification

## Overview

Removed `RlmContextPruningPlugin` by replacing its two core responsibilities with explicit state mechanics:

1. **Staleness Prevention**: Replaced bulk state clearing with a stage-based state machine (`temp:rlm:stage`)
2. **Artifact Lifecycle**: Moved "mark consumed" to `JobBuilderAgent` after successful execution

## Phase 0: Semantics Decision

**"Consumed" Definition:** An artifact is marked "consumed" when `JobBuilderAgent` completes execution successfully (`status == "completed"`).

This is a **behavior change** from the prior implementation where `RlmContextPruningPlugin` marked artifacts consumed after `results_processor` completed analysis. The new semantics ("consumed == executed successfully") better reflects the artifact lifecycle: consumption happens at the execution boundary, not the analysis boundary.

## Changes by Phase

### Phase 1: Stage Tracking State Machine

Introduced `temp:rlm:stage` with three states: `delegated` → `executed` → `processed`

Each component now checks the stage before acting, preventing stale state from triggering work in later LoopAgent iterations.

#### `databricks_rlm_agent/tools/delegate_code_results.py`

```python
# New state key constants
STATE_STAGE = "temp:rlm:stage"
STATE_ACTIVE_ARTIFACT_ID = "temp:rlm:active_artifact_id"

# After setting existing state keys:
tool_context.state[STATE_STAGE] = "delegated"
tool_context.state[STATE_ACTIVE_ARTIFACT_ID] = artifact_id
```

#### `databricks_rlm_agent/agents/job_builder.py`

```python
# New state key constants
STATE_STAGE = "temp:rlm:stage"
STATE_ACTIVE_ARTIFACT_ID = "temp:rlm:active_artifact_id"

# Stage gating in _run_async_impl:
current_stage = ctx.session.state.get(STATE_STAGE)
if current_stage != "delegated":
    # Skip - not in correct stage
    return

# After successful execution:
set_state(STATE_STAGE, "executed")
```

#### `databricks_rlm_agent/plugins/rlm_context_injection_plugin.py`

```python
# New state key constants
STATE_STAGE = "temp:rlm:stage"
STATE_ACTIVE_ARTIFACT_ID = "temp:rlm:active_artifact_id"

# Stage gating in before_agent_callback:
current_stage = callback_context.state.get(STATE_STAGE)
if current_stage != "executed":
    # Skip injection - not in correct stage
    return None

# New after_agent_callback to complete the stage machine:
async def after_agent_callback(self, *, callback_context, **kwargs):
    if callback_context.agent_name == self._target_agent_name:
        if callback_context.state.get(STATE_STAGE) == "executed":
            callback_context.state[STATE_STAGE] = "processed"
```

### Phase 2: Persist results_processor Output

Made the results_processor output explicitly persist to session state instead of relying on prompt-history carry.

#### `databricks_rlm_agent/agent.py`

```python
results_processor_agent = LlmAgent(
    name="results_processor",
    model=_agent_model,
    output_key="rlm:last_results_summary",  # NEW: Persist output to session state
    instruction="""...""",
)
```

#### `databricks_rlm_agent/prompts.py`

```python
# New section for state templating
RESULTS_SUMMARY_SECTION = """
## Prior Results Context
{rlm:last_results_summary?}
"""

# Updated ROOT_AGENT_INSTRUCTION
ROOT_AGENT_INSTRUCTION = RLM_SYSTEM_PROMPT + "\n" + HEALTHCARE_VENDOR_EXTENSION + RESULTS_SUMMARY_SECTION
```

### Phase 3: Move "mark consumed" to JobBuilderAgent

Artifact consumed marking now happens in JobBuilderAgent after successful execution (semantics: "consumed" == "executed successfully").

#### `databricks_rlm_agent/agents/job_builder.py`

```python
def _update_artifact_registry(self, artifact_id, stdout, stderr, status, result_json_path=None):
    # ... existing update logic ...

    # NEW: Mark artifact as consumed on successful execution
    if status == "completed":
        try:
            registry.mark_consumed(artifact_id)
            logger.info(f"[JOB_BUILDER] Marked artifact {artifact_id} as consumed")
        except Exception as e:
            logger.warning(f"[JOB_BUILDER] Could not mark artifact as consumed: {e}")
```

### Phase 4: Remove RlmContextPruningPlugin

#### Files Modified

- `databricks_rlm_agent/plugins/__init__.py` - Removed import/export of `RlmContextPruningPlugin`
- `databricks_rlm_agent/agent.py` - Removed import, instantiation, and plugin list entry
- `databricks_rlm_agent/run.py` - Removed import and plugin list entry, updated comments

#### Files Deleted

- `databricks_rlm_agent/plugins/rlm_context_pruning_plugin.py`

## State Contract (Final)

| Key | Writer | Reader | Scope |
|-----|--------|--------|-------|
| `temp:rlm:stage` | delegate_code_results ("delegated"), JobBuilderAgent ("executed"), RlmContextInjectionPlugin ("processed") | JobBuilderAgent, RlmContextInjectionPlugin | Invocation (NOT persisted) |
| `temp:rlm:active_artifact_id` | delegate_code_results | JobBuilderAgent, RlmContextInjectionPlugin | Invocation (NOT persisted) |
| `temp:rlm:artifact_id` | delegate_code_results | JobBuilderAgent, RlmContextInjectionPlugin | Invocation (NOT persisted) |
| `temp:rlm:*` execution keys | JobBuilderAgent | RlmContextInjectionPlugin | Invocation (NOT persisted) |
| `rlm:iteration` | delegate_code_results | Various | Session (persisted) |
| `rlm:last_results_summary` | results_processor (via output_key) | databricks_analyst (via instruction templating) | Session (persisted) |

## Verification Checklist

> **Note:** Mark items `[x]` only after running verification and recording evidence below.

- [ ] **No pruning plugin wired**: `RlmContextPruningPlugin` is not constructed and not in the plugin list
  - *Verify:* `grep -r "RlmContextPruningPlugin" databricks_rlm_agent/` returns no active imports/instantiations
  - *Evidence:* (pending)
- [ ] **Durable handoff explicit**: After results_processor runs, `state_delta_json` contains `rlm:last_results_summary`
  - *Verify:* Run SQL Query 1 below; expect rows with `rlm:last_results_summary` in `state_delta_json`
  - *Evidence:* (pending - paste query output or link to results)
- [ ] **No stale trigger regressions**: Repeated LoopAgent iterations do not re-run job_builder/results_processor unless a fresh delegation occurred
  - *Verify:* Review workflow logs showing stage gating skip messages; or run multi-iteration test
  - *Evidence:* (pending - note: `temp:` keys not in SQL; verify via logs)
- [ ] **Artifact lifecycle preserved**: Artifacts transition to "consumed" after successful execution
  - *Verify:* Query `artifact_registry` for `consumed_time IS NOT NULL` after successful runs
  - *Evidence:* (pending)

## SQL Verification Queries

```sql
-- Verify results are persisted to session state
SELECT
    session_id,
    agent_name,
    state_delta_json
FROM silo_dev_rs.adk.events
WHERE state_delta_json LIKE '%rlm:last_results_summary%'
ORDER BY timestamp DESC
LIMIT 10;

-- Verify temp:* keys are NOT persisted (should return 0 rows)
SELECT COUNT(*)
FROM silo_dev_rs.adk.events
WHERE state_delta_json LIKE '%temp:%';
```

## Architecture Diagram

```
                    ┌─────────────────────────────────────────────────────────────────┐
                    │                        LoopAgent                                 │
                    │                                                                  │
                    │  ┌──────────────────┐    ┌──────────────────┐    ┌────────────┐ │
                    │  │databricks_analyst│───►│   job_builder    │───►│ results_   │ │
                    │  │                  │    │                  │    │ processor  │ │
                    │  └──────────────────┘    └──────────────────┘    └────────────┘ │
                    │         │                       │                      │        │
                    │         ▼                       ▼                      ▼        │
                    │  stage="delegated"      stage="executed"       stage="processed"│
                    │                                                                  │
                    │  [If stage != current expected, component skips execution]      │
                    └─────────────────────────────────────────────────────────────────┘
```

## Migration Notes

- The `temp:rlm:*` keys continue to be invocation-scoped and auto-discarded by `DeltaSessionService`
- Legacy `rlm:*` glue keys (except `rlm:iteration` and new session keys) can be removed in Phase 5
- The dual-read pattern via `get_rlm_state()` remains for backward compatibility during migration
