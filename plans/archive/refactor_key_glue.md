# Refactor: Move invocation-only RLM glue to `temp:` state

## Problem
Today the RLM workflow uses session-scoped state keys (e.g. `rlm:artifact_id`) as **invocation glue** to pass information across the `LoopAgent` chain:

- `databricks_analyst` → `delegate_code_results()` sets `rlm:*`
- `job_builder` reads/writes `rlm:*`
- `RlmContextInjectionPlugin` gates + injects using `rlm:*`
- `RlmContextPruningPlugin` clears `rlm:*` *after* `results_processor`

Because these keys are session-scoped (and we use `DeltaSessionService` persistence), stale glue can leak across invocations if the chain aborts early (fatal error, timeout, etc.). This matches ADK’s warning: use `temp:` for invocation-only scratch state.

## ADK mechanics (why `temp:` is correct)
ADK state supports prefixes:

- `temp:` **invocation-scoped**, discarded after invocation, shared across sub-agents within the same invocation
- no-prefix **session-scoped**, persisted by persistent session services
- `user:` and `app:` are broader scopes

### Clarifications aligned with ADK docs
- During an invocation, your code sees a **single combined mapping** (`session.state` / `callback_context.state` / `tool_context.state`) that includes `temp:` keys and non-`temp:` keys.
- `temp:` is **not persistent** by design: you should **not** expect `temp:` keys to be visible if you later fetch the session from the `SessionService` after the invocation completes.
- ADK’s recommended state update mechanics are:
  - **Tools / plugins / callbacks**: mutate `tool_context.state[...]` or `callback_context.state[...]` (the framework will capture this in the event’s `state_delta`).
  - **Agents that manually yield `Event`s (e.g., `BaseAgent`)**: ensure you include the keys you want to propagate in `EventActions.state_delta`. Do not rely on mutating a `Session` fetched from the `SessionService` outside of the invocation lifecycle.

Our `DeltaSessionService` explicitly **ignores** `temp:*` for persistence (`_extract_state_delta`), which is desirable for glue: it must not survive into the next invocation. (This is consistent with ADK’s `temp:` semantics.)

## Target design
- **Use `temp:` for invocation glue** (keys that should not survive beyond one user turn / one invocation).
- **Keep only true cross-invocation values in session state** (e.g., counters like `rlm:iteration` if desired).

### Naming convention
Use `temp:rlm:<name>` for all invocation-only keys.

Examples:
- `temp:rlm:artifact_id`
- `temp:rlm:sublm_instruction`
- `temp:rlm:result_json_path`

## Key classification and mapping

### Invocation-only (move to `temp:rlm:*`)
These are purely pipeline glue and should not persist beyond one invocation:

- Delegation inputs
  - `rlm:artifact_id` → `temp:rlm:artifact_id`
  - `rlm:sublm_instruction` → `temp:rlm:sublm_instruction`
  - `rlm:has_agent_code` → `temp:rlm:has_agent_code`
  - `rlm:code_artifact_key` → `temp:rlm:code_artifact_key`
  - `rlm:session_id` → `temp:rlm:session_id` (likely optional; can be derived)
  - `rlm:invocation_id` → `temp:rlm:invocation_id` (likely optional; can be derived)

- Execution results (only needed to feed `results_processor`)
  - `rlm:execution_stdout` → `temp:rlm:execution_stdout`
  - `rlm:execution_stderr` → `temp:rlm:execution_stderr`
  - `rlm:execution_success` → `temp:rlm:execution_success`
  - `rlm:databricks_run_id` → `temp:rlm:databricks_run_id`
  - `rlm:run_url` → `temp:rlm:run_url`
  - `rlm:result_json_path` → `temp:rlm:result_json_path`
  - `rlm:stdout_truncated` → `temp:rlm:stdout_truncated`
  - `rlm:stderr_truncated` → `temp:rlm:stderr_truncated`

- Control flags (should never leak)
  - `rlm:exit_requested` → `temp:rlm:exit_requested`
  - `rlm:fatal_error` → `temp:rlm:fatal_error`
  - `rlm:fatal_error_msg` → `temp:rlm:fatal_error_msg`

Notes:
- `temp:parsed_blob` already exists and is correctly invocation-scoped.
- Artifact payloads themselves should remain in ArtifactService / UC Volumes / artifact registry; state only carries identifiers/paths.

### Session-scoped (keep as-is)
- `rlm:iteration` (counter across invocations). Keep session-scoped unless there’s a strong reason not to.

## Code changes (by file)

### 1) `databricks_rlm_agent/tools/delegate_code_results.py`
Change writes:
- Write delegation glue to `temp:rlm:*` instead of `rlm:*`.
- Keep `rlm:iteration` as session-scoped.
- Keep `temp:parsed_blob` as-is.

### 2) `databricks_rlm_agent/agents/job_builder.py`
Change reads/writes:
- Read delegation glue from `temp:rlm:*` (artifact_id, has_agent_code, code_artifact_key).
- Write execution outputs to `temp:rlm:*` (stdout preview, stderr preview, result_json_path, success flags, run metadata).

Remove/adjust misleading comment that “temp isn’t safe for cross-step transport”:
- `temp:*` is unsafe for *persistence across invocations*, but it is correct for *cross-sub-agent transport inside the same invocation*.
- Ensure the `BaseAgent` continues to emit these updates via `EventActions.state_delta` so that downstream steps (plugins and `results_processor`) observe them through `callback_context.state`.

### 3) `databricks_rlm_agent/plugins/rlm_context_injection_plugin.py`
Update gating + reads:
- Gate on `temp:rlm:artifact_id` (and/or fallback during migration).
- Prefer `temp:rlm:result_json_path` and `temp:rlm:execution_stdout|stderr`.

### 4) `databricks_rlm_agent/plugins/rlm_context_pruning_plugin.py`
After migration, most clearing becomes unnecessary (temp is auto-discarded).
- Keep optional “mark artifact consumed” behavior.
- Only clear session-scoped keys if any remain sticky (ideally none besides `rlm:iteration`).

During migration, clear both `rlm:*` and `temp:rlm:*` to be safe.

### 5) `databricks_rlm_agent/tools/exit_loop.py`
Write `temp:rlm:exit_requested=True` instead of `rlm:exit_requested=True`.

### 6) `databricks_rlm_agent/run.py`
Escalation detection:
- Check `temp:rlm:exit_requested`, `temp:rlm:fatal_error`, `temp:rlm:fatal_error_msg` in `state_delta`.

Cleanup block at invocation start:
- Once flags are temp-scoped, remove the “clear stale escalation flags via cleanup event” logic (it exists today because flags persist).

### 7) `databricks_rlm_agent/cli.py`
Currently it reads persisted `session.state["rlm:fatal_error"]` after the run to decide status.
Options:
- **Preferred**: have `run_conversation()` return structured status (success / exit_loop / fatal_error + message), and stop relying on persisted state.
- Transitional: keep fatal errors session-scoped until CLI is refactored.

## Migration plan (safe rollout)

### Phase 1: Dual-read (no behavior change)
Update all readers to:
- Read `temp:rlm:*` first
- Fall back to legacy `rlm:*`

### Phase 2: Dual-write (begin using temp)
Update writers to write `temp:rlm:*`.
Optionally keep writing `rlm:*` for one release to maintain compatibility with any remaining readers.

### Phase 3: Cleanup
- Remove legacy `rlm:*` writes for invocation glue.
- Remove legacy read fallbacks.
- Simplify / retire `RlmContextPruningPlugin` clearing logic and `run.py` stale-flag cleanup.

## Acceptance criteria
- A failed invocation (fatal error/timeout before `results_processor`) does **not** pollute the next invocation with stale `artifact_id` or output.
- `RlmContextInjectionPlugin` only injects when the current invocation’s `temp:rlm:artifact_id` is present.
- `DeltaSessionService` tables remain small (execution output stays in UC Volumes `result.json`; state stores previews/paths).


Claude-Code:
● The state key-pair glue refactoring has been implemented. Here's a summary:

  Changes Made

  New file created:
  - databricks_rlm_agent/utils/state_helpers.py - Helper functions for dual-read/write patterns         
  Files updated (7):
  1. tools/delegate_code_results.py - Writes to temp:rlm:* (keeps rlm:iteration session-scoped)
  2. tools/exit_loop.py - Writes temp:rlm:exit_requested
  3. agents/job_builder.py - Uses get_rlm_state() for dual-read, writes to temp:rlm:*
  4. plugins/rlm_context_injection_plugin.py - Dual-read pattern for gating/injection
  5. plugins/rlm_context_pruning_plugin.py - Clears both temp:rlm:* and legacy rlm:*
  6. run.py - Checks both key prefixes for escalation detection
  7. cli.py - Dual-read for fatal error status

  Key Design

  - Invocation-only keys → temp:rlm:* (auto-discarded after invocation)
  - Session-scoped keys → rlm:iteration only (persisted)
  - Migration safety → Dual-read pattern (temp first, fallback to legacy)

  This prevents stale glue from leaking across invocations if the chain aborts early.

