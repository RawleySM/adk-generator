# Bug Fixes & Hardening Plan - 2025-01-26

## Overview

This plan addresses control-flow and state-management gaps identified during review of the recent escalation/plugin changes to the RLM workflow.

---

## 1. Normalize Execution Failure State Across All JobBuilderAgent Exits

### Problem
Only the "missing executor job ID" path sets `rlm:execution_success=False` and `rlm:execution_stderr`. Other failure exits (artifact load, write, submit) return early without updating state, leaving `results_processor` and plugins unable to distinguish "execution failed" from "execution never happened."

### Solution
Wrap all failure exits in a helper that consistently sets failure state before yielding the final event.

### Changes
**File:** `databricks_rlm_agent/agents/job_builder.py`

1. Add a `_set_failure_state()` helper method:
   ```python
   def _set_failure_state(self, ctx: InvocationContext, error_msg: str) -> None:
       """Mark execution as failed in session state."""
       ctx.session.state["rlm:execution_success"] = False
       ctx.session.state["rlm:execution_stderr"] = error_msg
   ```

2. Call `_set_failure_state()` before every early-return failure:
   - Artifact load failure (lines ~175-179)
   - Code not found (lines ~183-187)
   - Write to path failure (lines ~194-198)
   - Job submission exception (lines ~214-218)

---

## 2. Introduce Dedicated State Key for Fatal Error Escalation

### Problem
`run_conversation()` currently only distinguishes `exit_loop` (via `rlm:exit_requested`) from "everything else" (treated as delegation). Fatal errors from `JobBuilderAgent` are mislabeled as "delegation escalations," and timeout forgiveness logic doesn't apply correctly.

### Solution
Introduce `rlm:fatal_error` state key for unrecoverable workflow failures. Update `run_conversation()` to detect and handle fatal errors distinctly from delegation and exit_loop.

### New State Key
```python
STATE_FATAL_ERROR = "rlm:fatal_error"
```

### Changes

**File:** `databricks_rlm_agent/agents/job_builder.py`

1. Add constant:
   ```python
   STATE_FATAL_ERROR = "rlm:fatal_error"
   ```

2. Update `_create_error_event()` to set the fatal error flag:
   ```python
   def _create_error_event(self, ctx: InvocationContext, error_msg: str) -> Event:
       # Set fatal error flag for run_conversation() to detect
       ctx.session.state["rlm:fatal_error"] = True
       ctx.session.state["rlm:fatal_error_msg"] = error_msg
       return Event(
           ...
           actions=EventActions(
               state_delta=dict(ctx.session.state),
               escalate=True,
           ),
       )
   ```

**File:** `databricks_rlm_agent/run.py`

1. Add tracking variable:
   ```python
   fatal_error_detected = False
   fatal_error_msg = None
   ```

2. Update escalation detection block:
   ```python
   if hasattr(event, 'actions') and event.actions and getattr(event.actions, 'escalate', False):
       author = getattr(event, 'author', 'unknown')
       state_delta = getattr(event.actions, 'state_delta', {}) or {}
       
       # Check escalation type (priority: fatal > exit_loop > delegation)
       is_fatal = state_delta.get('rlm:fatal_error', False)
       is_exit_loop = state_delta.get('rlm:exit_requested', False)
       
       if is_fatal:
           fatal_error_detected = True
           fatal_error_msg = state_delta.get('rlm:fatal_error_msg', 'Unknown fatal error')
           print(f"ERROR: Fatal error detected from {author}: {fatal_error_msg}")
           if last_text_response:
               final_response = last_text_response
       elif is_exit_loop:
           print(f"INFO: exit_loop termination detected from {author}")
           exit_loop_detected = True
           if last_text_response:
               final_response = last_text_response
       else:
           delegation_count += 1
           print(f"INFO: Delegation escalation #{delegation_count} from {author}")
   ```

3. Update timeout handling to forgive fatal errors (workflow is done):
   ```python
   except asyncio.TimeoutError:
       ...
       if exit_loop_detected or fatal_error_detected:
           print(f"INFO: Timeout after {'fatal error' if fatal_error_detected else 'exit_loop'} - treating as completed")
           break
       raise
   ```

4. Update stream completion logging:
   ```python
   except StopAsyncIteration:
       if final_response == "No response generated." and last_text_response:
           final_response = last_text_response
       if fatal_error_detected:
           print(f"INFO: Stream completed after fatal error")
       elif exit_loop_detected:
           print(f"INFO: Stream completed after exit_loop (delegations: {delegation_count})")
       elif delegation_count > 0:
           print(f"INFO: Stream completed after {delegation_count} delegation(s)")
       break
   ```

---

## 3. Clear Escalation State Keys Deterministically

### Problem
`rlm:exit_requested` is never cleared. If a session is resumed/reused, this flag can remain `True` and pollute future escalation classification.

### Solution
Add escalation state keys to the pruning plugin's clear list, and optionally clear them at conversation start.

### Changes

**File:** `databricks_rlm_agent/plugins/rlm_context_pruning_plugin.py`

1. Add escalation keys to `ADDITIONAL_CLEAR_KEYS`:
   ```python
   ADDITIONAL_CLEAR_KEYS = [
       ...
       # Escalation state keys (clear after each iteration)
       "rlm:exit_requested",
       "rlm:fatal_error",
       "rlm:fatal_error_msg",
   ]
   ```

**File:** `databricks_rlm_agent/run.py` (optional hardening)

1. Clear escalation keys at conversation start in `run_conversation()`:
   ```python
   # Clear stale escalation state at conversation start
   # (This is defensive - pruning plugin should handle this)
   session = await session_service.get_session(...)
   for key in ["rlm:exit_requested", "rlm:fatal_error", "rlm:fatal_error_msg"]:
       if key in session.state:
           session.state[key] = None
   ```

---

## 4. Propagate Fatal Errors to Job Exit Code 

### Problem
"Hard error" in `JobBuilderAgent` halts the agent loop but doesn't fail the Databricks Job_A run (exits 0). This may be desired behavior, but if true "fail the run" semantics are needed, explicit propagation is required.

### Solution (if desired)
Check for fatal error state after `run_conversation()` and return non-zero exit code.

### Changes

**File:** `databricks_rlm_agent/cli.py`

1. Update `_run_orchestrator()` after the agent conversation:
   ```python
   response = await run_conversation(...)
   
   # Check if the workflow hit a fatal error
   session = await session_service.get_session(
       app_name=APP_NAME,
       user_id=args.user_id,
       session_id=args.session_id,
   )
   if session.state.get("rlm:fatal_error"):
       fatal_msg = session.state.get("rlm:fatal_error_msg", "Unknown")
       logger.error(f"Workflow terminated with fatal error: {fatal_msg}")
       final_status = "fatal_error"
   ```

2. The existing exit code logic will then return `1` for `fatal_error` status.

---

## 5. Extend JobBuilderAgent Error Escalation to Other Failures

### Problem
Currently only missing executor job ID triggers `_create_error_event()` with escalation. Other failures (artifact load, write, submit) return early without escalating, allowing `results_processor` to run with incomplete context.

### Decision Point
Choose one of:
- **Option A:** All `JobBuilderAgent` failures escalate as fatal (workflow stops)
- **Option B:** Only missing executor is fatal; other failures let `results_processor` run with error context injected

### Recommended: Option A
For consistency, treat all `JobBuilderAgent` failures as fatal since the workflow cannot proceed meaningfully without successful execution.

### Changes (if Option A)

**File:** `databricks_rlm_agent/agents/job_builder.py`

Replace all `yield self._create_text_event(ctx, error_msg, is_final=True); return` patterns with:
```python
self._set_failure_state(ctx, error_msg)
yield self._create_error_event(ctx, error_msg)
return
```

---

## Test Plan

### Unit Tests

1. **Missing executor job ID:**
   - Verify `rlm:execution_success=False`, `rlm:execution_stderr` set
   - Verify `rlm:fatal_error=True` set
   - Verify escalation event emitted

2. **Artifact load failure:**
   - Mock artifact service to raise exception
   - Verify consistent failure state and escalation

3. **Job submission failure:**
   - Mock `submit_and_wait()` to raise
   - Verify consistent failure state and escalation

### Integration Tests

4. **Session reuse after exit_loop:**
   - Run conversation that calls `exit_loop`
   - Start new conversation with same session
   - Verify `rlm:exit_requested` is cleared and doesn't affect new run

5. **Session reuse after fatal error:**
   - Run conversation that triggers fatal error
   - Verify state is properly cleared for next run

6. **End-to-end fatal error propagation:**
   - Configure orchestrator without `ADK_EXECUTOR_JOB_ID`
   - Run task that generates code
   - Verify Job_A exits non-zero (if Option 4 implemented)

---

## Implementation Order

1. Add `STATE_FATAL_ERROR` constant and update `_create_error_event()` in `job_builder.py`
2. Add `_set_failure_state()` helper and normalize all failure exits
3. Update `run_conversation()` escalation detection in `run.py`
4. Update pruning plugin to clear escalation keys
5. (Optional) Add fatal error exit code propagation in `cli.py`
6. Add tests

---

## State Key Summary

| Key | Type | Set By | Cleared By | Purpose |
|-----|------|--------|------------|---------|
| `rlm:exit_requested` | bool | `exit_loop` tool | pruning_plugin | Signals intentional loop termination |
| `rlm:fatal_error` | bool | `JobBuilderAgent` | pruning_plugin | Signals unrecoverable workflow failure |
| `rlm:fatal_error_msg` | str | `JobBuilderAgent` | pruning_plugin | Human-readable error description |
| `rlm:execution_success` | bool | `JobBuilderAgent` | pruning_plugin | Whether code execution succeeded |
| `rlm:execution_stderr` | str | `JobBuilderAgent` | pruning_plugin | Captured stderr or error message |
