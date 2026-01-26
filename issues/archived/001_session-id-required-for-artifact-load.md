# Issue 001: Session ID Required for Artifact Load

**Date**: 2026-01-26
**Iteration**: 1 of 20
**Severity**: Critical

## Problem

The `job_builder` agent fails with a fatal error when trying to load code artifacts saved by `delegate_code_results`. The error message is:

```
ERROR: FATAL: Failed to load code artifact 'art_21653b387a60_code.py': Session ID must be provided for session-scoped artifacts.
```

## Root Cause

In `databricks_rlm_agent/agents/job_builder.py`, the `_load_artifact_part()` method calls `artifact_service.load_artifact()` with `app_name` and `user_id` parameters, but does NOT pass `session_id`.

The `InMemoryArtifactService` (used in run.py line 148) stores artifacts at session scope and requires `session_id` to retrieve them.

## Symptoms

- Agent completes iteration but creates no deliverables
- `job_builder` agent yields error event and escalates
- Telemetry shows: `after_tool_callback` for `delegate_code_results` succeeds, but `job_builder` fails immediately
- Session state shows `{"rlm:iteration": 1}` but no execution results

## Fix Applied

Modified `_load_artifact_part()` in `job_builder.py` to pass `session_id` when loading artifacts from the artifact service.

### Files Modified

- `databricks_rlm_agent/agents/job_builder.py` - Added session_id parameter to artifact service load call

### Code Changes

```diff
--- a/databricks_rlm_agent/agents/job_builder.py
+++ b/databricks_rlm_agent/agents/job_builder.py
@@ -385,12 +385,18 @@ class JobBuilderAgent(BaseAgent):
         artifact_service = getattr(ctx, "artifact_service", None)
         if artifact_service and hasattr(artifact_service, "load_artifact"):
-            # InMemoryArtifactService requires app_name/user_id for session-scoped artifacts.
+            # InMemoryArtifactService requires app_name/user_id/session_id for session-scoped artifacts.
             # Try with session context first, then without as fallback.
+            session_id = getattr(session, "id", None) or ctx.session.id
             try:
                 result = artifact_service.load_artifact(
                     filename=filename,
                     app_name=app_name,
                     user_id=user_id,
+                    session_id=session_id,
                 )
```

## Verification

After the fix, re-run test level 12:
```bash
./scripts/deploy_rlm_two_job_bundle.sh --run --test-level 12
```

Expected: `job_builder` should successfully load the code artifact and proceed to executor job submission.

## Prevention

- Add integration test that verifies artifact save/load round-trip with InMemoryArtifactService
- Consider switching to DeltaArtifactService (file-based) to avoid in-memory session scoping issues
- Document artifact service configuration requirements in run.py
