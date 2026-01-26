You are working in the repo `/home/rawleysm/dev/adk-generator`.

## Background / problem statement
We already record LLM context telemetry, plus lightweight state metrics. When debugging multi-call sessions (e.g., ~14 calls), we need the **exact** `callback_context.state` content per call to understand which “components” are present/absent and how they evolve.

This plan adds an **opt-in**, per-call “full state snapshot” capture that is:
- **Disabled by default** (no behavior/cost/storage change unless explicitly enabled)
- **Stored as files** in UC Volumes (under `ADK_ARTIFACTS_PATH`) to avoid inflating Delta tables
- **Referenced by pointers** (path/hash/size) in telemetry payload JSON for easy query/joins

## Goals
- Add an **OPT-IN runtime flag** (default OFF) that captures the full `callback_context.state` JSON for **each** `before_model_callback` (i.e., each LLM call).
- Capture must work for **all agents**, including `results_processor` and `databricks_analyst`.
- Store only **pointer metadata** in telemetry; store the full snapshot in UC Volumes.
- Keep capture **best-effort**: failures must not break the run.

## Non-goals
- Changing what `DeltaSessionService` persists (keep existing behavior: `temp:*` keys are not persisted to Delta session tables).
- Changing existing telemetry schemas/metrics (only add new optional fields/columns).
- Providing a UI for browsing snapshots (we’ll rely on paths + simple queries).

## Key constraints / guardrails
- **No default behavior change**: With the flag OFF, do nothing (no file writes, no additional compute).
- **Do not bloat Delta tables**: No full state JSON stored in Delta; only pointers/hashes/sizes in payload JSON.
- **Do not block the run**: Any snapshot write failure logs a warning and continues.
- **Safe paths**: sanitize `agent_name` and avoid path traversal.
- **Stable snapshot schema**: wrap the raw state in a small metadata envelope so the file format is self-describing.

## Configuration (how the flag actually reaches the job)
Important: in this repo, Databricks **job parameters are not environment variables** on existing clusters. `databricks_rlm_agent/cli.py` already fetches job parameters and then materializes selected ones into `os.environ`.

### Primary knob (job parameter)
- **Name**: `ADK_CAPTURE_STATE_SNAPSHOTS`
- **Values**: truthy (`"1"`, `"true"`, `"yes"`) enables capture; falsy/unset disables.

### Materialization into env vars (required for plugin simplicity)
- Update `databricks_rlm_agent/cli.py` to set:
  - `os.environ["ADK_CAPTURE_STATE_SNAPSHOTS"] = _get_job_parameter("ADK_CAPTURE_STATE_SNAPSHOTS", "")`

### Local deploy/run wiring
- Update `scripts/deploy_rlm_two_job_bundle.sh` to optionally pass:
  - `--param ADK_CAPTURE_STATE_SNAPSHOTS=1`
  - (parallel to how `TEST_LEVEL` is passed today)

## Data model
### File layout (UC Volumes under `ADK_ARTIFACTS_PATH`)
Write one JSON file per `before_model_callback`:

Base directory:
- `${ADK_ARTIFACTS_PATH}/telemetry/state_snapshots/`

Partitioned subdirs (for easy listing and fewer huge directories):
- `session_id=<session_id>/invocation_id=<invocation_id>/agent_name=<sanitized_agent>/`

Filename:
- `llm_call_index=<index>.json`

Example:
- `${ADK_ARTIFACTS_PATH}/telemetry/state_snapshots/session_id=.../invocation_id=.../agent_name=results_processor/llm_call_index=3.json`

### Snapshot JSON schema (envelope)
Store an envelope to make files self-describing and future-proof:
- `schema_version`: integer (start at 1)
- `captured_at`: ISO8601 UTC timestamp
- `session_id`, `invocation_id`, `agent_name`, `llm_call_index`
- `state`: the dict produced by `_safe_state_to_dict(callback_context.state)` (includes `temp:*` keys in-memory; that is OK because this is a file snapshot, not Delta session persistence)

## Telemetry pointer metadata (in payload_json)
In `databricks_rlm_agent/plugins/uc_delta_telemetry_plugin.py`, in `before_model_callback` only, when capture is enabled:
- Write snapshot file (best-effort)
- Compute:
  - sha256 of written bytes
  - bytes length
- Add an object in `payload_json`, e.g.:
  - `state_snapshot_full.schema_version`
  - `state_snapshot_full.path`
  - `state_snapshot_full.sha256`
  - `state_snapshot_full.bytes`
  - `state_snapshot_full.content_type` = `"application/json"`
  - `state_snapshot_full.write_error` (optional string; only set if write fails)

Do **not** remove or change existing fields like `state_snapshot.state_token_estimate`.

## Write semantics / hardening
- Ensure the target directory exists (create if needed).
- Use an **atomic write** pattern:
  - write to `.../llm_call_index=<index>.json.tmp`
  - fsync/close
  - rename/replace to final `.json`
- Any exception:
  - log `WARNING` with enough context (session_id, invocation_id, agent_name, llm_call_index)
  - set `state_snapshot_full.write_error` in telemetry payload JSON (but keep it short; no huge stack traces)
  - continue

## Security / privacy notes
`callback_context.state` may contain sensitive information (tokens, user content, proprietary data).
- Add a prominent note in docs.
- (Optional follow-up) Add redaction support for known-sensitive keys (e.g., `*token*`, `*secret*`) before writing snapshots.

## SQL view update
Update `scripts/llm_context_telemetry_view.sql` to surface pointer metadata as columns (from the **before_model_callback** payload):
- `state_snapshot_path`
- `state_snapshot_sha256`
- `state_snapshot_bytes`
- (optional) `state_snapshot_write_error`

With capture OFF, these should be NULL.

## Implementation checklist
- `databricks_rlm_agent/cli.py`
  - Materialize `ADK_CAPTURE_STATE_SNAPSHOTS` job param into `os.environ`.
- `databricks_rlm_agent/plugins/uc_delta_telemetry_plugin.py`
  - Gate on `ADK_CAPTURE_STATE_SNAPSHOTS`
  - Build envelope + canonical JSON
  - Write to UC Volumes under `ADK_ARTIFACTS_PATH`
  - Add pointer metadata under `state_snapshot_full` in telemetry payload JSON
  - Best-effort failure handling
- `scripts/deploy_rlm_two_job_bundle.sh`
  - Add optional CLI flag (or reuse env-to-param mapping) to pass `--param ADK_CAPTURE_STATE_SNAPSHOTS=1` when running
- `scripts/llm_context_telemetry_view.sql`
  - Add columns extracting the new fields
- `docs/llm_context_telemetry_implementation.md`
  - Document the flag (default OFF), storage location, privacy note, and example query

## Acceptance criteria
- **Flag OFF**:
  - No snapshot files written.
  - Telemetry view unchanged except new columns exist and are NULL.
- **Flag ON** (and `ADK_ARTIFACTS_PATH` configured):
  - Each LLM call produces exactly one snapshot file.
  - Telemetry contains non-NULL `state_snapshot_path` per call, including for `results_processor`.
- **Failure safety**:
  - Snapshot write failure does not fail the job; warning is logged and telemetry records `write_error`.

## Test plan (E2E)
- Deploy/run baseline:
  - `./scripts/deploy_rlm_two_job_bundle.sh --run --test-level 7`
- Deploy/run with capture enabled (via job param pass-through):
  - `./scripts/deploy_rlm_two_job_bundle.sh --run --test-level 7 --param ADK_CAPTURE_STATE_SNAPSHOTS=1`
  - (If the deploy script does not yet support a generic `--param`, add a dedicated flag or add the param directly in the script.)
- Query pointers:
  - `SELECT llm_call_index, agent_name, state_snapshot_path, state_snapshot_sha256, state_snapshot_bytes`
    `FROM silo_dev_rs.adk.llm_context_telemetry`
    `WHERE session_id = '<new_session>'`
    `ORDER BY ts_before;`
- Validate file existence:
  - Use the returned `state_snapshot_path` values to confirm files exist in UC Volumes and are valid JSON.