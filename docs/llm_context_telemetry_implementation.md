# LLM Context Telemetry Implementation Summary

## Overview

This document describes the implementation of invocation-context measurement for each `LlmAgent` model call, enabling post-run evaluation of token usage and context window composition.

**Implementation Date:** January 2026  
**Primary File:** `databricks_rlm_agent/plugins/uc_delta_telemetry_plugin.py`  
**SQL View:** `scripts/llm_context_telemetry_view.sql`

## Problem Statement

Prior to this implementation, there was no way to answer these questions for every LLM call:
- How many tokens were in the prompt (authoritative)?
- What portion came from session state (including `temp:` keys)?
- What was the last message that entered the request?
- What actually entered the LLM?
- How do we pair errors to their request?

## Solution Architecture

The solution instruments the `UcDeltaTelemetryPlugin` at the `before_model_callback`, `after_model_callback`, and `on_model_error_callback` hooks, persisting metrics to `silo_dev_rs.adk.adk_telemetry`.

### Key Design Decisions

1. **Uses `temp:` state for call indexing** - The `llm_call_index` is tracked via `temp:telemetry:llm_call_index:{agent_name}`, which is invocation-scoped and not persisted to Delta (per ADK design).

2. **Keeps heavy payloads out of Delta** - Full request/response snapshots are written to UC Volumes (when `ADK_ARTIFACTS_PATH` is set), with only pointer+hash stored in telemetry.

3. **Provides both estimates and authoritative counts** - State token estimates are computed locally (via tiktoken or heuristic), while `prompt_token_count` comes from model usage metadata.

4. **Tiktoken for token estimation** - Uses `tiktoken` with `cl100k_base` encoding for accurate token counts. Falls back to character-based heuristic (~4 chars/token) if tiktoken is unavailable.

5. **Error pairing via llm_call_index** - `on_model_error_callback` now includes `llm_call_index` for reliable pairing with its `before_model_callback` row.

6. **De-duplication of streaming responses** - SQL view uses `ROW_NUMBER()` window function to select one `after_model_callback` row per key, preferring `turn_complete=true`.

7. **Request last message relies on SDK visibility** - The `request_last_message` metric is computed from `LlmRequest.contents[-1]` when available. This assumes the ADK SDK exposes request contents in `before_model_callback`. If future SDK versions restrict this access, an alternative fallback exists in the plan (rolling `temp:` buffer via `on_event_callback` or post-run join to `silo_dev_rs.adk.events`), but is not currently implemented since request contents are reliably available.

## Data Model

### `before_model_callback` Payload

```json
{
  "llm_call": {
    "llm_call_index": 1,
    "model_name": "gemini-2.0-flash"
  },
  "state_snapshot": {
    "state_keys_count": 5,
    "state_json_bytes": 1024,
    "state_sha256": "abc123...",
    "state_token_estimate": 256,
    "state_token_estimate_persistable_only": 200
  },
  "token_estimation": {
    "method": "tiktoken",
    "encoding": "cl100k_base"
  },
  "request_last_message": {
    "role": "user",
    "token_estimate": 50,
    "preview": "Generate a Python function..."
  },
  "prev_message": {
    "prev_message_role": "user",
    "prev_message_token_estimate": 50,
    "prev_message_preview": "Generate a Python function..."
  },
  "request_sampling": {
    "request_preview": "model=gemini-2.0-flash | system=You are... | messages=3",
    "request_snapshot_path": "/Volumes/.../request_snapshot_....json",
    "request_snapshot_sha256": "def456...",
    "request_snapshot_bytes": 4096
  },
  "system_instruction_preview": "You are a helpful...",
  "available_tools": ["get_repo_file", "save_artifact_to_volumes"]
}
```

### `after_model_callback` Payload

```json
{
  "llm_call": {
    "llm_call_index": 1
  },
  "usage_metadata": {
    "prompt_token_count": 1500,
    "candidates_token_count": 250,
    "cached_content_token_count": 0
  },
  "response_sampling": {
    "response_preview": "Here is the implementation...",
    "response_snapshot_path": "/Volumes/.../response_snapshot_....json",
    "response_snapshot_sha256": "abc123...",
    "response_snapshot_bytes": 2048
  },
  "content": {...},
  "partial": false,
  "turn_complete": true
}
```

### `on_model_error_callback` Payload (NEW)

```json
{
  "llm_call": {
    "llm_call_index": 1
  },
  "error": "Connection timeout after 30s",
  "error_type": "TimeoutError",
  "request_preview": "model=gemini-2.0-flash | messages=3 | ..."
}
```

## Field Semantics

### `request_last_message` vs `prev_message` (DEPRECATED)

The `prev_message` naming was ambiguous ("previous turn" vs "last request message"). 

- **`request_last_message`** (NEW): The last message in `LlmRequest.contents` at the time `before_model_callback` is invoked. This is the most recent message that will be sent to the LLM.
  - `role`: Role of the message (user, model, function, etc.)
  - `token_estimate`: Estimated tokens in this message
  - `preview`: Truncated text preview (max 500 chars)

- **`prev_message`** (DEPRECATED): Kept for backward compatibility with historical rows. Use `request_last_message` for new queries.

The SQL view uses `COALESCE` to prefer `request_last_message` fields when present, falling back to `prev_message` for older rows.

### Token Estimation Metadata

The `token_estimation` object indicates which method was used:

| Field | Value | Description |
|-------|-------|-------------|
| `method` | `"tiktoken"` | Using tiktoken cl100k_base encoding (accurate) |
| `method` | `"heuristic"` | Using ~4 chars/token estimate (fallback) |
| `encoding` | `"cl100k_base"` | Tiktoken encoding name (when applicable) |
| `chars_per_token` | `4.0` | Heuristic divisor (when applicable) |

## Helper Functions

| Function | Purpose |
|----------|---------|
| `_canonical_json()` | Serializes to JSON with sorted keys for consistent hashing |
| `_compute_sha256()` | Computes SHA-256 hash for snapshots |
| `_estimate_tokens()` | Token estimation using tiktoken (with heuristic fallback) |
| `_get_tiktoken_encoder()` | Lazy-loads tiktoken encoder with caching |
| `_get_token_estimation_metadata()` | Returns metadata about estimation method |
| `_filter_persistable_state()` | Removes `temp:*` keys from state |
| `_compute_state_metrics()` | Computes all state metrics (bytes, hash, tokens) |
| `_compute_content_metrics()` | Computes message metrics (role, tokens, preview) |
| `_get_llm_call_index_key()` | Generates temp: state key for tracking |
| `_build_request_snapshot()` | Builds full request structure for saving |
| `_save_request_snapshot()` | Saves request snapshot to UC Volumes |
| `_build_request_preview()` | Creates inline request preview for SQL browsing |
| `_build_response_snapshot()` | Builds full response structure for saving |
| `_save_response_snapshot()` | Saves response snapshot to UC Volumes (large responses) |

## Configuration

| Environment Variable | Purpose | Default |
|---------------------|---------|---------|
| `ADK_ARTIFACTS_PATH` | Path for request snapshots | Not set (snapshots disabled) |
| `ADK_DELTA_CATALOG` | Unity Catalog name | `silo_dev_rs` |
| `ADK_DELTA_SCHEMA` | Schema name | `adk` |
| `ADK_AGENT_TELEMETRY_TABLE` | Table name | `adk_telemetry` |

## SQL View for Analysis

The SQL view `silo_dev_rs.adk.llm_context_telemetry` pairs before/after/error callbacks:

### Key Columns

| Column | Source | Description |
|--------|--------|-------------|
| `llm_call_index` | before | Monotonic call index per (invocation_id, agent_name) |
| `state_token_estimate` | before | Estimated tokens in state |
| `request_last_message_role` | before | Role of last message in request |
| `request_last_message_token_estimate` | before | Estimated tokens in last message |
| `request_snapshot_path` | before | Path to full request snapshot (if saved) |
| `prompt_token_count` | after | Authoritative input token count from model |
| `candidates_token_count` | after | Authoritative output token count from model |
| `response_snapshot_path` | after | Path to full response snapshot (for large responses) |
| `response_snapshot_bytes` | after | Size of response snapshot in bytes |
| `latency_ms` | computed | Time between before and after/error |
| `call_status` | computed | 'completed', 'error', or 'pending_or_orphaned' |
| `error_code` | after/error | Error code if call failed |
| `error_message` | after/error | Error message if call failed |
| `model_error_type` | error | Python exception type (e.g., TimeoutError) |

### Example Queries

```sql
-- Token usage trends over LLM calls
SELECT 
    llm_call_index,
    state_token_estimate,
    state_token_estimate_persistable_only,
    request_last_message_token_estimate,
    prompt_token_count,
    candidates_token_count,
    latency_ms,
    call_status
FROM silo_dev_rs.adk.llm_context_telemetry
WHERE session_id = '<session_id>'
ORDER BY llm_call_index;

-- Compare estimated vs actual token usage
SELECT 
    session_id,
    token_estimation_method,
    AVG(state_token_estimate) AS avg_state_estimate,
    AVG(prompt_token_count) AS avg_prompt_actual,
    AVG(prompt_token_count - state_token_estimate) AS avg_overhead
FROM silo_dev_rs.adk.llm_context_telemetry
WHERE ts_before >= CURRENT_DATE - INTERVAL 7 DAY
GROUP BY session_id, token_estimation_method;

-- Find all errored LLM calls
SELECT 
    session_id,
    llm_call_index,
    model_name,
    error_code,
    error_message,
    model_error_type,
    error_request_preview
FROM silo_dev_rs.adk.llm_context_telemetry
WHERE call_status = 'error'
ORDER BY ts_before DESC;

-- Verify no duplicate rows per key (should return 0)
SELECT 
    session_id, invocation_id, agent_name, llm_call_index, 
    COUNT(*) AS cnt
FROM silo_dev_rs.adk.llm_context_telemetry
GROUP BY session_id, invocation_id, agent_name, llm_call_index
HAVING cnt > 1;
```

## Acceptance Criteria (Met)

- [x] For every LLM call, `silo_dev_rs.adk.adk_telemetry` contains:
  - `state_token_estimate` (including `temp:` keys)
  - `state_token_estimate_persistable_only` (excluding `temp:` keys)
  - `request_last_message_token_estimate` (NEW: clarified semantics)
  - `prompt_token_count` (authoritative from model)
- [x] Token estimation uses tiktoken when available (with heuristic fallback)
- [x] No large blobs added to `silo_dev_rs.adk.sessions` or `silo_dev_rs.adk.events`
- [x] Request snapshot pointers exist when `ADK_ARTIFACTS_PATH` is set
- [x] View returns exactly 1 row per `(session_id, invocation_id, agent_name, llm_call_index)`
- [x] Error rows pair to before-rows via `llm_call_index`
- [x] Streaming/partial responses de-duplicated using window function

## Files Changed

| File | Changes |
|------|---------|
| `databricks_rlm_agent/plugins/uc_delta_telemetry_plugin.py` | Added tiktoken estimation, `request_last_message` payload, `llm_call_index` in error callback |
| `databricks_rlm_agent/pyproject.toml` | Added `tiktoken>=0.5.0` dependency |
| `scripts/llm_context_telemetry_view.sql` | De-duplication, error pairing, new field fallbacks |
| `docs/llm_context_telemetry_implementation.md` | Updated semantics and documentation |

## Usage Notes

1. **Enable request snapshots** by setting `ADK_ARTIFACTS_PATH`:
   ```bash
   export ADK_ARTIFACTS_PATH="/Volumes/silo_dev_rs/adk/artifacts"
   ```

2. **Query telemetry** after a run:
   ```sql
   SELECT * FROM silo_dev_rs.adk.llm_context_telemetry
   WHERE session_id = '<your_session_id>'
   ORDER BY ts_before;
   ```

3. **Analyze state growth** over conversation:
   ```sql
   SELECT 
       llm_call_index,
       state_token_estimate,
       state_token_estimate - LAG(state_token_estimate) OVER (ORDER BY llm_call_index) AS growth
   FROM silo_dev_rs.adk.llm_context_telemetry
   WHERE session_id = '<session_id>';
   ```

4. **Check token estimation method**:
   ```sql
   SELECT DISTINCT token_estimation_method, token_estimation_encoding
   FROM silo_dev_rs.adk.llm_context_telemetry
   WHERE ts_before >= CURRENT_DATE - INTERVAL 1 DAY;
   ```

## Deprecation Timeline

| Field | Status | Removal Target |
|-------|--------|----------------|
| `prev_message.prev_message_role` | Deprecated | TBD |
| `prev_message.prev_message_token_estimate` | Deprecated | TBD |
| `prev_message.prev_message_preview` | Deprecated | TBD |

Use `request_last_message.*` fields instead. The SQL view provides automatic fallback for historical data.

## Future Enhancements

- Add total request token estimate (system + tools + messages)
- Automatic context pruning recommendations based on telemetry
- Model-specific tokenizer selection (e.g., o200k_base for GPT-4o)
- Phase 2 fallback: rolling `temp:` buffer via `on_event_callback` if SDK stops exposing request contents
