### Spec plan: Invocation-context measurement for each `LlmAgent` context window

#### Goal
Add **post-run evaluable** instrumentation that lets us answer, for every `LlmAgent` model call:
- **Total prompt tokens** (authoritative, from model usage metadata)
- **Token estimate contributed by prior state** (what was in `callback_context.state` at request time, including `temp:`)
- **Token estimate contributed by the previous event** (the *last message* that entered the request)
- **Best case**: a **sample / snapshot** of what actually entered the LLM (request contents), without bloating `silo_dev_rs.adk.sessions` / `silo_dev_rs.adk.events`

#### Constraints (must respect)
- **`temp:` state is invocation-only and not persistent** per ADK docs, and it’s also **dropped by Delta persistence**:
  - `temp:` definition: `ai_docs/adk_session_state.md`
  - trimming in persistence: `databricks_rlm_agent/sessions/delta_session_service.py` `_extract_state_delta(...)` ignores `temp:*`
- Delta table rows should not carry megabyte payloads (keep `sessions.state_json` / `events.event_data_json` lean).

---

### Approach
Instrument at the plugin layer, using the existing telemetry pattern:
- **Primary hook**: `UcDeltaTelemetryPlugin.before_model_callback(...)` and `after_model_callback(...)` in `databricks_rlm_agent/plugins/uc_delta_telemetry_plugin.py`.
- **Primary storage**: `silo_dev_rs.adk.adk_telemetry` (already exists and is append-only).
- **Optional full-payload sampling**: write large request snapshots to **UC Volumes/DBFS** and persist only a **pointer + hash** in `adk_telemetry.payload_json`.

This avoids relying on persisted session state (which excludes `temp:`) and avoids inflating the session/event Delta tables.

---

### Data model (telemetry payload schema)

Each `adk_telemetry` row already has identifiers; we’ll enrich `payload_json` for two callback types:

#### 1) `callback_name = "before_model_callback"`
`payload_json` fields:
- **llm_call**
  - `llm_call_index` (int, monotonic per `(invocation_id, agent_name)`; maintained in `callback_context.state['temp:telemetry:llm_call_index']`)
  - `model_name` (string)
- **state_snapshot (request-time)**
  - `state_keys_count` (int)
  - `state_json_bytes` (int) — size of JSON-serialized `callback_context.state`
  - `state_sha256` (string) — hash of canonical JSON
  - `state_token_estimate` (int) — estimated tokens for serialized state
  - `state_token_estimate_persistable_only` (int, optional) — same estimate after dropping `temp:*` (useful to quantify “invisible” invocation glue)
- **prev_message (request-time)**
  - `prev_message_role` (string, optional)
  - `prev_message_token_estimate` (int, optional)
  - `prev_message_preview` (string, optional; small, e.g. 300–1000 chars)
- **request_sampling (best case; optional)**
  - `request_preview` (string; small inline preview for SQL browsing)
  - `request_snapshot_path` (string, optional)
  - `request_snapshot_sha256` (string, optional)
  - `request_snapshot_bytes` (int, optional)

#### 2) `callback_name = "after_model_callback"`
`payload_json` fields:
- **llm_call**
  - `llm_call_index` (int) — same index as `before_model_callback` (read from temp state)
- **usage_metadata (authoritative)**
  - `prompt_token_count` (int)
  - `candidates_token_count` (int)
  - `cached_content_token_count` (int, optional if available in SDK)
- **response_sampling (optional)**
  - `response_preview` (string, small inline preview)
  - `response_snapshot_path` (string, optional; for large responses)
  - `response_snapshot_sha256` (string, optional)
  - `response_snapshot_bytes` (int, optional)

---

### Token-counting method (estimation)
- **Ground truth total**: from `llm_response.usage_metadata.prompt_token_count` (already persisted today in `after_model_callback`).
- **Estimates (state + prev event)**:
  - Use a deterministic tokenizer (preferred for consistency across runs).
  - Serialize inputs canonically (e.g., sorted keys, stable separators) before hashing/tokenizing.
  - Store estimates + byte sizes + hashes so we can compare across runs even if tokenization evolves later.

---

### Implementation steps (incremental)

#### Phase 0 — confirm request visibility
- In `before_model_callback`, determine what of the actual request is accessible in `LlmRequest` (messages/contents). Right now we only touch `model`, `config.system_instruction`, and `tools_dict` in `uc_delta_telemetry_plugin.py`.
- If full message list is not exposed by this SDK type, fall back to “previous event token estimate” computed from the **most recent persisted ADK Event** (more expensive; see Phase 2).

#### Phase 1 — minimum viable metrics (meets “at minimum” requirement)
Add to `UcDeltaTelemetryPlugin`:
- Maintain `temp:telemetry:llm_call_index` per `(invocation_id, agent_name)` at runtime.
- In `before_model_callback`:
  - Serialize `callback_context.state` and compute:
    - bytes, hash, token estimate (including temp keys)
    - token estimate excluding temp keys
  - Persist those fields to `adk_telemetry.payload_json`.
- In `after_model_callback`:
  - Persist usage metadata as you already do, plus the `llm_call_index`.

This yields post-run eval for:
- “previous state token count” (estimate)
- “total prompt tokens” (authoritative)
- “growth trend over calls”

#### Phase 2 — “previous event contribution” (token estimate)
Preferred (accurate, low ambiguity):
- In `before_model_callback`, compute token estimate for the **last message** in the request (if the SDK exposes request messages).
Fallback (works even if request messages aren’t available):
- Derive “previous event” as the most recent `Event` prior to the model call within this invocation/agent:
  - Either maintain a small rolling `temp:` buffer updated in `on_event_callback`
  - Or join later using `silo_dev_rs.adk.events` sequence numbers (post-run job).
- Store the last-event token estimate + preview.

#### Phase 3 — “best case” sampling of what entered an LLM
- In `before_model_callback`, create a **request snapshot** (full request structure: system instruction, tool schema names, message list).
- Write it to UC Volumes/DBFS (or any artifact store you already trust), and store only pointer+hash+bytes in telemetry.
- Keep an inline `request_preview` (small) for easy SQL browsing.

---

### Post-run evaluation outputs (what we’ll build)
- **A SQL view** (or notebook query) that pairs `before_model_callback` and `after_model_callback` by `(session_id, invocation_id, agent_name, llm_call_index)` and emits:
  - timestamps
  - state token estimate (+ persistable-only variant)
  - previous event token estimate
  - prompt_token_count (authoritative)
  - cached content tokens (if available)
  - snapshot pointers

This can power your existing `scripts/inspect_events_context_flow.py`-style analyses, but with hard token accounting and optional deep sampling.

---

### Acceptance criteria
- For every LLM call, we can query `silo_dev_rs.adk.adk_telemetry` and retrieve:
  - **state_token_estimate** (incl `temp:`) and **state_token_estimate_persistable_only**
  - **prev_message_token_estimate** (or the fallback “previous event” estimate)
  - **prompt_token_count** (from usage metadata)
- No new large blobs are added to `silo_dev_rs.adk.sessions` or `silo_dev_rs.adk.events`.
- Optional: request snapshot pointers exist and can be pulled for audit on demand.

---

### Why this matches the ADK + Delta constraints
- We explicitly avoid depending on persisted `temp:` state (it’s non-persistent by design and trimmed by `DeltaSessionService`).
- We keep the heavy “what entered the model” payload out of Delta session tables and use telemetry + artifact pointers instead, enabling post-run eval without destabilizing storage.

### Plan: Telemetry spec updates (naming, de-dupe, error pairing, `tiktoken`)

#### Assumptions (defaults since you skipped the prompts)
- **Prev-message semantics**: keep current behavior = **last message in the request** (`llm_request.contents[-1]`), but rename to make that explicit.
- **Schema strategy**: **backward compatible** (keep existing `prev_message.*` fields, add new clearer fields; update the view to prefer new fields when present).

Current behavior reference:

```990:1004:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/plugins/uc_delta_telemetry_plugin.py
        # --- Phase 2: Previous message metrics ---
        prev_message_metrics: dict[str, Any] = {}
        if llm_request.contents and len(llm_request.contents) > 0:
            # Get the last message in the request
            last_content = llm_request.contents[-1]
            prev_message_metrics = _compute_content_metrics(last_content)
```

## GAPS TO PATCH (STATUS: ALL COMPLETED)

> **Note:** All gaps identified below have been addressed. See `docs/llm_context_telemetry_implementation.md` for the implemented solution and `scripts/llm_context_telemetry_view.sql` for the SQL view.

---

### 1) Fix “prev_message” naming vs behavior ✅ COMPLETED (payload + view)
- **Goal**: remove ambiguity (“previous turn” vs “last request message”).
- **Approach (backward compatible)**:
  - **Add new payload object** in `before_model_callback`, e.g.:
    - `request_last_message.role`
    - `request_last_message.token_estimate`
    - `request_last_message.preview`
  - **Keep existing** `prev_message.prev_message_*` fields for now (or mark deprecated in docs).
  - **Update** `scripts/llm_context_telemetry_view.sql` to read the new fields, but **fallback** to old fields when new fields are absent (so historical rows still query cleanly).
- **Doc updates**: update `docs/llm_context_telemetry_implementation.md` to explicitly define “last message in LlmRequest” and mention deprecation timeline for `prev_message.*`.

---

### 2) Avoid multiple `after_model_callback` rows ✅ COMPLETED: “latest per key” in view (window function)
- **Problem**: if ADK emits multiple `after_model_callback` rows per `(session_id, invocation_id, agent_name, llm_call_index)` (streaming/partials), the current view join can duplicate the before-row.
- **Fix**: rank `after_model_callback` rows and select the winner per key.
- **Implementation in Databricks SQL**:
  - Add an `after_model_ranked` CTE with `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`.
  - Pick **one** row per key by filtering `rn = 1`.
  - Ordering recommendation:
    - Prefer `turn_complete = true` when present; otherwise take latest `ts`.
    - Since your `after_model_callback` payload includes `turn_complete` and `partial`:

```1103:1111:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/plugins/uc_delta_telemetry_plugin.py
            if llm_response.turn_complete is not None:
                self._log(f"   Turn Complete: {llm_response.turn_complete}")
            payload["partial"] = llm_response.partial
            payload["turn_complete"] = llm_response.turn_complete
```

Example SQL sketch (to be adapted into your view):

```sql
after_model_raw AS (
  SELECT
    session_id, invocation_id, agent_name,
    ts AS ts_after,
    telemetry_id AS telemetry_id_after,
    get_json_object(payload_json, '$.llm_call.llm_call_index') AS llm_call_index,
    get_json_object(payload_json, '$.turn_complete') AS turn_complete,
    -- ... existing extracted fields ...
    payload_json
  FROM silo_dev_rs.adk.adk_telemetry
  WHERE callback_name = 'after_model_callback'
),
after_model_ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY session_id, invocation_id, agent_name, llm_call_index
      ORDER BY
        CASE WHEN turn_complete = 'true' THEN 1 ELSE 0 END DESC,
        ts_after DESC,
        telemetry_id_after DESC
    ) AS rn
  FROM after_model_raw
),
after_model AS (
  SELECT * FROM after_model_ranked WHERE rn = 1
)
```

---

### 3) Error pairing with same call index ✅ COMPLETED (read from temp state)
- **Problem**: `on_model_error_callback` persists telemetry but does **not** include `llm_call_index`, so you can’t reliably pair an error to its `before_model_callback` row in the view.

Current code reference:

```1203:1222:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/plugins/uc_delta_telemetry_plugin.py
    async def on_model_error_callback(
        ...
        self._persist(
            callback_name="on_model_error_callback",
            callback_context=callback_context,
            model_name=model_name,
            payload={"error": str(error), "error_type": type(error).__name__},
        )
```

- **Fix**:
  - In `on_model_error_callback`, read `llm_call_index` from the same temp key used elsewhere (`temp:telemetry:llm_call_index:{agent_name}`) and include it in payload under `llm_call.llm_call_index`.
  - Optionally include a small `request_preview` so errors remain diagnosable without depending on the before-row.
- **View changes**:
  - Add an `on_model_error_callback` CTE that extracts `llm_call_index`, error type/message, and de-dupes similarly if needed.
  - Join strategy:
    - Keep `after_model` as the “success/response” side.
    - Left join `model_error` too, and use `COALESCE` to populate `error_code/error_message` columns when no after-row exists.

---

### 4) Use `tiktoken` for token estimation ✅ COMPLETED (state + message estimates)
- **Repo reality check**: `tiktoken` is present in `uv.lock`, but not in the wheel dependencies listed in `databricks_rlm_agent/pyproject.toml` (so it may not be available in Databricks jobs unless provided some other way).
- **Implementation spec**:
  - Replace `_estimate_tokens(text)` with a `tiktoken`-based estimator:
    - `enc = tiktoken.get_encoding("cl100k_base")` (stable default; consistent across runs)
    - tokens = `len(enc.encode(text))`
  - Add a **fallback** to chars/4 if `tiktoken` import fails at runtime (keeps telemetry robust).
  - Add metadata fields into telemetry payloads:
    - `token_estimation.method` = `"tiktoken"` or `"heuristic"`
    - `token_estimation.encoding` = `"cl100k_base"` (when applicable)
- **Packaging spec**:
  - Add `tiktoken>=...` to `databricks_rlm_agent/pyproject.toml` dependencies (so the deployed wheel includes it).
  - (Optional) also add to root `pyproject.toml` if you run the same code locally outside Databricks.

---

### 5) Acceptance criteria ✅ MET / validation steps
- **Correctness**
  - View returns **exactly 1 row per** `(session_id, invocation_id, agent_name, llm_call_index)` (or returns nulls for after-side when a call errored).
  - “Prev message” columns reflect the clarified semantics and remain compatible with historical rows.
  - Error rows pair to before-rows via `llm_call_index`.
- **Observability**
  - For sessions with streaming/partials, the view uses the intended final/most-recent `after_model_callback`.
- **Performance**
  - Window function operates on `adk_telemetry` filtered by callback_name; consider adding `WHERE ts >= ...` in ad-hoc queries if the table grows large.

---

### 6) Deliverables ✅ COMPLETED (what will change)
- **Code**: `databricks_rlm_agent/plugins/uc_delta_telemetry_plugin.py`
  - Add new `request_last_message` payload fields
  - Add `llm_call_index` into `on_model_error_callback`
  - Swap estimation to `tiktoken` (+ fallback) and add estimator metadata
- **SQL**: `scripts/llm_context_telemetry_view.sql`
  - De-dupe `after_model_callback` per key using window function
  - Add/merge model-error pairing
  - Prefer new message fields, fallback to old
- **Docs**: `docs/llm_context_telemetry_implementation.md`
  - Update definitions + note deprecations

If you want, I can also propose a minimal “v2 view” name (e.g., `llm_context_telemetry_v2`) to avoid breaking anyone relying on the current view while you roll this out.