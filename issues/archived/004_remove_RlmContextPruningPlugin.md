# Issue 004 — Remove `RlmContextPruningPlugin` by replacing its responsibilities

### Goal
Remove `databricks_rlm_agent/plugins/rlm_context_pruning_plugin.py` **without** regressing correctness by making the state contract explicit and durable where it should be durable.

- **Replace behavior #1 (correctness)**: prevent stale glue keys from triggering work in later LoopAgent iterations *within the same invocation*.
- **Replace behavior #2 (artifact lifecycle)**: mark artifacts “consumed” somewhere other than the pruning plugin.
- **Fix the underlying state gap**: persist results from `results_processor` via **session-scoped** state (not via prompt-history carry).

### Non-goals
- Reworking how `RlmContextInjectionPlugin` injects context (we’ll just add gating if needed).
- Changing the Delta event schema/tables.
- Persisting large stdout/stderr in session state (stdout/stderr should remain artifacts and/or temp glue).

### What’s actually being persisted vs “passed along”

- **Persisted state (`context.state` → `state_delta_json` in `silo_dev_rs.adk.events`)**: In the sessions inspected, **`results_processor` does not persist any state delta** (`has_state_delta=false`, `state_delta_json` empty/NULL). The only recurring persisted key I saw in analyst-side deltas was **`rlm:iteration`**, plus some **`last_metadata_search_*`** cache keys in certain sessions.
- **Passed along as working context**: The **execution-results payload and/or results_processor output is being carried forward via the conversation/prompt context**, not via `context.state`. You can see this indirectly because the **`databricks_analyst` `promptTokenCount` jumps sharply immediately after the `results_processor` “EXECUTION RESULTS” event** (e.g. in `test_level_12_1769429783`, analyst prompt tokens jump from ~6.5k earlier to **10,027** right after the results_processor injection event).

### Evidence in code (why you don’t see it in `state_delta_json`)

`RlmContextInjectionPlugin` **injects a user content message** and does not write to state:

```200:317:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/plugins/rlm_context_injection_plugin.py
    async def before_agent_callback(
        self,
        *,
        callback_context: CallbackContext,
        **kwargs,  # Accept additional ADK-provided arguments (e.g., agent)
    ) -> Optional[types.Content]:
        """Inject execution context before results_processor runs.
        ...
        """
        # Check if this is the target agent
        agent_name = callback_context.agent_name
        if agent_name != self._target_agent_name:
            return None
        ...
        # Format the injection content
        content_text = self._format_injection_content(
            artifact_id=artifact_id,
            sublm_instruction=sublm_instruction,
            stdout=stdout,
            stderr=stderr,
            iteration=iteration,
        )
        ...
        # Return as a user message Content object
        return types.Content(
            role="user",
            parts=[types.Part.from_text(text=content_text)],
        )
```

`RlmContextPruningPlugin` clears **invocation/temp keys** by setting them to `None` (deletion semantics), which typically won’t show up as durable “context” for later agents:

```141:209:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/plugins/rlm_context_pruning_plugin.py
    async def after_agent_callback(
        self,
        *,
        callback_context: CallbackContext,
        **kwargs,  # Accept additional ADK-provided arguments (e.g., agent)
    ) -> Optional[types.Content]:
        ...
        # Clear invocation-scoped state keys (both temp:rlm:* and legacy rlm:*)
        keys_to_clear = INVOCATION_KEYS_TO_CLEAR + LEGACY_KEYS_TO_CLEAR

        cleared_keys = []
        for key in keys_to_clear:
            if key in callback_context.state:
                # Setting to None signals deletion in ADK state delta
                callback_context.state[key] = None
                cleared_keys.append(key)
```

### What the “close inspection + similarity” shows (from the events table)

Using `scripts/execute_sql.py` + a deeper parsing script:

- **`results_processor` → `databricks_analyst` state transfer**: **No**—there’s **no persisted state delta from `results_processor`** that would populate analyst `context.state`.
- **Information flow**: **Yes**—the injected/output text affects the analyst through prompt context:
  - prompt token counts jump after results_processor events
  - similarity checks show modest overlap between results_processor injected stdout and the analyst’s final narrative response (expected because it’s paraphrased, not copied)

### Bottom line

**The `results_processor` content is not being passed via `context.state` (no `state_delta_json`), but it *is* being passed along via the conversation context/prompt history**, which is why the next `databricks_analyst` prompt token counts jump after `results_processor` runs.

### Why `RlmContextPruningPlugin` exists today (and what “proper state mechanics” implies)

- **ADK semantics**: `temp:` state is **invocation-scoped** and shared across sub-agents in the same invocation (`ai_docs/adk_session_state.md` L66–L76). It is **not persisted** across invocations (`ai_docs/adk_session_state.md` L66–L73) and in your implementation it’s also explicitly **ignored for persistence** (`databricks_rlm_agent/sessions/delta_session_service.py` L43–L61).
- **Your plugin’s current job**: after `results_processor` it:
  1) **marks the artifact “consumed”** (`mark_consumed=True` by default), and  
  2) **clears state keys** to avoid stale intra-invocation leakage:
     - `INVOCATION_KEYS_TO_CLEAR` includes `temp:rlm:*` execution + delegation keys and `temp:parsed_blob`
     - `LEGACY_KEYS_TO_CLEAR` includes `rlm:*` legacy glue keys
     (see `databricks_rlm_agent/plugins/rlm_context_pruning_plugin.py` L41–L89).

So if you remove the plugin, you must replace **both** behaviors:
1) **prevent stale glue keys from affecting later loop iterations inside the same invocation**, and  
2) **mark artifacts consumed somewhere else**.

---

### Target end-state (aligned to `adk_session_state.md`)

#### State scopes & responsibilities
- **Session-scoped (no `temp:`)**
  - Small, serializable, durable breadcrumbs needed across turns/iterations (per docs: simple serializable types only; `ai_docs/adk_session_state.md` L20–L26).
  - Examples for RLM:
    - `rlm:iteration` (already) (`databricks_rlm_agent/utils/state_helpers.py` L165–L168; `databricks_rlm_agent/tools/delegate_code_results.py` L63–L66)
    - `rlm:last_artifact_id`
    - `rlm:last_results_summary` (**persisted** results_processor final text)
    - `rlm:last_results_structured` (optional JSON dict—keep small)
- **Invocation-scoped (`temp:`)**
  - Only the “glue” required to move execution through the current loop iteration:
    - `temp:rlm:artifact_id`, `temp:rlm:sublm_instruction`, `temp:rlm:result_json_path`, etc. (already used) (`databricks_rlm_agent/tools/delegate_code_results.py` L53–L66; `databricks_rlm_agent/agents/job_builder.py` L45–L61)
  - **Do not rely on “auto-discard after invocation” to clean per-iteration state** inside a `LoopAgent`; instead make glue **self-invalidating** per iteration (stage/token gating below).

#### “Proper way” to update state
- Update state via **`ToolContext.state` / `CallbackContext.state`**, letting ADK capture it into the event `state_delta` (docs: `ai_docs/adk_session_state.md` L468–L477). Avoid directly mutating a retrieved `Session` outside context (docs warning: `ai_docs/adk_session_state.md` L559–L572).

---

### Migration plan (phased, low-risk)

### Phase 0 — Inventory and lock the state contract
Decide semantics up front, then encode them as “who writes what key, when”.

- **Define “consumed”** for the artifact registry:
  - **Option 1 (executor-consumed)**: consumed when `JobBuilderAgent` finishes execution successfully.
  - **Option 2 (analysis-consumed)**: consumed when `results_processor` completes analysis (current behavior).
- **Freeze the glue-key list** you must not allow to “accidentally re-trigger” work:
  - `temp:rlm:*` keys listed in `INVOCATION_KEYS_TO_CLEAR` (includes `temp:rlm:artifact_id`, `temp:rlm:sublm_instruction`, `temp:rlm:execution_stdout`, `temp:rlm:execution_success`, etc.)
  - `temp:parsed_blob`
  - legacy `rlm:*` keys listed in `LEGACY_KEYS_TO_CLEAR`

Deliverable: a short table (in this issue) mapping keys → writer → reader → scope.

#### State contract table (current + proposed)

| Key (or key family) | Writer (where) | Reader (where) | Scope / persistence |
|---|---|---|---|
| `rlm:iteration` | `delegate_code_results` and/or loop bookkeeping (see `databricks_rlm_agent/utils/state_helpers.py`) | `delegate_code_results`, `JobBuilderAgent`, logging/telemetry | **Session-scoped** (persists; shows in `state_delta_json`) |
| `rlm:last_artifact_id` *(proposed)* | `delegate_code_results` (when creating artifact) | `databricks_analyst` via instruction templating; debugging | **Session-scoped** (persists) |
| `rlm:last_results_summary` *(proposed)* | `results_processor_agent` via `output_key` (preferred) OR a minimal `after_agent_callback` | `databricks_analyst` via `{rlm:last_results_summary?}` templating in `ROOT_AGENT_INSTRUCTION` | **Session-scoped** (persists; queryable in `state_delta_json`) |
| `rlm:last_results_structured` *(optional, proposed)* | `results_processor` (tool/callback writing a small JSON-serializable dict) | `databricks_analyst` and/or downstream tools | **Session-scoped** (persists; keep small) |
| `temp:rlm:artifact_id` | `delegate_code_results` | `JobBuilderAgent`, `RlmContextInjectionPlugin`, `RlmContextPruningPlugin` (today) | **Invocation-scoped** (NOT persisted by `DeltaSessionService`) |
| `temp:rlm:sublm_instruction` | `delegate_code_results` | `RlmContextInjectionPlugin` (formats injected prompt) and `results_processor` | **Invocation-scoped** (NOT persisted) |
| `temp:rlm:has_agent_code`, `temp:rlm:code_artifact_key` | `delegate_code_results` | `JobBuilderAgent` | **Invocation-scoped** (NOT persisted) |
| `temp:rlm:execution_stdout`, `temp:rlm:execution_stderr`, `temp:rlm:execution_success`, `temp:rlm:databricks_run_id`, `temp:rlm:run_url`, `temp:rlm:result_json_path`, `temp:rlm:*_truncated` | `JobBuilderAgent` | `RlmContextInjectionPlugin` (to inject into `results_processor`); `results_processor` | **Invocation-scoped** (NOT persisted; large payload should remain temp/artifact) |
| `temp:rlm:fatal_error`, `temp:rlm:fatal_error_msg`, `temp:rlm:exit_requested` | `JobBuilderAgent` and/or tools | Loop control / escalation plugins | **Invocation-scoped** (NOT persisted) |
| `temp:parsed_blob` | formatting/validation plugins (parsed tool payload) | downstream validators/execution path | **Invocation-scoped** (NOT persisted) |
| `temp:rlm:stage`, `temp:rlm:active_artifact_id` *(proposed)* | `delegate_code_results` / `JobBuilderAgent` / post-`results_processor` callback | gating checks in `JobBuilderAgent` and `RlmContextInjectionPlugin` | **Invocation-scoped** (NOT persisted; prevents stale reuse within LoopAgent) |
| legacy `rlm:*` glue (e.g. `rlm:artifact_id`, `rlm:execution_stdout`, …) | legacy writers during migration | `get_rlm_state(...)` dual-read sites (`JobBuilderAgent`, pruning plugin) | **Session-scoped** today (persists) but **target is removal** in Phase 5 |

### Phase 1 — Add an explicit per-iteration “stage/token” (replaces pruning for correctness)
Goal: make each step act **only** when state indicates it is the correct stage, so stale `temp:rlm:*` does not trigger work in later loop iterations.

- **Add stage keys (invocation-scoped)**:
  - `temp:rlm:stage` in {`delegated`, `executed`, `processed`}
  - `temp:rlm:active_artifact_id` (or reuse `temp:rlm:artifact_id`, but only trust it when stage matches)
- **Update producers/consumers (by file owner)**:
  - **`databricks_rlm_agent/tools/delegate_code_results.py`**:
    - writes: `temp:rlm:artifact_id`, `temp:rlm:sublm_instruction`, … (existing)
    - add: set `temp:rlm:stage = delegated` and `temp:rlm:active_artifact_id = <artifact_id>`
  - **`databricks_rlm_agent/agents/job_builder.py`** (`JobBuilderAgent`):
    - precondition: only submit/run if stage is `delegated` and artifact matches `active_artifact_id`
    - on completion: write execution keys (existing) and set `temp:rlm:stage = executed`
  - **`databricks_rlm_agent/plugins/rlm_context_injection_plugin.py`** (`RlmContextInjectionPlugin.before_agent_callback`):
    - precondition: only inject if stage is `executed` (and artifact matches)
  - **`databricks_rlm_agent/agent.py`** (`results_processor_agent`):
    - after `results_processor` final response, set `temp:rlm:stage = processed` (either via a tiny new plugin or via agent output handling; see Phase 2/3).

Why this works: you no longer need “bulk clear” to prevent accidental reuse; stale keys are inert unless stage/token indicates they’re current.

### Phase 2 — Persist results_processor output properly (stop relying on prompt-history “carry”)
Goal: explicitly persist the results_processor “summary” into session state so the analyst can reliably reference it.

Two options (both align with docs):
- **Option A (preferred, minimal code)**: set `results_processor_agent.output_key = "rlm:last_results_summary"` in `databricks_rlm_agent/agent.py` where `results_processor_agent = LlmAgent(...)` is constructed (currently no `output_key`; see `agent.py` ~L208).
- **Option B**: add a minimal `after_agent_callback` plugin that writes `callback_context.state["rlm:last_results_summary"] = <final text>` (docs: `ai_docs/adk_session_state.md` L468–L477).

Then, update the analyst instruction to consume it deterministically via instruction templating:
- Add `{rlm:last_results_summary?}` (and optionally `{rlm:last_artifact_id?}`) into `ROOT_AGENT_INSTRUCTION` in `databricks_rlm_agent/prompts.py` (docs: `ai_docs/adk_session_state.md` L81–L131).

This eliminates ambiguity about whether “context got passed” and makes the transfer queryable in `silo_dev_rs.adk.events.state_delta_json`.

### Phase 3 — Move “mark consumed” to the right owner
Depending on semantics chosen in Phase 0:
- If “consumed” == “executed successfully”:
  - mark consumed in `JobBuilderAgent` after successful executor run (it already updates registry metadata; see `databricks_rlm_agent/agents/job_builder.py` around the “update registry” step).
- If “consumed” == “analysis completed”:
  - mark consumed after `results_processor` final response (e.g., in the same minimal plugin used for stage updates in Phase 1/2).

### Phase 4 — Remove pruning plugin safely
- **Stop wiring it into the app**:
  - remove export/import from `databricks_rlm_agent/plugins/__init__.py`
  - remove instantiation + plugin list entry from `databricks_rlm_agent/agent.py` (currently `context_pruning_plugin = RlmContextPruningPlugin(...)` and included in `App(... plugins=[..., context_pruning_plugin])`)
  - remove references/comments in `databricks_rlm_agent/run.py` (it currently lists the pruning plugin in the orchestration comments and imports)
- **Delete the file** once the above phases are validated (or keep it around temporarily but unused; deletion is the actual end-state).

### Phase 5 — Retire legacy key migration
Once stable:
- Remove dual-read (`get_rlm_state`) and legacy `rlm:*` glue keys (keep `rlm:iteration` and new session keys).
- Collapse on one authoritative schema:
  - **Glue**: `temp:rlm:*`
  - **Durable**: `rlm:*` (session), maybe `user:`/`app:` if needed later

---

### Definition of Done (verifiable outcomes)
- **No pruning plugin wired**: `RlmContextPruningPlugin` is not constructed and not present in the `App(... plugins=[...])` list.
- **Durable handoff is explicit**: after `results_processor` runs, `silo_dev_rs.adk.events.state_delta_json` contains `rlm:last_results_summary`.
- **No intra-invocation “stale trigger” regressions**: repeated LoopAgent iterations do not re-run `job_builder` / `results_processor` unless a fresh `delegate_code_results` occurred (stage/token gating).
- **Artifact lifecycle preserved**: artifacts still transition to “consumed” according to the chosen semantics.

### Verification checklist (SQL + runtime evidence)
Use `scripts/execute_sql.py` where possible, and logs where temp state is involved:

- **`temp:` never persists** (should be absent from `state_delta_json` due to your service ignoring it):
  - see `_extract_state_delta` ignoring `temp:*` (`delta_session_service.py` L43–L61)
- **Results are persisted to session state**:
  - Query `silo_dev_rs.adk.events` for `state_delta_json` containing `rlm:last_results_summary` after results_processor runs.
- **Stage gating works**:
  - Verify via workflow logs/telemetry that `JobBuilderAgent` and the injection callback no-op unless stage is correct (temp keys are intentionally not persisted, so this won’t be visible in SQL deltas).

---

### Key takeaway
Removing `RlmContextPruningPlugin` is safe **only if** you replace its two real functions: **(1) intra-invocation staleness protection** (best done with a `temp:rlm:stage` state machine) and **(2) artifact consumed marking** (move to job_builder or results_processor). Separately, to “use proper context.state mechanics,” persist `results_processor` output into **session-scoped** state via `output_key` and reference it via `{state_key}` templating so the transfer is explicit and queryable.