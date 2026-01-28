## How `callback_context` carries “offloadable context” through the RLM loop

At a high level, **`callback_context.state` is the “handoff bus” between sub-agents inside the same LoopAgent invocation**, and Issue 004 makes that bus explicit by (a) using **invocation-scoped `temp:rlm:*` glue** + a **3-state gate**, and (b) persisting only **small, durable summaries** back into session state.

### 1) The three state surfaces (same underlying mapping, different entrypoints)

- **`ToolContext.state`** (inside tools like `delegate_code_results`) is where the *LLM agent* writes glue keys for downstream agents.  
  See: `delegate_code_results` writing state and transferring control:

```165:233:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/tools/delegate_code_results.py
    tool_context.state[STATE_ARTIFACT_ID] = artifact_id
    tool_context.state[STATE_SUBLM_INSTRUCTION] = parsed.sublm_instruction
    tool_context.state[STATE_HAS_AGENT_CODE] = bool(parsed.agent_code)
    tool_context.state[STATE_ITERATION] = new_iteration
    tool_context.state[STATE_CODE_ARTIFACT_KEY] = code_artifact_key
    tool_context.state[STATE_SESSION_ID] = session_id
    tool_context.state[STATE_INVOCATION_ID] = invocation_id
    tool_context.state[STATE_STAGE] = "delegated"
    tool_context.state[STATE_ACTIVE_ARTIFACT_ID] = artifact_id
    tool_context.actions.transfer_to_agent = "job_builder"
```

- **`InvocationContext.session.state`** (inside the deterministic `JobBuilderAgent`) is where the *executor-plane agent* reads those glue keys and writes execution results back.
- **`CallbackContext.state`** (inside plugins) is where *callbacks* can read the same state and inject or update it around an agent’s run.

Issue 004’s key observation is: **you should move bulky payloads out of state and into artifacts/Volumes, and use state primarily as pointers + gating**.

---

### 2) Stage gating: preventing stale glue from re-triggering work (the “state machine”)

The stage key is invocation-scoped: **`temp:rlm:stage`** progresses:

- `"delegated"` → `"executed"` → `"processed"`

This replaces “clear a bunch of keys” with **“only act if the stage is right”**.

**a) `delegate_code_results` starts the stage + pins the active artifact**

```179:183:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/tools/delegate_code_results.py
    tool_context.state[STATE_STAGE] = "delegated"
    tool_context.state[STATE_ACTIVE_ARTIFACT_ID] = artifact_id
```

**b) `JobBuilderAgent` only executes when stage is delegated and artifact matches**

```179:207:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/agents/job_builder.py
        current_stage = ctx.session.state.get(STATE_STAGE)
        active_artifact_id = ctx.session.state.get(STATE_ACTIVE_ARTIFACT_ID)

        if current_stage != "delegated":
            ... skip ...
            return

        if active_artifact_id and active_artifact_id != artifact_id:
            ... skip ...
            return
```

**c) `JobBuilderAgent` sets stage to executed after it has real execution results**

```364:371:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/agents/job_builder.py
        set_state(STATE_RESULT_JSON_PATH, result_json_path)
        set_state(STATE_STDOUT_TRUNCATED, len(final_stdout) > len(stdout_preview))
        set_state(STATE_STDERR_TRUNCATED, len(stderr) > len(stderr_preview))
        set_state(STATE_STAGE, "executed")
```

**d) `RlmContextInjectionPlugin` only injects when stage is executed, then flips to processed**

Before callback (gate + inject):

```242:263:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/plugins/rlm_context_injection_plugin.py
        current_stage = callback_context.state.get(STATE_STAGE)
        if current_stage != "executed":
            return None

        active_artifact_id = callback_context.state.get(STATE_ACTIVE_ARTIFACT_ID)
        if active_artifact_id and active_artifact_id != artifact_id:
            return None
```

After callback (complete the state machine):

```371:385:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/plugins/rlm_context_injection_plugin.py
        current_stage = callback_context.state.get(STATE_STAGE)
        if current_stage == "executed":
            callback_context.state[STATE_STAGE] = "processed"
```

This is the “mechanics” piece: **`callback_context.state` carries the stage, the artifact pointer, and the instruction across agent boundaries, and each boundary enforces the expected stage.**

---

### 3) Offloading the token-heavy parts: pointers in state, payloads in Volumes/artifacts

#### The key design choice
`job_builder` deliberately stores only **previews** in state and stores a **path pointer** to full output:

```346:368:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/agents/job_builder.py
        # Store only a preview in session.state to keep Delta session tables small.
        # The injection plugin will load full output from result.json via the path.
        stdout_preview = self._create_preview(final_stdout, max_lines=50)
        stderr_preview = self._create_preview(stderr, max_lines=20)
        ...
        set_state(STATE_EXECUTION_STDOUT, stdout_preview)
        set_state(STATE_EXECUTION_STDERR, stderr_preview)
        set_state(STATE_RESULT_JSON_PATH, result_json_path)
```

Then the injection callback uses `callback_context.state` to find `result_json_path` and **loads the full stdout/stderr on-demand**, injecting it only into the downstream processor:

```281:343:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/plugins/rlm_context_injection_plugin.py
        result_json_path = get_rlm_state(callback_context.state, "result_json_path")
        if result_json_path:
            stdout, stderr = self._load_from_result_json(result_json_path)
            ...
        if stdout is None:
            stdout = get_rlm_state(callback_context.state, "execution_stdout")  # fallback preview
```

So the RLM “offload benefit” is achieved by splitting:
- **State**: small glue + pointers + gating (`temp:rlm:*`, `rlm:*`)
- **Storage**: big payloads (UC Volumes `result.json`, code artifacts, etc.)
- **A dedicated consumer**: `results_processor` is the one that receives the fat payload.

---

### 4) Shielding the upstream agent from “context rot”: persist only the compact summary

Issue 004 also fixes the earlier problem where results were “passed along” by prompt-history carry. Now, the processor agent writes its final output directly into **session-scoped state** via `output_key`:

```199:207:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/agent.py
results_processor_agent = LlmAgent(
    name="results_processor",
    model=_agent_model,
    output_key="rlm:last_results_summary",
    instruction="""...""",
)
```

Then the upstream analyst’s instruction template pulls that summary in, optionally:

```251:258:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/prompts.py
RESULTS_SUMMARY_SECTION = """
## Prior Results Context
{rlm:last_results_summary?}
"""
ROOT_AGENT_INSTRUCTION = RLM_SYSTEM_PROMPT + "\n" + HEALTHCARE_VENDOR_EXTENSION + RESULTS_SUMMARY_SECTION
```

Mechanically, this means:
- The **token-heavy stdout/stderr never needs to be pasted into the analyst’s context**.
- The analyst sees a **small, stable “digest”** (`rlm:last_results_summary`) and can decide whether to delegate again.

---

### 5) Why `temp:` glue doesn’t persist (and why that matters)

Your `DeltaSessionService` explicitly drops `temp:*` keys during persistence:

```43:61:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/sessions/delta_session_service.py
def _extract_state_delta(state: Optional[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    ...
    - "temp:*" -> ignored (temporary state)
    ...
            elif not key.startswith(State.TEMP_PREFIX):
                deltas["session"][key] = value
```

So the durable record gets only the “long-lived” bits (like `rlm:iteration`, `rlm:last_results_summary`), while all the per-iteration glue (`temp:rlm:*`) is **intra-invocation only**.

---

## Putting it together in RLM terms (“probe via code, offload heavy outputs, summarize elsewhere”)

1. **Probe / compute in code**: `databricks_analyst` delegates executable work via `delegate_code_results` and passes a *processor instruction* (`temp:rlm:sublm_instruction`).  
2. **Run outside the LLM context**: `job_builder` executes in Job_B, writes full outputs to `result.json` (Volumes), and only keeps small previews + the path in `temp:rlm:*`.  
3. **Inject only into the processor**: the plugin’s `before_agent_callback(callback_context)` loads the full payload by pointer and injects it as a `types.Content` message **only for `results_processor`**.  
4. **Return only the digest to the analyst**: `results_processor` persists a compact summary to `rlm:last_results_summary`, which gets templated into the analyst’s next prompt.  
5. **Stage gating prevents accidental re-use**: `temp:rlm:stage` + `temp:rlm:active_artifact_id` ensures each step runs once per delegation and ignores stale keys.

That’s the concrete mechanism by which **`callback_context.state` carries the minimal routing + pointers across states, while “token-heavy stdout / big files” are offloaded to storage + a dedicated summarizer agent**, protecting the upstream analyst from context bloat.