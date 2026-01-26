## Recommendation: make **UC Volumes + artifact registry** the canonical stdout/stderr transport (keep `session.state` small)

Given how Google-ADK `session.state` behaves (persisted via `DeltaSessionService`, replayed across runs, and not a great place for large blobs), the most robust fix is:

- **Treat stdout/stderr as artifacts, not state.**
- Use **UC Volumes** (already shared between Job_A and Job_B) to store the *full* execution outputs.
- Use the **artifact registry** to point to those stored outputs.
- Use `session.state` only for **small pointers + small previews** needed to drive the next sub-agent.

### Why this is the best fit for your architecture
- **Job_B already writes durable output**: `execute_artifact()` captures stdout/stderr and writes a durable `result_{run_id}_iter{iteration}.json` next to the artifact file in UC Volumes.

```171:180:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/executor.py
    # Write result.json
    result_path = _write_result_json(...)
    result["result_json_path"] = result_path
```

- **JobBuilderAgent currently pulls `stdout` from Jobs API logs**, which is the wrong transport because the executor redirects stdout into a buffer (so the run logs don’t contain user-code prints reliably), and even executor logging only shows a short preview.

```130:156:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/executor.py
        with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            exec(code, exec_globals)
...
        result["stdout"] = stdout_buffer.getvalue() or None
        result["stderr"] = stderr_buffer.getvalue() or None
```

```229:237:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/agents/job_builder.py
        stdout = result.get("logs", "")
        stderr = result.get("error", "")
        ...
        ctx.session.state["rlm:execution_stdout"] = parsed_output or stdout
```

- **Your injection plugin is already designed for “artifact keys first, state fallback”**, which is exactly what you want—except today no one sets `rlm:stdout_artifact_key` / `rlm:stderr_artifact_key`, so it always falls back to state.

```191:215:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/plugins/rlm_context_injection_plugin.py
        stdout_key = callback_context.state.get("rlm:stdout_artifact_key")
        ...
        if stdout is None:
            stdout = callback_context.state.get("rlm:execution_stdout")
```

### Concrete shape of the fix (conceptual, not editing in Ask mode)
1. **In `JobBuilderAgent` after the executor run finishes**, read the executor’s `result_{run_id}_iter{iteration}.json` **from UC Volumes** (you already know `artifact_path`, `run_id=session_id`, `iteration`).
   - Extract `stdout`, `stderr`, and `status` from that JSON.
   - Optionally still keep `result.get("logs")` for debugging, but it shouldn’t be the canonical stdout.

2. **Persist full stdout/stderr as files in UC Volumes** (same artifacts directory), e.g.:
   - `{artifact_id}_stdout.txt`
   - `{artifact_id}_stderr.txt`
   - (Or keep only `result.json` and treat it as the artifact; either is fine, but separate files simplify injection.)

3. **Update the artifact registry** with those references:
   - set `stdout_artifact_key` / `stderr_artifact_key` to the filenames (or full paths).
   - keep `metadata_json` with sizes, truncation flags, etc.

4. **In `session.state` set only the pointers + a small preview**, e.g.:
   - `rlm:stdout_artifact_key`, `rlm:stderr_artifact_key`
   - `rlm:execution_success`, `rlm:databricks_run_id`, `rlm:run_url`
   - `rlm:execution_stdout_preview` (say first/last N lines) rather than full stdout

5. **Adjust `RlmContextInjectionPlugin` to load from UC Volumes paths** when keys are present.
   - Today it calls `callback_context.load_artifact()` (ArtifactService). That’s great if/when you swap to a durable ArtifactService, but right now the runner uses `InMemoryArtifactService` (not shared across jobs), so for Job_B outputs you need a path-based loader.
   - The plugin can still keep the state fallback for safety.

Plan: I’ll pin down how `artifact_path` is constructed in `JobBuilderAgent`, compare it to how the executor derives the `result_*.json` directory, then give you the exact deterministic derivation. Then I’ll point to the narrowest “swap” location in `JobBuilderAgent` where you can replace `result["logs"]` with `result.json["stdout"]` while preserving current behavior as fallback.

[5 tools called]

## Exact path math: `artifact_path` → `result_{run_id}_iter{iteration}.json`

### What Job_B writes
The executor’s `execute_artifact()` writes the result JSON **into the same directory as the executed artifact**:

```229:250:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/executor.py
        artifact_dir = os.path.dirname(artifact_path)
        ...
        result_filename = f"result_{run_id}_iter{iteration}.json"
        result_path = os.path.join(artifact_dir, result_filename)
```

So the deterministic mapping is:

- **Input**: `artifact_path` (full path to the code file Job_B executed), e.g.  
  `/Volumes/silo_dev_rs/adk/artifacts/agent_code_<session_id>_iter<iteration>_<artifact_id>.py`
- **Derivation**:
  - `artifact_dir = os.path.dirname(artifact_path)`
  - `result_filename = f"result_{run_id}_iter{iteration}.json"`
  - `result_path = os.path.join(artifact_dir, result_filename)`
- **Output**:  
  `/Volumes/silo_dev_rs/adk/artifacts/result_<run_id>_iter<iteration>.json`

**Key subtlety:** `run_id` here is the *executor arg* `run_id` (in your orchestrator flow you pass `session_id` as `run_id`), not `databricks_run_id`.

### How Job_A sets `artifact_path`
`JobBuilderAgent` writes the code to a fixed artifacts directory:

```349:363:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/agents/job_builder.py
            filename = f"agent_code_{session_id}_iter{iteration}_{artifact_id}.py"
            path = os.path.join(self._artifacts_path, filename)
```

So `artifact_dir` is effectively `self._artifacts_path` and the result JSON will land alongside it.

## Cleanest place in `JobBuilderAgent` to switch `result["logs"]` → `result.json["stdout"]`

The narrowest, lowest-risk swap point is **right where you currently assign `stdout`/`stderr`**, before `_parse_rlm_markers()` and before setting `session.state`:

```229:247:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/agents/job_builder.py
        # Step 7: Parse stdout and extract RLM markers if present
        stdout = result.get("logs", "")
        stderr = result.get("error", "")
        parsed_output = self._parse_rlm_markers(stdout)

        # Step 8: Update state for results_processor_agent
        ctx.session.state["rlm:execution_stdout"] = parsed_output or stdout
        ctx.session.state["rlm:execution_stderr"] = stderr
```

**Recommendation for the switch (conceptually):**
- Derive `result_path` from the *same* `artifact_path`/`session_id`/`iteration` you already have.
- Load that JSON and set:
  - `stdout = result_json.get("stdout", "")`
  - `stderr = result_json.get("stderr", "")`
- Keep the old `result.get("logs")` as a **fallback** (and/or debug field), not as primary.

That keeps the rest of the flow unchanged:
- `_parse_rlm_markers(stdout)` still runs (it will just operate on real stdout).
- state keys `rlm:execution_stdout`/`rlm:execution_stderr` still get populated in one place.
- injection plugin keeps working via state fallback until you wire artifact keys.

### What this buys you
- **Correctness**: you get the *real* stdout/stderr captured by the executor, not whatever Databricks run logs happened to retain.
- **Durability across retries/restarts**: outputs live in UC Volumes and registry, independent of ADK session event size limits.
- **Better `session.state` hygiene**: you avoid blowing up Delta session tables with megabytes of stdout that you later prune anyway.
- **Scalability**: you can choose how much to inject into `results_processor` (preview vs full) without losing the full output.
