### Recommendation (given your current code)

We are halfway between **an in-memory ADK artifact workflow** and **a cross-job “Volumes file” workflow**. For Databricks Job_A → Job_B, make the **Volumes file path the single source of truth** *now*


- **Standardize the contract as “executor runs a file path”**
  - Keep what `JobBuilderAgent` already does: write `agent_code_<...>.py` under `ADK_ARTIFACTS_PATH` and pass it to Job_B as `ARTIFACT_PATH` (that’s already wired end-to-end).
  - Treat ADK ArtifactService (`InMemoryArtifactService`) as *Job_A-local only* (it can’t span Job_A → Job_B anyway).

- **Stop relying on `temp:parsed_blob` for anything durable**
  - Delta session persistence trims temp state before persisting events (`DeltaSessionService.append_event` calls `_trim_temp_delta_state`), so temp state isn’t a safe cross-step/cross-run transport.

```670:700:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/sessions/delta_session_service.py
# Trim temp state before persisting
event = self._trim_temp_delta_state(event)
```

- **Delete `ADK_AGENT_CODE_PATH`**
  - It’s configured in the bundle, but the current execution path doesn’t use it (JobBuilder writes to `ADK_ARTIFACTS_PATH` with its own filename). Keeping both paths invites drift.

```343:374:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/agents/job_builder.py
filename = f"agent_code_{session_id}_iter{iteration}_{artifact_id}.py"
path = os.path.join(self._artifacts_path, filename)
with open(path, 'w') as f:
    f.write(code)
return path
```

