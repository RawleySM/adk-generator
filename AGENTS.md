# Agent workflow notes (Databricks RLM Agent)

## Local deploy + run (required entrypoint)

When iterating locally, **always use** `scripts/deploy_rlm_two_job_bundle.sh` to deploy and run the latest wheel (otherwise you can easily be running stale code in Databricks).

### Canonical command (test task level 7)

```bash
./scripts/deploy_rlm_two_job_bundle.sh --run --test-level 7
```

### Why this script matters

This script is the authoritative “local entrypoint” because it:

- **Up-revs the wheel version** in `databricks_rlm_agent/pyproject.toml` (cache-busting).
- **Builds and deploys** the Databricks Asset Bundle (`databricks bundle deploy`), uploading the new wheel to the bundle workspace path.
- **Resolves and wires job IDs** (Orchestrator / Executor / Ingestor) and updates the orchestrator’s `ADK_EXECUTOR_JOB_ID`.
- **Ensures secrets exist** in the configured secret scope.
- Optionally **triggers a run** with `--run`, and sets `TEST_LEVEL` via `--test-level <N>` to drive deterministic E2E tasks.

### Observability checks after a run

- Session/event persistence:
  - `silo_dev_rs.adk.sessions`
  - `silo_dev_rs.adk.events`
- Artifact output (written by `job_builder` + executor):
  - `ADK_ARTIFACTS_PATH` (default: `/Volumes/silo_dev_rs/adk/artifacts`)

