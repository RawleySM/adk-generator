You are writing a bash startup script to act as the local entrypoint for the Databricks RLM Agent.

Context:
- Repo root contains `databricks_rlm_agent/` and `scripts/`.
- Local runtime is enabled via `ADK_RUN_MODE=local`.
- UC queries in local mode use `databricks_rlm_agent/sql_warehouse.py` which authenticates via databricks-sdk WorkspaceClient using:
  - `DATABRICKS_PROFILE` (default `rstanhope`) OR
  - `DATABRICKS_HOST` + `DATABRICKS_TOKEN`.
- SQL warehouse selection is:
  - `ADK_SQL_WAREHOUSE_ID` if set; otherwise auto-discovers a RUNNING warehouse; otherwise starts a STOPPED one.
- Test task prompts exist at `scripts/test_tasks.py` with `get_task_prompt(level)` returning a prompt string.
- The agent can be run locally via Python by calling:
  - `from databricks_rlm_agent.run import main` and `asyncio.run(main(prompt=..., run_mode="local"))`.

Goal:
Create `scripts/run_local_rlm.sh` that:
1) Loads `.env` from repo root if present (like deploy script).
2) Performs startup checks (fail-fast where appropriate):
   - Verify required commands exist: `python` (or `uv` if you choose), and optionally `databricks` CLI + `jq` (warn or fail depending on whether used).
   - Verify `GOOGLE_API_KEY` is set (hard fail if missing).
   - Warn if `OPENAI_API_KEY` missing.
   - Warn if `GITHUB_TOKEN` missing.
   - Verify Databricks authentication is available:
     - either `DATABRICKS_PROFILE` is configured for databricks-sdk / CLI auth, OR `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are set.
   - Verify SQL Warehouse connectivity:
     - run a Python “health check” that imports `databricks_rlm_agent.sql_warehouse.execute_sql`,
       authenticates, resolves/starts a warehouse, and executes `SELECT 1` successfully.
3) Accept CLI args similar in style to `scripts/deploy_rlm_two_job_bundle.sh`:
   - `--test-level <N>`: loads prompt from `scripts/test_tasks.py:get_task_prompt(N)`
   - `--prompt-file <path>`: read prompt from local file if provided
   - `--prompt <string>`: literal prompt override
   - `--session-id <id>` optional; if omitted, generate one with timestamp and include test level
   - `--max-iterations <N>` optional (export ADK_MAX_ITERATIONS)
   - `--profile <name>` optional (sets DATABRICKS_PROFILE for the health check + runtime)
   - `--warehouse-id <id>` optional (sets ADK_SQL_WAREHOUSE_ID)
   - `--dry-run-checks`: run checks only, don’t start the agent
   - `--help`
4) After checks, run the agent locally by invoking Python:
   - Set env vars:
     - `ADK_RUN_MODE=local`
     - `ADK_LOCAL_DB_PATH` default `.adk_local/adk.duckdb` (allow override)
     - `ADK_LOCAL_ARTIFACTS_PATH` default `.adk_local/artifacts` (allow override)
     - `ADK_SQL_WAREHOUSE_ID` if provided
     - `DATABRICKS_PROFILE` if provided
   - Then execute the agent with the chosen prompt (test task prompt or user prompt/file prompt).
5) Output should be well-logged, with clear [INFO]/[WARN]/[ERROR] prefixes.
6) Use strict bash settings (`set -euo pipefail`) and careful quoting.

Observability requirements (stdout visibility):
- The script MUST be “chatty” by default: print a step banner for each major phase (config load, dependency checks, env summary, auth check, warehouse check, prompt resolution, agent run).
- Implement timestamped log helpers: log_info/log_warn/log_error/log_success and die().
- Provide flags:
  - --log-level {INFO,DEBUG} (default INFO)
  - --trace (enables set -x with PS4 including time + file:line)
  - --checks-only (alias of --dry-run-checks) prints what would run then exits 0
- Print a sanitized environment snapshot at start (only YES/NO for secrets; never print secret values).
- Print a “Resolved run plan” block before executing:
  - prompt source + size in chars
  - session id
  - DATABRICKS_PROFILE and whether DATABRICKS_HOST/TOKEN are set (YES/NO)
  - ADK_SQL_WAREHOUSE_ID if provided; otherwise say “auto-discover”
  - ADK_LOCAL_DB_PATH and ADK_LOCAL_ARTIFACTS_PATH
  - git commit hash + git status summary if available
- The Python SQL warehouse health check MUST print:
  - auth method used (profile vs env), current user identifier, chosen warehouse name/id/state, statement_id, and query elapsed time for SELECT 1
  - on failure, print actionable remediation text
- Prefix subprocess output lines with tags ([CHECK], [AGENT]) so humans can skim.

Deliverables:
- Provide the complete `scripts/run_local_rlm.sh` contents.
- Include usage examples in comments at top:
  - `./scripts/run_local_rlm.sh --test-level 7`
  - `./scripts/run_local_rlm.sh --prompt-file ./my_task.txt`
  - `./scripts/run_local_rlm.sh --profile rstanhope --warehouse-id <id> --test-level 3`
- Include a small embedded Python snippet in the script for the SQL warehouse health check.