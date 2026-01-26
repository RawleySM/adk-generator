## Prompt for Coding Agent

---

### Task: Refactor CLI to Use ADK LoopAgent for Code Execution

**Objective:** Remove the custom execution loop from `databricks_rlm_agent/cli.py` and rely entirely on `JobBuilderAgent` inside the ADK LoopAgent for code execution. This eliminates the redundant parallel execution mechanism and unifies the architecture.

---

### Background

Currently there are **two parallel execution paths**:

1. **CLI orchestration path** (`cli.py`): `_run_orchestrator()` checks for `AGENT_CODE_PATH`, calls `_execute_code_inline()` or `submit_and_wait()`, manages its own feedback loop.

2. **ADK LoopAgent path** (`agent.py` + `run.py`): `databricks_analyst` → `delegate_code_results()` → `JobBuilderAgent` → `results_processor_agent`, with context injection/pruning plugins.

This causes `delegate_code_results` to not trigger Job_B execution properly because the CLI loop is looking for a different artifact mechanism (`AGENT_CODE_PATH` from `save_python_code`) than what `delegate_code_results` produces (state keys + ArtifactService).

---

### Requirements

1. **Simplify `orchestrator_main()` and `_run_orchestrator()`** to:
   - Set up the ADK Runner via `create_runner()` from `run.py`
   - Create/resume session
   - Call `run_conversation()` with the user prompt
   - Let the ADK LoopAgent (with `JobBuilderAgent`) handle all iteration, execution, and feedback internally
   - Remove the manual artifact detection, inline execution, and feedback loop logic

2. **Remove these CLI helper functions** (now handled by `JobBuilderAgent`):
   - `_should_use_inline_execution()`
   - `_execute_code_inline()`
   - The `AGENT_CODE_PATH` detection and manual execution block in `_run_orchestrator()`

3. **Keep these CLI components**:
   - `orchestrator_main()`, `executor_main()`, `ingestor_main()`, `test_main()` as entrypoints
   - `_setup_logging()`, `_get_job_parameter()`, `_get_job_parameters_from_run()`, `_get_spark_session()`, `_read_prompt_file()` — these are still useful
   - Telemetry event recording for orchestrator start/complete
   - The `--test-level` handling for loading test tasks

4. **Ensure `JobBuilderAgent`** properly handles:
   - Writing code to artifact path for executor job submission
   - Submitting executor jobs via `_submit_and_wait()` (inline execution removed)
   - Setting state keys for `results_processor_agent`

5. **Verify the LoopAgent iteration** is controlled by:
   - `max_iterations` on the LoopAgent (in `agent.py`)
   - `exit_loop` tool for early termination
   - Not by the CLI's `args.max_iterations` loop

6. **Update imports** in `cli.py` to remove unused imports (e.g., `from .tools.save_python_code import AGENT_CODE_PATH` if no longer needed).

---

### Key Files to Update

| File | Changes |
|------|---------|
| `databricks_rlm_agent/cli.py` | **Major refactor**: Remove `_should_use_inline_execution()`, `_execute_code_inline()`, and the manual execution/feedback loop in `_run_orchestrator()`. Simplify to just call `run_conversation()` and let ADK handle iteration. |
| `databricks_rlm_agent/run.py` | **Minor**: Possibly add a parameter to `run_conversation()` for `max_iterations` override if needed, or ensure the LoopAgent's `max_iterations` is configurable. |
| `databricks_rlm_agent/agent.py` | **Review**: Ensure `root_agent` (LoopAgent) has appropriate `max_iterations` and that `JobBuilderAgent` is properly wired. Consider making `max_iterations` configurable via env var. |
| `databricks_rlm_agent/agents/job_builder.py` | **Updated**: Inline execution removed; all code now submits to executor job via `_submit_and_wait()`. Confirm it correctly reads `ADK_EXECUTOR_JOB_ID`. |
| `databricks_rlm_agent/tools/save_python_code.py` | **Deprecate/remove**: This was used by the CLI path; `delegate_code_results` uses ArtifactService instead. Remove from agent tools if not needed. |
| `databricks_rlm_agent/tools/__init__.py` | **Update**: Remove `save_python_code` export if deprecated. |

---

### Testing

1. Run `rlm-test --level 1` and verify the agent:
   - Calls `delegate_code_results()`
   - `JobBuilderAgent` executes the code (check logs for `[JOB_BUILDER]` messages)
   - `results_processor_agent` receives and processes output
   - Loop terminates via `exit_loop` or max iterations

2. Set `ADK_EXECUTOR_JOB_ID` and verify Job_B is submitted via the Jobs API (inline execution removed; all code runs in separate executor jobs).

---

### Acceptance Criteria

- [ ] `rlm-orchestrator` entrypoint works and delegates all execution to ADK LoopAgent
- [ ] `delegate_code_results()` triggers code execution via `JobBuilderAgent`
- [ ] Job submission works via `JobBuilderAgent._submit_and_wait()` (all code runs in separate executor jobs)
- [ ] `results_processor_agent` receives execution context via `RlmContextInjectionPlugin`
- [ ] `exit_loop` tool properly terminates the LoopAgent
- [ ] Telemetry events are still recorded for orchestrator start/complete
- [ ] `--test-level` flag still works for loading test tasks
- [ ] No duplicate/redundant execution logic remains in `cli.py`