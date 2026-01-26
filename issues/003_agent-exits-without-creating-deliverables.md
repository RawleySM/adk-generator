# Issue 003: Agent Exits Without Creating Deliverables

**Date**: 2026-01-26
**Iteration**: 3 of 20
**Severity**: High

## Problem

The agent completes the RLM workflow successfully (metadata search -> delegate_code_results -> executor -> results_processor) but then calls `exit_loop` and provides a summary response instead of creating the required deliverables in `silo_dev_rs.test` schema.

## Root Cause

The agent interprets the task as an analysis/exploration task rather than a creation task. After gathering information about the source tables, it reports findings rather than proceeding to create:
1. Views in `silo_dev_rs.test`
2. A Databricks job specification
3. A submit script

The task prompt may not be sufficiently explicit about the need to create actual database objects.

## Symptoms

- Workflow completes with status: success
- Conversation completes with 1 delegation (executor ran)
- Agent provides detailed analysis of source tables
- No views/tables created in `silo_dev_rs.test`
- `exit_loop` called prematurely after analysis

## Session Timeline (test_level_12_1769429783)

1. `databricks_analyst`: Metadata searches for unsubmitted_vendors and customer_jobs
2. `delegate_code_results`: Delegated exploration code
3. `job_builder`: Submitted executor job (run ID: 157267910180968)
4. `results_processor`: Showed execution results
5. `databricks_analyst`: Additional metadata search
6. `databricks_analyst`: Called `exit_loop`
7. `databricks_analyst`: Provided final analysis response

## Expected Behavior

The agent should:
1. Use execution results to understand source data
2. Create `silo_dev_rs.test.unsubmitted_vendors_view` (or similar)
3. Create a job specification document
4. Save artifacts to `/Volumes/silo_dev_rs/adk/artifacts/`
5. Call `exit_loop` only after deliverables are created

## Potential Fixes

1. **Task Prompt Enhancement**: Add explicit instructions in test_level_12 task that the agent MUST create views/tables in the test schema before calling exit_loop

2. **Exit Loop Validation**: Modify `exit_loop` tool to validate that required deliverables exist before allowing termination

3. **Agent Instruction Update**: Add to system prompt that analysis-only responses are not acceptable for creation tasks

## Verification

After fix, verify:
```bash
uv run scripts/execute_sql.py --sql "SHOW VIEWS IN silo_dev_rs.test"
# Should show new views created by agent

uv run scripts/execute_sql.py --sql "SHOW TABLES IN silo_dev_rs.test"
# Should show new tables if applicable
```

## Current Workaround

The agent successfully identifies the correct source tables:
- `main.sm_datamart.report_mvm__unsubmitted_vendors` (~3,300 rows)
- `main.dev_datamart.report_mvm__unsubmitted_vendors` (~748 rows)

But does not create the required test schema views.

## Investigation Finding

Upon review of `databricks_rlm_agent/test_tasks.py`, the test level 12 task (DATA-792) deliverables are defined as:
1. Identify the correct source tables/views
2. Outline the logic for the submission script
3. Spec out the Databricks job configuration

The acceptance criteria are:
- Correctly identified source tables in UC
- Logic covers all filtering requirements

**The agent IS completing these deliverables correctly.** The discrepancy is that `ralphPrompt.md` expects actual creation in `silo_dev_rs.test`, but the test task definition asks for analysis/specification only.

This is NOT a bug in the agent - it's completing the task as defined in `test_tasks.py`.

To require actual object creation, the test task definition should be updated to explicitly require:
- CREATE VIEW statements executed in `silo_dev_rs.test`
- Artifacts saved to UC Volumes
