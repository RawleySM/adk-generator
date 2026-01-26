I'll help you generate a ralph:loop prompt. First, let me gather the necessary context by examining the test level 12 task definition and the relevant scripts.

[3 tools called]

Based on my analysis of the test level 12 task (DATA-792: "MVM - Create/Modify SM Common submit script") and the deployment workflow, here's your ralph:loop prompt:

---

## ralph:loop Prompt

```
ralph:loop --max-iterations 5 --completion-promise "All deliverables created in silo_dev_rs.test schema and task DATA-792 marked complete"
```

### Prompt Body

```markdown
## Objective

Execute test level 12 (DATA-792) via the RLM Agent and iterate until the task is completed end-to-end without failure.

## Pre-Execution: Locate the Jira Ticket

Before beginning iterations, query the Jira source of truth to confirm task details:

```bash
uv run scripts/execute_sql.py select silo_dev_rs.task.jira_raw_data \
  -w "key = 'DATA-792'" \
  -c "key, summary, description, status"
```

Verify the ticket exists and capture the full description for reference.

---

## Loop Task: Deploy & Run Test Level 12

Execute the canonical deployment + run command:

```bash
./scripts/deploy_rlm_two_job_bundle.sh --run --test-level 12
```

### Task Summary (DATA-792)

**MVM - Create/Modify SM Common submit script**

Create a script and Databricks job that submits unsubmitted vendors from SM Common to Master Vendor Management's submission API with configurable parameters:

| Parameter            | Default Value | Description                                      |
|----------------------|---------------|--------------------------------------------------|
| `min_volume`         | $5000         | Minimum spend threshold                          |
| `spend_volume_rank`  | 95%           | Top percentile of vendor spend                   |
| `is_person`          | false         | Exclude vendors marked as persons                |

### Required Deliverables

1. **Source Table Identification**: Identify correct UC tables/views for:
   - Unsubmitted vendors: `silo_{code}.sm.export_mvm__unsubmitted_vendors`
   - Submitted jobs mapping: `main.mvm.customer_jobs`

2. **Script Logic Specification**: Document the full workflow:
   - Query unsubmitted vendors with filtering logic
   - Submit to MVM API
   - Append customer/job mapping to MVM Submitted Jobs

3. **Databricks Job Configuration Spec**: Define job parameters, cluster config, and scheduling

---

## CRITICAL CONSTRAINTS

### Schema Restrictions (MANDATORY)

⚠️ **ALL deliverables MUST be created in `silo_dev_rs.test` schema**

- ✅ `silo_dev_rs.test.unsubmitted_vendors_view`
- ✅ `silo_dev_rs.test.mvm_submission_staging`
- ✅ `silo_dev_rs.test.customer_jobs`
- ❌ **NO views or tables in production schemas** (e.g., `silo_dev_rs.sm.*`, `main.mvm.*`)

Views will only be promoted to production after human review.

### Success Criteria

The agent MUST:
1. Complete all acceptance criteria from DATA-792
2. Create any required views/tables ONLY in `silo_dev_rs.test`
3. Save artifacts to `/Volumes/silo_dev_rs/adk/artifacts/`
4. Exit cleanly via `exit_loop` tool with success status

### Failure Handling

If the agent fails or errors:
1. Review the run output in `runtime_monitor/`
2. Check session state: `silo_dev_rs.adk.sessions`
3. Check event log: `silo_dev_rs.adk.events`
4. Iterate with `./scripts/deploy_rlm_two_job_bundle.sh --run --test-level 12`

---

## Post-Completion Verification

After successful completion, verify deliverables:

```bash
# Check test schema for created views
uv run scripts/execute_sql.py --sql "SHOW VIEWS IN silo_dev_rs.test"

# Verify no production writes occurred
uv run scripts/execute_sql.py history silo_dev_rs.sm.export_mvm__unsubmitted_vendors --limit 1

# Check artifact output
ls -la /Volumes/silo_dev_rs/adk/artifacts/
```

---

## Exit Condition

Loop terminates when:
- Agent completes DATA-792 with all deliverables in `silo_dev_rs.test`
- No errors in run output
- `exit_loop` called with success status
- OR max iterations (5) reached
```

---

### Key Points

| Aspect | Value |
|--------|-------|
| Test Level | 12 |
| Jira Ticket | DATA-792 |
| Target Schema | `silo_dev_rs.test` (NOT production) |
| Max Iterations | 5 |
| Deploy Command | `./scripts/deploy_rlm_two_job_bundle.sh --run --test-level 12` |
| Jira Query | `uv run scripts/execute_sql.py select silo_dev_rs.task.jira_raw_data -w "key = 'DATA-792'"` |