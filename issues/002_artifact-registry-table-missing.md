# Issue 002: Artifact Registry Table Missing

**Date**: 2026-01-26
**Iteration**: 2 of 20
**Severity**: Critical

## Problem

The `artifact_registry` table does not exist in the `silo_dev_rs.adk` schema. This table is required by the RLM workflow to track artifact metadata across delegations.

When `delegate_code_results` tool tries to create an artifact registry entry, it fails silently because the table doesn't exist.

## Root Cause

The artifact_registry table was never created in the adk schema. The table should be auto-created by the `ArtifactRegistry.ensure_table_exists()` method, but this may not be getting called during initialization.

## Symptoms

- `delegate_code_results` logs "Warning: Could not create registry entry"
- Artifact tracking between delegations is broken
- `job_builder` may fail to find artifact metadata
- Agent workflow completes without creating deliverables

## Fix Applied

Create the artifact_registry table in silo_dev_rs.adk with the required schema.

### SQL to Create Table

```sql
CREATE TABLE IF NOT EXISTS silo_dev_rs.adk.artifact_registry (
    artifact_id STRING NOT NULL,
    session_id STRING,
    invocation_id STRING,
    iteration INT,
    artifact_type STRING,
    status STRING,
    sublm_instruction STRING,
    code_artifact_key STRING,
    metadata STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
```

## Verification

After creating the table, verify:
```bash
uv run scripts/execute_sql.py --sql "DESCRIBE silo_dev_rs.adk.artifact_registry"
```

Then re-run test level 12 to confirm artifacts are being tracked.

## Prevention

- Add table creation check to the deployment script
- Include artifact_registry table in schema initialization
- Add integration test that verifies artifact registry operations
