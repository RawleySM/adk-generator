# Issue 005: GITHUB_TOKEN Not Stored in Databricks Secrets

**Date**: 2026-01-26
**Iteration**: Test Level 15
**Severity**: Medium

## Problem

The `get_repo_file` tool fails with error "GitHub token not found" because the deploy script does not store the `GITHUB_TOKEN` environment variable in the Databricks secret scope, and the tool does not check the `adk-secrets` scope used by the deployment.

## Root Cause

Two configuration gaps:

1. **Deploy script omission**: `scripts/deploy_rlm_two_job_bundle.sh` reads `GITHUB_TOKEN` from `.env` but does not store it in the `adk-secrets` Databricks secret scope (unlike `google-api-key` and `openai-api-key` which are stored).

2. **Scope mismatch**: `get_repo_file.py` checks for the GitHub token in scopes `github`, `secrets`, `rlm` with keys `pat` and `github_token`, but the deploy script uses the `adk-secrets` scope.

## Symptoms

- `get_repo_file` tool returns: `{"status": "error", "message": "GitHub token not found. Please set GITHUB_TOKEN environment variable or configure Databricks secrets."}`
- Agent cannot download code files from GitHub repositories
- Agent compensates by using Unity Catalog metadata instead (degraded but functional)
- Task can still complete but with reduced codebase exploration capability

## Session Timeline (test_level_15_1769438334)

1. `databricks_analyst`: Called `repo_filename_search` → SUCCESS
2. `databricks_analyst`: Called `get_repo_file` → FAILED (GitHub token not found)
3. `databricks_analyst`: Proceeded with `delegate_code_results` using metadata instead
4. `job_builder`: Submitted executor job → SUCCESS
5. `results_processor`: Processed results → SUCCESS
6. `databricks_analyst`: Called `get_repo_file` again → FAILED (same error)
7. `databricks_analyst`: Called `exit_loop` and provided spec based on metadata

**Task Outcome**: Completed with workaround (agent used UC metadata instead of code inspection)

## Expected Behavior

The `get_repo_file` tool should:
1. Successfully retrieve the GitHub token from Databricks secrets
2. Download requested code files from SpendMend GitHub repositories
3. Return file contents to the agent for analysis

## Fix Implementation

### Fix 1: Update deploy script to store GITHUB_TOKEN

Add GITHUB_TOKEN storage in `scripts/deploy_rlm_two_job_bundle.sh` after the OpenAI key storage section:

```bash
# GitHub Token (needed for get_repo_file tool)
GITHUB_TOKEN_EXISTS=$(databricks secrets list-secrets "$SECRET_SCOPE" --profile "$DATABRICKS_PROFILE" --output json 2>/dev/null | \
    jq -r '.[]? | select(.key == "github-token") | .key' || echo "")

if [[ -n "$GITHUB_TOKEN" && "$GITHUB_TOKEN" != "your-github-token-here" ]]; then
    log_info "Storing github-token in secret scope..."
    echo -n "$GITHUB_TOKEN" | databricks secrets put-secret "$SECRET_SCOPE" "github-token" --profile "$DATABRICKS_PROFILE" 2>/dev/null || \
        databricks secrets put-secret "$SECRET_SCOPE" "github-token" --string-value "$GITHUB_TOKEN" --profile "$DATABRICKS_PROFILE"
    log_success "github-token stored"
elif [[ -n "$GITHUB_TOKEN_EXISTS" ]]; then
    log_success "github-token already exists in secret scope"
else
    log_warn "GITHUB_TOKEN not set in .env and not found in secret scope '$SECRET_SCOPE'"
    log_warn "The get_repo_file tool will not be able to download code from GitHub."
fi
```

### Fix 2: Update get_repo_file.py to check adk-secrets scope

Modify `_get_github_token()` in `databricks_rlm_agent/tools/get_repo_file.py`:

```python
# Try Databricks secrets - add adk-secrets scope
for scope in ["adk-secrets", "github", "secrets", "rlm"]:
    try:
        secret = client.secrets.get_secret(scope=scope, key="github-token")
        if secret and secret.value:
            return secret.value
    except Exception:
        pass
    # ... existing key checks
```

## Verification

After fix, verify:
```bash
# Redeploy with GITHUB_TOKEN in .env
./scripts/deploy_rlm_two_job_bundle.sh --run --test-level 15

# Check secret exists
databricks secrets list-secrets adk-secrets --profile rstanhope

# Expected: github-token should appear in the list
```

## Fix Verified: 2026-01-26

**Session**: `test_level_15_1769439156`

Deploy output confirmed:
```
[INFO] Storing github-token in secret scope...
[OK] github-token stored
```

Tool response changed from authentication error to proper HTTP responses:
- **Before fix**: `{"status": "error", "message": "GitHub token not found..."}`
- **After fix**: `{"status": "error", ... "error": "File not found"}` (404 - valid response)

The `get_repo_file` tool now authenticates successfully with GitHub. File not found errors are valid responses indicating the requested files don't exist at those paths in the repository (not an authentication failure).

## Impact Assessment

- **Affected Tasks**: Any task requiring code file inspection from GitHub
- **Workaround**: Agent uses UC metadata tables (`silo_dev_rs.repos.files`, `silo_dev_rs.metadata.columnnames`) instead
- **Quality Impact**: Reduced accuracy when tasks require reading actual code implementation details

## Related Files

- `scripts/deploy_rlm_two_job_bundle.sh` - Deploy script missing GITHUB_TOKEN storage
- `databricks_rlm_agent/tools/get_repo_file.py` - Tool missing adk-secrets scope check
- `.env` - Contains GITHUB_TOKEN (confirmed present)

## Patch Implementation Summary (follow-up: filepath ambiguity)

### What we confirmed in UC ADK event logs

In `silo_dev_rs.adk.events` for session `test_level_15_1769439156` (the “second run” after `github-token` was stored), the `get_repo_file` tool call **did not** submit a single composite `<repo_name>.<relative_path>` string. Instead it passed:

- `repo_name` as its own arg, and
- `filepaths` as *relative dotted paths* (no repo prefix).

This makes it easy for the agent to accidentally omit/forget the repo when constructing “UC dotted” filepaths.

### Code changes applied to remove ambiguity

Goal: Make the repo+path join explicit and tool-driven so the agent does not need to “remember to merge” fields.

- **`databricks_rlm_agent/tools/repo_filename_search.py`**
  - Added computed columns to the SELECT:
    - `uc_filepath = replace(filepath, '/', '.')`
    - `repo_uc_filepath = concat(repo_name, '.', replace(filepath, '/', '.'))`
  - Result rows now include `repo_uc_filepath` as a **single unified string**: `<repo_name>.<uc_filepath>`.

- **`databricks_rlm_agent/tools/get_repo_file.py`**
  - Updated docs to accept either `uc_filepath` (old style) or `repo_uc_filepath` (new preferred style).
  - Implemented parsing so callers can pass `filepaths=["<repo_name>.<uc_filepath>", ...]` and omit `repo_name`.
  - Safety:
    - Prefers validating the repo prefix against `tool_context.state["last_repo_search_rows"]` (repos returned by `repo_filename_search`).
    - Conservative fallback: only treats the prefix as a repo if it contains `-` (matching SpendMend repo naming) when no prior search context exists.
    - Rejects multi-repo batches (must call once per repo).

- **`databricks_rlm_agent/prompts.py`**
  - Updated guidance to prefer passing `repo_uc_filepath` directly to `get_repo_file` (single string), allowing repo inference.
