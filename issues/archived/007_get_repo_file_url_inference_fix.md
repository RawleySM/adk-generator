# Issue 007: get_repo_file URL Inference Failure

## Problem Summary

From run `785535593357729`, the model failed to download files due to incorrect repo inference:

```
Arguments: {'filepaths': ['Master-Vendor-Alignment.README.md', 'SpendMend-Data-Databricks.README.md', 'jira_toggl_sync.README.md'], ...}

Starting download of 5 files from SM_Portal  ← WRONG REPO!
FAILED: File not found (all 5 files)
```

### Root Cause

The coupling between `repo_filename_search` and `get_repo_file` was fragile:

1. `repo_filename_search` stores `last_repo_search_rows` in `tool_context.state`
2. `get_repo_file._maybe_split_repo_uc_filepath()` validates repo prefixes against `known_repos`
3. If state is stale/mismatched, validation fails and the wrong repo (`SM_Portal`) is inferred
4. The model also mixed filepaths from **multiple repos** in a single call (not supported in legacy mode)

### Error Flow

```
Search A → returns {Master-Vendor-Alignment, SpendMend-Data-Databricks, jira_toggl_sync}
Search B → returns {SM_Portal} (overwrites state!)
get_repo_file(filepaths from Search A) → known_repos = {SM_Portal} → inference fails → uses SM_Portal
```

## Solution Implemented

### 1. Added `raw_github_url` column to `repo_filename_search`

```sql
concat('https://raw.githubusercontent.com/SpendMend/', repo_name, '/main/', filepath) AS raw_github_url
```

Now returns ready-to-use URLs like:
```
https://raw.githubusercontent.com/SpendMend/Master-Vendor-Alignment/main/README.md
```

### 2. Added RAW URL MODE to `get_repo_file`

When filepaths start with `https://raw.githubusercontent.com/`, the tool:
- Bypasses all repo inference logic
- Uses `_download_from_raw_url()` directly
- Supports **multiple repos** in a single call
- Is completely stateless and deterministic

### 3. Updated Docstrings

Both tools now emphasize the recommended workflow:
1. Search → get `raw_github_url` from results
2. Download → pass URLs directly to `get_repo_file`

## Files Modified

- `databricks_rlm_agent/tools/repo_filename_search.py`
  - Added `raw_github_url` computed column
  - Updated docstring with sample row and workflow

- `databricks_rlm_agent/tools/get_repo_file.py`
  - Added `_is_raw_github_url()` detection function
  - Added `_parse_raw_github_url()` URL parser
  - Added `_download_from_raw_url()` direct download function
  - Added RAW URL MODE at start of main function
  - Updated docstring to emphasize URL-based workflow

## Testing

Deploy and run with test level 16:
```bash
./scripts/deploy_rlm_two_job_bundle.sh --run --test-level 16
```

Expected behavior:
- `repo_filename_search` results include `raw_github_url` column
- Model uses `raw_github_url` values directly in `get_repo_file`
- Downloads succeed without repo inference errors
- Log shows `[RAW URL MODE]` for deterministic downloads

## Benefits

| Before | After |
|--------|-------|
| Stateful repo inference | Stateless URL parsing |
| Single-repo per call | Multi-repo per call |
| Fragile state coupling | Self-contained URLs |
| Model must assemble URLs | URLs ready-to-use |
