# Issue 006: get_repo_file Tool Mis-infers Repository Name Leading to 404 Errors

**Date**: 2026-01-26
**Iteration**: Test Level 16
**Severity**: High

## Problem

The `get_repo_file` tool fails with "File not found" (HTTP 404) errors when attempting to download files from repositories that have not been recently returned by the `repo_filename_search` tool. Even when the agent provides correct repo-prefixed filepaths, the tool's inference logic overrides the intended repository if exactly one unrelated repository is present in the session's search context.

## Root Cause

A bug in the repository inference logic in `databricks_rlm_agent/tools/get_repo_file.py`:

1. **Greedy Inference**: In the `get_repo_file` function, if `len(known_repos) == 1`, the tool automatically sets `inferred_repo` to that single known repository.
2. **Heuristic Suppression**: The hyphen-based heuristic (`-` in prefix) which correctly identifies SpendMend repositories is only applied if `known_repos` is completely empty. 
3. **Context Sensitivity**: If a previous unrelated search (e.g., for general documentation) found a single file in a different repository (e.g., `SM_Portal`), `known_repos` is no longer empty, and the tool will incorrectly force all subsequent downloads to use `SM_Portal` unless they exactly match that one known repo.

## Symptoms

- `get_repo_file` tool returns: `{"status": "error", "message": "Failed to download all 5 files. Check individual file errors for details."}`
- Individual file errors are: `"error": "File not found"`
- Logs indicate the wrong repository is being targeted: `Starting download of 5 files from SM_Portal` (when the files belong to `Master-Vendor-Alignment`).
- Agent is unable to read codebase contents, forcing reliance on metadata or direct Spark SQL workarounds.

## Session Timeline (test_level_16_1769446985)

1. `databricks_analyst`: Called `repo_filename_search` for `Master-Vendor-Alignment` → SUCCESS but returned 0 rows (Secondary issue: tool failed to find existing data).
2. `databricks_analyst`: Used `delegate_code_results` to run Spark SQL → SUCCESS (confirmed 63k+ rows and existence of `Master-Vendor-Alignment` files).
3. `databricks_analyst`: Called `repo_filename_search` for 'README.md' → SUCCESS (found match in `SM_Portal`). `known_repos` now contains only `{'SM_Portal'}`.
4. `databricks_analyst`: Called `get_repo_file` for `Master-Vendor-Alignment` paths → FAILED (Tool mis-inferred `SM_Portal` as the repo; 404 errors).
5. `databricks_analyst`: Attempted download again with different paths → FAILED (same error).
6. `databricks_analyst`: Completed task using direct volume scans and table metadata as a workaround.

**Task Outcome**: Completed with workaround (agent bypassed the download tool).

## Expected Behavior

The `get_repo_file` tool should:
1. Prioritize repo name inference from the filepath prefix (e.g., `Master-Vendor-Alignment.README.md` -> `repo_name='Master-Vendor-Alignment'`).
2. Only default to a 'known' repository if the prefix matches it or if no prefix is present.
3. Avoid defaulting to an unrelated repository just because it was the only one found in a previous search.

## Fix Implementation (Draft)

### Fix: Update get_repo_file.py inference logic

Modify `databricks_rlm_agent/tools/get_repo_file.py` to allow the hyphen heuristic to run even when `known_repos` is not empty:

```python
    def _maybe_split_repo_uc_filepath(fp: str) -> tuple[Optional[str], str]:
        if not fp or "." not in fp:
            return None, fp
        prefix, rest = fp.split(".", 1)
        # Check known repos first
        if known_repos and prefix in known_repos:
            return prefix, rest
        # Fallback heuristic: SpendMend repo names typically include '-' 
        # Apply this heuristic to detect new repos even if known_repos is populated.
        if "-" in prefix:
            return prefix, rest
        return None, fp
```

And ensure the default logic is more conservative:

```python
    if not inferred_repo:
        if len(known_repos) == 1:
            # Only default if the first requested path doesn't look like it has a different repo prefix
            first_prefix, _ = _maybe_split_repo_uc_filepath(str(filepaths[0]))
            if not first_prefix or first_prefix == next(iter(known_repos)):
                inferred_repo = next(iter(known_repos))
```

## Impact Assessment

- **Affected Tasks**: Any multi-repo discovery task or tasks where initial searches are sparse.
- **Workaround**: Agent uses `delegate_code_results` to query tables directly or `ls` on volumes.
- **Quality Impact**: High. Prevents the agent from accessing the actual implementation details of the codebase, which is the primary purpose of the RLM system.

## Related Files

- `databricks_rlm_agent/tools/get_repo_file.py` - Primary location of the inference bug.
- `databricks_rlm_agent/tools/repo_filename_search.py` - Secondary issue (failed to find repos initially).
- `run_output_785535593357729.json` - Source of error logs.
