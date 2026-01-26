"""State helper utilities for RLM workflow.

This module provides helper functions for reading and writing state keys
with support for the migration from session-scoped (rlm:*) to invocation-scoped
(temp:rlm:*) state keys.

The dual-read pattern supports backward compatibility during migration:
- Read from temp:rlm:* first (new invocation-scoped keys)
- Fall back to rlm:* (legacy session-scoped keys)

See plans/refactor_key_glue.md for the full migration plan.
"""

from typing import Any, Optional, Union

# Type alias for state-like objects (dict or ADK State)
StateLike = Union[dict, Any]


def get_rlm_state(
    state: StateLike,
    key: str,
    default: Any = None,
) -> Any:
    """Read from temp:rlm:* with fallback to legacy rlm:*.

    This helper implements the dual-read pattern for migrating from
    session-scoped (rlm:*) to invocation-scoped (temp:rlm:*) state keys.

    Args:
        state: State dict or ADK State object.
        key: The key to read. Can be specified as:
            - "rlm:foo" -> reads temp:rlm:foo, falls back to rlm:foo
            - "temp:rlm:foo" -> reads temp:rlm:foo, falls back to rlm:foo
            - "foo" -> reads temp:rlm:foo, falls back to rlm:foo
        default: Default value if key not found in either location.

    Returns:
        The value from temp:rlm:* if present, else rlm:*, else default.

    Example:
        >>> state = {"temp:rlm:artifact_id": "art_123"}
        >>> get_rlm_state(state, "artifact_id")
        'art_123'
        >>> get_rlm_state(state, "rlm:artifact_id")
        'art_123'
        >>> get_rlm_state(state, "temp:rlm:artifact_id")
        'art_123'
    """
    # Normalize the key to get the base name
    base_key = key
    if key.startswith("temp:rlm:"):
        base_key = key[9:]  # Remove "temp:rlm:" prefix
    elif key.startswith("rlm:"):
        base_key = key[4:]  # Remove "rlm:" prefix

    # Build temp and legacy key names
    temp_key = f"temp:rlm:{base_key}"
    legacy_key = f"rlm:{base_key}"

    # Try temp key first, then legacy
    # IMPORTANT: Check key existence (via `in`), not `is not None`
    # This prevents: temp:rlm:artifact_id = None (cleared for deletion)
    # accidentally falling back to a stale rlm:artifact_id
    #
    # Defensive: ADK State might not implement __contains__, so we wrap
    # in try/except and fall back to the .get() pattern if needed.
    def _key_exists(s, k):
        try:
            return k in s
        except TypeError:
            # State object doesn't support `in` - fall back to get() != None
            # This is less correct (can't distinguish None value from missing)
            # but avoids breaking on non-dict-like State types
            return s.get(k) is not None

    if _key_exists(state, temp_key):
        return state.get(temp_key)

    if _key_exists(state, legacy_key):
        return state.get(legacy_key)

    return default


def set_rlm_state(
    state: StateLike,
    key: str,
    value: Any,
    *,
    use_temp: bool = True,
    dual_write: bool = False,
) -> None:
    """Write to temp:rlm:* (and optionally legacy rlm:*).

    This helper implements the migration pattern for writing state keys.
    By default, writes only to temp:rlm:* (invocation-scoped).
    Set dual_write=True to also write to rlm:* during migration.

    Args:
        state: State dict or ADK State object to modify.
        key: The key to write. Can be specified as:
            - "rlm:foo" -> writes to temp:rlm:foo (or rlm:foo if use_temp=False)
            - "temp:rlm:foo" -> writes to temp:rlm:foo
            - "foo" -> writes to temp:rlm:foo (or rlm:foo if use_temp=False)
        value: The value to write.
        use_temp: If True, write to temp:rlm:* (default). If False, write to rlm:*.
        dual_write: If True, write to both temp:rlm:* and rlm:* (migration mode).

    Example:
        >>> state = {}
        >>> set_rlm_state(state, "artifact_id", "art_123")
        >>> state
        {'temp:rlm:artifact_id': 'art_123'}
        >>> set_rlm_state(state, "artifact_id", "art_456", dual_write=True)
        >>> state
        {'temp:rlm:artifact_id': 'art_456', 'rlm:artifact_id': 'art_456'}
    """
    # Normalize the key to get the base name
    base_key = key
    if key.startswith("temp:rlm:"):
        base_key = key[9:]  # Remove "temp:rlm:" prefix
    elif key.startswith("rlm:"):
        base_key = key[4:]  # Remove "rlm:" prefix

    # Build key names
    temp_key = f"temp:rlm:{base_key}"
    legacy_key = f"rlm:{base_key}"

    # Write to appropriate key(s)
    if use_temp:
        state[temp_key] = value
        if dual_write:
            state[legacy_key] = value
    else:
        state[legacy_key] = value


# Key constants for invocation-scoped state (temp:rlm:*)
# These replace the legacy session-scoped rlm:* keys for invocation glue

# Delegation inputs (set by delegate_code_results, read by job_builder)
TEMP_ARTIFACT_ID = "temp:rlm:artifact_id"
TEMP_SUBLM_INSTRUCTION = "temp:rlm:sublm_instruction"
TEMP_HAS_AGENT_CODE = "temp:rlm:has_agent_code"
TEMP_CODE_ARTIFACT_KEY = "temp:rlm:code_artifact_key"
TEMP_SESSION_ID = "temp:rlm:session_id"
TEMP_INVOCATION_ID = "temp:rlm:invocation_id"

# Execution results (set by job_builder, read by results_processor/plugins)
TEMP_EXECUTION_STDOUT = "temp:rlm:execution_stdout"
TEMP_EXECUTION_STDERR = "temp:rlm:execution_stderr"
TEMP_EXECUTION_SUCCESS = "temp:rlm:execution_success"
TEMP_DATABRICKS_RUN_ID = "temp:rlm:databricks_run_id"
TEMP_RUN_URL = "temp:rlm:run_url"
TEMP_RESULT_JSON_PATH = "temp:rlm:result_json_path"
TEMP_STDOUT_TRUNCATED = "temp:rlm:stdout_truncated"
TEMP_STDERR_TRUNCATED = "temp:rlm:stderr_truncated"

# Control flags (set by tools/agents, read by run.py/cli.py)
TEMP_EXIT_REQUESTED = "temp:rlm:exit_requested"
TEMP_FATAL_ERROR = "temp:rlm:fatal_error"
TEMP_FATAL_ERROR_MSG = "temp:rlm:fatal_error_msg"

# Session-scoped keys (NOT migrated to temp:*)
# These persist across invocations intentionally
SESSION_ITERATION = "rlm:iteration"

# Legacy key constants (for reference during migration)
LEGACY_ARTIFACT_ID = "rlm:artifact_id"
LEGACY_SUBLM_INSTRUCTION = "rlm:sublm_instruction"
LEGACY_HAS_AGENT_CODE = "rlm:has_agent_code"
LEGACY_CODE_ARTIFACT_KEY = "rlm:code_artifact_key"
LEGACY_EXIT_REQUESTED = "rlm:exit_requested"
LEGACY_FATAL_ERROR = "rlm:fatal_error"
LEGACY_FATAL_ERROR_MSG = "rlm:fatal_error_msg"
