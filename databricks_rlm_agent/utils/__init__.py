"""Utility modules for Databricks RLM Agent."""

from .docstring_parser import (
    ParsedDelegationBlob,
    parse_delegation_blob,
    DelegationBlobParseError,
)
from .state_helpers import (
    get_rlm_state,
    set_rlm_state,
    # Temp key constants (invocation-scoped)
    TEMP_ARTIFACT_ID,
    TEMP_SUBLM_INSTRUCTION,
    TEMP_HAS_AGENT_CODE,
    TEMP_CODE_ARTIFACT_KEY,
    TEMP_SESSION_ID,
    TEMP_INVOCATION_ID,
    TEMP_EXECUTION_STDOUT,
    TEMP_EXECUTION_STDERR,
    TEMP_EXECUTION_SUCCESS,
    TEMP_DATABRICKS_RUN_ID,
    TEMP_RUN_URL,
    TEMP_RESULT_JSON_PATH,
    TEMP_STDOUT_TRUNCATED,
    TEMP_STDERR_TRUNCATED,
    TEMP_EXIT_REQUESTED,
    TEMP_FATAL_ERROR,
    TEMP_FATAL_ERROR_MSG,
    # Session-scoped keys
    SESSION_ITERATION,
    # Legacy key constants
    LEGACY_ARTIFACT_ID,
    LEGACY_SUBLM_INSTRUCTION,
    LEGACY_HAS_AGENT_CODE,
    LEGACY_EXIT_REQUESTED,
    LEGACY_FATAL_ERROR,
    LEGACY_FATAL_ERROR_MSG,
)

__all__ = [
    "ParsedDelegationBlob",
    "parse_delegation_blob",
    "DelegationBlobParseError",
    "get_rlm_state",
    "set_rlm_state",
    # Temp key constants
    "TEMP_ARTIFACT_ID",
    "TEMP_SUBLM_INSTRUCTION",
    "TEMP_HAS_AGENT_CODE",
    "TEMP_CODE_ARTIFACT_KEY",
    "TEMP_SESSION_ID",
    "TEMP_INVOCATION_ID",
    "TEMP_EXECUTION_STDOUT",
    "TEMP_EXECUTION_STDERR",
    "TEMP_EXECUTION_SUCCESS",
    "TEMP_DATABRICKS_RUN_ID",
    "TEMP_RUN_URL",
    "TEMP_RESULT_JSON_PATH",
    "TEMP_STDOUT_TRUNCATED",
    "TEMP_STDERR_TRUNCATED",
    "TEMP_EXIT_REQUESTED",
    "TEMP_FATAL_ERROR",
    "TEMP_FATAL_ERROR_MSG",
    "SESSION_ITERATION",
    # Legacy keys
    "LEGACY_ARTIFACT_ID",
    "LEGACY_SUBLM_INSTRUCTION",
    "LEGACY_HAS_AGENT_CODE",
    "LEGACY_EXIT_REQUESTED",
    "LEGACY_FATAL_ERROR",
    "LEGACY_FATAL_ERROR_MSG",
]
