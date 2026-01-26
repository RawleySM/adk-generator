"""RLM Context Pruning Plugin.

This plugin provides an after_agent_callback for the results_processor agent
that cleans up state after processing is complete.

When results_processor_agent finishes, this plugin:
1. Marks the artifact as consumed in the registry
2. Clears invocation-scoped state keys (both temp:rlm:* and legacy rlm:*)
3. Preserves the session-scoped iteration counter for tracking

State key design:
- During migration, clears both temp:rlm:* and legacy rlm:* keys
- After migration, temp:rlm:* keys auto-discard (clearing is defensive)
- See plans/refactor_key_glue.md for the migration plan

This prevents stale artifact data from affecting subsequent iterations
and keeps the state clean for the next loop cycle.
"""

from __future__ import annotations

import logging
from typing import Any, Optional, TYPE_CHECKING

from google.adk.agents.callback_context import CallbackContext
from google.adk.plugins.base_plugin import BasePlugin
from google.genai import types

if TYPE_CHECKING:
    from google.adk.agents.base_agent import BaseAgent

logger = logging.getLogger(__name__)

# Import state helpers for dual-read pattern
from databricks_rlm_agent.utils.state_helpers import get_rlm_state

# State keys to check for artifact (dual-read)
STATE_ARTIFACT_ID = "temp:rlm:artifact_id"
STATE_TEMP_PARSED_BLOB = "temp:parsed_blob"

# Keys to clear during migration (both temp:rlm:* and legacy rlm:*)
# After migration completes, temp:rlm:* keys auto-discard so this is defensive
INVOCATION_KEYS_TO_CLEAR = [
    # Delegation inputs (temp:rlm:*)
    "temp:rlm:artifact_id",
    "temp:rlm:sublm_instruction",
    "temp:rlm:has_agent_code",
    "temp:rlm:code_artifact_key",
    "temp:rlm:session_id",
    "temp:rlm:invocation_id",
    # Execution results (temp:rlm:*)
    "temp:rlm:execution_stdout",
    "temp:rlm:execution_stderr",
    "temp:rlm:execution_success",
    "temp:rlm:databricks_run_id",
    "temp:rlm:run_url",
    "temp:rlm:result_json_path",
    "temp:rlm:stdout_truncated",
    "temp:rlm:stderr_truncated",
    # Control flags (temp:rlm:*)
    "temp:rlm:exit_requested",
    "temp:rlm:fatal_error",
    "temp:rlm:fatal_error_msg",
    # Temp state
    "temp:parsed_blob",
]

# Legacy keys to clear during migration (will be removed after migration)
LEGACY_KEYS_TO_CLEAR = [
    "rlm:artifact_id",
    "rlm:sublm_instruction",
    "rlm:has_agent_code",
    "rlm:code_artifact_key",
    "rlm:stdout_artifact_key",
    "rlm:stderr_artifact_key",
    "rlm:session_id",
    "rlm:invocation_id",
    "rlm:execution_stdout",
    "rlm:execution_stderr",
    "rlm:execution_success",
    "rlm:databricks_run_id",
    "rlm:run_url",
    "rlm:result_json_path",
    "rlm:stdout_truncated",
    "rlm:stderr_truncated",
    "rlm:exit_requested",
    "rlm:fatal_error",
    "rlm:fatal_error_msg",
]

# State keys to preserve (session-scoped)
PRESERVED_KEYS = [
    "rlm:iteration",  # Keep the iteration counter
]


class RlmContextPruningPlugin(BasePlugin):
    """Plugin that cleans up state after results_processor_agent completes.

    This plugin implements an after_agent_callback that activates when
    results_processor agent finishes. It marks artifacts as consumed and
    clears temporary state keys.

    Example:
        >>> plugin = RlmContextPruningPlugin(
        ...     target_agent_name="results_processor"
        ... )
        >>> app = App(
        ...     name="my_app",
        ...     root_agent=agent,
        ...     plugins=[plugin, ...],
        ... )
    """

    def __init__(
        self,
        name: str = "rlm_context_pruning",
        target_agent_name: str = "results_processor",
        enable_logging: bool = True,
        mark_consumed: bool = True,
    ):
        """Initialize the context pruning plugin.

        Args:
            name: Plugin instance name.
            target_agent_name: Name of the agent after which to prune state.
            enable_logging: Whether to log pruning events.
            mark_consumed: Whether to mark artifacts as consumed in registry.
        """
        super().__init__(name)
        self._target_agent_name = target_agent_name
        self._enable_logging = enable_logging
        self._mark_consumed = mark_consumed
        self.prune_count = 0
        self.skip_count = 0

        logger.info(
            f"RlmContextPruningPlugin initialized for agent '{target_agent_name}'"
        )

    async def after_agent_callback(
        self,
        *,
        callback_context: CallbackContext,
        **kwargs,  # Accept additional ADK-provided arguments (e.g., agent)
    ) -> Optional[types.Content]:
        """Prune state after results_processor completes.

        Args:
            callback_context: The callback context with agent and state info.
            **kwargs: Additional ADK-provided keyword arguments.

        Returns:
            None (this plugin doesn't inject content after agent runs).
        """
        # Check if this is the target agent
        agent_name = callback_context.agent_name
        if agent_name != self._target_agent_name:
            return None

        # Check if we have an artifact to clean up
        # Use dual-read: try temp:rlm:* first, fall back to legacy rlm:*
        artifact_id = get_rlm_state(callback_context.state, "artifact_id")
        if not artifact_id:
            self.skip_count += 1
            if self._enable_logging:
                logger.debug(
                    f"[{self.name}] Skipping pruning for {agent_name} - "
                    f"no artifact_id in state"
                )
            return None

        if self._enable_logging:
            print(
                f"[{self.name}] Pruning state after {agent_name} processed "
                f"artifact {artifact_id}"
            )
            logger.info(
                f"[{self.name}] Pruning state: artifact_id={artifact_id}"
            )

        # Mark artifact as consumed in the registry
        if self._mark_consumed:
            try:
                self._mark_artifact_consumed(artifact_id, callback_context)
            except Exception as e:
                logger.warning(
                    f"[{self.name}] Could not mark artifact as consumed: {e}"
                )

        # Clear invocation-scoped state keys (both temp:rlm:* and legacy rlm:*)
        # During migration, we clear both to ensure no stale state leaks
        # After migration, temp:rlm:* keys auto-discard so this is defensive
        keys_to_clear = INVOCATION_KEYS_TO_CLEAR + LEGACY_KEYS_TO_CLEAR

        cleared_keys = []
        for key in keys_to_clear:
            if key in callback_context.state:
                # Setting to None signals deletion in ADK state delta
                callback_context.state[key] = None
                cleared_keys.append(key)

        if self._enable_logging:
            logger.debug(f"[{self.name}] Cleared state keys: {cleared_keys}")

        self.prune_count += 1

        # Don't return any content - just cleaned up state
        return None

    def _mark_artifact_consumed(
        self,
        artifact_id: str,
        callback_context: CallbackContext,
    ) -> None:
        """Mark an artifact as consumed in the registry.

        Args:
            artifact_id: The artifact identifier.
            callback_context: The callback context (may have spark access).
        """
        # Try to get spark session and registry
        try:
            from pyspark.sql import SparkSession
            from databricks_rlm_agent.artifact_registry import get_artifact_registry

            spark = SparkSession.builder.getOrCreate()
            registry = get_artifact_registry(spark, ensure_exists=False)
            registry.mark_consumed(artifact_id)

            if self._enable_logging:
                logger.info(f"[{self.name}] Marked artifact {artifact_id} as consumed")

        except ImportError:
            # Not in Databricks environment
            logger.debug(
                f"[{self.name}] Could not import Spark - "
                f"artifact registry marking skipped"
            )
        except Exception as e:
            logger.warning(
                f"[{self.name}] Failed to mark artifact as consumed: {e}"
            )

    def get_stats(self) -> dict[str, Any]:
        """Get plugin statistics.

        Returns:
            Dictionary with plugin statistics.
        """
        return {
            "plugin_name": self.name,
            "target_agent": self._target_agent_name,
            "prune_count": self.prune_count,
            "skip_count": self.skip_count,
        }
