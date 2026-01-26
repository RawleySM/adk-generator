"""RLM Context Injection Plugin.

This plugin provides a before_agent_callback for the results_processor agent
that injects execution context from the artifact registry.

When results_processor_agent is about to run (before_agent_callback):
1. Verifies temp:rlm:stage == "executed" (stage gating)
2. Reads temp:rlm:artifact_id from state (with fallback to legacy rlm:*)
3. Loads the artifact metadata from the registry
4. Loads stdout/stderr from result.json or state
5. Returns a types.Content message injecting this context

When results_processor_agent completes (after_agent_callback):
6. Updates temp:rlm:stage to "processed" (completes stage state machine)

State key design:
- Uses dual-read pattern: temp:rlm:* first, fallback to rlm:* for migration
- Stage gating: only injects when temp:rlm:stage == "executed"
- See plans/refactor_key_glue.md for the migration plan

This enables results_processor_agent to analyze execution output based on
the sublm_instruction without needing the orchestrator to manually format
the context.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional, Tuple, TYPE_CHECKING

from google.adk.agents.callback_context import CallbackContext
from google.adk.plugins.base_plugin import BasePlugin
from google.genai import types

if TYPE_CHECKING:
    from google.adk.agents.base_agent import BaseAgent

logger = logging.getLogger(__name__)

# Import state helpers for dual-read pattern
from databricks_rlm_agent.utils.state_helpers import get_rlm_state

# State key constants - invocation-scoped (temp:rlm:*)
# Use get_rlm_state() for dual-read with fallback to legacy keys
STATE_ARTIFACT_ID = "temp:rlm:artifact_id"
STATE_SUBLM_INSTRUCTION = "temp:rlm:sublm_instruction"
STATE_HAS_AGENT_CODE = "temp:rlm:has_agent_code"
STATE_RESULT_JSON_PATH = "temp:rlm:result_json_path"
STATE_EXECUTION_STDOUT = "temp:rlm:execution_stdout"
STATE_EXECUTION_STDERR = "temp:rlm:execution_stderr"
STATE_STDOUT_TRUNCATED = "temp:rlm:stdout_truncated"

# Stage tracking keys - invocation-scoped (replaces pruning plugin for correctness)
# Stage progression: "delegated" -> "executed" -> "processed"
STATE_STAGE = "temp:rlm:stage"
STATE_ACTIVE_ARTIFACT_ID = "temp:rlm:active_artifact_id"

# Session-scoped keys
STATE_ITERATION = "rlm:iteration"


class RlmContextInjectionPlugin(BasePlugin):
    """Plugin that injects execution context into results_processor_agent.

    This plugin implements a before_agent_callback that activates when
    results_processor agent is about to run. It loads the artifact metadata
    and execution results (stdout/stderr) to inject as context.

    Example:
        >>> plugin = RlmContextInjectionPlugin(
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
        name: str = "rlm_context_injection",
        target_agent_name: str = "results_processor",
        enable_logging: bool = True,
    ):
        """Initialize the context injection plugin.

        Args:
            name: Plugin instance name.
            target_agent_name: Name of the agent to inject context into.
            enable_logging: Whether to log injection events.
        """
        super().__init__(name)
        self._target_agent_name = target_agent_name
        self._enable_logging = enable_logging
        self.injection_count = 0
        self.skip_count = 0

        logger.info(
            f"RlmContextInjectionPlugin initialized for agent '{target_agent_name}'"
        )

    def _load_from_result_json(
        self,
        result_json_path: str,
    ) -> Tuple[Optional[str], Optional[str]]:
        """Load stdout/stderr from result.json in UC Volumes.

        This is the primary source of execution output, written by the executor
        to the same directory as the artifact.

        Args:
            result_json_path: Full path to the result.json file.

        Returns:
            Tuple of (stdout, stderr), both may be None if loading failed.
        """
        try:
            if not result_json_path or not os.path.exists(result_json_path):
                logger.debug(f"[{self.name}] result.json not found: {result_json_path}")
                return None, None

            with open(result_json_path, 'r') as f:
                result_data = json.load(f)

            stdout = result_data.get("stdout")
            stderr = result_data.get("stderr")

            if self._enable_logging:
                logger.info(
                    f"[{self.name}] Loaded from result.json: "
                    f"stdout={len(stdout) if stdout else 0} chars, "
                    f"stderr={len(stderr) if stderr else 0} chars"
                )

            return stdout, stderr

        except json.JSONDecodeError as e:
            logger.warning(f"[{self.name}] Invalid JSON in result file: {e}")
            return None, None
        except Exception as e:
            logger.warning(f"[{self.name}] Failed to load result.json: {e}")
            return None, None

    def _format_injection_content(
        self,
        artifact_id: str,
        sublm_instruction: Optional[str],
        stdout: Optional[str],
        stderr: Optional[str],
        iteration: int,
    ) -> str:
        """Format the injection content as a structured message.

        Args:
            artifact_id: The artifact identifier.
            sublm_instruction: The instruction for result processing.
            stdout: Captured standard output from execution.
            stderr: Captured standard error from execution.
            iteration: The current iteration number.

        Returns:
            Formatted string for injection.
        """
        parts = [
            "=== EXECUTION RESULTS ===",
            f"Artifact ID: {artifact_id}",
            f"Iteration: {iteration}",
            "",
        ]

        if sublm_instruction:
            parts.extend([
                "=== ANALYSIS INSTRUCTION ===",
                sublm_instruction,
                "",
            ])

        if stdout:
            parts.extend([
                "=== STDOUT ===",
                stdout,
                "",
            ])

        if stderr:
            parts.extend([
                "=== STDERR ===",
                stderr,
                "",
            ])

        if not stdout and not stderr:
            parts.extend([
                "=== OUTPUT ===",
                "(No output captured from execution)",
                "",
            ])

        parts.extend([
            "=== INSTRUCTIONS ===",
            "Based on the execution output above and the analysis instruction,",
            "provide your analysis and recommendations.",
        ])

        return '\n'.join(parts)

    async def before_agent_callback(
        self,
        *,
        callback_context: CallbackContext,
        **kwargs,  # Accept additional ADK-provided arguments (e.g., agent)
    ) -> Optional[types.Content]:
        """Inject execution context before results_processor runs.

        Args:
            callback_context: The callback context with agent and state info.
            **kwargs: Additional ADK-provided keyword arguments.

        Returns:
            types.Content with injected context, or None to skip injection.
        """
        # Check if this is the target agent
        agent_name = callback_context.agent_name
        if agent_name != self._target_agent_name:
            return None

        # Check if we have an artifact to process
        # Use dual-read: try temp:rlm:* first, fall back to legacy rlm:*
        artifact_id = get_rlm_state(callback_context.state, "artifact_id")
        if not artifact_id:
            self.skip_count += 1
            if self._enable_logging:
                logger.debug(
                    f"[{self.name}] Skipping injection for {agent_name} - "
                    f"no artifact_id in state"
                )
            return None

        # Stage gating: only inject if stage is "executed"
        # This prevents injecting stale execution results from previous iterations
        current_stage = callback_context.state.get(STATE_STAGE)
        if current_stage != "executed":
            self.skip_count += 1
            if self._enable_logging:
                logger.debug(
                    f"[{self.name}] Skipping injection for {agent_name} - "
                    f"stage is '{current_stage}', not 'executed'"
                )
            return None

        # Verify artifact matches the active artifact (if set)
        active_artifact_id = callback_context.state.get(STATE_ACTIVE_ARTIFACT_ID)
        if active_artifact_id and active_artifact_id != artifact_id:
            self.skip_count += 1
            if self._enable_logging:
                logger.debug(
                    f"[{self.name}] Skipping injection for {agent_name} - "
                    f"active_artifact_id mismatch"
                )
            return None

        # Get the sublm_instruction from state (dual-read)
        sublm_instruction = get_rlm_state(callback_context.state, "sublm_instruction")

        # Get iteration (session-scoped, no dual-read needed)
        iteration = callback_context.state.get(STATE_ITERATION, 0)

        if self._enable_logging:
            print(
                f"[{self.name}] Injecting context for artifact {artifact_id} "
                f"into {agent_name}"
            )
            logger.info(
                f"[{self.name}] Injecting execution context: "
                f"artifact_id={artifact_id}, iteration={iteration}"
            )

        # Priority 1: Load stdout/stderr from result.json in UC Volumes
        # This is the canonical source - executor writes full output here
        stdout = None
        stderr = None
        source = "none"

        # Use dual-read for result_json_path
        result_json_path = get_rlm_state(callback_context.state, "result_json_path")
        if result_json_path:
            stdout, stderr = self._load_from_result_json(result_json_path)
            if stdout is not None or stderr is not None:
                source = "result_json"

        # Priority 2: Try ArtifactService keys (legacy path)
        if stdout is None:
            # Use dual-read for legacy artifact keys
            stdout_key = get_rlm_state(callback_context.state, "stdout_artifact_key")
            try:
                if stdout_key and hasattr(callback_context, "load_artifact"):
                    stdout_part = callback_context.load_artifact(filename=stdout_key)
                    if stdout_part:
                        stdout = stdout_part.text if hasattr(stdout_part, "text") else str(stdout_part)
                        source = "artifact_service"
            except Exception as e:
                logger.debug(f"Could not load stdout artifact: {e}")

        if stderr is None:
            stderr_key = get_rlm_state(callback_context.state, "stderr_artifact_key")
            try:
                if stderr_key and hasattr(callback_context, "load_artifact"):
                    stderr_part = callback_context.load_artifact(filename=stderr_key)
                    if stderr_part:
                        stderr = stderr_part.text if hasattr(stderr_part, "text") else str(stderr_part)
            except Exception as e:
                logger.debug(f"Could not load stderr artifact: {e}")

        # Priority 3: Fallback to state (may be truncated preview)
        if stdout is None:
            # Use dual-read for execution output
            stdout = get_rlm_state(callback_context.state, "execution_stdout")
            if stdout:
                source = "state"
                # Note if this is truncated (dual-read)
                if get_rlm_state(callback_context.state, "stdout_truncated"):
                    logger.info(
                        f"[{self.name}] Using truncated stdout from state "
                        f"(result.json not available)"
                    )
        if stderr is None:
            stderr = get_rlm_state(callback_context.state, "execution_stderr")

        if self._enable_logging:
            logger.info(f"[{self.name}] Loaded output from source: {source}")

        # Format the injection content
        content_text = self._format_injection_content(
            artifact_id=artifact_id,
            sublm_instruction=sublm_instruction,
            stdout=stdout,
            stderr=stderr,
            iteration=iteration,
        )

        self.injection_count += 1

        # Return as a user message Content object
        return types.Content(
            role="user",
            parts=[types.Part.from_text(text=content_text)],
        )

    async def after_agent_callback(
        self,
        *,
        callback_context: CallbackContext,
        **kwargs,  # Accept additional ADK-provided arguments
    ) -> Optional[types.Content]:
        """Update stage to "processed" after results_processor completes.

        This completes the stage state machine: delegated -> executed -> processed.
        The stage update prevents stale state from triggering re-processing in
        later loop iterations.

        Args:
            callback_context: The callback context with agent and state info.
            **kwargs: Additional ADK-provided keyword arguments.

        Returns:
            None - this callback only updates state, doesn't inject content.
        """
        # Only run for the target agent
        agent_name = callback_context.agent_name
        if agent_name != self._target_agent_name:
            return None

        # Only update stage if we're in "executed" state (meaning we actually processed)
        current_stage = callback_context.state.get(STATE_STAGE)
        if current_stage == "executed":
            callback_context.state[STATE_STAGE] = "processed"
            if self._enable_logging:
                logger.info(
                    f"[{self.name}] Stage updated to 'processed' after {agent_name}"
                )

        return None

    def get_stats(self) -> dict[str, Any]:
        """Get plugin statistics.

        Returns:
            Dictionary with plugin statistics.
        """
        return {
            "plugin_name": self.name,
            "target_agent": self._target_agent_name,
            "injection_count": self.injection_count,
            "skip_count": self.skip_count,
        }
