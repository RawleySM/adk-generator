"""RLM Context Injection Plugin.

This plugin provides a before_agent_callback for the results_processor agent
that injects execution context from the artifact registry.

When results_processor_agent is about to run, this plugin:
1. Reads rlm:artifact_id from state
2. Loads the artifact metadata from the registry
3. Loads stdout/stderr from the ArtifactService
4. Returns a types.Content message injecting this context

This enables results_processor_agent to analyze execution output based on
the sublm_instruction without needing the orchestrator to manually format
the context.
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


# State key constants (must match delegate_code_results.py)
STATE_ARTIFACT_ID = "rlm:artifact_id"
STATE_SUBLM_INSTRUCTION = "rlm:sublm_instruction"
STATE_HAS_AGENT_CODE = "rlm:has_agent_code"


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
    ) -> Optional[types.Content]:
        """Inject execution context before results_processor runs.

        Args:
            callback_context: The callback context with agent and state info.

        Returns:
            types.Content with injected context, or None to skip injection.
        """
        # Check if this is the target agent
        agent_name = callback_context.agent_name
        if agent_name != self._target_agent_name:
            return None

        # Check if we have an artifact to process
        artifact_id = callback_context.state.get(STATE_ARTIFACT_ID)
        if not artifact_id:
            self.skip_count += 1
            if self._enable_logging:
                logger.debug(
                    f"[{self.name}] Skipping injection for {agent_name} - "
                    f"no artifact_id in state"
                )
            return None

        # Get the sublm_instruction from state
        sublm_instruction = callback_context.state.get(STATE_SUBLM_INSTRUCTION)

        # Get iteration
        iteration = callback_context.state.get("rlm:iteration", 0)

        if self._enable_logging:
            print(
                f"[{self.name}] Injecting context for artifact {artifact_id} "
                f"into {agent_name}"
            )
            logger.info(
                f"[{self.name}] Injecting execution context: "
                f"artifact_id={artifact_id}, iteration={iteration}"
            )

        # Try to load stdout/stderr from ArtifactService
        stdout = None
        stderr = None

        stdout_key = callback_context.state.get("rlm:stdout_artifact_key")
        stderr_key = callback_context.state.get("rlm:stderr_artifact_key")

        # Try loading from ArtifactService if keys are available
        try:
            if stdout_key and hasattr(callback_context, "load_artifact"):
                stdout_part = callback_context.load_artifact(filename=stdout_key)
                if stdout_part:
                    stdout = stdout_part.text if hasattr(stdout_part, "text") else str(stdout_part)
        except Exception as e:
            logger.debug(f"Could not load stdout artifact: {e}")

        try:
            if stderr_key and hasattr(callback_context, "load_artifact"):
                stderr_part = callback_context.load_artifact(filename=stderr_key)
                if stderr_part:
                    stderr = stderr_part.text if hasattr(stderr_part, "text") else str(stderr_part)
        except Exception as e:
            logger.debug(f"Could not load stderr artifact: {e}")

        # Fallback: check if stdout/stderr are in state directly
        if stdout is None:
            stdout = callback_context.state.get("rlm:execution_stdout")
        if stderr is None:
            stderr = callback_context.state.get("rlm:execution_stderr")

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
