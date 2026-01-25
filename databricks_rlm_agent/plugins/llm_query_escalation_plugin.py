"""Delegation Escalation Plugin for ADK.

This plugin monitors delegate_code_results() tool usage by the databricks_analyst agent
and escalates the LoopAgent if the tool is called more than twice consecutively
without an after_agent_callback from the configured downstream agent.

This prevents infinite loops where databricks_analyst keeps delegating work
without downstream progress being observed.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Optional, TYPE_CHECKING

from google.adk.agents.base_agent import BaseAgent
from google.adk.agents.callback_context import CallbackContext
from google.adk.plugins.base_plugin import BasePlugin
from google.adk.tools.base_tool import BaseTool
from google.adk.tools.tool_context import ToolContext

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class LlmQueryEscalationPlugin(BasePlugin):
    """Plugin that escalates the LoopAgent if delegate_code_results is called too frequently.

    This plugin tracks consecutive delegate_code_results() tool calls from the databricks_analyst
    agent. If the tool is invoked more than `max_consecutive_calls` times without
    an after_agent_callback from the configured downstream agent, the plugin triggers escalation
    to terminate the loop and prevent runaway behavior.

    Args:
        name: Plugin instance name.
        max_consecutive_calls: Maximum consecutive delegate_code_results calls allowed before
            escalation (default: 2).
        monitored_tool_name: Name of the tool to monitor (default: "delegate_code_results").
        monitored_agent_name: Name of the agent calling the tool (default: "databricks_analyst").
        subagent_name: Name of the sub-agent whose completion resets the counter
            (default: "results_processor").
        enable_stdout: Whether to print log messages to stdout (default: True).

    Example:
        >>> plugin = LlmQueryEscalationPlugin(max_consecutive_calls=2)
        >>> app = App(
        ...     name="my_app",
        ...     root_agent=root_agent,
        ...     plugins=[plugin, other_plugins...]
        ... )
    """

    _lock = threading.RLock()

    def __init__(
        self,
        name: str = "delegate_code_results_escalation_plugin",
        max_consecutive_calls: int = 2,
        monitored_tool_name: str = "delegate_code_results",
        monitored_agent_name: str = "databricks_analyst",
        subagent_name: str = "results_processor",
        enable_stdout: bool = True,
    ):
        """Initialize the delegation escalation plugin."""
        super().__init__(name)
        self._max_consecutive_calls = max_consecutive_calls
        self._monitored_tool_name = monitored_tool_name
        self._monitored_agent_name = monitored_agent_name
        self._subagent_name = subagent_name
        self._enable_stdout = enable_stdout
        # Track consecutive tool calls per session
        # Key: session_id, Value: consecutive_count
        self._consecutive_counts: dict[str, int] = {}

    def _log(self, message: str) -> None:
        """Print log message to stdout."""
        if self._enable_stdout:
            formatted_message = f"\033[93m[{self.name}] {message}\033[0m"
            print(formatted_message)

    def _get_session_id(self, tool_context: Optional[ToolContext] = None,
                        callback_context: Optional[CallbackContext] = None) -> str:
        """Extract session ID from context for tracking state."""
        session_id = "default"

        if tool_context and hasattr(tool_context, "_invocation_context"):
            ic = tool_context._invocation_context
            if ic.session:
                session_id = ic.session.id

        if callback_context and hasattr(callback_context, "_invocation_context"):
            ic = callback_context._invocation_context
            if ic.session:
                session_id = ic.session.id

        return session_id

    def _get_count(self, session_id: str) -> int:
        """Get the current consecutive count for a session."""
        with self._lock:
            return self._consecutive_counts.get(session_id, 0)

    def _increment_count(self, session_id: str) -> int:
        """Increment and return the consecutive count for a session."""
        with self._lock:
            current = self._consecutive_counts.get(session_id, 0)
            self._consecutive_counts[session_id] = current + 1
            return self._consecutive_counts[session_id]

    def _reset_count(self, session_id: str) -> None:
        """Reset the consecutive count for a session."""
        with self._lock:
            self._consecutive_counts[session_id] = 0

    async def after_tool_callback(
        self,
        *,
        tool: BaseTool,
        tool_args: dict[str, Any],
        tool_context: ToolContext,
        result: dict,
    ) -> Optional[dict]:
        """Monitor delegate_code_results tool calls and escalate if threshold exceeded.

        This callback fires after every tool invocation. It specifically monitors
        calls to delegate_code_results() from databricks_analyst and tracks consecutive usage.
        If the tool is called more than max_consecutive_calls times without
        the configured downstream agent completing, it triggers escalation.

        Args:
            tool: The tool that was invoked.
            tool_args: Arguments passed to the tool.
            tool_context: Context providing agent info and escalation controls.
            result: The result returned by the tool.

        Returns:
            None to use the original result, or a modified result dict.
        """
        # Only monitor the specific tool and agent combination
        if tool.name != self._monitored_tool_name:
            return None

        if tool_context.agent_name != self._monitored_agent_name:
            return None

        session_id = self._get_session_id(tool_context=tool_context)
        new_count = self._increment_count(session_id)

        self._log(
            f"Tool '{tool.name}' called by '{tool_context.agent_name}' "
            f"(consecutive count: {new_count}/{self._max_consecutive_calls})"
        )

        # Check if we've exceeded the threshold
        if new_count > self._max_consecutive_calls:
            self._log(
                f"ESCALATION TRIGGERED: '{self._monitored_tool_name}' called "
                f"{new_count} times by '{self._monitored_agent_name}' without "
                f"'{self._subagent_name}' agent completing. "
                f"Threshold: {self._max_consecutive_calls}"
            )

            # Signal escalation to terminate the loop
            tool_context.actions.escalate = True

            # Reset counter after escalation
            self._reset_count(session_id)

            # Return modified result indicating escalation
            return {
                **result,
                "escalation_triggered": True,
                "escalation_reason": (
                    f"{self._monitored_tool_name} called {new_count} consecutive times without "
                    f"{self._subagent_name} completion. Loop terminated."
                ),
            }

        return None

    async def after_agent_callback(
        self, *, agent: BaseAgent, callback_context: CallbackContext
    ) -> Optional[Any]:
        """Reset consecutive count when downstream agent completes.

        This callback fires after every agent completes. When the downstream agent
        finishes its execution, we reset the consecutive tool call counter
        since the downstream agent properly processed the pending work.

        Args:
            agent: The agent that completed.
            callback_context: Context providing agent and session info.

        Returns:
            None to allow normal execution flow.
        """
        # Only reset when the monitored sub-agent completes
        if callback_context.agent_name != self._subagent_name:
            return None

        session_id = self._get_session_id(callback_context=callback_context)
        previous_count = self._get_count(session_id)

        if previous_count > 0:
            self._log(
                f"Agent '{callback_context.agent_name}' completed. "
                f"Resetting consecutive tool count from {previous_count} to 0."
            )
            self._reset_count(session_id)

        return None

    async def close(self) -> None:
        """Clean up resources."""
        with self._lock:
            self._consecutive_counts.clear()
