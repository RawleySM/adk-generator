"""Formatting Check Plugin for RLM Workflow.

This plugin provides a before_tool_callback that validates the docstring format
for the delegate_code_results tool. It ensures that delegation blobs are properly
formatted before being processed.

Validation checks:
- Blob is not empty
- If docstring is present, it must be properly closed
- Code portion exists after the docstring
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from google.adk.plugins.base_plugin import BasePlugin
from google.adk.tools.base_tool import BaseTool
from google.adk.tools.tool_context import ToolContext

from databricks_rlm_agent.utils.docstring_parser import (
    validate_delegation_blob_format,
    DelegationBlobParseError,
)

logger = logging.getLogger(__name__)


class FormattingCheckPlugin(BasePlugin):
    """Plugin that validates delegation blob formatting before tool execution.

    This plugin implements a before_tool_callback that checks the format of
    delegation blobs passed to delegate_code_results tool.

    If the blob format is invalid (malformed docstring, empty code, etc.),
    the tool execution is blocked and a helpful error message is returned.

    Example:
        >>> plugin = FormattingCheckPlugin()
        >>> app = App(
        ...     name="my_app",
        ...     root_agent=agent,
        ...     plugins=[plugin, ...],
        ... )

    Attributes:
        blocked_count: Number of tool calls blocked due to formatting errors.
    """

    # Tools that should be validated
    VALIDATED_TOOLS = {"delegate_code_results"}

    # Parameter names that contain code/blobs to validate
    CODE_PARAMETERS = {"code", "blob", "content"}

    def __init__(
        self,
        name: str = "formatting_check",
        enable_logging: bool = True,
        strict_mode: bool = False,
    ):
        """Initialize the formatting check plugin.

        Args:
            name: Plugin instance name.
            enable_logging: Whether to log validation failures.
            strict_mode: If True, require docstring for delegate_code_results.
        """
        super().__init__(name)
        self._enable_logging = enable_logging
        self._strict_mode = strict_mode
        self.blocked_count = 0
        self.validated_count = 0

        logger.info(
            f"FormattingCheckPlugin initialized (strict_mode={strict_mode})"
        )

    def _find_code_parameter(self, tool_args: dict[str, Any]) -> Optional[str]:
        """Find the code/blob parameter in tool arguments.

        Args:
            tool_args: The tool arguments dictionary.

        Returns:
            The code string if found, None otherwise.
        """
        for param_name in self.CODE_PARAMETERS:
            if param_name in tool_args:
                value = tool_args[param_name]
                if isinstance(value, str):
                    return value
        return None

    async def before_tool_callback(
        self,
        *,
        tool: BaseTool,
        tool_args: dict[str, Any],
        tool_context: ToolContext,
    ) -> Optional[dict]:
        """Validate formatting before tool execution.

        Args:
            tool: The tool about to be executed.
            tool_args: Arguments being passed to the tool.
            tool_context: The tool execution context.

        Returns:
            None to proceed, or dict with error to block execution.
        """
        # Only validate specific tools
        if tool.name not in self.VALIDATED_TOOLS:
            return None

        # Find the code parameter
        code = self._find_code_parameter(tool_args)
        if code is None:
            # No code parameter found - let the tool handle it
            return None

        self.validated_count += 1

        # Validate the format
        is_valid, error_message = validate_delegation_blob_format(code)

        if not is_valid:
            self.blocked_count += 1

            if self._enable_logging:
                logger.warning(
                    f"[{self.name}] BLOCKED tool '{tool.name}' - "
                    f"Format validation failed: {error_message}"
                )
                print(
                    f"\033[93m[{self.name}] BLOCKED: Tool '{tool.name}' "
                    f"format validation failed: {error_message}\033[0m"
                )

            return {
                "error": "FORMAT_VALIDATION_ERROR",
                "blocked": True,
                "message": (
                    f"Delegation blob format validation failed: {error_message}\n\n"
                    f"Expected format:\n"
                    f"  '''<instruction for results processor>'''\n"
                    f"  <python code to execute>\n\n"
                    f"Or without instruction:\n"
                    f"  <python code to execute>\n\n"
                    f"Please correct the format and try again."
                ),
                "tool_name": tool.name,
            }

        # In strict mode, delegate_code_results must have a docstring instruction
        if self._strict_mode and tool.name == "delegate_code_results":
            from databricks_rlm_agent.utils.docstring_parser import parse_delegation_blob
            try:
                parsed = parse_delegation_blob(code)
                if not parsed.has_instruction:
                    self.blocked_count += 1

                    if self._enable_logging:
                        logger.warning(
                            f"[{self.name}] BLOCKED tool '{tool.name}' - "
                            f"Strict mode requires instruction docstring"
                        )

                    return {
                        "error": "MISSING_INSTRUCTION",
                        "blocked": True,
                        "message": (
                            "Strict mode is enabled. delegate_code_results requires an "
                            "instruction docstring at the beginning:\n\n"
                            "  '''<instruction for how to process results>'''\n"
                            "  <python code>\n\n"
                            "The instruction tells results_processor_agent how to "
                            "analyze the execution output."
                        ),
                        "tool_name": tool.name,
                    }
            except DelegationBlobParseError:
                pass  # Already handled above

        # Validation passed
        if self._enable_logging:
            logger.debug(f"[{self.name}] Tool '{tool.name}' passed format validation")

        return None

    def get_stats(self) -> dict[str, Any]:
        """Get plugin statistics.

        Returns:
            Dictionary with plugin statistics.
        """
        return {
            "plugin_name": self.name,
            "validated_count": self.validated_count,
            "blocked_count": self.blocked_count,
            "strict_mode": self._strict_mode,
        }
