"""Code Linting Plugin for RLM Workflow.

This plugin provides a before_tool_callback that validates Python syntax
for code generation tools. It uses ast.parse() to check for syntax errors
before the code is saved or executed.

This catches common issues like:
- Syntax errors (missing colons, parentheses, quotes)
- Invalid Python constructs
- Malformed string literals
- Indentation errors (detected by ast)
"""

from __future__ import annotations

import ast
import logging
from typing import Any, Optional

from google.adk.plugins.base_plugin import BasePlugin
from google.adk.tools.base_tool import BaseTool
from google.adk.tools.tool_context import ToolContext

logger = logging.getLogger(__name__)


class CodeLintingPlugin(BasePlugin):
    """Plugin that validates Python syntax before code generation tools execute.

    This plugin implements a before_tool_callback that runs ast.parse() on
    code passed to delegate_code_results and save_python_code tools.

    If the code has syntax errors, the tool execution is blocked and a
    detailed error message is returned including the error location.

    Example:
        >>> plugin = CodeLintingPlugin()
        >>> app = App(
        ...     name="my_app",
        ...     root_agent=agent,
        ...     plugins=[plugin, ...],
        ... )

    Attributes:
        blocked_count: Number of tool calls blocked due to syntax errors.
    """

    # Tools that should have their code linted
    LINTED_TOOLS = {"delegate_code_results", "save_python_code"}

    # Parameter names that contain code to lint
    CODE_PARAMETERS = {"code", "blob", "content"}

    def __init__(
        self,
        name: str = "code_linting",
        enable_logging: bool = True,
        include_code_context: bool = True,
        context_lines: int = 3,
    ):
        """Initialize the code linting plugin.

        Args:
            name: Plugin instance name.
            enable_logging: Whether to log linting failures.
            include_code_context: Whether to include surrounding code in errors.
            context_lines: Number of lines to show around the error.
        """
        super().__init__(name)
        self._enable_logging = enable_logging
        self._include_code_context = include_code_context
        self._context_lines = context_lines
        self.blocked_count = 0
        self.linted_count = 0
        self.passed_count = 0

        logger.info(f"CodeLintingPlugin initialized")

    def _find_code_parameter(self, tool_args: dict[str, Any]) -> Optional[str]:
        """Find the code parameter in tool arguments.

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

    def _extract_code_from_blob(self, blob: str) -> str:
        """Extract the code portion from a delegation blob.

        Args:
            blob: The full delegation blob.

        Returns:
            Just the code portion (after the instruction docstring).
        """
        try:
            from databricks_rlm_agent.utils.docstring_parser import parse_delegation_blob
            parsed = parse_delegation_blob(blob)
            return parsed.agent_code
        except Exception:
            # If parsing fails, try to lint the whole blob
            # This handles the case where the blob is just code
            return blob

    def _get_code_context(self, code: str, lineno: int) -> str:
        """Get code context around an error line.

        Args:
            code: The full code string.
            lineno: The error line number (1-indexed).

        Returns:
            Formatted string showing code context.
        """
        lines = code.split('\n')
        start = max(0, lineno - self._context_lines - 1)
        end = min(len(lines), lineno + self._context_lines)

        context_lines = []
        for i in range(start, end):
            line_num = i + 1
            prefix = ">>> " if line_num == lineno else "    "
            context_lines.append(f"{prefix}{line_num:4d} | {lines[i]}")

        return '\n'.join(context_lines)

    def _lint_code(self, code: str, tool_name: str) -> Optional[dict]:
        """Lint Python code using ast.parse().

        Args:
            code: The Python code to lint.
            tool_name: Name of the tool for error messages.

        Returns:
            None if code is valid, or dict with error details.
        """
        if not code or not code.strip():
            return {
                "error": "EMPTY_CODE",
                "blocked": True,
                "message": "The code block is empty. Please provide Python code to execute.",
                "tool_name": tool_name,
            }

        try:
            ast.parse(code)
            return None  # Code is valid
        except SyntaxError as e:
            error_details = {
                "lineno": e.lineno,
                "offset": e.offset,
                "text": e.text,
                "msg": e.msg,
            }

            # Build error message
            message_parts = [
                f"Python syntax error detected: {e.msg}",
                f"  Location: line {e.lineno}, column {e.offset}",
            ]

            if e.text:
                message_parts.append(f"  Line content: {e.text.strip()}")

            if self._include_code_context and e.lineno:
                context = self._get_code_context(code, e.lineno)
                message_parts.append(f"\nCode context:\n{context}")

            message_parts.append(
                "\nPlease fix the syntax error and try again."
            )

            return {
                "error": "SYNTAX_ERROR",
                "blocked": True,
                "message": '\n'.join(message_parts),
                "tool_name": tool_name,
                "syntax_error": error_details,
            }

    async def before_tool_callback(
        self,
        *,
        tool: BaseTool,
        tool_args: dict[str, Any],
        tool_context: ToolContext,
    ) -> Optional[dict]:
        """Lint code before tool execution.

        Args:
            tool: The tool about to be executed.
            tool_args: Arguments being passed to the tool.
            tool_context: The tool execution context.

        Returns:
            None to proceed, or dict with error to block execution.
        """
        # Only lint specific tools
        if tool.name not in self.LINTED_TOOLS:
            return None

        # Find the code parameter
        blob = self._find_code_parameter(tool_args)
        if blob is None:
            # No code parameter found - let the tool handle it
            return None

        self.linted_count += 1

        # Extract just the code portion (after instruction docstring)
        code = self._extract_code_from_blob(blob)

        # Lint the code
        lint_result = self._lint_code(code, tool.name)

        if lint_result is not None:
            self.blocked_count += 1

            if self._enable_logging:
                logger.warning(
                    f"[{self.name}] BLOCKED tool '{tool.name}' - "
                    f"Syntax error at line {lint_result.get('syntax_error', {}).get('lineno', '?')}"
                )
                print(
                    f"\033[91m[{self.name}] BLOCKED: Tool '{tool.name}' "
                    f"has Python syntax error\033[0m"
                )

            return lint_result

        # Linting passed
        self.passed_count += 1
        if self._enable_logging:
            logger.debug(f"[{self.name}] Tool '{tool.name}' passed syntax check")

        return None

    def get_stats(self) -> dict[str, Any]:
        """Get plugin statistics.

        Returns:
            Dictionary with plugin statistics.
        """
        return {
            "plugin_name": self.name,
            "linted_count": self.linted_count,
            "passed_count": self.passed_count,
            "blocked_count": self.blocked_count,
        }
