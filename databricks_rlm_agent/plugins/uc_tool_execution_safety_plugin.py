"""UC Tool Execution Safety Plugin for ADK.

A global before_tool callback plugin that scans agent-generated strings
for destructive or irreversible commands before they are passed to tools.

Blocks tool execution if dangerous patterns are detected, such as:
- SQL: DROP, DELETE, TRUNCATE, ALTER TABLE ... DROP
- Shell: rm -rf, rm -r, rmdir, format, mkfs, dd
- File: overwrite operations, recursive deletes
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Optional, TYPE_CHECKING

from google.adk.plugins.base_plugin import BasePlugin
from google.adk.tools.base_tool import BaseTool
from google.adk.tools.tool_context import ToolContext

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


@dataclass
class BlockedPattern:
    """Represents a dangerous pattern that should block tool execution."""

    name: str
    pattern: re.Pattern
    description: str
    severity: str = "high"  # high, medium, low


@dataclass
class SafetyCheckResult:
    """Result of a safety check on tool arguments."""

    blocked: bool
    matched_patterns: list[BlockedPattern] = field(default_factory=list)
    matched_values: list[str] = field(default_factory=list)
    tool_name: str = ""

    @property
    def block_reason(self) -> str:
        """Generate a human-readable block reason."""
        if not self.blocked:
            return ""

        reasons = []
        for pattern, value in zip(self.matched_patterns, self.matched_values):
            reasons.append(
                f"{pattern.name} ({pattern.severity}): '{value[:100]}...'"
                if len(value) > 100 else f"{pattern.name} ({pattern.severity}): '{value}'"
            )
        return "; ".join(reasons)


# SQL Dangerous Patterns
SQL_PATTERNS = [
    BlockedPattern(
        name="DROP_TABLE",
        pattern=re.compile(r"\bDROP\s+TABLE\b", re.IGNORECASE),
        description="DROP TABLE command will permanently delete table and all data",
        severity="high",
    ),
    BlockedPattern(
        name="DROP_DATABASE",
        pattern=re.compile(r"\bDROP\s+(DATABASE|SCHEMA)\b", re.IGNORECASE),
        description="DROP DATABASE/SCHEMA will permanently delete entire database",
        severity="high",
    ),
    BlockedPattern(
        name="DROP_VIEW",
        pattern=re.compile(r"\bDROP\s+VIEW\b", re.IGNORECASE),
        description="DROP VIEW will permanently delete view definition",
        severity="medium",
    ),
    BlockedPattern(
        name="DROP_INDEX",
        pattern=re.compile(r"\bDROP\s+INDEX\b", re.IGNORECASE),
        description="DROP INDEX will remove index, may impact performance",
        severity="medium",
    ),
    BlockedPattern(
        name="DROP_FUNCTION",
        pattern=re.compile(r"\bDROP\s+(FUNCTION|PROCEDURE)\b", re.IGNORECASE),
        description="DROP FUNCTION/PROCEDURE will permanently delete stored code",
        severity="medium",
    ),
    BlockedPattern(
        name="TRUNCATE",
        pattern=re.compile(r"\bTRUNCATE\s+TABLE\b", re.IGNORECASE),
        description="TRUNCATE will permanently delete all rows from table",
        severity="high",
    ),
    BlockedPattern(
        name="DELETE_ALL",
        pattern=re.compile(r"\bDELETE\s+FROM\s+\S+\s*(?:;|$)", re.IGNORECASE),
        description="DELETE without WHERE clause will remove all rows",
        severity="high",
    ),
    BlockedPattern(
        name="ALTER_DROP_COLUMN",
        pattern=re.compile(r"\bALTER\s+TABLE\s+\S+\s+DROP\s+COLUMN\b", re.IGNORECASE),
        description="ALTER TABLE DROP COLUMN will permanently remove column and data",
        severity="high",
    ),
    BlockedPattern(
        name="ALTER_DROP_PARTITION",
        pattern=re.compile(r"\bALTER\s+TABLE\s+\S+\s+DROP\s+PARTITION\b", re.IGNORECASE),
        description="ALTER TABLE DROP PARTITION will permanently remove partition data",
        severity="high",
    ),
    BlockedPattern(
        name="VACUUM_FULL",
        pattern=re.compile(r"\bVACUUM\s+(?:\S+\s+)?RETAIN\s+0\s+HOURS\b", re.IGNORECASE),
        description="VACUUM with 0 hours retention removes all historical data",
        severity="high",
    ),
    BlockedPattern(
        name="OPTIMIZE_ZORDER_DESTRUCTIVE",
        pattern=re.compile(r"\bOPTIMIZE\s+\S+\s+ZORDER\b", re.IGNORECASE),
        description="OPTIMIZE ZORDER rewrites data files; requires review",
        severity="low",
    ),
    BlockedPattern(
        name="UPDATE_ALL",
        pattern=re.compile(r"\bUPDATE\s+\S+\s+SET\s+[^;]+(?:;|$)(?!\s*WHERE)", re.IGNORECASE),
        description="UPDATE without WHERE clause will modify all rows",
        severity="high",
    ),
    BlockedPattern(
        name="GRANT_ALL",
        pattern=re.compile(r"\bGRANT\s+ALL\b", re.IGNORECASE),
        description="GRANT ALL provides excessive permissions",
        severity="medium",
    ),
    BlockedPattern(
        name="REVOKE",
        pattern=re.compile(r"\bREVOKE\s+", re.IGNORECASE),
        description="REVOKE may remove critical permissions",
        severity="medium",
    ),
]

# Shell/System Dangerous Patterns
SHELL_PATTERNS = [
    BlockedPattern(
        name="RM_RECURSIVE_FORCE",
        pattern=re.compile(r"\brm\s+(-[a-zA-Z]*r[a-zA-Z]*f|-[a-zA-Z]*f[a-zA-Z]*r)\b|\brm\s+-rf\b|\brm\s+-r\s+-f\b"),
        description="rm -rf will recursively force delete files without confirmation",
        severity="high",
    ),
    BlockedPattern(
        name="RM_RECURSIVE",
        pattern=re.compile(r"\brm\s+(-r|--recursive)\b"),
        description="rm -r will recursively delete directory trees",
        severity="high",
    ),
    BlockedPattern(
        name="RM_FORCE",
        pattern=re.compile(r"\brm\s+-f\b"),
        description="rm -f will force delete without confirmation",
        severity="medium",
    ),
    BlockedPattern(
        name="RMDIR",
        pattern=re.compile(r"\brmdir\s+"),
        description="rmdir will remove directories",
        severity="medium",
    ),
    BlockedPattern(
        name="DD_COMMAND",
        pattern=re.compile(r"\bdd\s+(?:if|of)=", re.IGNORECASE),
        description="dd can overwrite disk devices and cause data loss",
        severity="high",
    ),
    BlockedPattern(
        name="MKFS",
        pattern=re.compile(r"\bmkfs\b", re.IGNORECASE),
        description="mkfs will format filesystem destroying all data",
        severity="high",
    ),
    BlockedPattern(
        name="FORMAT",
        pattern=re.compile(r"\bformat\s+[a-zA-Z]:", re.IGNORECASE),
        description="format command will erase drive contents",
        severity="high",
    ),
    BlockedPattern(
        name="DEL_RECURSIVE",
        pattern=re.compile(r"\bdel\s+/[sS]\b", re.IGNORECASE),
        description="del /s will recursively delete files (Windows)",
        severity="high",
    ),
    BlockedPattern(
        name="DELTREE",
        pattern=re.compile(r"\bdeltree\b", re.IGNORECASE),
        description="deltree will delete directory tree (Windows)",
        severity="high",
    ),
    BlockedPattern(
        name="SUDO_RM",
        pattern=re.compile(r"\bsudo\s+rm\b"),
        description="sudo rm bypasses permission checks for deletion",
        severity="high",
    ),
    BlockedPattern(
        name="CHMOD_DANGEROUS",
        pattern=re.compile(r"\bchmod\s+(-R\s+)?777\b"),
        description="chmod 777 removes all permission restrictions",
        severity="high",
    ),
    BlockedPattern(
        name="CHOWN_RECURSIVE",
        pattern=re.compile(r"\bchown\s+-R\b"),
        description="chown -R will recursively change ownership",
        severity="medium",
    ),
    BlockedPattern(
        name="SHRED",
        pattern=re.compile(r"\bshred\b"),
        description="shred will securely delete and overwrite files",
        severity="high",
    ),
    BlockedPattern(
        name="KILL_ALL",
        pattern=re.compile(r"\bkillall\s+-9\b|\bkill\s+-9\s+-1\b"),
        description="Kill all processes can crash the system",
        severity="high",
    ),
    BlockedPattern(
        name="FORK_BOMB",
        pattern=re.compile(r":\(\)\{\s*:\|:&\s*\};:"),
        description="Fork bomb will crash the system by exhausting resources",
        severity="high",
    ),
    BlockedPattern(
        name="DEV_NULL_REDIRECT",
        pattern=re.compile(r">\s*/dev/sda|>\s*/dev/hda|>\s*/dev/nvme"),
        description="Writing to device files can corrupt disk",
        severity="high",
    ),
    BlockedPattern(
        name="CURL_PIPE_BASH",
        pattern=re.compile(r"curl\s+[^|]+\|\s*(sudo\s+)?bash"),
        description="Piping curl to bash executes untrusted remote code",
        severity="high",
    ),
    BlockedPattern(
        name="WGET_PIPE_BASH",
        pattern=re.compile(r"wget\s+[^|]+\|\s*(sudo\s+)?bash"),
        description="Piping wget to bash executes untrusted remote code",
        severity="high",
    ),
]

# File/Data Dangerous Patterns
FILE_PATTERNS = [
    BlockedPattern(
        name="OVERWRITE_SYSTEM",
        pattern=re.compile(r">\s*/etc/|>\s*/boot/|>\s*/sys/"),
        description="Overwriting system files can break the system",
        severity="high",
    ),
    BlockedPattern(
        name="WRITE_PASSWD",
        pattern=re.compile(r">\s*/etc/passwd|>\s*/etc/shadow"),
        description="Modifying password files can lock out users",
        severity="high",
    ),
    BlockedPattern(
        name="REMOVE_ROOT",
        pattern=re.compile(r"rm\s+[^;]*\s+/\s*$|rm\s+[^;]*\s+/\s+"),
        description="Attempting to delete root filesystem",
        severity="high",
    ),
]

# Databricks/Spark Specific Patterns
DATABRICKS_PATTERNS = [
    BlockedPattern(
        name="DBUTILS_RM_RECURSIVE",
        pattern=re.compile(r"dbutils\.fs\.rm\s*\([^)]*,\s*True\s*\)"),
        description="dbutils.fs.rm with recursive=True deletes entire directory trees",
        severity="high",
    ),
    BlockedPattern(
        name="SPARK_DROP_TABLE",
        pattern=re.compile(r"spark\.sql\s*\(\s*['\"].*DROP\s+TABLE", re.IGNORECASE),
        description="Spark SQL DROP TABLE will permanently delete table",
        severity="high",
    ),
    BlockedPattern(
        name="DELTA_DELETE_ALL",
        pattern=re.compile(r"\.delete\s*\(\s*\)"),
        description="DeltaTable.delete() without condition removes all rows",
        severity="high",
    ),
    BlockedPattern(
        name="UNITY_CATALOG_DROP",
        pattern=re.compile(r"\bDROP\s+(CATALOG|SCHEMA|VOLUME)\b", re.IGNORECASE),
        description="Dropping Unity Catalog objects is irreversible",
        severity="high",
    ),
]

# Combine all patterns
ALL_DANGEROUS_PATTERNS = SQL_PATTERNS + SHELL_PATTERNS + FILE_PATTERNS + DATABRICKS_PATTERNS


class UcToolExecutionSafetyPlugin(BasePlugin):
    """ADK plugin that blocks tool execution if dangerous patterns are detected.

    This plugin implements a before_tool_callback that scans all string arguments
    passed to tools for potentially destructive or irreversible commands. If any
    dangerous patterns are matched, the tool execution is blocked and an error
    response is returned to the agent.

    The plugin detects:
    - SQL destructive commands (DROP, TRUNCATE, DELETE without WHERE, etc.)
    - Shell destructive commands (rm -rf, dd, mkfs, format, etc.)
    - File system dangerous operations
    - Databricks/Spark specific dangerous operations

    Example:
        >>> plugin = UcToolExecutionSafetyPlugin()
        >>> runner = Runner(
        ...     agent=my_agent,
        ...     plugins=[plugin, UcDeltaTelemetryPlugin()],
        ...     ...
        ... )

    Attributes:
        blocked_count: Number of tool executions blocked by this plugin.
        severity_threshold: Minimum severity level to block ("low", "medium", "high").
    """

    SEVERITY_LEVELS = {"low": 1, "medium": 2, "high": 3}

    def __init__(
        self,
        name: str = "uc_tool_execution_safety_plugin",
        severity_threshold: str = "medium",
        additional_patterns: Optional[list[BlockedPattern]] = None,
        enable_logging: bool = True,
        on_block_callback: Optional[callable] = None,
    ):
        """Initialize the safety plugin.

        Args:
            name: Plugin instance name.
            severity_threshold: Minimum severity to block ("low", "medium", "high").
                - "high": Only block high-severity patterns (most permissive)
                - "medium": Block medium and high severity (default)
                - "low": Block all dangerous patterns (most restrictive)
            additional_patterns: Additional BlockedPattern instances to check.
            enable_logging: Whether to log blocked tool calls.
            on_block_callback: Optional callback function called when a tool is blocked.
                Signature: (tool_name: str, result: SafetyCheckResult) -> None
        """
        super().__init__(name)
        self._severity_threshold = severity_threshold
        self._additional_patterns = additional_patterns or []
        self._enable_logging = enable_logging
        self._on_block_callback = on_block_callback
        self.blocked_count = 0

        # Combine default and additional patterns
        self._all_patterns = ALL_DANGEROUS_PATTERNS + self._additional_patterns

        # Filter patterns by severity threshold
        threshold_level = self.SEVERITY_LEVELS.get(severity_threshold, 2)
        self._active_patterns = [
            p for p in self._all_patterns
            if self.SEVERITY_LEVELS.get(p.severity, 3) >= threshold_level
        ]

        logger.info(
            f"UcToolExecutionSafetyPlugin initialized with {len(self._active_patterns)} "
            f"active patterns (severity >= {severity_threshold})"
        )

    def _extract_strings_from_value(self, value: Any, depth: int = 0) -> list[str]:
        """Recursively extract all string values from nested structures.

        Args:
            value: The value to extract strings from (can be nested dict/list/str).
            depth: Current recursion depth (to prevent infinite recursion).

        Returns:
            List of all string values found.
        """
        if depth > 10:  # Prevent infinite recursion
            return []

        strings = []

        if isinstance(value, str):
            strings.append(value)
        elif isinstance(value, dict):
            for v in value.values():
                strings.extend(self._extract_strings_from_value(v, depth + 1))
        elif isinstance(value, (list, tuple)):
            for item in value:
                strings.extend(self._extract_strings_from_value(item, depth + 1))

        return strings

    def _check_string_for_patterns(
        self, text: str, patterns: list[BlockedPattern]
    ) -> list[tuple[BlockedPattern, str]]:
        """Check a string against all patterns.

        Args:
            text: The text to check.
            patterns: List of patterns to check against.

        Returns:
            List of (pattern, matched_text) tuples for all matches.
        """
        matches = []
        for pattern in patterns:
            match = pattern.pattern.search(text)
            if match:
                matches.append((pattern, match.group()))
        return matches

    def check_tool_args(
        self, tool_name: str, tool_args: dict[str, Any]
    ) -> SafetyCheckResult:
        """Check tool arguments for dangerous patterns.

        Args:
            tool_name: Name of the tool being called.
            tool_args: Dictionary of arguments being passed to the tool.

        Returns:
            SafetyCheckResult indicating whether execution should be blocked.
        """
        result = SafetyCheckResult(blocked=False, tool_name=tool_name)

        # Extract all strings from the arguments
        all_strings = self._extract_strings_from_value(tool_args)

        # Check each string against all patterns
        for text in all_strings:
            matches = self._check_string_for_patterns(text, self._active_patterns)
            for pattern, matched_text in matches:
                result.blocked = True
                result.matched_patterns.append(pattern)
                result.matched_values.append(matched_text)

        return result

    async def before_tool_callback(
        self,
        *,
        tool: BaseTool,
        tool_args: dict[str, Any],
        tool_context: ToolContext,
    ) -> Optional[dict]:
        """Callback executed before a tool is called.

        Scans all string arguments for dangerous patterns and blocks execution
        if any are found.

        Args:
            tool: The tool instance about to be executed.
            tool_args: The arguments being passed to the tool.
            tool_context: The tool execution context.

        Returns:
            None to proceed with tool execution, or a dict with error message
            to block execution and return that response instead.
        """
        result = self.check_tool_args(tool.name, tool_args)

        if result.blocked:
            self.blocked_count += 1

            # Log the blocked attempt
            if self._enable_logging:
                logger.warning(
                    f"[{self.name}] BLOCKED tool '{tool.name}' - "
                    f"Dangerous patterns detected: {result.block_reason}"
                )
                # Also print to stdout for visibility
                print(
                    f"\033[91m[{self.name}] BLOCKED: Tool '{tool.name}' "
                    f"contains dangerous patterns: {result.block_reason}\033[0m"
                )

            # Call optional callback
            if self._on_block_callback:
                try:
                    self._on_block_callback(tool.name, result)
                except Exception as e:
                    logger.error(f"Error in on_block_callback: {e}")

            # Return error response to block execution
            return {
                "error": "SAFETY_BLOCK",
                "blocked": True,
                "message": (
                    f"Tool execution blocked by safety plugin. "
                    f"The requested operation contains potentially destructive or "
                    f"irreversible commands that are not allowed. "
                    f"Detected patterns: {result.block_reason}"
                ),
                "patterns_matched": [p.name for p in result.matched_patterns],
                "tool_name": tool.name,
            }

        # Allow execution to proceed
        return None

    def get_stats(self) -> dict[str, Any]:
        """Get plugin statistics.

        Returns:
            Dictionary with plugin statistics including blocked count and
            active pattern information.
        """
        return {
            "plugin_name": self.name,
            "blocked_count": self.blocked_count,
            "severity_threshold": self._severity_threshold,
            "active_pattern_count": len(self._active_patterns),
            "active_patterns": [p.name for p in self._active_patterns],
        }
