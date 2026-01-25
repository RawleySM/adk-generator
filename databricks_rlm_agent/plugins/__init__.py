"""ADK plugins for Databricks RLM Agent.

This package provides custom plugins for the RLM workflow:

Core Plugins:
- UcDeltaTelemetryPlugin: Persists callback-level telemetry to UC Delta tables
- UcToolExecutionSafetyPlugin: Blocks destructive SQL/shell operations

RLM Workflow Plugins:
- FormattingCheckPlugin: Validates delegation blob format
- CodeLintingPlugin: Validates Python syntax before execution
- RlmContextInjectionPlugin: Injects execution results into results_processor_agent
- RlmContextPruningPlugin: Clears state after results_processor_agent completes
"""

from .uc_delta_telemetry_plugin import UcDeltaTelemetryPlugin
from .uc_tool_execution_safety_plugin import (
    UcToolExecutionSafetyPlugin,
    BlockedPattern,
    SafetyCheckResult,
)
from .formatting_check_plugin import FormattingCheckPlugin
from .code_linting_plugin import CodeLintingPlugin
from .rlm_context_injection_plugin import RlmContextInjectionPlugin
from .rlm_context_pruning_plugin import RlmContextPruningPlugin

__all__ = [
    # Core plugins
    "UcDeltaTelemetryPlugin",
    "UcToolExecutionSafetyPlugin",
    "BlockedPattern",
    "SafetyCheckResult",
    # RLM workflow plugins
    "FormattingCheckPlugin",
    "CodeLintingPlugin",
    "RlmContextInjectionPlugin",
    "RlmContextPruningPlugin",
]
