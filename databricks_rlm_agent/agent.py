"""Agent definitions for Databricks RLM Agent.

This module defines the agent hierarchy for the RLM (Recursive Language Model) workflow:

Agent Sequence in LoopAgent:
    root_agent (LoopAgent)
      └── sub_agents:
            1. databricks_analyst (LlmAgent)       - generates code, calls delegate_code_results()
            2. job_builder (BaseAgent)             - deterministic job submission
            3. results_processor_agent (LlmAgent)  - processes results with injected context

Workflow:
    databricks_analyst → delegate_code_results() → [validation plugins] → artifact registry
                                                                               ↓
                                                                         job_builder (BaseAgent)
                                                                               ↓
                                                                          Job_B executor
                                                                               ↓
                                                                 results_processor_agent (with injected context)
                                                                               ↓
                                                                         stage="processed" → loop continues
"""

import os
import logging
from google.adk.agents import LlmAgent
from google.adk.agents.loop_agent import LoopAgent
from google.adk.apps import App
from google.adk.tools import FunctionTool

# Import pre-built plugins
from google.adk.plugins.logging_plugin import LoggingPlugin
from google.adk.plugins.global_instruction_plugin import GlobalInstructionPlugin

# Import custom plugins
from databricks_rlm_agent.plugins import (
    UcDeltaTelemetryPlugin,
    UcToolExecutionSafetyPlugin,
    FormattingCheckPlugin,
    CodeLintingPlugin,
    RlmContextInjectionPlugin,
)

# Import agents
from databricks_rlm_agent.agents import JobBuilderAgent

# Import prompts
from databricks_rlm_agent.prompts import GLOBAL_INSTRUCTIONS, ROOT_AGENT_INSTRUCTION

# Import tools
from databricks_rlm_agent.tools import (
    save_artifact_to_volumes,
    exit_loop,
    delegate_code_results,
    repo_filename_search,
    metadata_keyword_search,
    get_repo_file,
)

# Import model factory for provider selection (Gemini vs LiteLLM)
from databricks_rlm_agent.modeling import (
    build_agent_model,
    get_model_config,
    get_fallback_router,
)

# Note: API keys (GOOGLE_API_KEY, etc.) are loaded from Databricks Secrets
# via the secrets module at runtime startup in run.py. Do NOT hardcode keys here.

# =============================================================================
# Model Configuration
# =============================================================================

# Build the model for LlmAgents based on environment configuration
# Supports: native ADK Gemini (default) or LiteLLM (OpenAI/Anthropic/local)
# Set ADK_MODEL_PROVIDER=litellm to switch providers
# See databricks_rlm_agent/modeling/model_factory.py for all config options
_agent_model = build_agent_model()


def get_logging_plugin() -> LoggingPlugin:
    """
    Returns the standard LoggingPlugin for runtime observability.

    Returns:
        LoggingPlugin instance.
    """
    return LoggingPlugin(name="adk_poc_logging")


def get_telemetry_plugin(
    enable_stdout: bool = True,
    catalog: str | None = None,
    schema: str | None = None,
    table: str | None = None,
) -> UcDeltaTelemetryPlugin:
    """
    Returns the UC Delta telemetry plugin for callback-level telemetry persistence.

    This plugin preserves stdout print() logging (like LoggingPlugin) while also
    persisting callback-level telemetry to a Unity Catalog Delta table.

    Args:
        enable_stdout: Whether to print logs to stdout (default True).
        catalog: Unity Catalog name (default from ADK_DELTA_CATALOG env var or 'silo_dev_rs').
        schema: Schema name (default from ADK_DELTA_SCHEMA env var or 'adk').
        table: Table name (default from ADK_AGENT_TELEMETRY_TABLE env var or 'adk_telemetry').

    Returns:
        UcDeltaTelemetryPlugin instance.
    """
    # Use env var defaults if not specified
    catalog = catalog or os.environ.get("ADK_DELTA_CATALOG", "silo_dev_rs")
    schema = schema or os.environ.get("ADK_DELTA_SCHEMA", "adk")
    table = table or os.environ.get("ADK_AGENT_TELEMETRY_TABLE", "adk_telemetry")

    print(f"[UC TELEMETRY] Initializing UcDeltaTelemetryPlugin -> {catalog}.{schema}.{table}")

    return UcDeltaTelemetryPlugin(
        name="uc_delta_telemetry",
        catalog=catalog,
        schema=schema,
        table=table,
        enable_stdout=enable_stdout,
    )


# =============================================================================
# Plugin Initialization
# =============================================================================

# Use UC Delta telemetry plugin by default (replaces LoggingPlugin with UC persistence)
# Set ADK_USE_LOGGING_PLUGIN=1 to use the original LoggingPlugin/DebugLoggingPlugin instead
if os.environ.get("ADK_USE_LOGGING_PLUGIN", "").lower() in ("1", "true", "yes"):
    logging_plugin = get_logging_plugin()
else:
    logging_plugin = get_telemetry_plugin()

global_instruction_plugin = GlobalInstructionPlugin(
    global_instruction=GLOBAL_INSTRUCTIONS,
    name="adk_poc_global_instructions"
)

# Safety Plugin: Blocks destructive operations in generated code
safety_plugin = UcToolExecutionSafetyPlugin(
    name="uc_tool_execution_safety",
    severity_threshold="medium",  # Block medium and high severity patterns
)

# Formatting Check Plugin: Validates delegation blob format
formatting_plugin = FormattingCheckPlugin(
    name="formatting_check",
    strict_mode=False,  # Don't require instruction docstring
)

# Code Linting Plugin: Validates Python syntax before execution
linting_plugin = CodeLintingPlugin(
    name="code_linting",
    include_code_context=True,
    context_lines=3,
)

# Context Injection Plugin: Injects execution results into results_processor_agent
# Also handles stage tracking (before: gate on "executed", after: set "processed")
context_injection_plugin = RlmContextInjectionPlugin(
    name="rlm_context_injection",
    target_agent_name="results_processor",
)

# =============================================================================
# Sub-Agents (LlmAgent instances)
# =============================================================================

# Databricks Analyst sub-agent: Primary agent for data discovery and code generation
# This agent now uses delegate_code_results() for the RLM workflow
# Model is selected via ADK_MODEL_PROVIDER env var (gemini or litellm)
databricks_analyst = LlmAgent(
    name="databricks_analyst",
    model=_agent_model,
    instruction=ROOT_AGENT_INSTRUCTION,
    tools=[
        FunctionTool(save_artifact_to_volumes),
        FunctionTool(exit_loop),
        FunctionTool(delegate_code_results),  # RLM workflow delegation tool
        FunctionTool(repo_filename_search),   # Search repo file metadata
        FunctionTool(metadata_keyword_search),  # Search UC table metadata
        FunctionTool(get_repo_file),          # Download files from GitHub
    ]
)

# Job Builder Agent: Deterministic agent for Job_B submission
# This agent doesn't use an LLM - it executes pure Python logic
job_builder = JobBuilderAgent(
    name="job_builder",
    # executor_job_id is read from ADK_EXECUTOR_JOB_ID env var
    # catalog and schema are read from env vars
)

# Results Processor Agent: Analyzes execution results with injected context
# The context_injection_plugin injects stdout/stderr and sublm_instruction
# Model is selected via ADK_MODEL_PROVIDER env var (gemini or litellm)
# output_key persists the final response to session state for analyst consumption
results_processor_agent = LlmAgent(
    name="results_processor",
    model=_agent_model,
    output_key="rlm:last_results_summary",  # Persist output to session state
    instruction="""You are a specialist sub-agent for processing code execution results.

Your role is to:
1. Analyze the execution output (stdout/stderr) provided to you
2. Follow the analysis instruction that was specified when the code was delegated
3. Summarize findings and provide actionable recommendations
4. Identify any errors and suggest fixes if execution failed

When analyzing results:
- Look for patterns, anomalies, and key data points
- Compare results against the original analysis goals
- Provide clear, structured summaries
- If the execution failed, analyze the error and suggest corrections

Structure your response with:
- **Summary**: Brief overview of the execution result
- **Key Findings**: Main insights from the output
- **Recommendations**: Next steps or actions to take
- **Issues** (if any): Problems identified and suggested fixes""",
)

# =============================================================================
# Root Agent: LoopAgent Orchestrator
# =============================================================================

# Orchestrator Loop: Iteratively executes sub-agents until completion or max_iterations
# Agent sequence: databricks_analyst -> job_builder -> results_processor
# max_iterations is configurable via ADK_MAX_ITERATIONS env var (default: 10)
_max_iterations = int(os.environ.get("ADK_MAX_ITERATIONS", "10"))
root_agent = LoopAgent(
    name="orchestrator_loop",
    max_iterations=_max_iterations,
    sub_agents=[
        databricks_analyst,       # 1. Generates code, calls delegate_code_results()
        job_builder,              # 2. Submits Job_B, waits, writes results
        results_processor_agent,  # 3. Processes with injected context
    ]
)

# =============================================================================
# App Configuration
# =============================================================================

# Wrap the agent in the google-adk App class
# Available Plugins (7 total):
#   1. UcDeltaTelemetryPlugin     - UC Delta telemetry persistence + stdout logging (DEFAULT)
#   2. LoggingPlugin              - Standard runtime logging at workflow callback points
#   3. GlobalInstructionPlugin    - Injects global instructions into all agent interactions
#   4. UcToolExecutionSafetyPlugin - Blocks destructive SQL/shell operations
#   5. FormattingCheckPlugin      - Validates delegation blob format
#   6. CodeLintingPlugin          - Validates Python syntax before execution
#   7. RlmContextInjectionPlugin  - Injects execution context to results_processor_agent
#                                   + handles stage tracking (before/after callbacks)
#
# Note: UcDeltaTelemetryPlugin is the default. Set ADK_USE_LOGGING_PLUGIN=1 to use LoggingPlugin instead.
# Note: RlmContextPruningPlugin has been removed - its responsibilities are now handled by:
#   - Stage tracking: RlmContextInjectionPlugin (temp:rlm:stage state machine)
#   - Artifact consumed: JobBuilderAgent (marks consumed on successful execution)
app = App(
    name="adk_poc_plugins",
    root_agent=root_agent,
    plugins=[
        # Step 1: Safety - Block destructive operations first
        safety_plugin,
        # Step 2: Validation - Format check before processing
        formatting_plugin,
        # Step 3: Validation - Syntax check before execution
        linting_plugin,
        # Step 4: Telemetry and logging
        logging_plugin,
        # Step 5: Global instructions
        global_instruction_plugin,
        # Step 6: Context injection for results_processor (+ stage tracking)
        context_injection_plugin,
    ]
)

# Export key components for use by run.py and other entry points
__all__ = [
    # Agents
    "root_agent",               # LoopAgent orchestrator (orchestrator_loop)
    "databricks_analyst",       # LlmAgent for data analysis and code generation
    "job_builder",              # BaseAgent for Job_B submission
    "results_processor_agent",  # LlmAgent for processing execution results
    # App
    "app",
    # Plugins
    "logging_plugin",
    "global_instruction_plugin",
    "safety_plugin",
    "formatting_plugin",
    "linting_plugin",
    "context_injection_plugin",
    # Tools
    "save_artifact_to_volumes",
    "exit_loop",
    "delegate_code_results",
    "repo_filename_search",
    "metadata_keyword_search",
    "get_repo_file",
    # Plugin factories
    "get_logging_plugin",
    "get_telemetry_plugin",
    # Model configuration
    "build_agent_model",
    "get_model_config",
    "get_fallback_router",
]
