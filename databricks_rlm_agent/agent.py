import os
import logging
from google.adk.agents import LlmAgent
from google.adk.agents.loop_agent import LoopAgent
from google.adk.apps import App
from google.adk.tools import FunctionTool

# Import pre-built plugins
from google.adk.plugins.logging_plugin import LoggingPlugin
from google.adk.plugins.global_instruction_plugin import GlobalInstructionPlugin

# Import custom UC telemetry plugin
from databricks_rlm_agent.plugins import UcDeltaTelemetryPlugin

# Import prompts
from databricks_rlm_agent.prompts import GLOBAL_INSTRUCTIONS, ROOT_AGENT_INSTRUCTION

# Import tools
from databricks_rlm_agent.tools import save_python_code, save_artifact_to_volumes, llm_query, exit_loop

# Note: API keys (GOOGLE_API_KEY, etc.) are loaded from Databricks Secrets
# via the secrets module at runtime startup in run.py. Do NOT hardcode keys here.


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


# Initialize plugins
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

# =============================================================================
# Sub-Agents (LlmAgent instances)
# =============================================================================

# LLM Query sub-agent: Handles semantic analysis queries from the orchestrator
llm_query_agent = LlmAgent(
    name="llm_query",
    model="gemini-3-pro-preview",
    instruction="""You are a specialist sub-agent for semantic analysis within a REPL environment.
Your role is to:
1. Process queries containing context data (table schemas, code snippets, data samples)
2. Analyze the provided context based on embedded instructions
3. Provide detailed, structured answers optimized for further processing

You can handle around 500K characters in your context window. When receiving batched records,
analyze them efficiently and return consolidated findings.

Always structure your responses clearly with:
- Key findings
- Specific recommendations
- Confidence levels (HIGH/MEDIUM/LOW) where applicable""",
)

# Databricks Analyst sub-agent: Primary agent for data discovery and code generation
databricks_analyst = LlmAgent(
    name="databricks_analyst",
    model="gemini-3-pro-preview",
    instruction=ROOT_AGENT_INSTRUCTION,
    tools=[
        FunctionTool(save_python_code),
        FunctionTool(save_artifact_to_volumes),
        FunctionTool(llm_query),
        FunctionTool(exit_loop),
    ]
)

# =============================================================================
# Root Agent: LoopAgent Orchestrator
# =============================================================================

# Orchestrator Loop: Iteratively executes sub-agents until completion or max_iterations
root_agent = LoopAgent(
    name="orchestrator_loop",
    max_iterations=10,
    sub_agents=[databricks_analyst, llm_query_agent]
)

# Wrap the agent in the google-adk App class
# Available Plugins (3 total):
#   1. UcDeltaTelemetryPlugin     - UC Delta telemetry persistence + stdout logging (DEFAULT)
#   2. LoggingPlugin              - Standard runtime logging at workflow callback points
#   3. GlobalInstructionPlugin    - Injects global instructions into all agent interactions
#
# Note: UcDeltaTelemetryPlugin is the default. Set ADK_USE_LOGGING_PLUGIN=1 to use LoggingPlugin instead.
app = App(
    name="adk_poc_plugins",
    root_agent=root_agent,
    plugins=[
        logging_plugin,            # UcDeltaTelemetryPlugin (default) or LoggingPlugin
        global_instruction_plugin, # GlobalInstructionPlugin
    ]
)

# Export key components for use by run.py and other entry points
__all__ = [
    # Agents
    "root_agent",           # LoopAgent orchestrator (orchestrator_loop)
    "databricks_analyst",   # LlmAgent for data analysis and code generation
    "llm_query_agent",      # LlmAgent for semantic analysis queries
    # App
    "app",
    # Plugins
    "logging_plugin",
    "global_instruction_plugin",
    # Tools
    "save_python_code",
    "save_artifact_to_volumes",
    "llm_query",
    "exit_loop",
    # Plugin factories
    "get_logging_plugin",
    "get_telemetry_plugin",
]
