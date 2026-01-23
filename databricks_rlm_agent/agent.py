import os
import logging
from google.adk.agents import Agent
from google.adk.apps import App
from google.adk.tools import FunctionTool, ToolContext, AgentTool

# Import pre-built plugins
from google.adk.plugins.logging_plugin import LoggingPlugin
from google.adk.plugins.global_instruction_plugin import GlobalInstructionPlugin

# Import custom UC telemetry plugin
from databricks_rlm_agent.plugins import UcDeltaTelemetryPlugin

# Import prompts
from databricks_rlm_agent.prompts import GLOBAL_INSTRUCTIONS, ROOT_AGENT_INSTRUCTION

# Note: API keys (GOOGLE_API_KEY, etc.) are loaded from Databricks Secrets
# via the secrets module at runtime startup in run.py. Do NOT hardcode keys here.

# Databricks Volumes paths - configurable via environment variables
ARTIFACTS_PATH = os.environ.get("ADK_ARTIFACTS_PATH", "/Volumes/silo_dev_rs/adk/artifacts")
AGENT_CODE_PATH = os.environ.get("ADK_AGENT_CODE_PATH", "/Volumes/silo_dev_rs/adk/agent_code/agent_code_raw.py")

def save_python_code(code: str, tool_context: ToolContext) -> dict:
    """
    Saves generated Python code to a specific Databricks Volume location.
    Target path: /Volumes/silo_dev_rs/adk/agent_code/agent_code_raw.py

    Args:
        code (str): The Python code to save.
        tool_context (ToolContext): The tool context.

    Returns:
        dict: Status of the save operation.
    """
    import os

    try:
        # Ensure the directory exists
        directory = os.path.dirname(AGENT_CODE_PATH)
        os.makedirs(directory, exist_ok=True)

        # Write the content to the file
        with open(AGENT_CODE_PATH, 'w') as f:
            f.write(code)

        print(f"Agent code saved successfully to: {AGENT_CODE_PATH}")
        return {
            "status": "success",
            "message": f"Code saved to {AGENT_CODE_PATH}",
            "file_path": AGENT_CODE_PATH
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


def save_artifact_to_volumes(filename: str, content: str, tool_context: ToolContext) -> dict:
    """
    Saves a file artifact to Databricks Volumes.

    Args:
        filename (str): The name of the file to save.
        content (str): The content to write to the file.
        tool_context (ToolContext): The tool context.

    Returns:
        dict: Status of the save operation with the file path.
    """
    import os

    try:
        # Ensure the artifacts directory exists
        os.makedirs(ARTIFACTS_PATH, exist_ok=True)

        # Full path for the artifact
        file_path = os.path.join(ARTIFACTS_PATH, filename)

        # Write the content to the file
        with open(file_path, 'w') as f:
            f.write(content)

        print(f"Artifact saved successfully: {file_path}")
        return {
            "status": "success",
            "message": f"File saved to {file_path}",
            "file_path": file_path
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


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

# Define the sub-agent
rlm_subLM = Agent(
    name="rlm_subLM",
    model="gemini-3-pro-preview",
    instruction="You are a specialist sub-agent. Provide detailed answers to the query provided to you.",
)

# Define the Agent with enhanced instruction
root_agent = Agent(
    name="databricks_analyst_with_plugins",
    model="gemini-3-pro-preview",
    instruction=ROOT_AGENT_INSTRUCTION,
    tools=[
        FunctionTool(save_python_code),
        FunctionTool(save_artifact_to_volumes),
        AgentTool(agent=rlm_subLM)
    ]
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
    "root_agent",
    "app",
    "logging_plugin",
    "global_instruction_plugin",
    "save_python_code",
    "save_artifact_to_volumes",
    "get_logging_plugin",
    "get_telemetry_plugin",
]