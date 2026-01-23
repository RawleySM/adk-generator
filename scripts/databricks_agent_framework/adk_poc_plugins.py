#!/usr/bin/env python3
"""
ADK POC Agent with Pre-Built Plugins
=====================================
This script demonstrates Google ADK integration with Databricks using:
- LoggingPlugin: Records information at each agent workflow callback point
- GlobalInstructionPlugin: Provides global instructions at the App level
- SaveFilesAsArtifactsPlugin: Saves files included in messages as Artifacts

Artifacts are saved to: /Volumes/silo_dev_rs/test/artifacts
"""

import asyncio
import os
import time
import mlflow
import nest_asyncio
from pyspark.sql import SparkSession

from google.adk.agents import Agent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.artifacts import InMemoryArtifactService
from google.adk.tools import FunctionTool, ToolContext
from google.genai import types

# Import pre-built plugins
from google.adk.plugins.logging_plugin import LoggingPlugin
from google.adk.plugins.global_instruction_plugin import GlobalInstructionPlugin
from google.adk.plugins.save_files_as_artifacts_plugin import SaveFilesAsArtifactsPlugin

# Apply nest_asyncio to allow nested event loops in Databricks notebooks
nest_asyncio.apply()

# Hardcoded API Key as requested
os.environ["GOOGLE_API_KEY"] = "AIzaSyCMl414hyeEJPKrdHeHKGynb6ETOs65e3c"

# Databricks Volumes path for artifacts
ARTIFACTS_PATH = "/Volumes/silo_dev_rs/test/artifacts"

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Global execution context to maintain state across tool calls
EXECUTION_CONTEXT = {"spark": spark, "ARTIFACTS_PATH": ARTIFACTS_PATH}


def databricks_code_interpreter(code: str, tool_context: ToolContext) -> dict:
    """
    Executes Python code locally on the Databricks driver.
    This allows access to the 'spark' session and maintains state across calls.

    Args:
        code (str): The Python code to execute.
        tool_context (ToolContext): The tool context.

    Returns:
        dict: The result of the execution (stdout and/or error).
    """
    import io
    import contextlib
    import traceback

    # Buffer to capture stdout
    f = io.StringIO()

    try:
        print(f"Executing code:\n{code}")
        with contextlib.redirect_stdout(f):
            # Execute code in the global execution context
            exec(code, EXECUTION_CONTEXT)

        output = f.getvalue()
        return {"status": "success", "output": output}

    except Exception as e:
        # Capture the full traceback for debugging
        tb = traceback.format_exc()
        return {"status": "error", "message": str(e), "traceback": tb}


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


# Global instructions applied to all agent interactions
GLOBAL_INSTRUCTIONS = """
IMPORTANT GUIDELINES FOR ALL INTERACTIONS:
1. Always provide clear, well-commented code when generating scripts.
2. When saving files, use the save_artifact_to_volumes tool to persist to Databricks Volumes.
3. Include proper error handling in generated code.
4. Log all significant operations for observability.
5. Follow Python best practices (PEP 8 style guide).
"""


async def main():
    # Capture start time to filter traces later
    start_time_ms = int(time.time() * 1000)
    print(f"Script started at: {start_time_ms} ms")
    print(f"Artifacts will be saved to: {ARTIFACTS_PATH}")

    # Set up MLflow Experiment (required for search_traces)
    # Creates the experiment if it doesn't exist
    mlflow.set_experiment("/Users/rstanhope@spendmend.com/adk_poc_plugins")

    # Enable MLflow Tracing for Gemini
    mlflow.gemini.autolog()

    # Initialize plugins
    logging_plugin = LoggingPlugin(name="adk_poc_logging")
    global_instruction_plugin = GlobalInstructionPlugin(
        global_instruction=GLOBAL_INSTRUCTIONS,
        name="adk_poc_global_instructions"
    )
    save_files_plugin = SaveFilesAsArtifactsPlugin(
        name="adk_poc_save_files"
    )

    # Initialize artifact service for the SaveFilesAsArtifactsPlugin
    artifact_service = InMemoryArtifactService()

    # Define the Agent with enhanced instruction
    agent = Agent(
        name="databricks_analyst_with_plugins",
        model="gemini-3-pro-preview",
        instruction="""You are a Databricks data analysis and code generation agent.

        You have access to two tools:
        1. databricks_code_interpreter: Runs Python code on the Databricks cluster
        2. save_artifact_to_volumes: Saves generated files to Databricks Volumes

        When asked to generate code artifacts:
        - Create well-documented, production-ready Python code
        - Use save_artifact_to_volumes to persist the file
        - Confirm the file was saved successfully with the full path

        When asked to profile a table:
        - Use PySpark to load the table and print summary statistics
        - ALWAYS print the output in your python code so it is captured
        """,
        tools=[
            FunctionTool(databricks_code_interpreter),
            FunctionTool(save_artifact_to_volumes)
        ]
    )

    # Initialize Session Service
    session_service = InMemorySessionService()
    await session_service.create_session(
        app_name="adk_poc_plugins",
        user_id="poc_user",
        session_id="session_plugins_001"
    )

    # Initialize Runner with plugins
    runner = Runner(
        agent=agent,
        app_name="adk_poc_plugins",
        session_service=session_service,
        artifact_service=artifact_service,
        plugins=[
            logging_plugin,
            global_instruction_plugin,
            save_files_plugin
        ]
    )

    # User prompt to generate a hello-world Python artifact
    prompt = """Generate a Python script that prints 'Hello, World!' and save it as an artifact.

    The script should:
    1. Be named 'hello_world.py'
    2. Include a docstring explaining what it does
    3. Include a main() function with proper if __name__ == '__main__' guard
    4. Print 'Hello, World!' when executed

    Save this script to Databricks Volumes using the save_artifact_to_volumes tool.
    """

    print(f"User Prompt: {prompt}")

    session_id = "session_plugins_001"
    final_response_text = "No response generated."

    async for event in runner.run_async(
        user_id="poc_user",
        session_id=session_id,
        new_message=types.Content(role="user", parts=[types.Part.from_text(text=prompt)])
    ):
        if event.is_final_response():
            print("\nFinal Response:")
            final_response_text = event.content.parts[0].text
            print(final_response_text)

    # --- Observability: Retrieve and Print MLflow Traces ---
    trace_summary = "\n\n" + "="*50 + "\nOBSERVABILITY: MLFLOW TRACES\n" + "="*50 + "\n"

    try:
        # Allow a brief moment for async logging to flush
        time.sleep(2)

        # Search for traces generated during this execution
        traces = mlflow.search_traces(
            filter_string=f"attributes.timestamp_ms >= {start_time_ms}",
            max_results=10,
            order_by=["attributes.timestamp_ms DESC"]
        )

        if not traces.empty:
            trace_summary += f"Found {len(traces)} trace(s).\n"
            for index, trace in traces.iterrows():
                trace_summary += f"\nTrace #{index + 1}\n"
                trace_summary += f"  ID: {trace.get('request_id', 'N/A')}\n"
                trace_summary += f"  Status: {trace.get('status', 'N/A')}\n"
                trace_summary += f"  Latency: {trace.get('execution_time_ms', 'N/A')} ms\n"

                # Truncate large inputs/outputs for readability
                inputs = str(trace.get('request', 'N/A'))
                outputs = str(trace.get('response', 'N/A'))
                inputs_str = f"{inputs[:200]}..." if len(inputs) > 200 else inputs
                outputs_str = f"{outputs[:200]}..." if len(outputs) > 200 else outputs
                trace_summary += f"  Inputs: {inputs_str}\n"
                trace_summary += f"  Outputs: {outputs_str}\n"
        else:
            trace_summary += "No traces found for this run.\n"

    except Exception as e:
        trace_summary += f"Failed to retrieve/print traces: {e}\n"

    # --- Plugin Summary ---
    trace_summary += "\n" + "="*50 + "\n"
    trace_summary += "PLUGINS ACTIVE:\n"
    trace_summary += "  - LoggingPlugin: Logging agent workflow events\n"
    trace_summary += "  - GlobalInstructionPlugin: Applied global instructions\n"
    trace_summary += "  - SaveFilesAsArtifactsPlugin: File artifact handling enabled\n"
    trace_summary += "="*50 + "\n"

    print(trace_summary)

    # Exit with result
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        full_output = final_response_text + trace_summary
        dbutils.notebook.exit(full_output)
    except Exception as e:
        print(f"Could not exit with dbutils: {e}")


if __name__ == "__main__":
    asyncio.run(main())
