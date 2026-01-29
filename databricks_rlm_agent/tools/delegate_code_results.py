"""Delegate Code Results Tool for RLM Workflow.

This tool enables the databricks_analyst agent to delegate code execution
and result processing to downstream agents in the RLM workflow.

The tool:
1. Parses the docstring header to extract sublm_instruction and agent_code
2. Saves the code to the ADK ArtifactService
3. Creates a metadata entry in the artifact registry Delta table
4. Sets state keys for downstream agents:
   - temp:rlm:artifact_id - The artifact identifier (invocation-scoped)
   - temp:rlm:sublm_instruction - The instruction for results_processor_agent
   - temp:rlm:has_agent_code - Whether there is code to execute
   - temp:rlm:stage - Stage tracking: "delegated" (invocation-scoped)
   - temp:rlm:active_artifact_id - Active artifact for stage gating (invocation-scoped)
   - rlm:iteration - Incremented iteration counter (session-scoped)
5. Triggers escalation to let LoopAgent invoke the next sub-agent

State key design:
- Invocation glue keys use temp:rlm:* prefix (auto-discarded after invocation)
- Only rlm:iteration is session-scoped (persists across invocations)
- See plans/refactor_key_glue.md for the migration plan

Usage:
    The agent calls delegate_code_results with a blob containing:

    '''<instruction for results processor>'''
    <python code to execute>

    Or without instruction:

    <python code to execute>
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import Any, Optional, TYPE_CHECKING

from google.adk.tools import ToolContext
from google.genai import types

from databricks_rlm_agent.utils.docstring_parser import (
    parse_delegation_blob,
    DelegationBlobParseError,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# State key constants - invocation-scoped (temp:rlm:*)
# These are auto-discarded after invocation by DeltaSessionService
STATE_ARTIFACT_ID = "temp:rlm:artifact_id"
STATE_SUBLM_INSTRUCTION = "temp:rlm:sublm_instruction"
STATE_HAS_AGENT_CODE = "temp:rlm:has_agent_code"
STATE_CODE_ARTIFACT_KEY = "temp:rlm:code_artifact_key"
STATE_SESSION_ID = "temp:rlm:session_id"
STATE_INVOCATION_ID = "temp:rlm:invocation_id"
STATE_TEMP_PARSED_BLOB = "temp:parsed_blob"

# Stage tracking keys - invocation-scoped (replaces pruning plugin for correctness)
# Stage progression: "delegated" -> "executed" -> "processed"
STATE_STAGE = "temp:rlm:stage"
STATE_ACTIVE_ARTIFACT_ID = "temp:rlm:active_artifact_id"

# Session-scoped state key (persists across invocations)
STATE_ITERATION = "rlm:iteration"


async def delegate_code_results(code: str, tool_context: ToolContext) -> dict[str, Any]:
    """Delegate code execution and results processing to the RLM workflow.

    This tool saves generated code for execution by job_builder and sets up
    the context for results_processor_agent to analyze the output.

    The code parameter should be formatted as:

        '''<instruction for analyzing results>'''
        <python code to execute>

    Or without instruction (code will be executed but no special processing):

        <python code to execute>

    Args:
        code: The delegation blob containing optional instruction and Python code.
        tool_context: The ADK tool context providing state and artifact access.

    Returns:
        dict: Status of the delegation operation with:
            - status: "success" or "error"
            - artifact_id: The created artifact identifier (if successful)
            - message: Description of what happened
            - has_instruction: Whether an instruction was extracted
            - code_length: Length of the extracted code
    """
    print(f"[DELEGATE_CODE_RESULTS] Starting delegation from {tool_context.agent_name}")

    # Parse the delegation blob
    try:
        parsed = parse_delegation_blob(code)
    except DelegationBlobParseError as e:
        logger.error(f"Failed to parse delegation blob: {e}")
        return {
            "status": "error",
            "message": f"Failed to parse delegation blob: {e}",
            "error_type": "parse_error",
        }

    # Validate that we have code to execute
    if not parsed.is_valid:
        logger.warning("Delegation blob contains no executable code")
        return {
            "status": "error",
            "message": "Delegation blob contains no executable code",
            "error_type": "empty_code",
        }

    # Generate artifact ID
    artifact_id = f"art_{uuid.uuid4().hex[:12]}"

    # Get current iteration and increment
    current_iteration = tool_context.state.get(STATE_ITERATION, 0)
    new_iteration = current_iteration + 1

    # Get session info from tool_context
    # Note: These may need adjustment based on actual ADK ToolContext structure
    session_id = getattr(tool_context, "session_id", None)
    if session_id is None:
        session_id = tool_context.state.get("session_id", "unknown_session")

    invocation_id = getattr(tool_context, "invocation_id", None)
    if invocation_id is None:
        invocation_id = tool_context.state.get("invocation_id", f"inv_{uuid.uuid4().hex[:8]}")

    # Save code to ADK ArtifactService
    code_artifact_key = f"{artifact_id}_code.py"
    try:
        code_part = types.Part.from_text(text=parsed.agent_code)
        version = await tool_context.save_artifact(
            filename=code_artifact_key,
            artifact=code_part,
        )
        logger.info(f"Saved code artifact: {code_artifact_key} (version {version})")
        print(f"[DELEGATE_CODE_RESULTS] Saved code artifact: {code_artifact_key}")
    except Exception as e:
        logger.error(f"Failed to save code artifact: {e}")
        # Continue without ArtifactService if not available
        # The artifact registry will still track the metadata
        print(f"[DELEGATE_CODE_RESULTS] Warning: Could not save to ArtifactService: {e}")
        code_artifact_key = None

    # Store parsed blob in temp state for plugins/validators
    tool_context.state[STATE_TEMP_PARSED_BLOB] = {
        "sublm_instruction": parsed.sublm_instruction,
        "agent_code": parsed.agent_code,
        "has_instruction": parsed.has_instruction,
        "artifact_id": artifact_id,
    }

    # Set state keys for downstream agents
    # Invocation glue uses temp:rlm:* (auto-discarded after invocation)
    tool_context.state[STATE_ARTIFACT_ID] = artifact_id
    tool_context.state[STATE_SUBLM_INSTRUCTION] = parsed.sublm_instruction
    tool_context.state[STATE_HAS_AGENT_CODE] = bool(parsed.agent_code)

    # Session-scoped iteration counter (persists across invocations)
    tool_context.state[STATE_ITERATION] = new_iteration

    # Store additional context for job_builder (invocation-scoped)
    tool_context.state[STATE_CODE_ARTIFACT_KEY] = code_artifact_key
    tool_context.state[STATE_SESSION_ID] = session_id
    tool_context.state[STATE_INVOCATION_ID] = invocation_id

    # Set stage tracking keys (replaces pruning plugin for correctness)
    # This enables downstream agents to verify they should act on current state
    tool_context.state[STATE_STAGE] = "delegated"
    tool_context.state[STATE_ACTIVE_ARTIFACT_ID] = artifact_id

    # Create metadata entry in the artifact registry
    # Use local registry in local mode, Spark/Delta registry in Databricks mode
    registry_created = False
    run_mode = os.environ.get("ADK_RUN_MODE", "databricks")

    if run_mode == "local":
        # Use local DuckDB registry
        try:
            from databricks_rlm_agent.artifact_registry_local import get_local_artifact_registry

            registry = get_local_artifact_registry(ensure_exists=True)
            registry.create_artifact(
                artifact_id=artifact_id,
                session_id=session_id,
                invocation_id=invocation_id,
                iteration=new_iteration,
                artifact_type="delegation_request",
                sublm_instruction=parsed.sublm_instruction,
                code_artifact_key=code_artifact_key,
                metadata={
                    "code_length": len(parsed.agent_code),
                    "has_instruction": parsed.has_instruction,
                },
            )
            registry_created = True
            logger.info(f"Created local artifact registry entry: {artifact_id}")
            print(f"[DELEGATE_CODE_RESULTS] Created local registry entry: {artifact_id}")
        except Exception as e:
            logger.warning(f"Could not create local artifact registry entry: {e}")
            print(f"[DELEGATE_CODE_RESULTS] Warning: Could not create local registry entry: {e}")
    else:
        # Use Spark/Delta registry
        try:
            from pyspark.sql import SparkSession
            from databricks_rlm_agent.artifact_registry import get_artifact_registry

            spark = SparkSession.builder.getOrCreate()
            registry = get_artifact_registry(spark, ensure_exists=False)
            registry.create_artifact(
                artifact_id=artifact_id,
                session_id=session_id,
                invocation_id=invocation_id,
                iteration=new_iteration,
                artifact_type="delegation_request",
                sublm_instruction=parsed.sublm_instruction,
                code_artifact_key=code_artifact_key,
                metadata={
                    "code_length": len(parsed.agent_code),
                    "has_instruction": parsed.has_instruction,
                },
            )
            registry_created = True
            logger.info(f"Created artifact registry entry: {artifact_id}")
            print(f"[DELEGATE_CODE_RESULTS] Created registry entry: {artifact_id}")
        except ImportError:
            # Not in Databricks environment - registry will be created by job_builder
            logger.debug("Spark not available - skipping registry creation")
        except Exception as e:
            logger.warning(f"Could not create artifact registry entry: {e}")
            print(f"[DELEGATE_CODE_RESULTS] Warning: Could not create registry entry: {e}")

    logger.info(
        f"Delegation prepared: artifact_id={artifact_id}, "
        f"iteration={new_iteration}, has_instruction={parsed.has_instruction}"
    )
    print(
        f"[DELEGATE_CODE_RESULTS] Delegation ready: "
        f"artifact_id={artifact_id}, iteration={new_iteration}"
    )

    # IMPORTANT:
    # In an ADK LoopAgent, `escalate=True` terminates the loop (see ADK docs).
    # For delegation, we want to halt the current LLM agent and hand off control
    # to the deterministic `job_builder` agent, which writes the code file to UC
    # Volumes and submits Job_B.
    #
    # Therefore: use `transfer_to_agent`, not `escalate`.
    tool_context.actions.transfer_to_agent = "job_builder"
    print("[DELEGATE_CODE_RESULTS] Transfer to agent: job_builder")

    return {
        "status": "success",
        "artifact_id": artifact_id,
        "message": (
            f"Code delegation successful. Artifact {artifact_id} created for "
            f"iteration {new_iteration}. Execution will proceed via job_builder."
        ),
        "has_instruction": parsed.has_instruction,
        "instruction_preview": (
            parsed.sublm_instruction[:100] + "..."
            if parsed.sublm_instruction and len(parsed.sublm_instruction) > 100
            else parsed.sublm_instruction
        ),
        "code_length": len(parsed.agent_code),
        "code_artifact_key": code_artifact_key,
        "iteration": new_iteration,
        "registry_created": registry_created,
    }


# Docstring for the tool (used by ADK for function introspection)
delegate_code_results.__doc__ = """Delegate code execution and results processing to the RLM workflow.

This tool saves generated code for execution by the job_builder agent and sets up
context for the results_processor_agent to analyze the output.

Format your code parameter as:

    '''<instruction for analyzing results>'''
    <python code to execute>

Or without instruction:

    <python code to execute>

The instruction tells results_processor_agent how to analyze the execution output.
Without an instruction, the code runs but no special result processing occurs.

Args:
    code: The delegation blob containing optional instruction and Python code.
    tool_context: Provided by ADK - gives access to state and artifacts.

Returns:
    dict with status, artifact_id, and delegation details.

Example (the `code` argument value):
    \"\"\"Analyze vendor distribution across silos and identify duplicates.\"\"\"
    import pandas as pd
    df = spark.sql("SELECT * FROM vendors").toPandas()
    vendor_counts = df.groupby("vendor_name").size()
    print(f"Total vendors: {len(df)}")
    print(f"Duplicate vendors: {(vendor_counts > 1).sum()}")
"""
