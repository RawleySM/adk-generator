"""Entry point for running the Databricks RLM Agent with Delta session persistence.

This module wires the DeltaSessionService and ArtifactService to the ADK Runner,
enabling durable session state and artifact storage across Databricks Lakeflow job runs.

The RLM workflow uses:
- DeltaSessionService: Persists session state to Unity Catalog Delta tables
- InMemoryArtifactService: Stores code artifacts for execution (future: DeltaArtifactService)

Usage:
    python -m databricks_rlm_agent.run

Or from Databricks:
    from databricks_rlm_agent.run import main
    import asyncio
    asyncio.run(main())

Secrets Configuration:
    API keys and credentials are loaded from Databricks Secrets at startup.
    See databricks_rlm_agent/secrets.py for configuration details.

    Recommended: Configure secrets as environment variables in your Databricks Job:
        "env_vars": {
            "GOOGLE_API_KEY": "{{secrets/adk-secrets/google-api-key}}",
            "DATABRICKS_HOST": "{{secrets/adk-secrets/databricks-host}}",
            "DATABRICKS_TOKEN": "{{secrets/adk-secrets/databricks-token}}"
        }
"""

import asyncio
import os
from dataclasses import dataclass
from typing import Optional

from google.adk.runners import Runner
from google.adk.artifacts import InMemoryArtifactService
from google.genai import types

from .sessions import DeltaSessionService
from .secrets import load_secrets, validate_secrets


@dataclass(frozen=True, slots=True)
class ConversationResult:
    """Structured result from run_conversation().

    This avoids the broken pattern where cli.py reloads session state after
    the run and checks temp:* keys (which DeltaSessionService never persists).

    Attributes:
        response_text: The agent's response text.
        status: One of "success", "exit_loop", or "fatal_error".
        fatal_error_msg: Human-readable error message if status is "fatal_error".
        delegation_count: Number of delegation escalations (delegate_code_results).
    """

    response_text: str
    status: str  # "success" | "exit_loop" | "fatal_error"
    fatal_error_msg: Optional[str] = None
    delegation_count: int = 0


# Configuration from environment or defaults
CATALOG = os.environ.get("ADK_DELTA_CATALOG", "silo_dev_rs")
SCHEMA = os.environ.get("ADK_DELTA_SCHEMA", "adk")
APP_NAME = os.environ.get("ADK_APP_NAME", "databricks_rlm_agent")
DEFAULT_USER_ID = os.environ.get("ADK_DEFAULT_USER_ID", "job_user")

# Flag to track if secrets have been loaded (for lazy initialization)
_secrets_loaded = False


def _ensure_secrets_loaded(spark: Optional["SparkSession"] = None) -> None:
    """Ensure secrets are loaded before agent initialization.

    This must be called before importing agent components, as they may
    depend on environment variables being set (e.g., GOOGLE_API_KEY).

    Args:
        spark: Optional SparkSession for dbutils fallback.
    """
    global _secrets_loaded
    if _secrets_loaded:
        return

    print("Loading secrets from Databricks Secrets...")
    results = load_secrets(spark=spark)

    # Log results
    loaded = [k for k, v in results.items() if v]
    if loaded:
        print(f"  Loaded secrets: {', '.join(loaded)}")

    validation = validate_secrets()
    print(f"  Environment validation: {validation}")

    _secrets_loaded = True


async def create_runner(
    spark: Optional["SparkSession"] = None,  # noqa: F821
    catalog: str = CATALOG,
    schema: str = SCHEMA,
) -> tuple[Runner, DeltaSessionService]:
    """Create a Runner with DeltaSessionService and ArtifactService.

    Args:
        spark: Optional SparkSession. If None, will create one.
        catalog: Unity Catalog name for session tables.
        schema: Schema name within the catalog.

    Returns:
        Tuple of (Runner, DeltaSessionService) for use in agent execution.
    """
    # Get or create SparkSession
    if spark is None:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

    # Ensure secrets are loaded before importing agent (which may use env vars)
    _ensure_secrets_loaded(spark)

    # Import agent components after secrets are loaded
    # This ensures GOOGLE_API_KEY and other env vars are set before
    # the google.adk/google.genai clients are initialized
    from .agent import (
        root_agent,
        # Full plugin chain - order matters for execution sequence
        safety_plugin,              # 1. Block destructive operations first
        formatting_plugin,          # 2. Validate delegation blob format
        linting_plugin,             # 3. Validate Python syntax before execution
        logging_plugin,             # 4. Telemetry and logging (UC Delta or stdout)
        global_instruction_plugin,  # 5. Inject global instructions
        context_injection_plugin,   # 6. Inject execution context to results_processor (+ stage tracking)
    )

    # Initialize Delta session service
    session_service = DeltaSessionService(
        spark=spark,
        catalog=catalog,
        schema=schema,
    )

    # Initialize artifact service for RLM workflow
    # InMemoryArtifactService is used for development
    # Future: DeltaArtifactService for production persistence to UC Volumes
    artifact_service = InMemoryArtifactService()
    print(f"[ARTIFACTS] Using InMemoryArtifactService for artifact storage")

    # Create Runner with session service, artifact service, and full plugin chain
    # Plugin order mirrors agent.py App configuration:
    #   1. Safety - Block destructive operations first
    #   2. Formatting - Validate delegation blob format
    #   3. Linting - Validate Python syntax before execution
    #   4. Logging/Telemetry - Record events
    #   5. Global Instructions - Inject system prompts
    #   6. Context Injection - Inject execution results for results_processor (+ stage tracking)
    runner = Runner(
        agent=root_agent,
        app_name=APP_NAME,
        session_service=session_service,
        artifact_service=artifact_service,  # Enables context.save_artifact/load_artifact
        plugins=[
            safety_plugin,
            formatting_plugin,
            linting_plugin,
            logging_plugin,
            global_instruction_plugin,
            context_injection_plugin,
        ],
    )

    return runner, session_service


async def run_conversation(
    runner: Runner,
    session_service: DeltaSessionService,
    user_id: str,
    session_id: str,
    prompt: str,
    timeout_seconds: float = 900.0,
    event_timeout_seconds: float = 300.0,
) -> ConversationResult:
    """Run a single conversation turn with timeout protection.

    Args:
        runner: The ADK Runner instance.
        session_service: The DeltaSessionService instance.
        user_id: User identifier.
        session_id: Session identifier.
        prompt: User prompt text.
        timeout_seconds: Maximum total time for the entire conversation turn.
            Defaults to 900 seconds (15 minutes).
        event_timeout_seconds: Maximum time to wait between events from the
            stream. If no event is received within this time, the conversation
            is considered stalled. Defaults to 300 seconds (5 minutes).

    Returns:
        ConversationResult with response_text, status, fatal_error_msg, and delegation_count.

    Raises:
        asyncio.TimeoutError: If the conversation exceeds timeout_seconds or
            if no events are received within event_timeout_seconds.
    """
    final_response = "No response generated."
    last_text_response = None  # Track the last text response seen
    exit_loop_detected = False  # Track if exit_loop was called (vs delegate_code_results)
    fatal_error_detected = False  # Track if a fatal error was encountered
    fatal_error_msg = None  # Human-readable fatal error message
    delegation_count = 0  # Track delegation escalations (delegate_code_results)

    # NOTE: Previous versions had cleanup logic here that appended an Event with
    # role="user" content to clear stale escalation keys. This was removed because:
    # 1. It pollutes conversation history (model sees "[System cleanup: ...]" as user utterance)
    # 2. temp:rlm:* keys auto-discard after invocation, so cleanup is unnecessary
    # 3. Legacy rlm:* keys will be phased out; accepting minor stale-flag risk during migration
    #
    # Stale state is now prevented by stage tracking (temp:rlm:stage state machine):
    #   - delegate_code_results sets stage="delegated"
    #   - JobBuilderAgent only runs when stage="delegated", then sets stage="executed"
    #   - RlmContextInjectionPlugin only injects when stage="executed", then sets stage="processed"

    async def _iterate_with_event_timeout():
        """Iterate over events with per-event timeout watchdog."""
        nonlocal final_response, last_text_response, exit_loop_detected, fatal_error_detected, fatal_error_msg, delegation_count
        event_iter = runner.run_async(
            user_id=user_id,
            session_id=session_id,
            new_message=types.Content(
                role="user",
                parts=[types.Part.from_text(text=prompt)]
            ),
        ).__aiter__()

        while True:
            try:
                # Wait for next event with timeout - this is the watchdog
                event = await asyncio.wait_for(
                    event_iter.__anext__(),
                    timeout=event_timeout_seconds,
                )
                # Track all text responses - even if not marked as "final"
                # This handles cases where escalation triggers before final response
                if event.content and event.content.parts:
                    for part in event.content.parts:
                        if hasattr(part, 'text') and part.text:
                            last_text_response = part.text

                # Check for escalation events and distinguish fatal errors, exit_loop, and delegation
                # Priority: fatal_error > exit_loop > delegation
                # - fatal_error: unrecoverable workflow failure from JobBuilderAgent
                # - exit_loop: sets temp:rlm:exit_requested=True in state, signals intentional loop termination
                # - delegate_code_results: escalates to advance to job_builder sub-agent
                if hasattr(event, 'actions') and event.actions and getattr(event.actions, 'escalate', False):
                    author = getattr(event, 'author', 'unknown')
                    state_delta = getattr(event.actions, 'state_delta', {}) or {}

                    # Check escalation type (priority: fatal > exit_loop > delegation)
                    # Check both temp:rlm:* (new) and rlm:* (legacy) keys for migration
                    is_fatal = (
                        state_delta.get('temp:rlm:fatal_error', False) or
                        state_delta.get('rlm:fatal_error', False)
                    )
                    is_exit_loop = (
                        state_delta.get('temp:rlm:exit_requested', False) or
                        state_delta.get('rlm:exit_requested', False)
                    )
                    
                    if is_fatal:
                        fatal_error_detected = True
                        # Check both temp:rlm:* (new) and rlm:* (legacy) keys for error message
                        fatal_error_msg = (
                            state_delta.get('temp:rlm:fatal_error_msg') or
                            state_delta.get('rlm:fatal_error_msg', 'Unknown fatal error')
                        )
                        print(f"ERROR: Fatal error detected from {author}: {fatal_error_msg}")
                        if last_text_response:
                            final_response = last_text_response
                    elif is_exit_loop:
                        print(f"INFO: exit_loop termination detected from {author}")
                        exit_loop_detected = True
                        # Use last text response as the final response
                        if last_text_response:
                            final_response = last_text_response
                    else:
                        delegation_count += 1
                        print(f"INFO: Delegation escalation #{delegation_count} from {author} (workflow continues)")
                    # Continue processing remaining events - don't break here
                    # The stream should end naturally after escalation

                if event.is_final_response():
                    if event.content and event.content.parts:
                        final_response = event.content.parts[0].text
            except StopAsyncIteration:
                # Stream completed normally - use last text if no final response
                if final_response == "No response generated." and last_text_response:
                    final_response = last_text_response
                if fatal_error_detected:
                    print(f"INFO: Stream completed after fatal error")
                elif exit_loop_detected:
                    print(f"INFO: Stream completed after exit_loop (delegations: {delegation_count})")
                elif delegation_count > 0:
                    print(f"INFO: Stream completed after {delegation_count} delegation(s)")
                break
            except asyncio.TimeoutError:
                print(f"WARNING: Event stream stalled - no event received in {event_timeout_seconds}s")
                # If we have a last text response, use it before raising
                if last_text_response:
                    final_response = last_text_response
                    print(f"INFO: Using last captured text response before timeout")
                # Forgive timeout if exit_loop was explicitly called or fatal error occurred
                # (workflow is complete in both cases)
                # Delegation escalations (delegate_code_results) should NOT forgive timeouts
                # because the workflow is still in progress
                if exit_loop_detected or fatal_error_detected:
                    completion_reason = 'fatal error' if fatal_error_detected else 'exit_loop'
                    print(f"INFO: Timeout after {completion_reason} - treating as completed")
                    break
                raise

    try:
        # Overall timeout for the entire conversation turn
        await asyncio.wait_for(
            _iterate_with_event_timeout(),
            timeout=timeout_seconds,
        )
    except asyncio.TimeoutError:
        print(f"ERROR: Conversation timed out after {timeout_seconds}s total or {event_timeout_seconds}s between events")
        raise

    # Determine status based on flags detected during event processing
    if fatal_error_detected:
        status = "fatal_error"
    elif exit_loop_detected:
        status = "exit_loop"
    else:
        status = "success"

    return ConversationResult(
        response_text=final_response,
        status=status,
        fatal_error_msg=fatal_error_msg,
        delegation_count=delegation_count,
    )


async def main(
    prompt: Optional[str] = None,
    user_id: str = DEFAULT_USER_ID,
    session_id: str = "session_001",
):
    """Main entry point for running the agent.

    Args:
        prompt: Optional prompt. If None, uses a default test prompt.
        user_id: User identifier for the session.
        session_id: Session identifier.
    """
    from pyspark.sql import SparkSession

    print(f"Initializing Databricks RLM Agent...")
    print(f"  Catalog: {CATALOG}")
    print(f"  Schema: {SCHEMA}")
    print(f"  App Name: {APP_NAME}")

    # Get SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Create runner and session service
    runner, session_service = await create_runner(spark=spark)

    # Create or resume session
    try:
        session = await session_service.create_session(
            app_name=APP_NAME,
            user_id=user_id,
            session_id=session_id,
        )
        print(f"Created new session: {session.id}")
    except ValueError as e:
        if "already exists" in str(e):
            session = await session_service.get_session(
                app_name=APP_NAME,
                user_id=user_id,
                session_id=session_id,
            )
            print(f"Resumed existing session: {session.id} with {len(session.events)} events")
        else:
            raise

    # Use default prompt if none provided
    if prompt is None:
        prompt = "Hello! Please describe your capabilities."

    print(f"\nUser: {prompt}")
    print("-" * 50)

    # Run the conversation
    result = await run_conversation(
        runner=runner,
        session_service=session_service,
        user_id=user_id,
        session_id=session_id,
        prompt=prompt,
    )

    print(f"\nAgent: {result.response_text}")
    print(f"Status: {result.status}")
    if result.fatal_error_msg:
        print(f"Fatal Error: {result.fatal_error_msg}")
    if result.delegation_count > 0:
        print(f"Delegations: {result.delegation_count}")
    print("-" * 50)

    # Close session service
    await session_service.close()

    return result


if __name__ == "__main__":
    asyncio.run(main())

