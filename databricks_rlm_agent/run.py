"""Entry point for running the Databricks RLM Agent with Delta session persistence.

This module wires the DeltaSessionService to the ADK Runner, enabling durable
session state across Databricks Lakeflow job runs.

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
from typing import Optional

from google.adk.runners import Runner
from google.genai import types

from .sessions import DeltaSessionService
from .secrets import load_secrets, validate_secrets


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
    """Create a Runner with DeltaSessionService.

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
        logging_plugin,
        global_instruction_plugin,
    )

    # Initialize Delta session service
    session_service = DeltaSessionService(
        spark=spark,
        catalog=catalog,
        schema=schema,
    )

    # Create Runner with session service and plugins
    runner = Runner(
        agent=root_agent,
        app_name=APP_NAME,
        session_service=session_service,
        plugins=[
            logging_plugin,
            global_instruction_plugin,
        ],
    )

    return runner, session_service


async def run_conversation(
    runner: Runner,
    session_service: DeltaSessionService,
    user_id: str,
    session_id: str,
    prompt: str,
) -> str:
    """Run a single conversation turn.

    Args:
        runner: The ADK Runner instance.
        session_service: The DeltaSessionService instance.
        user_id: User identifier.
        session_id: Session identifier.
        prompt: User prompt text.

    Returns:
        The agent's response text.
    """
    final_response = "No response generated."

    async for event in runner.run_async(
        user_id=user_id,
        session_id=session_id,
        new_message=types.Content(
            role="user",
            parts=[types.Part.from_text(text=prompt)]
        ),
    ):
        if event.is_final_response():
            if event.content and event.content.parts:
                final_response = event.content.parts[0].text

    return final_response


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
    response = await run_conversation(
        runner=runner,
        session_service=session_service,
        user_id=user_id,
        session_id=session_id,
        prompt=prompt,
    )

    print(f"\nAgent: {response}")
    print("-" * 50)

    # Close session service
    await session_service.close()

    return response


if __name__ == "__main__":
    asyncio.run(main())

