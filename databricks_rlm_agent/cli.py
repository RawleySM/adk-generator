"""CLI entrypoints for the RLM Agent three-job pattern.

This module provides the console script entrypoints for:
- rlm-orchestrator: Job_A - Control plane orchestration
- rlm-executor: Job_B - Execution plane artifact runner
- rlm-ingestor: Job_C - CDF polling and trigger plane

These entrypoints are designed to be called via Databricks python_wheel_task.
"""

import argparse
import asyncio
import os
import sys
from typing import Optional

import nest_asyncio


# Apply nest_asyncio to allow running asyncio.run() within an existing event loop
# (required for Databricks notebook/job execution context which already has an event loop)
nest_asyncio.apply()


def _setup_logging():
    """Configure logging for CLI execution."""
    import logging

    log_level = os.environ.get("ADK_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Suppress noisy py4j logging (PySpark's Py4J bridge)
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("py4j.clientserver").setLevel(logging.WARNING)

    return logging.getLogger(__name__)


_job_params_cache: Optional[dict] = None


def _get_job_parameters_from_run() -> dict:
    """Fetch all job parameters from the current Databricks job run.

    Returns:
        Dict of parameter name -> value
    """
    global _job_params_cache
    if _job_params_cache is not None:
        return _job_params_cache

    _job_params_cache = {}
    import logging
    logger = logging.getLogger(__name__)

    # Method 1: Try dbutils context (works for wheel tasks)
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)

        # Get the notebook context which contains job info
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        run_id = ctx.jobRunId().get() if ctx.jobRunId().isDefined() else None
        logger.info(f"[JOB_PARAMS] dbutils context jobRunId = '{run_id}'")

        if run_id:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            run = w.jobs.get_run(int(run_id))
            logger.info(f"[JOB_PARAMS] Got run object, job_parameters count: {len(run.job_parameters) if run.job_parameters else 0}")

            if run.job_parameters:
                for param in run.job_parameters:
                    value = param.value if param.value else param.default
                    _job_params_cache[param.name] = value or ""
                    logger.info(f"[JOB_PARAMS] {param.name} = '{value[:50] if value else ''}...'")
                return _job_params_cache
    except Exception as e:
        logger.warning(f"[JOB_PARAMS] Failed to get job parameters from dbutils context: {e}")

    # Method 2: Try spark conf (backup)
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        run_id = spark.conf.get("spark.databricks.job.runId", "")
        logger.info(f"[JOB_PARAMS] spark.databricks.job.runId = '{run_id}'")

        if run_id:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            run = w.jobs.get_run(int(run_id))
            if run.job_parameters:
                for param in run.job_parameters:
                    value = param.value if param.value else param.default
                    _job_params_cache[param.name] = value or ""
    except Exception as e:
        logger.warning(f"[JOB_PARAMS] Failed to get job parameters from spark conf: {e}")

    return _job_params_cache


def _get_job_parameter(name: str, default: str = "") -> str:
    """Get a job parameter from Databricks job context or environment variable.

    Tries multiple sources in order:
    1. Databricks Jobs API (current run's job_parameters)
    2. dbutils.widgets - for notebook-style parameters
    3. Environment variables - for local testing

    Args:
        name: Parameter name (e.g., "ADK_PROMPT")
        default: Default value if not found

    Returns:
        Parameter value or default
    """
    import logging
    logger = logging.getLogger(__name__)

    # Try to get from current job run context via Jobs API
    params = _get_job_parameters_from_run()
    if name in params and params[name]:
        logger.info(f"[JOB_PARAMS] Using {name} from job run")
        return params[name]

    # Try dbutils.widgets (for notebook-style access)
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        value = dbutils.widgets.get(name)
        if value:
            logger.info(f"[JOB_PARAMS] Using {name} from dbutils.widgets")
            return value
    except Exception:
        pass  # Not in Databricks or widget not set

    # Fall back to environment variable
    env_val = os.environ.get(name, default)
    logger.info(f"[JOB_PARAMS] Using {name} from env/default: '{env_val[:50] if env_val else ''}'")
    return env_val


def _get_spark_session():
    """Get or create SparkSession."""
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()


def _read_prompt_file(prompt_file: str) -> str:
    """Read prompt text from a file (UC Volumes, DBFS, or local).

    Args:
        prompt_file: Path to the prompt file (e.g., /Volumes/silo_dev_rs/task/task_txt/task.txt).

    Returns:
        The prompt text with leading/trailing whitespace stripped.

    Raises:
        FileNotFoundError: If the file does not exist.
        IOError: If the file cannot be read.
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"[PROMPT_FILE] Reading prompt from: {prompt_file}")

    try:
        with open(prompt_file, "r", encoding="utf-8") as f:
            content = f.read().strip()
        if not content:
            logger.warning(f"[PROMPT_FILE] File exists but is empty: {prompt_file}")
        else:
            logger.info(f"[PROMPT_FILE] Loaded {len(content)} characters from {prompt_file}")
        return content
    except FileNotFoundError:
        logger.error(f"[PROMPT_FILE] File not found: {prompt_file}")
        raise
    except Exception as e:
        logger.error(f"[PROMPT_FILE] Failed to read file {prompt_file}: {e}")
        raise IOError(f"Cannot read prompt file: {prompt_file}") from e


# =============================================================================
# Job_A: Orchestrator Entrypoint
# =============================================================================

def orchestrator_main():
    """Main entrypoint for Job_A (Orchestrator).
    
    This is the control plane that:
    - Loads secrets/config
    - Creates/continues sessions via DeltaSessionService
    - Generates executable artifacts into UC Volumes
    - Submits Job_B runs via Jobs API
    - Records submission metadata and telemetry
    """
    logger = _setup_logging()
    logger.info("=" * 60)
    logger.info("RLM Orchestrator (Job_A) Starting")
    logger.info("=" * 60)
    
    parser = argparse.ArgumentParser(description="RLM Agent Orchestrator (Job_A)")
    parser.add_argument(
        "--catalog",
        default=None,
        help="Unity Catalog name for session tables",
    )
    parser.add_argument(
        "--schema",
        default=None,
        help="Schema name within the catalog",
    )
    parser.add_argument(
        "--session-id",
        default=None,
        help="Session identifier",
    )
    parser.add_argument(
        "--user-id",
        default=None,
        help="User identifier",
    )
    parser.add_argument(
        "--prompt",
        default=None,
        help="Optional prompt to send to the agent",
    )
    parser.add_argument(
        "--prompt-file",
        default=None,
        help="Path to a file containing the prompt (UC Volumes, DBFS, or local). "
             "Only used if --prompt is not provided. "
             "Default: /Volumes/silo_dev_rs/task/task_txt/task.txt",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="Maximum number of orchestration iterations",
    )
    parser.add_argument(
        "--test-level",
        type=int,
        default=None,
        choices=range(1, 11),
        metavar="1-10",
        help="Load test task by difficulty level (1-10) from test_tasks.py, bypassing ingestor polling",
    )

    args = parser.parse_args()

    # Resolve parameters: CLI args override job parameters override defaults
    args.catalog = args.catalog or _get_job_parameter("ADK_DELTA_CATALOG", "silo_dev_rs")
    args.schema = args.schema or _get_job_parameter("ADK_DELTA_SCHEMA", "adk")
    args.session_id = args.session_id or _get_job_parameter("ADK_SESSION_ID", "session_001")
    args.user_id = args.user_id or _get_job_parameter("ADK_USER_ID", "job_user")
    args.prompt = args.prompt or _get_job_parameter("ADK_PROMPT", "")
    args.max_iterations = args.max_iterations or int(_get_job_parameter("ADK_MAX_ITERATIONS", "1"))

    # IMPORTANT (Databricks Jobs):
    # Job "parameters" are NOT environment variables on existing clusters.
    # Our agent graph (imported in create_runner) reads config from os.environ
    # at import/init time (e.g., JobBuilderAgent reads ADK_EXECUTOR_JOB_ID).
    # Therefore we must materialize key job parameters into env vars BEFORE
    # calling create_runner() (which imports agent.py and constructs sub-agents).
    os.environ["ADK_DELTA_CATALOG"] = args.catalog
    os.environ["ADK_DELTA_SCHEMA"] = args.schema
    os.environ["ADK_SECRET_SCOPE"] = _get_job_parameter(
        "ADK_SECRET_SCOPE",
        os.environ.get("ADK_SECRET_SCOPE", "adk-secrets"),
    )
    os.environ["ADK_ARTIFACTS_PATH"] = _get_job_parameter(
        "ADK_ARTIFACTS_PATH",
        os.environ.get("ADK_ARTIFACTS_PATH", "/Volumes/silo_dev_rs/adk/artifacts"),
    )
    # Model selection/config (used at import time by agent/model factory)
    os.environ["ADK_MODEL_PROVIDER"] = _get_job_parameter(
        "ADK_MODEL_PROVIDER",
        os.environ.get("ADK_MODEL_PROVIDER", "gemini"),
    )
    os.environ["ADK_GEMINI_MODEL"] = _get_job_parameter(
        "ADK_GEMINI_MODEL",
        os.environ.get("ADK_GEMINI_MODEL", "gemini-3-pro-preview"),
    )
    os.environ["ADK_LITELLM_MODEL"] = _get_job_parameter(
        "ADK_LITELLM_MODEL",
        os.environ.get("ADK_LITELLM_MODEL", "openai/gpt-4o"),
    )
    os.environ["ADK_LITELLM_FALLBACK_MODELS"] = _get_job_parameter(
        "ADK_LITELLM_FALLBACK_MODELS",
        os.environ.get("ADK_LITELLM_FALLBACK_MODELS", ""),
    )
    os.environ["ADK_FALLBACK_ON_BLOCKED"] = _get_job_parameter(
        "ADK_FALLBACK_ON_BLOCKED",
        os.environ.get("ADK_FALLBACK_ON_BLOCKED", "true"),
    )
    os.environ["ADK_FALLBACK_GEMINI_TO_LITELLM"] = _get_job_parameter(
        "ADK_FALLBACK_GEMINI_TO_LITELLM",
        os.environ.get("ADK_FALLBACK_GEMINI_TO_LITELLM", "true"),
    )
    executor_job_id_param = _get_job_parameter("ADK_EXECUTOR_JOB_ID", "")
    if executor_job_id_param:
        os.environ["ADK_EXECUTOR_JOB_ID"] = executor_job_id_param

    # Resolve prompt file: CLI arg -> job parameter -> default path
    args.prompt_file = args.prompt_file or _get_job_parameter(
        "ADK_PROMPT_FILE", "/Volumes/silo_dev_rs/task/task_txt/task.txt"
    )

    # Precedence: literal prompt wins; only read from file if prompt is empty
    if not args.prompt and args.prompt_file:
        try:
            args.prompt = _read_prompt_file(args.prompt_file)
            logger.info(f"Loaded prompt from file: {args.prompt_file}")
        except FileNotFoundError:
            logger.warning(f"Prompt file not found: {args.prompt_file} (continuing without prompt)")
        except IOError as e:
            logger.error(f"Failed to read prompt file: {e}")
            sys.exit(1)

    # Load test task if --test-level is specified (bypasses ingestor polling)
    # Also check for TEST_LEVEL job parameter if CLI arg not provided
    if args.test_level is None:
        test_level_str = _get_job_parameter("TEST_LEVEL", "")
        if test_level_str:
            try:
                args.test_level = int(test_level_str)
                logger.info(f"Using TEST_LEVEL from job parameter: {args.test_level}")
            except ValueError:
                logger.warning(f"Invalid TEST_LEVEL job parameter: {test_level_str}")

    if args.test_level is not None:
        try:
            from .test_tasks import get_task_prompt
            test_prompt = get_task_prompt(args.test_level)
            if test_prompt:
                args.prompt = test_prompt
                # Use timestamp-based session ID to avoid stale session data
                import time
                ts = int(time.time())
                args.session_id = f"test_level_{args.test_level}_{ts}"
                logger.info(f"Loaded test task level {args.test_level} (bypassing ingestor)")
            else:
                logger.error(f"No test task found for level {args.test_level}")
                sys.exit(1)
        except ImportError as e:
            logger.error(f"Could not import test_tasks module: {e}")
            sys.exit(1)
    
    logger.info(f"Configuration:")
    logger.info(f"  Catalog: {args.catalog}")
    logger.info(f"  Schema: {args.schema}")
    logger.info(f"  Session ID: {args.session_id}")
    logger.info(f"  User ID: {args.user_id}")
    logger.info(f"  Prompt: {args.prompt[:100] if args.prompt else '(empty)'}...")
    logger.info(f"  Prompt File: {args.prompt_file}")
    logger.info(f"  Max Iterations: {args.max_iterations}")
    
    # Run the orchestrator
    try:
        exit_code = asyncio.run(_run_orchestrator(args, logger))
        # Only call sys.exit for failures - calling sys.exit(0) in Databricks IPython
        # context triggers a SystemExit that gets caught and reported as a failure
        if exit_code != 0:
            sys.exit(exit_code)
    except Exception as e:
        logger.exception(f"Orchestrator failed: {e}")
        sys.exit(1)


async def _run_orchestrator(args, logger) -> int:
    """Run the orchestrator logic via ADK LoopAgent.
    
    This delegates all orchestration to the ADK LoopAgent which handles:
    - databricks_analyst: Generates code, calls delegate_code_results()
    - job_builder: Submits Job_B, waits for completion, writes results to state
    - results_processor_agent: Analyzes execution results with injected context
    
    The LoopAgent iterates these sub-agents until exit_loop is called or
    max_iterations is reached (configurable via ADK_MAX_ITERATIONS env var).
    
    Args:
        args: Parsed command-line arguments.
        logger: Logger instance.
        
    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    from .run import create_runner, run_conversation, ConversationResult, CATALOG, SCHEMA, APP_NAME
    from .telemetry import ensure_telemetry_table, append_telemetry_event
    
    spark = _get_spark_session()
    
    # Use args or fall back to module defaults
    catalog = args.catalog or CATALOG
    schema = args.schema or SCHEMA
    
    # Set ADK_MAX_ITERATIONS env var for LoopAgent configuration
    # This allows the agent.py LoopAgent to read the configured max_iterations
    os.environ["ADK_MAX_ITERATIONS"] = str(args.max_iterations)
    
    # Ensure telemetry table exists
    logger.info("Ensuring telemetry table exists...")
    ensure_telemetry_table(spark, catalog, schema)
    
    # Record orchestrator start
    append_telemetry_event(
        spark=spark,
        catalog=catalog,
        schema=schema,
        event_type="orchestrator_start",
        component="orchestrator",
        run_id=args.session_id,
        iteration=0,
        metadata={"user_id": args.user_id, "max_iterations": args.max_iterations},
    )
    
    # Create runner and session service
    logger.info("Creating ADK Runner with DeltaSessionService...")
    runner, session_service = await create_runner(
        spark=spark,
        catalog=catalog,
        schema=schema,
    )
    
    # Create or resume session
    try:
        session = await session_service.create_session(
            app_name=APP_NAME,
            user_id=args.user_id,
            session_id=args.session_id,
        )
        logger.info(f"Created new session: {session.id}")
    except ValueError as e:
        if "already exists" in str(e):
            session = await session_service.get_session(
                app_name=APP_NAME,
                user_id=args.user_id,
                session_id=args.session_id,
            )
            logger.info(f"Resumed existing session: {session.id} with {len(session.events)} events")
        else:
            raise
    
    # Check executor job ID for logging purposes
    executor_job_id = os.environ.get("ADK_EXECUTOR_JOB_ID")
    if not executor_job_id:
        secret_scope = os.environ.get("ADK_SECRET_SCOPE", "adk-secrets")
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            executor_job_id = dbutils.secrets.get(scope=secret_scope, key="rlm-executor-job-id")
            if executor_job_id:
                logger.info(f"Loaded executor job ID from secret scope '{secret_scope}'")
                # Set env var for JobBuilderAgent to use
                os.environ["ADK_EXECUTOR_JOB_ID"] = executor_job_id
        except Exception as e:
            logger.debug(f"Could not read executor job ID from secrets: {e}")
    
    if executor_job_id:
        logger.info(f"Executor job ID configured: {executor_job_id}")
    else:
        logger.warning(
            "No ADK_EXECUTOR_JOB_ID configured - JobBuilderAgent will fail if code "
            "execution is requested. Set ADK_EXECUTOR_JOB_ID or store 'rlm-executor-job-id' "
            "in the secret scope."
        )
    
    final_status = "success"
    
    # Run the agent conversation - ADK LoopAgent handles all iteration
    # The LoopAgent will iterate: databricks_analyst -> job_builder -> results_processor
    # until exit_loop is called or max_iterations is reached
    if not args.prompt:
        logger.info("No prompt provided - skipping agent conversation")
    else:
        logger.info(f"Running agent with prompt: {args.prompt[:100]}...")
        try:
            result = await run_conversation(
                runner=runner,
                session_service=session_service,
                user_id=args.user_id,
                session_id=args.session_id,
                prompt=args.prompt,
            )
            response = result.response_text
            logger.info(f"Agent response: {response[:500] if response else '(empty)'}...")
            logger.info(f"Conversation status: {result.status}, delegations: {result.delegation_count}")

            # Use ConversationResult.status directly - no need to re-fetch session
            # The temp:rlm:* keys are not persisted by DeltaSessionService, so the
            # old pattern of re-fetching session and checking temp:* was broken
            if result.status == "fatal_error":
                fatal_msg = result.fatal_error_msg or "Unknown"
                logger.error(f"Workflow terminated with fatal error: {fatal_msg}")
                final_status = "fatal_error"
                append_telemetry_event(
                    spark=spark,
                    catalog=catalog,
                    schema=schema,
                    event_type="fatal_error",
                    component="orchestrator",
                    run_id=args.session_id,
                    iteration=1,
                    metadata={
                        "fatal_error_msg": fatal_msg,
                        "response_len": len(response) if response else 0,
                        "delegation_count": result.delegation_count,
                    },
                )
            elif result.status == "exit_loop":
                # Normal completion via exit_loop tool
                logger.info(f"Workflow completed via exit_loop after {result.delegation_count} delegation(s)")
                append_telemetry_event(
                    spark=spark,
                    catalog=catalog,
                    schema=schema,
                    event_type="conversation_complete",
                    component="orchestrator",
                    run_id=args.session_id,
                    iteration=1,
                    metadata={
                        "prompt_len": len(args.prompt),
                        "response_len": len(response) if response else 0,
                        "exit_reason": "exit_loop",
                        "delegation_count": result.delegation_count,
                    },
                )
            else:
                # Success without explicit exit_loop (max_iterations reached or simple response)
                append_telemetry_event(
                    spark=spark,
                    catalog=catalog,
                    schema=schema,
                    event_type="conversation_complete",
                    component="orchestrator",
                    run_id=args.session_id,
                    iteration=1,
                    metadata={
                        "prompt_len": len(args.prompt),
                        "response_len": len(response) if response else 0,
                        "delegation_count": result.delegation_count,
                    },
                )
        except Exception as e:
            logger.error(f"Agent conversation failed: {e}")
            final_status = "agent_error"
            append_telemetry_event(
                spark=spark,
                catalog=catalog,
                schema=schema,
                event_type="agent_error",
                component="orchestrator",
                run_id=args.session_id,
                iteration=1,
                metadata={"error": str(e)},
            )
    
    # Record orchestrator completion
    append_telemetry_event(
        spark=spark,
        catalog=catalog,
        schema=schema,
        event_type="orchestrator_complete",
        component="orchestrator",
        run_id=args.session_id,
        iteration=1,
        metadata={"status": final_status},
    )
    
    # Close session service
    await session_service.close()
    
    logger.info("=" * 60)
    logger.info("RLM Orchestrator (Job_A) Complete")
    logger.info("=" * 60)
    
    return 0 if final_status == "success" else 1


# =============================================================================
# Job_B: Executor Entrypoint
# =============================================================================

def executor_main():
    """Main entrypoint for Job_B (Executor).
    
    This is the execution plane that:
    - Reads parameters (artifact path, run_id, iteration)
    - Executes the generated artifact from UC Volumes
    - Writes result.json to Volumes
    - Appends telemetry row to UC Delta table
    """
    logger = _setup_logging()
    logger.info("=" * 60)
    logger.info("RLM Executor (Job_B) Starting")
    logger.info("=" * 60)
    
    parser = argparse.ArgumentParser(description="RLM Agent Executor (Job_B)")
    parser.add_argument(
        "--artifact-path",
        default=os.environ.get("ARTIFACT_PATH", ""),
        help="Path to the artifact to execute (in UC Volumes)",
    )
    parser.add_argument(
        "--run-id",
        default=os.environ.get("RUN_ID", ""),
        help="Run identifier from orchestrator",
    )
    parser.add_argument(
        "--iteration",
        type=int,
        default=int(os.environ.get("ITERATION", "0")),
        help="Iteration number",
    )
    parser.add_argument(
        "--catalog",
        default=os.environ.get("ADK_DELTA_CATALOG", "silo_dev_rs"),
        help="Unity Catalog name",
    )
    parser.add_argument(
        "--schema",
        default=os.environ.get("ADK_DELTA_SCHEMA", "adk"),
        help="Schema name",
    )
    
    args = parser.parse_args()
    
    logger.info(f"Configuration:")
    logger.info(f"  Artifact Path: {args.artifact_path}")
    logger.info(f"  Run ID: {args.run_id}")
    logger.info(f"  Iteration: {args.iteration}")
    logger.info(f"  Catalog: {args.catalog}")
    logger.info(f"  Schema: {args.schema}")
    
    # Run the executor
    try:
        exit_code = _run_executor(args, logger)
        # Only call sys.exit for failures - calling sys.exit(0) in Databricks IPython
        # context triggers a SystemExit that gets caught and reported as a failure
        if exit_code != 0:
            sys.exit(exit_code)
    except Exception as e:
        logger.exception(f"Executor failed: {e}")
        sys.exit(1)


def _run_executor(args, logger) -> int:
    """Run the executor logic.
    
    Args:
        args: Parsed command-line arguments.
        logger: Logger instance.
        
    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    from .executor import execute_artifact
    from .telemetry import ensure_telemetry_table, append_telemetry_event
    
    spark = _get_spark_session()
    
    # Ensure telemetry table exists
    logger.info("Ensuring telemetry table exists...")
    ensure_telemetry_table(spark, args.catalog, args.schema)
    
    # Record executor start
    append_telemetry_event(
        spark=spark,
        catalog=args.catalog,
        schema=args.schema,
        event_type="executor_start",
        component="executor",
        run_id=args.run_id,
        iteration=args.iteration,
        metadata={"artifact_path": args.artifact_path},
    )
    
    # Execute the artifact
    if args.artifact_path:
        logger.info(f"Executing artifact: {args.artifact_path}")
        result = execute_artifact(
            spark=spark,
            artifact_path=args.artifact_path,
            run_id=args.run_id,
            iteration=args.iteration,
            catalog=args.catalog,
            schema=args.schema,
        )
        
        status = "success" if result.get("status") == "success" else "failed"
        logger.info(f"Execution result: {status}")
        
        # Record execution result
        append_telemetry_event(
            spark=spark,
            catalog=args.catalog,
            schema=args.schema,
            event_type="executor_complete",
            component="executor",
            run_id=args.run_id,
            iteration=args.iteration,
            metadata=result,
        )
    else:
        logger.warning("No artifact path provided - nothing to execute")
        append_telemetry_event(
            spark=spark,
            catalog=args.catalog,
            schema=args.schema,
            event_type="executor_skip",
            component="executor",
            run_id=args.run_id,
            iteration=args.iteration,
            metadata={"reason": "no_artifact_path"},
        )
    
    logger.info("=" * 60)
    logger.info("RLM Executor (Job_B) Complete")
    logger.info("=" * 60)

    return 0


# =============================================================================
# Job_C: Ingestor Entrypoint
# =============================================================================

def ingestor_main():
    """Main entrypoint for Job_C (Ingestor).

    This is the CDF polling plane that:
    - Polls a trigger table using Change Data Feed (CDF)
    - Detects new tasks assigned to the RLM agent
    - Tracks watermark in a state table
    - Triggers Job_A (orchestrator) via Jobs API for each new task
    - Records telemetry events
    """
    logger = _setup_logging()
    logger.info("=" * 60)
    logger.info("RLM Ingestor (Job_C) Starting")
    logger.info("=" * 60)

    parser = argparse.ArgumentParser(description="RLM Agent Ingestor (Job_C)")
    parser.add_argument(
        "--catalog",
        default=os.environ.get("ADK_DELTA_CATALOG", "silo_dev_rs"),
        help="Unity Catalog name for state/telemetry tables",
    )
    parser.add_argument(
        "--schema",
        default=os.environ.get("ADK_DELTA_SCHEMA", "adk"),
        help="Schema name within the catalog",
    )
    parser.add_argument(
        "--trigger-table",
        default=os.environ.get("ADK_TRIGGER_TABLE", "silo_dev_rs.task.jira_raw_data"),
        help="Fully qualified name of the trigger table to poll",
    )
    parser.add_argument(
        "--assignee-filter",
        default=os.environ.get("ADK_ASSIGNEE_FILTER", "databricks-rlm-agent"),
        help="Assignee value to filter for",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=os.environ.get("ADK_DRY_RUN", "").lower() == "true",
        help="Poll and log but don't trigger jobs or update state",
    )

    args = parser.parse_args()

    logger.info(f"Configuration:")
    logger.info(f"  Catalog: {args.catalog}")
    logger.info(f"  Schema: {args.schema}")
    logger.info(f"  Trigger Table: {args.trigger_table}")
    logger.info(f"  Assignee Filter: {args.assignee_filter}")
    logger.info(f"  Dry Run: {args.dry_run}")

    # Run the ingestor
    try:
        exit_code = asyncio.run(_run_ingestor(args, logger))
        # Only call sys.exit for failures - calling sys.exit(0) in Databricks IPython
        # context triggers a SystemExit that gets caught and reported as a failure
        if exit_code != 0:
            sys.exit(exit_code)
    except Exception as e:
        logger.exception(f"Ingestor failed: {e}")
        sys.exit(1)


async def _run_ingestor(args, logger) -> int:
    """Run the ingestor logic.

    Args:
        args: Parsed command-line arguments.
        logger: Logger instance.

    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    from .ingestor import IngestorService, get_orchestrator_job_id
    from .telemetry import ensure_telemetry_table, append_telemetry_event

    spark = _get_spark_session()

    # Ensure telemetry table exists
    logger.info("Ensuring telemetry table exists...")
    ensure_telemetry_table(spark, args.catalog, args.schema)

    # Get orchestrator job ID from secrets or environment
    orchestrator_job_id = os.environ.get("ADK_ORCHESTRATOR_JOB_ID")

    if not orchestrator_job_id:
        # Try to read from secret scope
        secret_scope = os.environ.get("ADK_SECRET_SCOPE", "adk-secrets")
        orchestrator_job_id = get_orchestrator_job_id(spark, secret_scope)

    if orchestrator_job_id:
        orchestrator_job_id = int(orchestrator_job_id)
        logger.info(f"Orchestrator job ID: {orchestrator_job_id}")
    else:
        logger.warning("No orchestrator job ID configured - will poll but not trigger")

    # Record ingestor start
    run_id = f"ingest_{args.trigger_table.replace('.', '_')}"
    append_telemetry_event(
        spark=spark,
        catalog=args.catalog,
        schema=args.schema,
        event_type="ingestor_start",
        component="ingestor",
        run_id=run_id,
        iteration=0,
        metadata={
            "trigger_table": args.trigger_table,
            "assignee_filter": args.assignee_filter,
            "dry_run": args.dry_run,
        },
    )

    # Create and run ingestor service
    service = IngestorService(
        spark=spark,
        catalog=args.catalog,
        schema=args.schema,
        trigger_table=args.trigger_table,
        assignee_filter=args.assignee_filter,
        orchestrator_job_id=orchestrator_job_id,
    )

    # Run poll cycle
    logger.info("Running poll cycle...")
    result = service.run_poll_cycle(dry_run=args.dry_run)

    logger.info(f"Poll cycle complete:")
    logger.info(f"  Tasks Found: {result['tasks_found']}")
    logger.info(f"  Tasks Triggered: {result['tasks_triggered']}")
    logger.info(f"  Status: {result['status']}")

    if result['triggered_runs']:
        logger.info("Triggered runs:")
        for run in result['triggered_runs']:
            logger.info(f"  - {run['issue_key']}: {run['run_url']}")

    if result['errors']:
        logger.warning(f"Errors encountered: {len(result['errors'])}")
        for error in result['errors']:
            logger.warning(f"  - {error}")

    # Record poll event
    append_telemetry_event(
        spark=spark,
        catalog=args.catalog,
        schema=args.schema,
        event_type="ingestor_poll",
        component="ingestor",
        run_id=run_id,
        iteration=1,
        metadata={
            "last_version": result.get("last_version"),
            "current_version": result.get("current_version"),
            "tasks_found": result["tasks_found"],
            "tasks_triggered": result["tasks_triggered"],
        },
    )

    # Record completion
    append_telemetry_event(
        spark=spark,
        catalog=args.catalog,
        schema=args.schema,
        event_type="ingestor_complete",
        component="ingestor",
        run_id=run_id,
        iteration=1,
        metadata={
            "status": result["status"],
            "errors": len(result["errors"]),
        },
    )

    logger.info("=" * 60)
    logger.info("RLM Ingestor (Job_C) Complete")
    logger.info("=" * 60)

    # Return non-zero if there were errors
    return 0 if result["status"] == "success" else 1


# =============================================================================
# Test Runner Entrypoint
# =============================================================================

def test_main():
    """Main entrypoint for running test tasks directly.

    This is a convenience wrapper around the orchestrator that:
    - Requires a --level argument (1-10)
    - Bypasses the ingestor polling mechanism
    - Provides sensible defaults for test runs

    Usage:
        rlm-test --level 3
        rlm-test --level 5 --max-iterations 5
    """
    logger = _setup_logging()
    logger.info("=" * 60)
    logger.info("RLM Test Runner")
    logger.info("=" * 60)

    parser = argparse.ArgumentParser(
        description="RLM Agent Test Runner - run test tasks directly",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Test Levels:
  1-2:  Simple queries, minimal joins
  3-4:  Aggregations, basic joins, filtering
  5-6:  Multi-table analysis, metadata exploration
  7-8:  Cross-schema investigation, data quality analysis
  9-10: Complex workflow analysis, iterative exploration

Examples:
  rlm-test --level 1                    # Run simple vendor count
  rlm-test --level 5 --max-iterations 5 # Run profiling task with retries
  rlm-test --list                       # List all available test tasks
"""
    )
    parser.add_argument(
        "--level",
        type=int,
        choices=range(1, 11),
        metavar="1-10",
        help="Test task difficulty level (required unless --list)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all available test tasks and exit",
    )
    parser.add_argument(
        "--catalog",
        default=os.environ.get("ADK_DELTA_CATALOG", "silo_dev_rs"),
        help="Unity Catalog name for session tables",
    )
    parser.add_argument(
        "--schema",
        default=os.environ.get("ADK_DELTA_SCHEMA", "adk"),
        help="Schema name within the catalog",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=3,
        help="Maximum number of orchestration iterations (default: 3)",
    )
    parser.add_argument(
        "--session-id",
        default=None,
        help="Session identifier (auto-generated if not specified)",
    )

    args = parser.parse_args()

    # Handle --list
    if args.list:
        from .test_tasks import list_tasks
        print("\nAvailable Test Tasks:")
        print("=" * 70)
        for level, issue_key, summary in list_tasks():
            print(f"  Level {level:2d}: [{issue_key}] {summary}")
        print("=" * 70)
        return

    # Require --level if not --list
    if args.level is None:
        parser.error("--level is required (use --list to see available tasks)")

    # Generate session ID if not provided
    if args.session_id is None:
        import time
        args.session_id = f"test_L{args.level}_{int(time.time())}"

    logger.info(f"Running test level {args.level}")
    logger.info(f"Session ID: {args.session_id}")
    logger.info(f"Max iterations: {args.max_iterations}")

    # Delegate to orchestrator with --test-level
    sys.argv = [
        "rlm-orchestrator",
        "--test-level", str(args.level),
        "--catalog", args.catalog,
        "--schema", args.schema,
        "--session-id", args.session_id,
        "--max-iterations", str(args.max_iterations),
    ]

    orchestrator_main()


if __name__ == "__main__":
    # Default to orchestrator if run directly
    orchestrator_main()

