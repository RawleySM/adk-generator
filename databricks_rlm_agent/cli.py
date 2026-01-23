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
        "--max-iterations",
        type=int,
        default=None,
        help="Maximum number of orchestration iterations",
    )

    args = parser.parse_args()

    # Resolve parameters: CLI args override job parameters override defaults
    args.catalog = args.catalog or _get_job_parameter("ADK_DELTA_CATALOG", "silo_dev_rs")
    args.schema = args.schema or _get_job_parameter("ADK_DELTA_SCHEMA", "adk")
    args.session_id = args.session_id or _get_job_parameter("ADK_SESSION_ID", "session_001")
    args.user_id = args.user_id or _get_job_parameter("ADK_USER_ID", "job_user")
    args.prompt = args.prompt or _get_job_parameter("ADK_PROMPT", "")
    args.max_iterations = args.max_iterations or int(_get_job_parameter("ADK_MAX_ITERATIONS", "1"))
    
    logger.info(f"Configuration:")
    logger.info(f"  Catalog: {args.catalog}")
    logger.info(f"  Schema: {args.schema}")
    logger.info(f"  Session ID: {args.session_id}")
    logger.info(f"  User ID: {args.user_id}")
    logger.info(f"  Prompt: {args.prompt[:100] if args.prompt else '(empty)'}...")
    logger.info(f"  Max Iterations: {args.max_iterations}")
    
    # Run the orchestrator
    try:
        exit_code = asyncio.run(_run_orchestrator(args, logger))
        sys.exit(exit_code)
    except Exception as e:
        logger.exception(f"Orchestrator failed: {e}")
        sys.exit(1)


async def _run_orchestrator(args, logger) -> int:
    """Run the orchestrator logic with RLM loop.
    
    This implements the full orchestration pattern:
    1. Agent generates code artifact
    2. Submit Job_B (executor) to run the artifact
    3. Wait for Job_B completion
    4. Load result JSON with stdout/stderr
    5. Feed results back to agent for next iteration
    
    Args:
        args: Parsed command-line arguments.
        logger: Logger instance.
        
    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    from .run import create_runner, run_conversation, CATALOG, SCHEMA, APP_NAME
    from .telemetry import ensure_telemetry_table, append_telemetry_event
    from .executor import find_result_json, load_result_json
    from .jobs_api import submit_and_wait
    from .prompts import format_execution_feedback
    from .agent import AGENT_CODE_PATH
    
    spark = _get_spark_session()
    
    # Use args or fall back to module defaults
    catalog = args.catalog or CATALOG
    schema = args.schema or SCHEMA
    
    # Get artifacts path for result files
    artifacts_path = os.environ.get("ADK_ARTIFACTS_PATH", "/Volumes/silo_dev_rs/adk/artifacts")
    
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
    
    # Get executor job ID (needed for RLM loop)
    executor_job_id = os.environ.get("ADK_EXECUTOR_JOB_ID")
    if not executor_job_id:
        secret_scope = os.environ.get("ADK_SECRET_SCOPE", "adk-secrets")
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            executor_job_id = dbutils.secrets.get(scope=secret_scope, key="rlm-executor-job-id")
            if executor_job_id:
                logger.info(f"Loaded executor job ID from secret scope '{secret_scope}'")
        except Exception as e:
            logger.debug(f"Could not read executor job ID from secrets: {e}")
            executor_job_id = None
    
    if executor_job_id:
        executor_job_id = int(executor_job_id)
        logger.info(f"Executor job ID configured: {executor_job_id}")
    else:
        logger.info("No ADK_EXECUTOR_JOB_ID configured - will run agent without execution loop")
    
    # Store original prompt for feedback context
    original_prompt = args.prompt
    current_prompt = args.prompt
    final_status = "success"
    
    # RLM Orchestration Loop
    for iteration in range(1, args.max_iterations + 1):
        logger.info("=" * 40)
        logger.info(f"RLM Iteration {iteration}/{args.max_iterations}")
        logger.info("=" * 40)
        
        if not current_prompt:
            logger.info("No prompt provided - skipping iteration")
            break
        
        # Step 1: Run agent conversation to generate artifact
        logger.info(f"Running agent with prompt: {current_prompt[:100]}...")
        response = await run_conversation(
            runner=runner,
            session_service=session_service,
            user_id=args.user_id,
            session_id=args.session_id,
            prompt=current_prompt,
        )
        logger.info(f"Agent response: {response[:200]}...")
        
        # Record conversation completion
        append_telemetry_event(
            spark=spark,
            catalog=catalog,
            schema=schema,
            event_type="conversation_complete",
            component="orchestrator",
            run_id=args.session_id,
            iteration=iteration,
            metadata={"prompt_len": len(current_prompt), "response_len": len(response)},
        )
        
        # Step 2: Check if artifact was generated
        if not os.path.exists(AGENT_CODE_PATH):
            logger.info(f"No artifact at {AGENT_CODE_PATH} - agent may not have generated code")
            # No artifact means no execution needed; break loop
            break
        
        # Step 3: Submit Job_B (executor) if configured
        if not executor_job_id:
            logger.info("No executor job configured - cannot execute artifact")
            break
        
        logger.info(f"Submitting executor job for artifact: {AGENT_CODE_PATH}")
        append_telemetry_event(
            spark=spark,
            catalog=catalog,
            schema=schema,
            event_type="executor_submit",
            component="orchestrator",
            run_id=args.session_id,
            iteration=iteration,
            metadata={"artifact_path": AGENT_CODE_PATH, "executor_job_id": executor_job_id},
        )
        
        try:
            # Submit and wait for executor to complete
            exec_result = submit_and_wait(
                executor_job_id=executor_job_id,
                artifact_path=AGENT_CODE_PATH,
                run_id=args.session_id,
                iteration=iteration,
                timeout_minutes=int(os.environ.get("ADK_EXECUTOR_TIMEOUT_MINUTES", "60")),
                catalog=catalog,
                schema=schema,
            )
            
            logger.info(f"Executor completed: success={exec_result.get('success')}")
            logger.info(f"Run URL: {exec_result.get('run_url')}")
            
            # Record executor completion
            append_telemetry_event(
                spark=spark,
                catalog=catalog,
                schema=schema,
                event_type="executor_complete",
                component="orchestrator",
                run_id=args.session_id,
                iteration=iteration,
                metadata={
                    "databricks_run_id": exec_result.get("databricks_run_id"),
                    "success": exec_result.get("success"),
                    "life_cycle_state": exec_result.get("life_cycle_state"),
                    "result_state": exec_result.get("result_state"),
                },
            )
            
        except Exception as e:
            logger.error(f"Executor submission failed: {e}")
            append_telemetry_event(
                spark=spark,
                catalog=catalog,
                schema=schema,
                event_type="executor_error",
                component="orchestrator",
                run_id=args.session_id,
                iteration=iteration,
                metadata={"error": str(e)},
            )
            final_status = "executor_error"
            break
        
        # Step 4: Load result JSON from executor
        result_path = find_result_json(
            artifacts_path=artifacts_path,
            run_id=args.session_id,
            iteration=iteration,
        )
        
        if result_path:
            result_data = load_result_json(result_path)
            logger.info(f"Loaded result from: {result_path}")
        else:
            # Fallback: use data from the Jobs API response
            logger.warning(f"Result JSON not found at expected path, using Jobs API output")
            result_data = {
                "status": "success" if exec_result.get("success") else "failed",
                "output": exec_result.get("logs"),
                "error": exec_result.get("error"),
                "error_trace": exec_result.get("error_trace"),
                "duration_seconds": 0,
            }
        
        # Step 5: Check if we should continue iterating
        if iteration >= args.max_iterations:
            logger.info(f"Reached max iterations ({args.max_iterations})")
            break
        
        # Execution succeeded and no more work needed
        if result_data and result_data.get("status") == "success" and not result_data.get("error"):
            logger.info("Execution succeeded - task complete")
            break
        
        # Step 6: Format feedback prompt for next iteration
        logger.info("Preparing feedback prompt for next iteration...")
        current_prompt = format_execution_feedback(
            status=result_data.get("status", "unknown") if result_data else "unknown",
            duration_seconds=result_data.get("duration_seconds", 0) if result_data else 0,
            original_prompt=original_prompt,
            stdout=result_data.get("stdout") if result_data else exec_result.get("logs"),
            stderr=result_data.get("stderr") if result_data else None,
            error=result_data.get("error") if result_data else exec_result.get("error"),
            error_trace=result_data.get("error_trace") if result_data else exec_result.get("error_trace"),
        )
        
        logger.info("Continuing to next iteration with execution feedback...")
    
    # Record orchestrator completion
    append_telemetry_event(
        spark=spark,
        catalog=catalog,
        schema=schema,
        event_type="orchestrator_complete",
        component="orchestrator",
        run_id=args.session_id,
        iteration=args.max_iterations,
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


if __name__ == "__main__":
    # Default to orchestrator if run directly
    orchestrator_main()

