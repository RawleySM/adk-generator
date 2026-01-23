"""Jobs API client for submitting executor runs from the orchestrator.

This module provides functions for Job_A (orchestrator) to submit Job_B (executor)
runs via the Databricks Jobs API.

Authentication Strategy:
1. Try Databricks-native auth first (run identity / default credentials)
2. Fall back to PAT-based auth if environment variables are set

Usage:
    from databricks_rlm_agent.jobs_api import submit_executor_run, poll_run_status
    
    run_id, run_url = submit_executor_run(
        executor_job_id=12345,
        artifact_path="/Volumes/catalog/schema/artifacts/entrypoint.py",
        run_id="run_001",
        iteration=1,
    )
    
    status = poll_run_status(run_id, timeout_minutes=30)
"""

import logging
import os
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Terminal states for job runs
TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
SUCCESS_STATES = {"SUCCESS"}


def _get_workspace_client():
    """Get a Databricks WorkspaceClient using available auth.
    
    Tries:
    1. Default credentials (Databricks-native auth / run identity)
    2. PAT-based auth via environment variables
    
    Returns:
        WorkspaceClient instance.
        
    Raises:
        ImportError: If databricks-sdk is not installed.
        Exception: If authentication fails.
    """
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as e:
        raise ImportError(
            "databricks-sdk is required for Jobs API calls. "
            "Install with: pip install databricks-sdk"
        ) from e
    
    # Try default auth first (run identity / profile / env vars)
    try:
        client = WorkspaceClient()
        # Verify auth by making a simple API call
        client.current_user.me()
        logger.info("Using Databricks-native authentication (run identity / default)")
        return client
    except Exception as e:
        logger.debug(f"Default auth failed: {e}")
    
    # Fall back to explicit PAT auth
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    
    if host and token:
        logger.info(f"Using PAT-based authentication for host: {host}")
        return WorkspaceClient(host=host, token=token)
    
    # Re-raise the original error if no fallback available
    raise RuntimeError(
        "Could not authenticate to Databricks. Ensure either:\n"
        "  1. Running in Databricks with proper run identity, or\n"
        "  2. DATABRICKS_HOST and DATABRICKS_TOKEN env vars are set"
    )


def submit_executor_run(
    executor_job_id: int,
    artifact_path: str,
    run_id: str,
    iteration: int,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    additional_params: Optional[dict[str, str]] = None,
) -> tuple[int, str]:
    """Submit a Job_B (executor) run via the Jobs API.
    
    Args:
        executor_job_id: The Databricks job ID for the executor job.
        artifact_path: Path to the artifact in UC Volumes to execute.
        run_id: The orchestrator's run identifier.
        iteration: The current iteration number.
        catalog: Optional Unity Catalog name (defaults to env var).
        schema: Optional schema name (defaults to env var).
        additional_params: Additional job parameters to pass.
        
    Returns:
        Tuple of (databricks_run_id, run_url).
        
    Raises:
        Exception: If job submission fails.
    """
    client = _get_workspace_client()
    
    # Build job parameters
    job_params = {
        "ARTIFACT_PATH": artifact_path,
        "RUN_ID": run_id,
        "ITERATION": str(iteration),
    }
    
    if catalog:
        job_params["ADK_DELTA_CATALOG"] = catalog
    if schema:
        job_params["ADK_DELTA_SCHEMA"] = schema
    
    if additional_params:
        job_params.update(additional_params)
    
    logger.info(f"Submitting executor job {executor_job_id} with params: {job_params}")
    
    # Submit the job run
    response = client.jobs.run_now(
        job_id=executor_job_id,
        job_parameters=job_params,
    )
    
    databricks_run_id = response.run_id
    
    # Construct run URL
    host = client.config.host.rstrip("/")
    run_url = f"{host}/#job/{executor_job_id}/run/{databricks_run_id}"
    
    logger.info(f"Executor job submitted: run_id={databricks_run_id}")
    logger.info(f"Run URL: {run_url}")
    
    return databricks_run_id, run_url


def poll_run_status(
    databricks_run_id: int,
    timeout_minutes: int = 60,
    poll_interval_seconds: int = 10,
) -> dict[str, Any]:
    """Poll for a job run to complete.
    
    Args:
        databricks_run_id: The Databricks run ID to poll.
        timeout_minutes: Maximum time to wait in minutes.
        poll_interval_seconds: Time between polls in seconds.
        
    Returns:
        Dict with status information:
        - life_cycle_state: Final lifecycle state
        - result_state: Final result state (if terminal)
        - state_message: Status message
        - success: Boolean indicating success
        - timed_out: Boolean indicating timeout
    """
    client = _get_workspace_client()
    
    timeout_seconds = timeout_minutes * 60
    start_time = time.time()
    
    logger.info(f"Polling run {databricks_run_id} (timeout: {timeout_minutes} min)...")
    
    while True:
        elapsed = time.time() - start_time
        
        if elapsed >= timeout_seconds:
            logger.warning(f"Timeout reached after {timeout_minutes} minutes")
            return {
                "life_cycle_state": "RUNNING",
                "result_state": None,
                "state_message": "Polling timed out",
                "success": False,
                "timed_out": True,
            }
        
        run = client.jobs.get_run(run_id=databricks_run_id)
        state = run.state
        life_cycle_state = state.life_cycle_state.value if state.life_cycle_state else "UNKNOWN"
        result_state = state.result_state.value if state.result_state else None
        
        elapsed_mins = int(elapsed // 60)
        elapsed_secs = int(elapsed % 60)
        logger.info(
            f"[{elapsed_mins:02d}:{elapsed_secs:02d}] "
            f"State: {life_cycle_state}, Result: {result_state}"
        )
        
        if life_cycle_state in TERMINAL_STATES:
            success = result_state in SUCCESS_STATES
            return {
                "life_cycle_state": life_cycle_state,
                "result_state": result_state,
                "state_message": state.state_message if state.state_message else "",
                "success": success,
                "timed_out": False,
            }
        
        time.sleep(poll_interval_seconds)


def get_run_output(databricks_run_id: int) -> dict[str, Any]:
    """Get the output from a completed job run.
    
    Args:
        databricks_run_id: The Databricks run ID.
        
    Returns:
        Dict with output information.
    """
    client = _get_workspace_client()
    
    try:
        output = client.jobs.get_run_output(run_id=databricks_run_id)
        
        result = {
            "run_id": databricks_run_id,
            "error": output.error if output.error else None,
            "error_trace": output.error_trace if output.error_trace else None,
        }
        
        if output.notebook_output:
            result["notebook_result"] = output.notebook_output.result
            result["notebook_truncated"] = output.notebook_output.truncated
        
        if output.logs:
            result["logs"] = output.logs
        
        return result
        
    except Exception as e:
        logger.warning(f"Could not get run output: {e}")
        return {
            "run_id": databricks_run_id,
            "error": str(e),
        }


def submit_and_wait(
    executor_job_id: int,
    artifact_path: str,
    run_id: str,
    iteration: int,
    timeout_minutes: int = 60,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> dict[str, Any]:
    """Submit an executor run and wait for completion.
    
    Convenience function that combines submit_executor_run and poll_run_status.
    
    Args:
        executor_job_id: The Databricks job ID for the executor job.
        artifact_path: Path to the artifact in UC Volumes to execute.
        run_id: The orchestrator's run identifier.
        iteration: The current iteration number.
        timeout_minutes: Maximum time to wait in minutes.
        catalog: Optional Unity Catalog name.
        schema: Optional schema name.
        
    Returns:
        Dict with submission and result information.
    """
    # Submit the job
    databricks_run_id, run_url = submit_executor_run(
        executor_job_id=executor_job_id,
        artifact_path=artifact_path,
        run_id=run_id,
        iteration=iteration,
        catalog=catalog,
        schema=schema,
    )
    
    # Wait for completion
    status = poll_run_status(
        databricks_run_id=databricks_run_id,
        timeout_minutes=timeout_minutes,
    )
    
    # Get output if completed
    output = {}
    if not status["timed_out"]:
        output = get_run_output(databricks_run_id)
    
    return {
        "databricks_run_id": databricks_run_id,
        "run_url": run_url,
        **status,
        **output,
    }

