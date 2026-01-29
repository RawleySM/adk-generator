"""Execution Backend Abstraction for Local vs Databricks Mode.

This module provides a Protocol-based abstraction for code execution,
allowing the JobBuilderAgent to run artifacts either locally (for development)
or via the Databricks Jobs API (for production).

Classes:
    ExecutionBackend: Protocol defining the execution interface.
    DatabricksBackend: Production backend using Jobs API.
    LocalBackend: Development backend executing artifacts directly.

Usage:
    # Production (Databricks mode)
    from databricks_rlm_agent.jobs_api import submit_and_wait
    backend = DatabricksBackend(executor_job_id=12345)

    # Development (local mode)
    backend = LocalBackend(
        db_path=".adk_local/adk.duckdb",
        artifacts_path=".adk_local/artifacts",
    )

    # In JobBuilderAgent
    result = backend.submit_and_wait(
        artifact_path="/path/to/code.py",
        run_id="session_123",
        iteration=1,
        timeout_minutes=60,
        catalog="silo_dev_rs",
        schema="adk",
    )
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Callable

logger = logging.getLogger(__name__)


def _serialize_for_queue(output: Any) -> Any:
    """Serialize output to ensure it's picklable for multiprocessing.Queue.

    This function is used inside the subprocess to prepare results
    for passing back through the queue.

    Args:
        output: The output to serialize.

    Returns:
        A JSON-serializable (and picklable) representation of the output.
    """
    if output is None:
        return None

    # Handle common types that are already picklable
    if isinstance(output, (str, int, float, bool)):
        return output

    if isinstance(output, (list, tuple)):
        return [_serialize_for_queue(item) for item in output]

    if isinstance(output, dict):
        return {str(k): _serialize_for_queue(v) for k, v in output.items()}

    # Try to convert to string for other types
    try:
        return str(output)
    except Exception:
        return f"<non-serializable: {type(output).__name__}>"


def _execute_code_in_process(
    queue: "multiprocessing.Queue",
    artifact_path: str,
    catalog: str,
    schema: str,
    run_id: str,
    iteration: int,
    inject_execute_sql: bool,
) -> None:
    """Execute artifact code in a subprocess for timeout enforcement.

    This function is designed to run in a separate process via multiprocessing.Process.
    It can be terminated if execution times out, unlike threads which cannot be
    forcibly stopped in Python.

    Results (including stdout/stderr capture) are passed back via the Queue.

    Args:
        queue: multiprocessing.Queue to send results back to parent process.
        artifact_path: Path to the Python artifact file to execute.
        catalog: Unity Catalog name for execution context.
        schema: Schema name for execution context.
        run_id: The session/run identifier.
        iteration: The iteration number.
        inject_execute_sql: Whether to inject execute_sql into the execution context.
    """
    import io
    import json
    import os
    import sys
    import traceback
    from contextlib import redirect_stdout, redirect_stderr
    from datetime import datetime
    from pathlib import Path

    exec_result = {
        "output": None,
        "error": None,
        "error_trace": None,
        "stdout": None,
        "stderr": None,
    }

    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()

    try:
        # Validate artifact path
        if not artifact_path:
            raise ValueError("No artifact path provided")

        if not os.path.exists(artifact_path):
            raise FileNotFoundError(f"Artifact not found: {artifact_path}")

        # Read the artifact
        with open(artifact_path, 'r') as f:
            code = f.read()

        # Prepare execution environment - NO SPARK, uses execute_sql instead
        exec_globals = {
            "__name__": "__main__",
            "__file__": artifact_path,
            "catalog": catalog,
            "schema": schema,
            "run_id": run_id,
            "iteration": iteration,
        }

        # Inject execute_sql for UC data queries (import in subprocess)
        if inject_execute_sql:
            try:
                from databricks_rlm_agent.sql_warehouse import execute_sql
                exec_globals["execute_sql"] = execute_sql
            except ImportError:
                pass  # sql_warehouse not available

        # Add commonly used imports to the execution environment
        exec_globals["os"] = os
        exec_globals["sys"] = sys
        exec_globals["json"] = json
        exec_globals["datetime"] = datetime
        exec_globals["Path"] = Path

        # Execute the artifact with stdout/stderr capture
        with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            exec(code, exec_globals)

        # Check for a result variable in the execution context
        output = exec_globals.get("result", exec_globals.get("output", None))

        # Serialize output to ensure it's picklable for the queue
        exec_result["output"] = _serialize_for_queue(output)

    except Exception as e:
        exec_result["error"] = str(e)
        exec_result["error_trace"] = traceback.format_exc()

    finally:
        exec_result["stdout"] = stdout_buffer.getvalue() or None
        exec_result["stderr"] = stderr_buffer.getvalue() or None

    # Send results back through the queue
    queue.put(exec_result)


class ExecutionBackend(Protocol):
    """Protocol defining the execution backend interface.

    This abstraction allows JobBuilderAgent to use either the Databricks
    Jobs API (for production) or direct local execution (for development).
    """

    def submit_and_wait(
        self,
        artifact_path: str,
        run_id: str,
        iteration: int,
        timeout_minutes: int,
        catalog: str,
        schema: str,
    ) -> dict[str, Any]:
        """Submit an artifact for execution and wait for completion.

        Args:
            artifact_path: Path to the code artifact to execute.
            run_id: The session/run identifier.
            iteration: The iteration number.
            timeout_minutes: Maximum time to wait for completion.
            catalog: Unity Catalog name for execution context.
            schema: Schema name for execution context.

        Returns:
            Dict with execution results:
            - databricks_run_id: Run identifier (int for Databricks, str for local)
            - run_url: URL to view the run (or local path)
            - success: Boolean indicating execution success
            - life_cycle_state: Execution lifecycle state
            - result_state: Final result state
            - logs: Execution logs (if available)
            - error: Error message (if failed)
            - error_trace: Full traceback (if failed)
            - timed_out: Boolean indicating timeout
        """
        ...


class DatabricksBackend:
    """Production backend that delegates to the Databricks Jobs API.

    This backend submits artifacts for execution via Job_B using
    the Databricks Jobs API and polls for completion.

    Attributes:
        executor_job_id: The Databricks job ID for the executor job.
    """

    def __init__(self, executor_job_id: int):
        """Initialize the Databricks backend.

        Args:
            executor_job_id: The Databricks job ID for the executor job.
                If None, reads from ADK_EXECUTOR_JOB_ID env var.
        """
        self._executor_job_id = executor_job_id
        if self._executor_job_id is None:
            env_job_id = os.environ.get("ADK_EXECUTOR_JOB_ID")
            if env_job_id:
                self._executor_job_id = int(env_job_id)

        if self._executor_job_id is None:
            raise ValueError(
                "executor_job_id is required for DatabricksBackend. "
                "Set via constructor or ADK_EXECUTOR_JOB_ID env var."
            )

        logger.info(f"DatabricksBackend initialized with job_id={self._executor_job_id}")

    def submit_and_wait(
        self,
        artifact_path: str,
        run_id: str,
        iteration: int,
        timeout_minutes: int,
        catalog: str,
        schema: str,
    ) -> dict[str, Any]:
        """Submit artifact via Jobs API and wait for completion.

        Delegates to jobs_api.submit_and_wait() which handles:
        - Job submission via jobs.run_now()
        - Polling for completion
        - Fetching run output

        Args:
            artifact_path: Path to the code artifact in UC Volumes.
            run_id: The session/run identifier.
            iteration: The iteration number.
            timeout_minutes: Maximum time to wait for completion.
            catalog: Unity Catalog name.
            schema: Schema name.

        Returns:
            Dict with execution results from jobs_api.submit_and_wait().
        """
        from databricks_rlm_agent.jobs_api import submit_and_wait

        logger.info(
            f"[DatabricksBackend] Submitting job for artifact: {artifact_path}, "
            f"run_id={run_id}, iteration={iteration}"
        )

        return submit_and_wait(
            executor_job_id=self._executor_job_id,
            artifact_path=artifact_path,
            run_id=run_id,
            iteration=iteration,
            timeout_minutes=timeout_minutes,
            catalog=catalog,
            schema=schema,
        )


class LocalBackend:
    """Development backend that executes artifacts locally.

    This backend is used for local development and testing. It executes
    artifacts in a separate subprocess (via multiprocessing) so timeouts can be
    enforced by terminating the process. The execution context can include
    execute_sql for UC data queries.

    Attributes:
        db_path: Path to the local DuckDB database for control-plane state.
        artifacts_path: Path to local artifact storage directory.
        execute_sql: Optional SQL execution function for UC queries.
    """

    def __init__(
        self,
        db_path: Optional[str] = None,
        artifacts_path: Optional[str] = None,
        execute_sql: Optional[Callable[..., Any]] = None,
    ):
        """Initialize the local backend.

        Args:
            db_path: Path to the local DuckDB database. Defaults to
                ADK_LOCAL_DB_PATH env var or ".adk_local/adk.duckdb".
            artifacts_path: Path to local artifact storage. Defaults to
                ADK_LOCAL_ARTIFACTS_PATH env var or ".adk_local/artifacts".
            execute_sql: Optional SQL execution function for UC queries.
                If None, will be lazily loaded from sql_warehouse module.
        """
        self._db_path = db_path or os.environ.get(
            "ADK_LOCAL_DB_PATH", ".adk_local/adk.duckdb"
        )
        self._artifacts_path = artifacts_path or os.environ.get(
            "ADK_LOCAL_ARTIFACTS_PATH", ".adk_local/artifacts"
        )
        self._execute_sql = execute_sql

        # Ensure artifacts directory exists
        os.makedirs(self._artifacts_path, exist_ok=True)

        logger.info(
            f"LocalBackend initialized: db_path={self._db_path}, "
            f"artifacts_path={self._artifacts_path}"
        )

    def _get_execute_sql(self) -> Optional[Callable[..., Any]]:
        """Lazily load the execute_sql function if not provided.

        Returns:
            The execute_sql function, or None if not available.
        """
        if self._execute_sql is not None:
            return self._execute_sql

        try:
            from databricks_rlm_agent.sql_warehouse import execute_sql
            self._execute_sql = execute_sql
            return execute_sql
        except ImportError:
            logger.warning(
                "sql_warehouse module not available; execute_sql will not be "
                "injected into execution context"
            )
            return None

    def submit_and_wait(
        self,
        artifact_path: str,
        run_id: str,
        iteration: int,
        timeout_minutes: int,
        catalog: str,
        schema: str,
    ) -> dict[str, Any]:
        """Execute artifact locally with enforced timeout.

        Execution runs in a subprocess (via multiprocessing) so long-running
        user code can be terminated on timeout.

        Args:
            artifact_path: Path to the code artifact.
            run_id: The session/run identifier.
            iteration: The iteration number.
            timeout_minutes: Maximum time to wait (enforced via timeout).
            catalog: Unity Catalog name for execution context.
            schema: Schema name for execution context.

        Returns:
            Dict with execution results matching the Jobs API response format.
        """
        logger.info(
            f"[LocalBackend] Executing artifact locally: {artifact_path}, "
            f"run_id={run_id}, iteration={iteration}"
        )

        start_time = datetime.now(timezone.utc)
        local_run_id = f"local_{run_id}_iter{iteration}_{start_time.strftime('%Y%m%d_%H%M%S')}"

        try:
            result = self._execute_artifact_local(
                artifact_path=artifact_path,
                run_id=run_id,
                iteration=iteration,
                catalog=catalog,
                schema=schema,
                timeout_minutes=timeout_minutes,
            )

            success = result.get("status") == "success"
            timed_out = result.get("timed_out", False)

            # Determine result_state: TIMEDOUT takes precedence over FAILED
            if timed_out:
                result_state = "TIMEDOUT"
            elif success:
                result_state = "SUCCESS"
            else:
                result_state = "FAILED"

            return {
                "databricks_run_id": local_run_id,
                "run_url": f"file://{artifact_path}",
                "success": success,
                "life_cycle_state": "TERMINATED",
                "result_state": result_state,
                "logs": result.get("stdout"),
                "error": result.get("error"),
                "error_trace": result.get("error_trace"),
                "timed_out": timed_out,
                # Include local execution details
                "local_result": result,
            }

        except TimeoutError as e:
            logger.error(f"[LocalBackend] Execution timed out: {e}")
            return {
                "databricks_run_id": local_run_id,
                "run_url": f"file://{artifact_path}",
                "success": False,
                "life_cycle_state": "TERMINATED",
                "result_state": "TIMEDOUT",
                "logs": None,
                "error": str(e),
                "error_trace": None,
                "timed_out": True,
            }

        except Exception as e:
            logger.error(f"[LocalBackend] Execution failed: {e}")
            import traceback
            return {
                "databricks_run_id": local_run_id,
                "run_url": f"file://{artifact_path}",
                "success": False,
                "life_cycle_state": "INTERNAL_ERROR",
                "result_state": "FAILED",
                "logs": None,
                "error": str(e),
                "error_trace": traceback.format_exc(),
                "timed_out": False,
            }

    def _execute_artifact_local(
        self,
        artifact_path: str,
        run_id: str,
        iteration: int,
        catalog: str,
        schema: str,
        timeout_minutes: int,
    ) -> dict[str, Any]:
        """Execute an artifact locally with execute_sql in the context.

        This is a local version of executor.execute_artifact() that:
        - Does NOT require a SparkSession
        - Injects execute_sql for UC data queries
        - Captures stdout/stderr
        - Writes result.json to artifacts path
        - Enforces timeout by running code in a subprocess that can be terminated

        Args:
            artifact_path: Path to the code artifact.
            run_id: The session/run identifier.
            iteration: The iteration number.
            catalog: Unity Catalog name.
            schema: Schema name.
            timeout_minutes: Maximum execution time.

        Returns:
            Dict with execution results.
        """
        import io
        import json
        import multiprocessing
        import sys
        import traceback
        from contextlib import redirect_stdout, redirect_stderr
        from pathlib import Path

        start_time = datetime.now(timezone.utc)

        result = {
            "status": "pending",
            "artifact_path": artifact_path,
            "run_id": run_id,
            "iteration": iteration,
            "start_time": start_time.isoformat(),
            "end_time": None,
            "duration_seconds": None,
            "stdout": None,
            "stderr": None,
            "output": None,
            "error": None,
            "error_trace": None,
            "timed_out": False,
        }

        # Use multiprocessing.Queue to get results from subprocess
        result_queue: multiprocessing.Queue = multiprocessing.Queue()

        # Get execute_sql function path for subprocess injection
        execute_sql_available = self._get_execute_sql() is not None

        try:
            # Run execution in a subprocess with timeout enforcement
            timeout_seconds = timeout_minutes * 60

            logger.info(f"[LocalBackend] Starting subprocess execution with {timeout_minutes}min timeout")

            process = multiprocessing.Process(
                target=_execute_code_in_process,
                args=(
                    result_queue,
                    artifact_path,
                    catalog,
                    schema,
                    run_id,
                    iteration,
                    execute_sql_available,
                ),
            )
            process.start()
            process.join(timeout=timeout_seconds)

            if process.is_alive():
                # Timeout occurred - terminate the process
                logger.error(
                    f"[LocalBackend] Execution timed out after {timeout_minutes} minutes, "
                    "terminating subprocess"
                )
                process.terminate()
                process.join(timeout=5)  # Wait up to 5s for graceful termination

                if process.is_alive():
                    # Force kill if still running
                    logger.warning("[LocalBackend] Subprocess did not terminate, forcing kill")
                    process.kill()
                    process.join(timeout=2)

                result["status"] = "failed"
                result["error"] = f"Execution timed out after {timeout_minutes} minutes (process terminated)"
                result["timed_out"] = True
            else:
                # Process completed - get results from queue
                try:
                    exec_result = result_queue.get_nowait()

                    result["stdout"] = exec_result.get("stdout")
                    result["stderr"] = exec_result.get("stderr")

                    if exec_result.get("error"):
                        result["status"] = "failed"
                        result["error"] = exec_result["error"]
                        result["error_trace"] = exec_result.get("error_trace")
                        logger.error(f"[LocalBackend] Artifact execution failed: {exec_result['error']}")
                    else:
                        result["status"] = "success"
                        result["output"] = self._serialize_output(exec_result.get("output"))
                        logger.info("[LocalBackend] Artifact execution completed successfully")

                except Exception as queue_error:
                    # Queue read failed - process may have crashed
                    result["status"] = "failed"
                    result["error"] = f"Failed to retrieve execution results: {queue_error}"
                    logger.error(f"[LocalBackend] Queue read error: {queue_error}")

        except Exception as e:
            result["status"] = "failed"
            result["error"] = str(e)
            result["error_trace"] = traceback.format_exc()
            logger.error(f"[LocalBackend] Artifact execution failed: {e}")
            logger.error(result["error_trace"])

        finally:
            end_time = datetime.now(timezone.utc)
            result["end_time"] = end_time.isoformat()
            result["duration_seconds"] = (end_time - start_time).total_seconds()

            # Log captured output for debugging
            if result.get("stdout"):
                logger.info(f"[LocalBackend] Captured stdout ({len(result['stdout'])} chars)")
            if result.get("stderr"):
                logger.warning(f"[LocalBackend] Captured stderr ({len(result['stderr'])} chars)")

        # Write result.json
        result_path = self._write_result_json(
            artifact_path=artifact_path,
            result=result,
            run_id=run_id,
            iteration=iteration,
        )
        result["result_json_path"] = result_path

        return result

    def _serialize_output(self, output: Any) -> Any:
        """Serialize execution output to a JSON-compatible format.

        Args:
            output: The output to serialize.

        Returns:
            JSON-serializable representation of the output.
        """
        if output is None:
            return None

        # Handle common types
        if isinstance(output, (str, int, float, bool)):
            return output

        if isinstance(output, (list, tuple)):
            return [self._serialize_output(item) for item in output]

        if isinstance(output, dict):
            return {str(k): self._serialize_output(v) for k, v in output.items()}

        # Try to convert to string for other types
        try:
            return str(output)
        except Exception:
            return f"<non-serializable: {type(output).__name__}>"

    def _write_result_json(
        self,
        artifact_path: str,
        result: dict[str, Any],
        run_id: str,
        iteration: int,
    ) -> Optional[str]:
        """Write result.json to the artifacts directory.

        Args:
            artifact_path: Path to the executed artifact.
            result: The execution result dictionary.
            run_id: The run identifier.
            iteration: The iteration number.

        Returns:
            Path to the result.json file, or None if write failed.
        """
        import json

        try:
            # Use the configured artifacts path
            artifact_dir = self._artifacts_path

            # Ensure directory exists
            os.makedirs(artifact_dir, exist_ok=True)

            # Create result filename with run_id and iteration
            result_filename = f"result_{run_id}_iter{iteration}.json"
            result_path = os.path.join(artifact_dir, result_filename)

            # Write result
            with open(result_path, 'w') as f:
                json.dump(result, f, indent=2, default=str)

            logger.info(f"[LocalBackend] Result written to: {result_path}")
            return result_path

        except Exception as e:
            logger.error(f"[LocalBackend] Failed to write result.json: {e}")
            return None


def get_execution_backend(
    executor_job_id: Optional[int] = None,
    db_path: Optional[str] = None,
    artifacts_path: Optional[str] = None,
) -> ExecutionBackend:
    """Factory function to get the appropriate execution backend.

    Selects the backend based on ADK_RUN_MODE environment variable:
    - "databricks" (default): Uses DatabricksBackend with Jobs API
    - "local": Uses LocalBackend with direct execution

    Args:
        executor_job_id: Databricks job ID for DatabricksBackend.
        db_path: Local DB path for LocalBackend.
        artifacts_path: Local artifacts path for LocalBackend.

    Returns:
        An ExecutionBackend instance.

    Raises:
        ValueError: If required parameters are missing for the selected mode.
    """
    run_mode = os.environ.get("ADK_RUN_MODE", "databricks")

    logger.info(f"Selecting execution backend for mode: {run_mode}")

    if run_mode == "local":
        return LocalBackend(
            db_path=db_path,
            artifacts_path=artifacts_path,
        )
    else:
        # Default to Databricks mode
        job_id = executor_job_id
        if job_id is None:
            env_job_id = os.environ.get("ADK_EXECUTOR_JOB_ID")
            if env_job_id:
                job_id = int(env_job_id)

        if job_id is None:
            raise ValueError(
                "executor_job_id is required for Databricks mode. "
                "Set via parameter or ADK_EXECUTOR_JOB_ID env var."
            )

        return DatabricksBackend(executor_job_id=job_id)
