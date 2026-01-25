"""Job Builder Agent - Deterministic BaseAgent for Job_B Submission.

This module provides the JobBuilderAgent, a deterministic (non-LLM) agent
that handles Job_B executor submission and result collection as part of
the RLM workflow.

The JobBuilderAgent:
1. Reads rlm:artifact_id from state
2. Loads the code artifact from the ArtifactService
3. Builds job JSON with session parameters
4. Submits Job_B via the Databricks Jobs API
5. Waits for completion
6. Parses stdout between RLM markers
7. Updates the artifact registry with results
8. Sets state keys for results_processor_agent

This design provides:
- Observable job submission as an agent step (telemetry, callbacks)
- Clean separation from cli.py entrypoint logic
- Future capability for parallel job execution
"""

from __future__ import annotations

import logging
import os
from typing import Any, AsyncGenerator, Optional, TYPE_CHECKING

from google.adk.agents.base_agent import BaseAgent
from google.adk.agents.invocation_context import InvocationContext
from google.adk.events import Event, EventActions
from google.genai import types

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# State key constants (must match delegate_code_results.py)
STATE_ARTIFACT_ID = "rlm:artifact_id"
STATE_SUBLM_INSTRUCTION = "rlm:sublm_instruction"
STATE_HAS_AGENT_CODE = "rlm:has_agent_code"
STATE_ITERATION = "rlm:iteration"

# RLM output markers for parsing executor logs
RLM_EXEC_START_MARKER = "===RLM_EXEC_START"
RLM_EXEC_END_MARKER = "===RLM_EXEC_END"


class JobBuilderAgent(BaseAgent):
    """Deterministic agent that submits Job_B executor runs.

    This agent does not use an LLM - it executes pure Python logic to:
    1. Read rlm:artifact_id from state
    2. Build job JSON with session parameters
    3. Submit via jobs.run_now()
    4. Wait for completion via get_run_output()
    5. Parse stdout between RLM markers
    6. Write results to artifact registry
    7. Set state keys for results_processor_agent

    The agent yields events for observability (telemetry, callbacks).

    Example:
        >>> from databricks_rlm_agent.agents import JobBuilderAgent
        >>> job_builder = JobBuilderAgent(
        ...     executor_job_id=12345,
        ...     catalog="silo_dev_rs",
        ...     schema="adk",
        ... )
        >>> # Use in LoopAgent sub_agents
        >>> root_agent = LoopAgent(
        ...     sub_agents=[databricks_analyst, job_builder, results_processor],
        ... )
    """

    def __init__(
        self,
        name: str = "job_builder",
        executor_job_id: Optional[int] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        timeout_minutes: int = 60,
        artifacts_path: Optional[str] = None,
        description: str = "Deterministic job submission agent",
    ):
        """Initialize the JobBuilderAgent.

        Args:
            name: Agent name.
            executor_job_id: Databricks job ID for the executor. If None,
                reads from ADK_EXECUTOR_JOB_ID env var.
            catalog: Unity Catalog name. If None, reads from env var.
            schema: Schema name. If None, reads from env var.
            timeout_minutes: Maximum time to wait for job completion.
            artifacts_path: Path for artifacts in UC Volumes.
            description: Agent description for telemetry.
        """
        super().__init__(name=name, description=description)

        self._executor_job_id = executor_job_id or os.environ.get("ADK_EXECUTOR_JOB_ID")
        if self._executor_job_id:
            self._executor_job_id = int(self._executor_job_id)

        self._catalog = catalog or os.environ.get("ADK_DELTA_CATALOG", "silo_dev_rs")
        self._schema = schema or os.environ.get("ADK_DELTA_SCHEMA", "adk")
        self._timeout_minutes = timeout_minutes
        self._artifacts_path = artifacts_path or os.environ.get(
            "ADK_ARTIFACTS_PATH", "/Volumes/silo_dev_rs/adk/artifacts"
        )

        logger.info(
            f"JobBuilderAgent initialized: executor_job_id={self._executor_job_id}, "
            f"catalog={self._catalog}, schema={self._schema}"
        )

    async def _run_async_impl(
        self, ctx: InvocationContext
    ) -> AsyncGenerator[Event, None]:
        """Execute the job submission workflow.

        Args:
            ctx: The invocation context with session and state.

        Yields:
            Event objects for agent lifecycle tracking.
        """
        # Step 1: Check if we have an artifact to execute
        artifact_id = ctx.session.state.get(STATE_ARTIFACT_ID)
        has_code = ctx.session.state.get(STATE_HAS_AGENT_CODE, False)

        if not artifact_id or not has_code:
            # No code to execute - yield skip event and return
            yield self._create_text_event(
                ctx,
                "No artifact with code to execute - skipping job submission.",
                is_final=True,
            )
            return

        print(f"[JOB_BUILDER] Processing artifact: {artifact_id}")
        logger.info(f"[JOB_BUILDER] Starting job submission for artifact: {artifact_id}")

        # Step 2: Check executor job configuration
        if not self._executor_job_id:
            # No executor configured - skip execution
            error_msg = (
                "No executor job ID configured. Set ADK_EXECUTOR_JOB_ID or "
                "pass executor_job_id to JobBuilderAgent."
            )
            logger.warning(f"[JOB_BUILDER] {error_msg}")
            yield self._create_text_event(ctx, error_msg, is_final=True)
            return

        # Step 3: Get iteration and session info
        iteration = ctx.session.state.get(STATE_ITERATION, 1)
        session_id = ctx.session.id

        # Step 4: Load code from ArtifactService or state
        code_artifact_key = ctx.session.state.get("rlm:code_artifact_key")
        agent_code = None

        if code_artifact_key:
            try:
                # Try loading from ArtifactService
                code_part = ctx.load_artifact(filename=code_artifact_key)
                if code_part:
                    agent_code = code_part.text if hasattr(code_part, "text") else str(code_part)
                    logger.info(f"[JOB_BUILDER] Loaded code from artifact: {code_artifact_key}")
            except Exception as e:
                logger.warning(f"[JOB_BUILDER] Could not load code artifact: {e}")

        # Fallback: check if code is in temp state
        if not agent_code:
            parsed_blob = ctx.session.state.get("temp:parsed_blob", {})
            agent_code = parsed_blob.get("agent_code")

        if not agent_code:
            error_msg = f"Could not load code for artifact {artifact_id}"
            logger.error(f"[JOB_BUILDER] {error_msg}")
            yield self._create_text_event(ctx, error_msg, is_final=True)
            return

        # Step 5: Write code to artifact path for executor
        artifact_path = self._write_code_to_path(
            agent_code, artifact_id, session_id, iteration
        )

        if not artifact_path:
            error_msg = f"Failed to write code to artifacts path"
            logger.error(f"[JOB_BUILDER] {error_msg}")
            yield self._create_text_event(ctx, error_msg, is_final=True)
            return

        print(f"[JOB_BUILDER] Code written to: {artifact_path}")

        # Step 6: Submit job and wait for completion
        yield self._create_text_event(
            ctx,
            f"Submitting executor job {self._executor_job_id} for artifact {artifact_id}...",
        )

        try:
            result = self._submit_and_wait(
                artifact_path=artifact_path,
                run_id=session_id,
                iteration=iteration,
            )
        except Exception as e:
            error_msg = f"Job submission failed: {e}"
            logger.error(f"[JOB_BUILDER] {error_msg}")
            yield self._create_text_event(ctx, error_msg, is_final=True)
            return

        print(f"[JOB_BUILDER] Job completed: success={result.get('success')}")
        logger.info(f"[JOB_BUILDER] Executor job completed: {result}")

        # Step 7: Parse stdout and extract RLM markers if present
        stdout = result.get("logs", "")
        stderr = result.get("error", "")
        parsed_output = self._parse_rlm_markers(stdout)

        # Step 8: Update state for results_processor_agent
        ctx.session.state["rlm:execution_stdout"] = parsed_output or stdout
        ctx.session.state["rlm:execution_stderr"] = stderr
        ctx.session.state["rlm:execution_success"] = result.get("success", False)
        ctx.session.state["rlm:databricks_run_id"] = result.get("databricks_run_id")
        ctx.session.state["rlm:run_url"] = result.get("run_url")

        # Step 9: Update artifact registry
        self._update_artifact_registry(
            artifact_id=artifact_id,
            stdout=parsed_output or stdout,
            stderr=stderr,
            status="completed" if result.get("success") else "failed",
        )

        # Step 10: Yield final event with summary
        summary = (
            f"Executor job completed.\n"
            f"  Run ID: {result.get('databricks_run_id')}\n"
            f"  Status: {'success' if result.get('success') else 'failed'}\n"
            f"  Run URL: {result.get('run_url', 'N/A')}\n"
            f"  Output length: {len(parsed_output or stdout)} chars"
        )

        yield self._create_text_event(ctx, summary, is_final=True)

    def _create_text_event(
        self, ctx: InvocationContext, text: str, is_final: bool = False
    ) -> Event:
        """Create a text event for agent output.

        Args:
            ctx: The invocation context.
            text: The text content.
            is_final: Whether this is the final response.

        Returns:
            Event with the text content.
        """
        return Event(
            invocation_id=ctx.invocation_id,
            author=self.name,
            branch=ctx.branch,
            content=types.Content(
                role="model",
                parts=[types.Part.from_text(text=text)],
            ),
            actions=EventActions(
                state_delta=dict(ctx.session.state) if is_final else {},
            ),
        )

    def _write_code_to_path(
        self,
        code: str,
        artifact_id: str,
        session_id: str,
        iteration: int,
    ) -> Optional[str]:
        """Write code to the artifacts path for executor.

        Args:
            code: The Python code to write.
            artifact_id: The artifact identifier.
            session_id: The session identifier.
            iteration: The iteration number.

        Returns:
            The path where code was written, or None on failure.
        """
        try:
            # Create filename with artifact info
            filename = f"agent_code_{session_id}_iter{iteration}_{artifact_id}.py"
            path = os.path.join(self._artifacts_path, filename)

            # Ensure directory exists
            os.makedirs(self._artifacts_path, exist_ok=True)

            # Write the code
            with open(path, 'w') as f:
                f.write(code)

            logger.info(f"[JOB_BUILDER] Wrote code to: {path}")
            return path

        except Exception as e:
            logger.error(f"[JOB_BUILDER] Failed to write code: {e}")
            return None

    def _submit_and_wait(
        self,
        artifact_path: str,
        run_id: str,
        iteration: int,
    ) -> dict[str, Any]:
        """Submit executor job and wait for completion.

        Args:
            artifact_path: Path to the code artifact.
            run_id: The session/run identifier.
            iteration: The iteration number.

        Returns:
            Dict with job result information.
        """
        from databricks_rlm_agent.jobs_api import submit_and_wait

        return submit_and_wait(
            executor_job_id=self._executor_job_id,
            artifact_path=artifact_path,
            run_id=run_id,
            iteration=iteration,
            timeout_minutes=self._timeout_minutes,
            catalog=self._catalog,
            schema=self._schema,
        )

    def _parse_rlm_markers(self, stdout: str) -> Optional[str]:
        """Parse stdout to extract content between RLM markers.

        Args:
            stdout: The raw stdout from executor.

        Returns:
            Content between markers, or None if markers not found.
        """
        if not stdout:
            return None

        # Look for start marker
        start_idx = stdout.find(RLM_EXEC_START_MARKER)
        if start_idx == -1:
            return None

        # Find end of start marker line
        start_line_end = stdout.find('\n', start_idx)
        if start_line_end == -1:
            return None

        # Look for end marker
        end_idx = stdout.find(RLM_EXEC_END_MARKER, start_line_end)
        if end_idx == -1:
            # No end marker - return everything after start
            return stdout[start_line_end + 1:].strip()

        # Extract content between markers
        return stdout[start_line_end + 1:end_idx].strip()

    def _update_artifact_registry(
        self,
        artifact_id: str,
        stdout: str,
        stderr: str,
        status: str,
    ) -> None:
        """Update the artifact registry with execution results.

        Args:
            artifact_id: The artifact identifier.
            stdout: Captured standard output.
            stderr: Captured standard error.
            status: Execution status.
        """
        try:
            from pyspark.sql import SparkSession
            from databricks_rlm_agent.artifact_registry import get_artifact_registry

            spark = SparkSession.builder.getOrCreate()
            registry = get_artifact_registry(spark, ensure_exists=False)
            registry.update_artifact(
                artifact_id=artifact_id,
                status=status,
                metadata={"stdout_length": len(stdout), "stderr_length": len(stderr)},
            )
            logger.info(f"[JOB_BUILDER] Updated artifact registry: {artifact_id}")

        except ImportError:
            logger.debug("[JOB_BUILDER] Spark not available - skipping registry update")
        except Exception as e:
            logger.warning(f"[JOB_BUILDER] Failed to update artifact registry: {e}")
