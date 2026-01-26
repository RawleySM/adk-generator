"""Job Builder Agent - Deterministic BaseAgent for Job_B Submission.

This module provides the JobBuilderAgent, a deterministic (non-LLM) agent
that handles Job_B executor submission and result collection as part of
the RLM workflow.

The JobBuilderAgent:
1. Reads temp:rlm:artifact_id from state (with fallback to legacy rlm:*)
2. Loads the code artifact from the ArtifactService
3. Builds job JSON with session parameters
4. Submits Job_B via the Databricks Jobs API
5. Waits for completion
6. Parses stdout between RLM markers
7. Updates the artifact registry with results
8. Sets state keys for results_processor_agent (temp:rlm:* invocation-scoped)

State key design:
- Reads from temp:rlm:* with fallback to rlm:* (dual-read for migration)
- Writes to temp:rlm:* (invocation-scoped, auto-discarded after invocation)
- See plans/refactor_key_glue.md for the migration plan

This design provides:
- Observable job submission as an agent step (telemetry, callbacks)
- Clean separation from cli.py entrypoint logic
- Future capability for parallel job execution
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, AsyncGenerator, Optional

from google.adk.agents.base_agent import BaseAgent
from google.adk.agents.invocation_context import InvocationContext
from google.adk.events import Event, EventActions
from google.genai import types

logger = logging.getLogger(__name__)

# Import state helpers for dual-read pattern
from databricks_rlm_agent.utils.state_helpers import get_rlm_state

# State key constants - invocation-scoped (temp:rlm:*)
# These are auto-discarded after invocation by DeltaSessionService
STATE_ARTIFACT_ID = "temp:rlm:artifact_id"
STATE_SUBLM_INSTRUCTION = "temp:rlm:sublm_instruction"
STATE_HAS_AGENT_CODE = "temp:rlm:has_agent_code"
STATE_CODE_ARTIFACT_KEY = "temp:rlm:code_artifact_key"

# Execution result keys (written by this agent, read by results_processor)
STATE_EXECUTION_STDOUT = "temp:rlm:execution_stdout"
STATE_EXECUTION_STDERR = "temp:rlm:execution_stderr"
STATE_EXECUTION_SUCCESS = "temp:rlm:execution_success"
STATE_DATABRICKS_RUN_ID = "temp:rlm:databricks_run_id"
STATE_RUN_URL = "temp:rlm:run_url"
STATE_RESULT_JSON_PATH = "temp:rlm:result_json_path"
STATE_STDOUT_TRUNCATED = "temp:rlm:stdout_truncated"
STATE_STDERR_TRUNCATED = "temp:rlm:stderr_truncated"

# Fatal error state keys for escalation (invocation-scoped)
STATE_FATAL_ERROR = "temp:rlm:fatal_error"
STATE_FATAL_ERROR_MSG = "temp:rlm:fatal_error_msg"

# Session-scoped keys (persists across invocations)
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
        # Track state changes in a local dict for minimal state_delta emission
        # This is the ADK-correct pattern: emit only keys this agent modifies
        state_delta: dict[str, Any] = {}

        # Step 1: Check if we have an artifact to execute
        # Use dual-read pattern: try temp:rlm:* first, fall back to legacy rlm:*
        artifact_id = get_rlm_state(ctx.session.state, "artifact_id")
        has_code = get_rlm_state(ctx.session.state, "has_agent_code", False)

        if not artifact_id or not has_code:
            # No code to execute - yield skip event and return
            yield self._create_text_event(
                ctx,
                "No artifact with code to execute - skipping job submission.",
                is_final=True,
                state_delta=state_delta,
            )
            return

        print(f"[JOB_BUILDER] Processing artifact: {artifact_id}")
        logger.info(f"[JOB_BUILDER] Starting job submission for artifact: {artifact_id}")

        # Step 2: Check executor job configuration - HARD ERROR if missing
        if not self._executor_job_id:
            error_msg = (
                "FATAL: No executor job ID configured. Set ADK_EXECUTOR_JOB_ID env var, "
                "store 'rlm-executor-job-id' in ADK_SECRET_SCOPE, or pass executor_job_id "
                "to JobBuilderAgent. Code execution cannot proceed without an executor job."
            )
            logger.error(f"[JOB_BUILDER] {error_msg}")
            state_delta = self._set_failure_state(ctx, error_msg, state_delta)
            yield self._create_error_event(ctx, error_msg, state_delta)
            return

        # Step 3: Get iteration and session info
        iteration = ctx.session.state.get(STATE_ITERATION, 1)
        session_id = ctx.session.id

        # Step 4: Load code from ArtifactService
        # NOTE: temp:* state IS safe for cross-sub-agent transport within the same
        # invocation. DeltaSessionService only discards temp state after the
        # invocation completes (_extract_state_delta filters temp:* for persistence).
        # The ArtifactService + Volumes file path is the single source of truth.
        # Use dual-read: try temp:rlm:* first, fall back to legacy rlm:*
        code_artifact_key = get_rlm_state(ctx.session.state, "code_artifact_key")
        agent_code = None

        if code_artifact_key:
            try:
                # Load from ArtifactService (Job_A-local storage)
                code_part = ctx.load_artifact(filename=code_artifact_key)
                if code_part:
                    agent_code = code_part.text if hasattr(code_part, "text") else str(code_part)
                    logger.info(f"[JOB_BUILDER] Loaded code from artifact: {code_artifact_key}")
            except Exception as e:
                logger.error(f"[JOB_BUILDER] Failed to load code artifact: {e}")
                error_msg = f"FATAL: Failed to load code artifact '{code_artifact_key}': {e}"
                state_delta = self._set_failure_state(ctx, error_msg, state_delta)
                yield self._create_error_event(ctx, error_msg, state_delta)
                return
        else:
            logger.warning(f"[JOB_BUILDER] No code_artifact_key in state for artifact {artifact_id}")

        if not agent_code:
            error_msg = f"FATAL: Could not load code for artifact {artifact_id}"
            logger.error(f"[JOB_BUILDER] {error_msg}")
            state_delta = self._set_failure_state(ctx, error_msg, state_delta)
            yield self._create_error_event(ctx, error_msg, state_delta)
            return

        # Step 5: Write code to artifact path for executor
        artifact_path = self._write_code_to_path(
            agent_code, artifact_id, session_id, iteration
        )

        if not artifact_path:
            error_msg = f"FATAL: Failed to write code to artifacts path"
            logger.error(f"[JOB_BUILDER] {error_msg}")
            state_delta = self._set_failure_state(ctx, error_msg, state_delta)
            yield self._create_error_event(ctx, error_msg, state_delta)
            return

        print(f"[JOB_BUILDER] Code written to: {artifact_path}")

        # Step 6: Submit executor job and wait for completion
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
            error_msg = f"FATAL: Job submission failed: {e}"
            logger.error(f"[JOB_BUILDER] {error_msg}")
            state_delta = self._set_failure_state(ctx, error_msg, state_delta)
            yield self._create_error_event(ctx, error_msg, state_delta)
            return

        print(f"[JOB_BUILDER] Execution completed: success={result.get('success')}")
        logger.info(f"[JOB_BUILDER] Executor job completed: {result}")

        # Step 7: Load result.json from UC Volumes (primary source)
        # The executor writes full stdout/stderr to result.json in the same
        # directory as the artifact. This is more reliable than Databricks job
        # logs since the executor redirects stdout/stderr into buffers.
        result_json_path = self._derive_result_json_path(
            artifact_path=artifact_path,
            session_id=session_id,
            iteration=iteration,
        )
        result_data = self._load_result_json(result_json_path)

        # Extract stdout/stderr from result.json (with fallback to job logs)
        if result_data:
            stdout = result_data.get("stdout") or ""
            stderr = result_data.get("stderr") or ""
            logger.info(
                f"[JOB_BUILDER] Using result.json: stdout={len(stdout)} chars, "
                f"stderr={len(stderr)} chars"
            )
        else:
            # Fallback: use Databricks job logs (less reliable)
            logger.warning(
                "[JOB_BUILDER] result.json not available, falling back to job logs"
            )
            stdout = result.get("logs", "")
            stderr = result.get("error", "")

        # Parse RLM markers if present
        parsed_output = self._parse_rlm_markers(stdout)
        final_stdout = parsed_output or stdout

        # Step 8: Update state for results_processor_agent
        # Store only a preview in session.state to keep Delta session tables small.
        # The injection plugin will load full output from result.json via the path.
        stdout_preview = self._create_preview(final_stdout, max_lines=50)
        stderr_preview = self._create_preview(stderr, max_lines=20)

        # Helper to track both in-memory state and state_delta
        # Writes to temp:rlm:* for invocation-scoped state
        def set_state(key: str, value: Any) -> None:
            ctx.session.state[key] = value
            state_delta[key] = value

        set_state(STATE_EXECUTION_STDOUT, stdout_preview)
        set_state(STATE_EXECUTION_STDERR, stderr_preview)
        set_state(STATE_EXECUTION_SUCCESS, result.get("success", False))
        set_state(STATE_DATABRICKS_RUN_ID, result.get("databricks_run_id"))
        set_state(STATE_RUN_URL, result.get("run_url"))

        # Set artifact path for injection plugin to load full output
        set_state(STATE_RESULT_JSON_PATH, result_json_path)
        set_state(STATE_STDOUT_TRUNCATED, len(final_stdout) > len(stdout_preview))
        set_state(STATE_STDERR_TRUNCATED, len(stderr) > len(stderr_preview))

        # Step 9: Update artifact registry
        self._update_artifact_registry(
            artifact_id=artifact_id,
            stdout=final_stdout,
            stderr=stderr,
            status="completed" if result.get("success") else "failed",
            result_json_path=result_json_path,
        )

        # Step 10: Yield final event with summary
        truncated_note = " (truncated)" if state_delta.get(STATE_STDOUT_TRUNCATED) else ""
        summary = (
            f"Executor job completed.\n"
            f"  Run ID: {result.get('databricks_run_id')}\n"
            f"  Status: {'success' if result.get('success') else 'failed'}\n"
            f"  Run URL: {result.get('run_url', 'N/A')}\n"
            f"  Output length: {len(final_stdout)} chars{truncated_note}\n"
            f"  Result JSON: {result_json_path}"
        )

        yield self._create_text_event(ctx, summary, is_final=True, state_delta=state_delta)

    def _create_text_event(
        self,
        ctx: InvocationContext,
        text: str,
        is_final: bool = False,
        state_delta: Optional[dict[str, Any]] = None,
    ) -> Event:
        """Create a text event for agent output.

        Args:
            ctx: The invocation context.
            text: The text content.
            is_final: Whether this is the final response.
            state_delta: Optional state delta to emit. Only provided for final events.

        Returns:
            Event with the text content.
        """
        # Only emit state_delta for final events, and use the provided delta
        # (which contains only keys this agent modified) rather than whole state
        final_delta = state_delta if is_final and state_delta else {}

        return Event(
            invocation_id=ctx.invocation_id,
            author=self.name,
            branch=ctx.branch,
            content=types.Content(
                role="model",
                parts=[types.Part.from_text(text=text)],
            ),
            actions=EventActions(
                state_delta=final_delta,
            ),
        )

    def _set_failure_state(
        self,
        ctx: InvocationContext,
        error_msg: str,
        state_delta: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Mark execution as failed in session state.

        This helper ensures consistent failure state is set before all early-return
        failures, allowing results_processor and plugins to distinguish "execution
        failed" from "execution never happened."

        Uses temp:rlm:* keys for invocation-scoped state (auto-discarded after
        invocation, preventing stale failure state from leaking).

        Args:
            ctx: The invocation context.
            error_msg: The error message describing the failure.
            state_delta: Optional state delta dict to update.

        Returns:
            Updated state_delta dict with failure keys.
        """
        delta = state_delta.copy() if state_delta else {}
        delta[STATE_EXECUTION_SUCCESS] = False
        delta[STATE_EXECUTION_STDERR] = error_msg

        # Also update in-memory state for consistency
        ctx.session.state[STATE_EXECUTION_SUCCESS] = False
        ctx.session.state[STATE_EXECUTION_STDERR] = error_msg

        return delta

    def _create_error_event(
        self,
        ctx: InvocationContext,
        error_msg: str,
        state_delta: Optional[dict[str, Any]] = None,
    ) -> Event:
        """Create an error event that signals fatal execution failure.

        This sets the fatal error flag for run_conversation() to detect and
        properly handle as distinct from delegation and exit_loop escalations.

        Args:
            ctx: The invocation context.
            error_msg: The error message.
            state_delta: State delta containing keys this agent modified.

        Returns:
            Event with error content and escalation to halt the workflow.
        """
        # Build minimal state delta with only error-related keys
        error_delta = state_delta.copy() if state_delta else {}
        error_delta[STATE_FATAL_ERROR] = True
        error_delta[STATE_FATAL_ERROR_MSG] = error_msg

        # Also update in-memory state for consistency
        ctx.session.state[STATE_FATAL_ERROR] = True
        ctx.session.state[STATE_FATAL_ERROR_MSG] = error_msg

        return Event(
            invocation_id=ctx.invocation_id,
            author=self.name,
            branch=ctx.branch,
            content=types.Content(
                role="model",
                parts=[types.Part.from_text(text=f"ERROR: {error_msg}")],
            ),
            actions=EventActions(
                state_delta=error_delta,
                escalate=True,  # Signal LoopAgent to stop iteration
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

    def _derive_result_json_path(
        self,
        artifact_path: str,
        session_id: str,
        iteration: int,
    ) -> str:
        """Derive the result.json path from the artifact path.

        The executor writes result.json to the same directory as the artifact:
        - artifact_path: /Volumes/.../agent_code_{session}_{iter}_{id}.py
        - result_path: /Volumes/.../result_{session}_iter{iteration}.json

        Args:
            artifact_path: Path to the executed artifact.
            session_id: The session identifier (used as run_id in executor).
            iteration: The iteration number.

        Returns:
            Path to the expected result.json file.
        """
        artifact_dir = os.path.dirname(artifact_path) or self._artifacts_path
        result_filename = f"result_{session_id}_iter{iteration}.json"
        return os.path.join(artifact_dir, result_filename)

    def _load_result_json(
        self,
        result_path: str,
    ) -> Optional[dict[str, Any]]:
        """Load result.json from UC Volumes.

        Args:
            result_path: Full path to the result.json file.

        Returns:
            The parsed result dictionary, or None if loading failed.
        """
        try:
            if not os.path.exists(result_path):
                logger.warning(f"[JOB_BUILDER] Result file not found: {result_path}")
                return None

            with open(result_path, 'r') as f:
                result_data = json.load(f)

            logger.info(f"[JOB_BUILDER] Loaded result.json from: {result_path}")
            return result_data

        except json.JSONDecodeError as e:
            logger.error(f"[JOB_BUILDER] Invalid JSON in result file: {e}")
            return None
        except Exception as e:
            logger.error(f"[JOB_BUILDER] Failed to load result.json: {e}")
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

    def _create_preview(self, content: str, max_lines: int = 50) -> str:
        """Create a preview of content with first/last lines.

        This keeps session.state small while providing enough context
        for the results_processor agent to understand execution output.

        Args:
            content: The full content to preview.
            max_lines: Maximum number of lines to include.

        Returns:
            Preview string, possibly truncated with indicator.
        """
        if not content:
            return ""

        lines = content.split('\n')
        if len(lines) <= max_lines:
            return content

        # Show first and last portions with truncation indicator
        head_lines = max_lines // 2
        tail_lines = max_lines - head_lines - 1  # -1 for truncation line

        head = lines[:head_lines]
        tail = lines[-tail_lines:] if tail_lines > 0 else []
        omitted = len(lines) - head_lines - tail_lines

        preview_parts = head + [f"\n... ({omitted} lines omitted) ...\n"] + tail
        return '\n'.join(preview_parts)

    def _update_artifact_registry(
        self,
        artifact_id: str,
        stdout: str,
        stderr: str,
        status: str,
        result_json_path: Optional[str] = None,
    ) -> None:
        """Update the artifact registry with execution results.

        Args:
            artifact_id: The artifact identifier.
            stdout: Captured standard output.
            stderr: Captured standard error.
            status: Execution status.
            result_json_path: Path to the result.json file in UC Volumes.
        """
        try:
            from pyspark.sql import SparkSession
            from databricks_rlm_agent.artifact_registry import get_artifact_registry

            spark = SparkSession.builder.getOrCreate()
            registry = get_artifact_registry(spark, ensure_exists=False)

            metadata = {
                "stdout_length": len(stdout),
                "stderr_length": len(stderr),
            }
            if result_json_path:
                metadata["result_json_path"] = result_json_path

            registry.update_artifact(
                artifact_id=artifact_id,
                status=status,
                metadata=metadata,
            )
            logger.info(f"[JOB_BUILDER] Updated artifact registry: {artifact_id}")

        except ImportError:
            logger.debug("[JOB_BUILDER] Spark not available - skipping registry update")
        except Exception as e:
            logger.warning(f"[JOB_BUILDER] Failed to update artifact registry: {e}")
