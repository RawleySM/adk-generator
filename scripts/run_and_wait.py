#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "databricks-sdk>=0.20.0",
# ]
# ///
"""
Databricks Job Runner - Triggers a job and waits for completion.

This script runs a Databricks job with configurable parameters,
polls for completion, and reports the final status and outputs.

Usage:
    uv run scripts/run_and_wait.py --job-id 12345
    uv run scripts/run_and_wait.py --job-id 12345 --timeout 120

Environment Variables:
    DATABRICKS_PROFILE: CLI profile to use (default: rstanhope)

Examples:
    # Run orchestrator job
    uv run scripts/run_and_wait.py --job-id 12345

    # Run with custom timeout (2 hours)
    uv run scripts/run_and_wait.py --job-id 12345 --timeout 120

    # Run with job parameters
    uv run scripts/run_and_wait.py --job-id 12345 --param ADK_SESSION_ID=test_001
"""

import argparse
import os
import sys
import time
from datetime import datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState


# Terminal states that indicate the job has finished
TERMINAL_STATES = {
    RunLifeCycleState.TERMINATED,
    RunLifeCycleState.SKIPPED,
    RunLifeCycleState.INTERNAL_ERROR,
}


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Trigger a Databricks job and wait for completion.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run a job
  %(prog)s --job-id 12345

  # Run with custom timeout (2 hours)
  %(prog)s --job-id 12345 --timeout 120

  # Run with job parameters (inline prompt)
  %(prog)s --job-id 12345 --param ADK_SESSION_ID=test_001 --param ADK_PROMPT="Hello"

  # Run with prompt from file (literal prompt takes precedence if both are set)
  %(prog)s --job-id 12345 --param ADK_PROMPT_FILE=/Volumes/silo_dev_rs/task/task_txt/task.txt
        """,
    )

    parser.add_argument(
        "--job-id",
        type=int,
        required=True,
        help="Databricks job ID to run (required)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Maximum wait time in minutes (default: 60)",
    )
    parser.add_argument(
        "--profile",
        type=str,
        default=os.environ.get("DATABRICKS_PROFILE", "rstanhope"),
        help="Databricks CLI profile (default: DATABRICKS_PROFILE env var or 'rstanhope')",
    )
    parser.add_argument(
        "--param",
        type=str,
        action="append",
        dest="params",
        metavar="KEY=VALUE",
        help="Job parameter in KEY=VALUE format (can be specified multiple times)",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=10,
        help="Polling interval in seconds (default: 10)",
    )

    return parser.parse_args()


def parse_params(param_list: list[str] | None) -> dict[str, str]:
    """Parse KEY=VALUE parameters into a dictionary."""
    if not param_list:
        return {}
    
    params = {}
    for param in param_list:
        if "=" in param:
            key, value = param.split("=", 1)
            params[key] = value
        else:
            print(f"Warning: Ignoring invalid parameter (no '='): {param}", file=sys.stderr)
    
    return params


def run_job(w: WorkspaceClient, job_id: int, job_parameters: dict[str, str]) -> int:
    """
    Trigger the job and return the run ID.

    Args:
        w: Databricks WorkspaceClient
        job_id: The job ID to run
        job_parameters: Dictionary of job parameters

    Returns:
        The run ID of the triggered job
    """
    print(f"Triggering job {job_id}")
    if job_parameters:
        print("Job parameters:")
        for key, value in job_parameters.items():
            # Truncate long values for display
            display_value = value if len(str(value)) <= 80 else f"{str(value)[:77]}..."
            print(f"  {key}: {display_value}")

    response = w.jobs.run_now(job_id=job_id, job_parameters=job_parameters if job_parameters else None)
    run_id = response.run_id

    print(f"\nJob triggered successfully!")
    print(f"Run ID: {run_id}")
    
    # Get host for URL
    host = w.config.host.rstrip("/")
    print(f"Run URL: {host}/#job/{job_id}/run/{run_id}")

    return run_id


def wait_for_completion(
    w: WorkspaceClient, job_id: int, run_id: int, timeout_minutes: int, poll_interval: int
) -> tuple[RunLifeCycleState, RunResultState | None]:
    """
    Poll the job run until completion or timeout.

    Args:
        w: Databricks WorkspaceClient
        job_id: The job ID
        run_id: The run ID to monitor
        timeout_minutes: Maximum time to wait in minutes
        poll_interval: Time between polls in seconds

    Returns:
        Tuple of (lifecycle_state, result_state)
    """
    timeout_seconds = timeout_minutes * 60
    start_time = time.time()

    print(f"\nWaiting for job completion (timeout: {timeout_minutes} minutes)...")

    while True:
        elapsed = time.time() - start_time

        if elapsed >= timeout_seconds:
            print(f"\nTimeout reached after {timeout_minutes} minutes")
            return RunLifeCycleState.RUNNING, None

        run = w.jobs.get_run(run_id=run_id)
        state = run.state
        lifecycle_state = state.life_cycle_state
        result_state = state.result_state

        # Print status update
        elapsed_mins = int(elapsed // 60)
        elapsed_secs = int(elapsed % 60)
        status_msg = f"\r[{elapsed_mins:02d}:{elapsed_secs:02d}] State: {lifecycle_state.value}"
        if result_state:
            status_msg += f" | Result: {result_state.value}"
        print(status_msg, end="", flush=True)

        if lifecycle_state in TERMINAL_STATES:
            print()  # New line after status updates
            return lifecycle_state, result_state

        time.sleep(poll_interval)


def print_task_outputs(w: WorkspaceClient, run_id: int) -> None:
    """Fetch and print task outputs for the run."""
    print("\n" + "=" * 60)
    print("Task Outputs:")
    print("=" * 60)

    def print_output(output, indent: str = "") -> None:
        """Print run output fields."""
        # Notebook output (for notebook tasks)
        if output.notebook_output and output.notebook_output.result:
            print(f"{indent}Notebook Output: {output.notebook_output.result}")

        # Logs output (for wheel tasks and spark-submit)
        if output.logs:
            print(f"{indent}Logs:")
            # Print logs with additional indentation, limiting length
            log_lines = output.logs.strip().split("\n")
            max_lines = 100  # Limit output to avoid overwhelming terminal
            if len(log_lines) > max_lines:
                print(f"{indent}  (showing last {max_lines} of {len(log_lines)} lines)")
                log_lines = log_lines[-max_lines:]
            for line in log_lines:
                print(f"{indent}  {line}")
            if output.logs_truncated:
                print(f"{indent}  ... (logs truncated by Databricks)")

        # Error information
        if output.error:
            print(f"{indent}Error: {output.error}")
        if output.error_trace:
            print(f"{indent}Error Trace:")
            for line in output.error_trace.strip().split("\n"):
                print(f"{indent}  {line}")

    try:
        run = w.jobs.get_run(run_id=run_id)

        if run.tasks:
            for task in run.tasks:
                print(f"\nTask: {task.task_key}")
                print(f"  State: {task.state.life_cycle_state.value}")
                if task.state.result_state:
                    print(f"  Result: {task.state.result_state.value}")

                try:
                    output = w.jobs.get_run_output(run_id=task.run_id)
                    print_output(output, indent="  ")
                except Exception as e:
                    print(f"  Could not fetch output: {e}")
        else:
            # Single task job
            try:
                output = w.jobs.get_run_output(run_id=run_id)
                print_output(output, indent="")
            except Exception as e:
                print(f"Could not fetch output: {e}")

    except Exception as e:
        print(f"Error fetching task outputs: {e}")


def main() -> int:
    """Main entry point."""
    args = parse_args()

    # Parse job parameters
    job_parameters = parse_params(args.params)

    # Initialize Databricks client
    print(f"Connecting to Databricks with profile '{args.profile}'...")
    w = WorkspaceClient(profile=args.profile)

    # Run the job
    try:
        run_id = run_job(w, args.job_id, job_parameters)
    except Exception as e:
        print(f"Error triggering job: {e}")
        return 1

    # Wait for completion
    lifecycle_state, result_state = wait_for_completion(
        w, args.job_id, run_id, args.timeout, args.poll_interval
    )

    # Handle timeout
    if lifecycle_state == RunLifeCycleState.RUNNING:
        host = w.config.host.rstrip("/")
        print(f"\nJob did not complete within {args.timeout} minutes")
        print(f"Check status at: {host}/#job/{args.job_id}/run/{run_id}")
        return 2

    # Print task outputs
    print_task_outputs(w, run_id)

    # Determine exit code based on result
    print("\n" + "=" * 60)
    print(f"Final State: {lifecycle_state.value}")
    if result_state:
        print(f"Final Result: {result_state.value}")

    if result_state == RunResultState.SUCCESS:
        print("Job completed successfully!")
        return 0
    else:
        print("Job failed or was terminated abnormally")
        return 1


if __name__ == "__main__":
    sys.exit(main())

