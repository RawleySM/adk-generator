#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "databricks-sdk>=0.20.0",
# ]
# ///
import sys
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import argparse
import time
import os

# Terminal states that indicate the job has finished
TERMINAL_STATES = {
    RunLifeCycleState.TERMINATED,
    RunLifeCycleState.SKIPPED,
    RunLifeCycleState.INTERNAL_ERROR,
}

def parse_args():
    parser = argparse.ArgumentParser(description="Wait for a Databricks run to complete.")
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--profile", type=str, default="rstanhope")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--poll-interval", type=int, default=10)
    return parser.parse_args()

def print_task_outputs(w: WorkspaceClient, run_id: int) -> None:
    """Fetch and print task outputs for the run."""
    print("\n" + "=" * 60)
    print("Task Outputs:")
    print("=" * 60)

    def print_output(output, indent: str = "") -> None:
        if output.notebook_output and output.notebook_output.result:
            print(f"{indent}Notebook Output: {output.notebook_output.result}")
        if output.logs:
            print(f"{indent}Logs:")
            log_lines = output.logs.strip().split("\n")
            max_lines = 100
            if len(log_lines) > max_lines:
                print(f"{indent}  (showing last {max_lines} of {len(log_lines)} lines)")
                log_lines = log_lines[-max_lines:]
            for line in log_lines:
                print(f"{indent}  {line}")
            if output.logs_truncated:
                print(f"{indent}  ... (logs truncated by Databricks)")
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
            try:
                output = w.jobs.get_run_output(run_id=run_id)
                print_output(output, indent="")
            except Exception as e:
                print(f"Could not fetch output: {e}")
    except Exception as e:
        print(f"Error fetching task outputs: {e}")

def main():
    args = parse_args()
    w = WorkspaceClient(profile=args.profile)
    
    print(f"Waiting for run {args.run_id} to complete...")
    timeout_seconds = args.timeout * 60
    start_time = time.time()
    
    while True:
        elapsed = time.time() - start_time
        if elapsed >= timeout_seconds:
            print(f"\nTimeout reached.")
            return 1
            
        run = w.jobs.get_run(run_id=args.run_id)
        state = run.state.life_cycle_state
        result = run.state.result_state
        
        elapsed_mins = int(elapsed // 60)
        elapsed_secs = int(elapsed % 60)
        status_msg = f"\r[{elapsed_mins:02d}:{elapsed_secs:02d}] State: {state.value}"
        if result:
            status_msg += f" | Result: {result.value}"
        print(status_msg, end="", flush=True)
        
        if state in TERMINAL_STATES:
            print()
            print_task_outputs(w, args.run_id)
            return 0 if result == RunResultState.SUCCESS else 1
            
        time.sleep(args.poll_interval)

if __name__ == "__main__":
    sys.exit(main())
