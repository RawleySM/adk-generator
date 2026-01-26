from databricks.sdk import WorkspaceClient
import json
import os

# Initialize WorkspaceClient with the specified profile
w = WorkspaceClient(profile="rstanhope")

run_id = 785535593357729
job_id = 772128597578930
output_file = "run_output_785535593357729.json"

print(f"Fetching details for run_id: {run_id}...")

try:
    # Get the run details to check for tasks
    run = w.jobs.get_run(run_id)
    
    output_data = {
        "job_id": job_id,
        "run_id": run_id,
        "tasks": []
    }

    # Helper to process a run (task or main)
    def process_run_output(r_id, task_key=None):
        print(f"Fetching output for run_id: {r_id} (Task: {task_key})...")
        try:
            output = w.jobs.get_run_output(r_id)
            return {
                "run_id": r_id,
                "task_key": task_key,
                "logs": output.logs,
                "error": output.error,
                "error_trace": output.error_trace
            }
        except Exception as e:
            print(f"Error fetching output for {r_id}: {e}")
            return {"run_id": r_id, "error_fetching_output": str(e)}

    if hasattr(run, 'tasks') and run.tasks:
        print(f"Run {run_id} has {len(run.tasks)} tasks. Fetching output for each...")
        for task in run.tasks:
            task_output = process_run_output(task.run_id, task.task_key)
            output_data["tasks"].append(task_output)
    else:
        print(f"Run {run_id} is a single task run.")
        main_output = process_run_output(run_id, "Main")
        output_data["tasks"].append(main_output)

    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2)
    
    print(f"Output saved to {output_file}")

except Exception as e:
    print(f"An error occurred: {e}")
