I’m going to read `databricks_rlm_agent/agents/job_builder.py`, then trace the call chain immediately after it “preps” the code (looking for where it hands off execution). I’ll cross-check where `job_builder` is invoked from (CLI vs executor) to answer which component receives the prepared code.

[4 tools called]

Next I’m going to trace `_submit_and_wait()` to see exactly what it hands off (job parameters), and then confirm in `executor.py` what consumes those parameters. That will tell us whether the prepared code is routed through `cli.py` or directly into the executor run.

[4 tools called]

## What `job_builder.py` calls after it “preps” the code

In `databricks_rlm_agent/agents/job_builder.py`, once it loads the generated code and **writes it to a UC Volumes path**, the next “handoff” step is **submitting a Databricks Job_B run** via `_submit_and_wait()`:

```265:290:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/agents/job_builder.py
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
```

And `_submit_and_wait()` delegates to `databricks_rlm_agent.jobs_api.submit_and_wait`:

```694:720:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/agents/job_builder.py
    def _submit_and_wait(
        self,
        artifact_path: str,
        run_id: str,
        iteration: int,
    ) -> dict[str, Any]:
        """Submit executor job and wait for completion."""
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
```

## Does it pass the code to `executor.py` or `cli.py`?

**It passes it to the executor plane (Job_B), i.e. `executor.py`, not `cli.py`.**

Concretely, `jobs_api.submit_executor_run()` submits the Job_B run with `job_parameters` that include **`ARTIFACT_PATH`**:

```83:143:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/jobs_api.py
def submit_executor_run(
    executor_job_id: int,
    artifact_path: str,
    run_id: str,
    iteration: int,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    additional_params: Optional[dict[str, str]] = None,
) -> tuple[int, str]:
    # Build job parameters
    job_params = {
        "ARTIFACT_PATH": artifact_path,
        "RUN_ID": run_id,
        "ITERATION": str(iteration),
    }

    # Submit the job run
    response = client.jobs.run_now(
        job_id=executor_job_id,
        job_parameters=job_params,
    )
```

Then `databricks_rlm_agent/executor.py` is the component that **reads and executes the file at that `artifact_path`**:

```38:109:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/executor.py
def execute_artifact(
    spark: "SparkSession",
    artifact_path: str,
    run_id: str,
    iteration: int,
    catalog: str,
    schema: str,
    timeout_seconds: int = 3600,
) -> dict[str, Any]:
    ...
    if not os.path.exists(artifact_path):
        raise FileNotFoundError(f"Artifact not found: {artifact_path}")

    # Read the artifact
    with open(artifact_path, 'r') as f:
        code = f.read()
```

`cli.py` is the **orchestrator entrypoint** that runs the LoopAgent and *includes* `job_builder` as a sub-agent; it’s not the place where the prepared code is executed. You can see that directly in its orchestrator description:

```379:386:/home/rawleysm/dev/adk-generator/databricks_rlm_agent/cli.py
    This delegates all orchestration to the ADK LoopAgent which handles:
    - databricks_analyst: Generates code, calls delegate_code_results()
    - job_builder: Submits Job_B, waits for completion, writes results to state
    - results_processor_agent: Analyzes execution results with injected context
```