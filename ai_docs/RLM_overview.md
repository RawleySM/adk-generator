## RLM architecture on Databricks Lakeflow Jobs (wheel + notebook tasks + REPL-like execution)

In this deployment, RLM runs as a **Python wheel task inside a Databricks Lakeflow Jobs workflow**. The “REPL” is effectively implemented as **generated Databricks notebooks** (plus their execution logs/artifacts) rather than an in-process Python REPL. The system iterates by **writing code into notebooks, running them as downstream tasks, then parsing their outputs** to decide the next move and/or to recursively delegate work to other LLM agents.

### Runtime roles

* **Base LlmAgent (scarce, stateful)**
  A long-lived agent whose **persisted session context is treated as a limited resource**. It is used for:

  * orchestration decisions (what to run next, which notebook to generate),
  * maintaining high-level intent, constraints, and “what we’ve already tried,”
  * short, high-value reasoning steps (not long-text processing).

* **Ephemeral LlmAgent (stateless, scalable)**
  Fresh-context agents spun up for:

  * digesting **large notebook outputs** (logs, tables, long text),
  * chunking/summarization, extraction, validation, and report formatting,
  * parallel processing without polluting the scarce Base agent’s context window.

### “REPL” substrate in this pattern

* **Generated notebook = execution cell**: the wheel task **writes/updates a notebook** (or a notebook-like artifact) containing code that:

  * queries Unity Catalog / reads Delta tables,
  * runs Spark/Python transformations,
  * materializes results (Delta tables, files in Volumes/DBFS, JSON summaries),
  * emits structured logs/metrics to stdout and/or MLflow.
* **Notebook task execution = REPL run**: Lakeflow Jobs executes that notebook as a task; the notebook’s **stdout/stderr, artifacts, and written outputs** are the “return values.”

### Control loop (one iteration)

1. **Base agent plans**: decide the next computation step and generate notebook code.
2. **Wheel task writes notebook**: publish/update a notebook artifact (plus parameters).
3. **Lakeflow runs notebook task**: execute on cluster; produce outputs (tables/files/logs).
4. **Output ingestion**:

   * short/structured outputs go back to **Base agent** (minimal context impact),
   * long outputs are routed to **Ephemeral agents** for chunked processing.
5. **Parse + refine**: merge extracted signals back into a compact state summary.
6. Repeat until termination (success, budget, max iterations, or detected stagnation).

### Data plane vs thinking plane

* **Data plane (heavy work)**: notebooks do the expensive compute close to data (Spark/SQL).
* **Thinking plane (light orchestration)**:

  * Base agent keeps the strategic thread.
  * Ephemeral agents do bulk reading/compression of long outputs.
  * The “truth” of progress lives in persisted artifacts (not in chat history).

### Persistence and state (so you don’t rely on context)

* **Run state table (Delta)**: `run_id`, `iteration`, `task_refs`, `status`, pointers to artifacts.
* **Artifacts**: notebook paths/versions, execution logs, MLflow traces, summary JSONs, output table names.
* **Session capsule**: a compact, continuously-updated “state summary” that the Base agent loads each iteration instead of carrying everything in memory.

### Why this works well

* You get a **Databricks-native REPL loop**: generate code → run notebook → read outputs → iterate.
* You preserve the Base agent’s limited context by offloading long-output digestion to **ephemeral agents**.
* You keep reproducibility: every iteration leaves a notebook + run outputs you can replay, diff, and audit.
