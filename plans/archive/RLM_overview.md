## RLM architecture on Databricks Lakeflow Jobs (wheel + spark-python tasks + REPL-like execution)

In this deployment, a Recursive Language Model, RLM, runs as a **Python wheel task inside a Databricks Lakeflow Jobs workflow**. The “REPL” is effectively implemented as **agent-generated Databricks notebook files** in a Databricks Job (job_a) that are RUN in separate Databricks Job (job_b), with the job_b stdout/stderr results passed back to an agent in job_a.  This enacts a secure version of in-process Python REPL that is fundamental to the approach of RLMs, namely "context/prompt is the agent enviroment to probe via code, and the agent is responsible for its own context window management.The system iteration loop involves by **writing code into pyspark-python files, running them as downstream Jobs, then parsing their outputs** as recursively delegated work to an LLM agent who is tasked with managing/reviewing/processing a chunk of the a massive intial context, without invocation context accumulation from the read/thinking process of the downstream agent rotting the upstream agent's window. In addtion to the passing the downstream agent a context "chunk", the upstream agent's tool to initiate this process, llm_query(), requires instructions or a query be written to the code docstring, which is passed to the downstream agent as its User_Prompt.  The downstream agent must always  report back its answer/results/output to the upstring agent before the loop continues. 

### Runtime roles

* **Base LlmAgent (upstream, context-window strategic, stateful)**
  A long-lived agent whose **persisted session context is treated as a limited resource**. It is used for:

  * orchestration decisions (what to run next, what python to generate),
  * maintaining high-level intent, constraints, and “what we’ve already tried,”
  * short, high-value reasoning steps (not long-text processing).

* **Sublm LlmAgent (inherits Base LlmAgent context, scalable, terminiated upon completion)**
  Context-window dispensible agents spun up for:

  * digesting **large python stdout print() outputs** (logs, tables, long text),
  * chunking/summarization, extraction, validation, and report formatting,
  * parallel processing without polluting the scarce Base agent’s context window.

### “REPL” substrate in this pattern

* **python file = execution cell**: the wheel task **writes/updates a python file** containing code that:

  * queries Unity Catalog / reads Delta tables,
  * runs Spark/Python transformations,
  * materializes results (Delta tables, files in Volumes/DBFS, JSON summaries),
  * emits structured logs/metrics to stdout and/or MLflow.
* **spark_python task execution = REPL run**: Lakeflow Jobs executes that notebook as a task; the code's **stdout/stderr, artifacts, and written outputs** are the “return values.”

### Control loop (one iteration shown, many iterations possible)

## In RLM Orchestrator (resources/rlm_orchestrator_job.yml): 

1. **Base agent plans**: uses grep-like tools to locate files/tables, then decides the next computation step, and generates python code with instruction doc-string, passing it to the llm_query() FuncTool.
2. **Wheel task writes and reviews code**: deterministic function publishes/updates a python artifact (plus parameters) after running a security scan for "drop" "rm -rf", ect.

## In RLM Executor (resources/rlm_executor_job.yml): 

3. "Databricks Jobs runs spark-python task**: execute on cluster; produce outputs (tables/files/logs).
4. **Output ingestion**:
   * short (>50k-token) outputs go to **Downstream agent** (no exposure upstream),
   * long outputs are routed to multiple **Downstream agents** for chunked processing.
5. **Parse + refine**: validate and append mulitple downstream agent responses into single payload update of invocation context, persisted to unity catalog volume.
5. **Return Upstream**: Resume upstream agent with prior session plus invocation context update from downstream work (as LlmResponses to upstream agent instructions). 
6. Repeat until termination (success, budget, max iterations, or detected stagnation).

### Data plane vs thinking plane

* **Data plane (heavy work)**: Python code does the expensive compute close to data (Spark/SQL).
* **Thinking plane (light orchestration)**:

  * Base (upstream) agent keeps the strategic thread.
  * SubLM (downstream) agents do bulk reading/compression of long outputs.
  * The “truth” of progress lives in persisted artifacts (not in chat history).

### Persistence and state (so you don’t rely on context)

* **Run state table (Delta)**: `run_id`, `iteration`, `task_refs`, `status`, pointers to artifacts.
* **Artifacts**: spark-python filepaths/versions, Github execution logs, MLflow traces, summary JSONs, output table names.
* **Session capsule**: a compact, continuously-updated “state summary” using the adk ArtifactRegistry that the Base agent loads each iteration instead of carrying everything in memory.

### Why this works well

* You get a **Databricks-native REPL-like loop**: generate code → run code → read outputs → iterate.
* You preserve the Base agent’s limited context by offloading long-output digestion to **downsream agents**.
* You keep reproducibility: every iteration leaves a python file + logged run outputs + agent response/summary/answer you can replay, diff, and audit.
