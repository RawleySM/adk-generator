# Databricks CDF with Task Values for polling trigger
1. **Poll on a schedule** (every N minutes) and branch inside the workflow, or


Below is the “Databricks-native” way to do both, using only documented features.

---

## Pattern A (most common): Scheduled polling job + If/else + (optional) Run Job task

### 1) Read only *new* rows: Delta Change Data Feed (CDF)

Enable CDF on the trigger table so you can read “what changed since version X”, instead of scanning the whole table. Databricks documents enabling it via table properties (`delta.enableChangeDataFeed = true`) for new or existing tables. ([Databricks Documentation][1])

CDF gives you metadata columns like `_change_type`, `_commit_version`, `_commit_timestamp` so you can watermark what you’ve already processed. ([Databricks Documentation][1])

You can read changes in **batch** with `table_changes(...)` (SQL) or `spark.read.option("readChangeFeed","true")...` with starting/ending version. ([Databricks Documentation][1])

### 2) Poll task writes a boolean “should I fire?” using Task Values

Use `dbutils.jobs.taskValues.set()` in a **Python notebook task** to set a small flag (and optionally a small list of IDs). Task values are explicitly meant for passing info between tasks in a Databricks job. ([Databricks Documentation][2])

### 3) If/else task branches based on that flag

Databricks supports an **If/else condition task** where operands can reference task values, e.g. `{{tasks.poll.values.fire}} > 0` style. ([Databricks Documentation][3])

### 4) True branch either runs tasks or triggers another job

* If you want to kick off another workflow, use the **Run Job task**. ([Databricks Documentation][4])
  Note: Databricks warns against circular dependencies and nesting > 3 Run Job tasks. ([Databricks Documentation][4])
* Or call the REST API directly from code (`/api/2.2/jobs/run-now` or `/api/2.2/jobs/runs/submit`). ([Databricks Documentation][5])

### Optional: process each “trigger event” with For each

If you want “one trigger row = one processing iteration”, use the **For each** task. It can iterate a JSON array, including one sourced from a task value reference. ([Databricks Documentation][6])

---

### Concrete workflow shape

**Task 1 (Notebook / Python):** `poll_trigger_table`
**Task 2 (If/else):** `if_fire` condition on `{{tasks.poll_trigger_table.values.fire}} == "true"` ([Databricks Documentation][3])
**Task 3a (true):** `Run Job` task (or downstream notebook tasks) ([Databricks Documentation][4])
**Task 3b (false):** no-op / end

Here’s a minimal **poll task** sketch (uses CDF batch reads via `table_changes`):

```python
# poll_trigger_table (Python notebook task)

TRIGGER_TABLE = "catalog.schema.trigger_table"   # UC table with trigger rows
STATE_TABLE   = "catalog.schema.trigger_state"   # small UC table you own (stores last_commit_version)
TRIGGER_WORD  = "LAUNCH_THE_THING"

# 1) load last processed commit version (default 0)
try:
    last_v = spark.table(STATE_TABLE).selectExpr("max(last_commit_version) as v").collect()[0]["v"] or 0
except Exception:
    last_v = 0

# 2) read ONLY new changes since last_v
# (CDF docs show table_changes('tableName', startVersion [, endVersion])).
changes = spark.sql(f"SELECT * FROM table_changes('{TRIGGER_TABLE}', {last_v + 1})")

# 3) filter for new inserts that contain the trigger word
from pyspark.sql.functions import col, lower, lit

hits = (changes
        .filter(col("_change_type") == lit("insert"))
        .filter(lower(col("message")).contains(TRIGGER_WORD.lower()))
       )

fire = hits.limit(1).count() > 0

# 4) publish decision to downstream tasks
dbutils.jobs.taskValues.set(key="fire", value=str(fire).lower())

# Optional: pass a small list of IDs (keep it small: task values max JSON size is limited)
# ids = [r["event_id"] for r in hits.select("event_id").limit(200).collect()]
# dbutils.jobs.taskValues.set(key="event_ids", value=ids)

# 5) advance watermark (store max _commit_version we observed this run)
max_v = changes.selectExpr("max(_commit_version) as v").collect()[0]["v"]
if max_v is not None:
    # simplest: overwrite a 1-row table; or MERGE if you prefer
    spark.createDataFrame([(int(max_v),)], "last_commit_version long") \
         .write.mode("overwrite").saveAsTable(STATE_TABLE)
```

Key doc pieces that make the above “official”:

* Enable/read CDF + `table_changes` examples ([Databricks Documentation][1])
* Task values between tasks ([Databricks Documentation][2])
* If/else expressions referencing task values ([Databricks Documentation][3])
* Run Job task semantics + nesting warning ([Databricks Documentation][4])

---


[1]: https://docs.databricks.com/aws/en/delta/delta-change-data-feed "Use Delta Lake change data feed on Databricks | Databricks on AWS"
[2]: https://docs.databricks.com/aws/en/jobs/task-values "Use task values to pass information between tasks | Databricks on AWS"
[3]: https://docs.databricks.com/aws/en/jobs/if-else "Add branching logic to a job with the If/else task | Databricks on AWS"
[4]: https://docs.databricks.com/aws/en/jobs/run-job "Run Job task for jobs | Databricks on AWS"
[5]: https://docs.databricks.com/api/workspace/jobs/runnow?utm_source=chatgpt.com "Trigger a new job run | Jobs API | REST API reference"
[6]: https://docs.databricks.com/aws/en/jobs/for-each "Use a For each task to run another task in a loop | Databricks on AWS"
[7]: https://docs.databricks.com/aws/en/structured-streaming/delta-lake "Delta table streaming reads and writes | Databricks on AWS"
