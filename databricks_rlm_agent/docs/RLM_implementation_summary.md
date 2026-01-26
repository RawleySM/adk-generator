# RLM Workflow Implementation Summary

## Overview

This document summarizes the implementation of the 13-step RLM (Recursive Language Model) workflow that enables code generation, execution, and result processing through a coordinated agent system.

The core innovation is an **Artifact Registry** Delta table that decouples code generation from execution, enabling proper state propagation between agents.

## Configuration

### Job Parameters

The orchestrator job accepts the following parameters:

| Parameter | CLI Flag | Default | Description |
|-----------|----------|---------|-------------|
| `ADK_PROMPT` | `--prompt` | `""` | Inline prompt text to send to the agent |
| `ADK_PROMPT_FILE` | `--prompt-file` | `/Volumes/silo_dev_rs/task/task_txt/task.txt` | Path to file containing the prompt (UC Volumes, DBFS, or local) |
| `ADK_SESSION_ID` | `--session-id` | `"session_001"` | Session identifier for Delta persistence |
| `ADK_USER_ID` | `--user-id` | `"job_user"` | User identifier for session ownership |
| `ADK_MAX_ITERATIONS` | `--max-iterations` | `1` | Maximum RLM loop iterations |
| `ADK_DELTA_CATALOG` | `--catalog` | `"silo_dev_rs"` | Unity Catalog for session/telemetry tables |
| `ADK_DELTA_SCHEMA` | `--schema` | `"adk"` | Schema within the catalog |
| `ADK_EXECUTOR_JOB_ID` | - | - | Job ID of Job_B (executor) for RLM loop |
| `ADK_SECRET_SCOPE` | - | `"adk-secrets"` | Databricks secret scope for API keys |

### Prompt Precedence

The orchestrator resolves the prompt using the following precedence (highest to lowest):

1. **Literal prompt** (`--prompt` / `ADK_PROMPT`) - wins if non-empty
2. **Prompt file** (`--prompt-file` / `ADK_PROMPT_FILE`) - read only if literal prompt is empty
3. **Default file path** - `/Volumes/silo_dev_rs/task/task_txt/task.txt`

Example usage:

```bash
# Use default prompt file
rlm-orchestrator

# Specify inline prompt (wins over file)
rlm-orchestrator --prompt "Count all vendors"

# Specify custom prompt file
rlm-orchestrator --prompt-file /Volumes/silo_dev_rs/task/task_txt/custom.txt

# Via job parameters
uv run scripts/run_and_wait.py --job-id 12345 \
  --param ADK_PROMPT_FILE=/Volumes/silo_dev_rs/task/task_txt/task.txt
```

---

## Architecture

### Workflow Diagram

```
databricks_analyst → delegate_code_results() → [validation plugins] → artifact registry
                                                                           ↓
                                                                     job_builder (BaseAgent)
                                                                           ↓
                                                                      Job_B executor
                                                                           ↓
                                                             results_processor_agent (with injected context)
                                                                           ↓
                                                                     context pruning → loop continues
```

### Agent Sequence in LoopAgent

```
root_agent (LoopAgent)
  └── sub_agents:
        1. databricks_analyst (LlmAgent)       - generates code, calls delegate_code_results()
        2. job_builder (BaseAgent)             - deterministic job submission
        3. results_processor_agent (LlmAgent)  - processes results with injected context
```

## Files Created

### Phase 1: Artifact Registry Foundation

| File | Purpose |
|------|---------|
| `artifact_registry.py` | Delta table CRUD for artifact metadata tracking |
| `utils/__init__.py` | Utils package initialization |
| `utils/docstring_parser.py` | Parse `'''<instruction>'''\n<code>` format into components |

### Phase 2: delegate_code_results Tool

| File | Purpose |
|------|---------|
| `tools/delegate_code_results.py` | Tool for delegating code execution with state propagation |

### Phase 3: Validation Plugins

| File | Purpose |
|------|---------|
| `plugins/formatting_check_plugin.py` | Validates delegation blob docstring format |
| `plugins/code_linting_plugin.py` | Validates Python syntax using ast.parse() |

### Phase 4-5: Context Management Plugins

| File | Purpose |
|------|---------|
| `plugins/rlm_context_injection_plugin.py` | Injects execution results into results_processor_agent |
| `plugins/rlm_context_pruning_plugin.py` | Clears state after results_processor_agent completes |

### Phase 6: job_builder Agent

| File | Purpose |
|------|---------|
| `agents/__init__.py` | Agents package initialization |
| `agents/job_builder.py` | Deterministic BaseAgent for Job_B submission and result collection |

## Files Modified

| File | Changes |
|------|---------|
| `agent.py` | Added job_builder, results_processor_agent to LoopAgent; wired all new plugins |
| `run.py` | Added InMemoryArtifactService to Runner configuration |
| `executor.py` | Added `execute_from_registry()` function and RLM markers for log parsing |
| `plugins/__init__.py` | Exported new plugins (FormattingCheckPlugin, CodeLintingPlugin, RlmContextInjectionPlugin, RlmContextPruningPlugin) |
| `tools/__init__.py` | Exported delegate_code_results tool |

## Key ADK Patterns Used

### 1. State Propagation

State keys use the `temp:rlm:*` prefix for invocation-scoped glue (auto-discarded after invocation), while `rlm:iteration` remains session-scoped:

```python
# Invocation-scoped glue (auto-discarded after invocation)
tool_context.state["temp:rlm:artifact_id"] = artifact_id
tool_context.state["temp:rlm:sublm_instruction"] = parsed.sublm_instruction
tool_context.state["temp:rlm:has_agent_code"] = bool(agent_code)

# Session-scoped counter (persists across invocations)
tool_context.state["rlm:iteration"] += 1
```

State keys propagate via `EventActions.state_delta`. The `temp:` prefix is stripped by `DeltaSessionService` before persistence, ensuring invocation glue doesn't leak to subsequent invocations.

See `plans/refactor_key_glue.md` for the full migration plan.

### 2. Escalation Signal
```python
tool_context.actions.escalate = True
```
Stops the current agent and lets LoopAgent invoke the next sub-agent.

### 3. Context Injection

Context injection uses the dual-read pattern via `get_rlm_state()` helper (reads `temp:rlm:*` first, falls back to legacy `rlm:*` during migration):

```python
from databricks_rlm_agent.utils.state_helpers import get_rlm_state

async def before_agent_callback(self, *, callback_context):
    # Dual-read: temp:rlm:* first, fallback to rlm:*
    artifact_id = get_rlm_state(callback_context.state, "artifact_id")
    stdout = get_rlm_state(callback_context.state, "execution_stdout")
    stderr = get_rlm_state(callback_context.state, "execution_stderr")

    # Return types.Content to inject as user message
    return types.Content(
        role="user",
        parts=[types.Part.from_text(text=content_text)],
    )
```

> **Note:** The plugin attempts to load stdout/stderr from `result.json` in UC Volumes first (via `temp:rlm:result_json_path`), falling back to state values. This ensures full output is available even when state stores only truncated previews.

### 4. State Pruning

After migration, `temp:rlm:*` keys auto-discard so explicit pruning is mostly defensive. During migration, the plugin clears both `temp:rlm:*` and legacy `rlm:*` keys:

```python
async def after_agent_callback(self, *, callback_context):
    # Clear both temp:rlm:* and legacy rlm:* during migration
    for key in INVOCATION_KEYS_TO_CLEAR + LEGACY_KEYS_TO_CLEAR:
        if key in callback_context.state:
            callback_context.state[key] = None  # None signals deletion
```

### 5. ArtifactService Integration
```python
from google.adk.artifacts import InMemoryArtifactService

artifact_service = InMemoryArtifactService()
runner = Runner(
    agent=root_agent,
    artifact_service=artifact_service,
    ...
)
```

### 6. Hybrid Storage Design

| Layer | Storage | Purpose |
|-------|---------|---------|
| Metadata Registry | Delta Table | Structured tracking (artifact_id, status, timestamps) |
| Binary Content | ADK ArtifactService | Large blob storage within orchestrator runtime |
| **Cross-Job Handoff** | UC Volumes | Code files written to `/Volumes/{catalog}/{schema}/artifacts/` |

> **Important:** `InMemoryArtifactService` is **ephemeral** - it doesn't persist across job boundaries. For Job_A → Job_B code handoff, `job_builder` writes code to **UC Volumes paths** which the executor reads. The registry stores a `code_artifact_key` reference, but the executor resolves this to a Volumes path.

## Plugin Execution Order

### Plugin Wiring (App vs Runner)

Plugins are configured in **two locations** with different scopes:

**App plugins** (`agent.py`) - RLM workflow-specific plugins:
1. **safety_plugin** - Block destructive operations first
2. **formatting_plugin** - Validate docstring format
3. **linting_plugin** - Validate Python syntax
4. **logging_plugin** - Telemetry and logging
5. **global_instruction_plugin** - Inject global instructions
6. **context_injection_plugin** - Inject context to results_processor
7. **context_pruning_plugin** - Clear state after processing

**Runner plugins** (`run.py`) - Core infrastructure plugins:
- `logging_plugin` - Session-level logging
- `global_instruction_plugin` - Global context injection

> **Note:** ADK combines App and Runner plugins. The App plugin list is the **authoritative chain** for RLM workflow behavior.

## State Keys

### Invocation-Scoped State Keys (`temp:rlm:*`)

These keys use the `temp:` prefix and are auto-discarded by `DeltaSessionService` after each invocation. This prevents stale glue from leaking across invocations if the workflow aborts early.

| Key | Type | Description | Set By | Cleared By |
|-----|------|-------------|--------|------------|
| `temp:rlm:artifact_id` | str | Current artifact identifier | delegate_code_results | auto-discarded |
| `temp:rlm:sublm_instruction` | str | Instruction for results_processor_agent | delegate_code_results | auto-discarded |
| `temp:rlm:has_agent_code` | bool | Whether artifact has code to execute | delegate_code_results | auto-discarded |
| `temp:rlm:code_artifact_key` | str | Reference to code in ArtifactService | delegate_code_results | auto-discarded |
| `temp:rlm:session_id` | str | Session identifier | delegate_code_results | auto-discarded |
| `temp:rlm:invocation_id` | str | Invocation identifier | delegate_code_results | auto-discarded |
| `temp:rlm:execution_stdout` | str | Captured stdout preview (truncated) | job_builder | auto-discarded |
| `temp:rlm:execution_stderr` | str | Captured stderr preview (truncated) | job_builder | auto-discarded |
| `temp:rlm:execution_success` | bool | Whether execution succeeded | job_builder | auto-discarded |
| `temp:rlm:databricks_run_id` | str | Databricks job run ID | job_builder | auto-discarded |
| `temp:rlm:run_url` | str | URL to Databricks run | job_builder | auto-discarded |
| `temp:rlm:result_json_path` | str | Path to full result.json in UC Volumes | job_builder | auto-discarded |
| `temp:rlm:stdout_truncated` | bool | Whether stdout was truncated | job_builder | auto-discarded |
| `temp:rlm:stderr_truncated` | bool | Whether stderr was truncated | job_builder | auto-discarded |
| `temp:rlm:exit_requested` | bool | Exit loop signal | exit_loop tool | auto-discarded |
| `temp:rlm:fatal_error` | bool | Fatal error flag | job_builder | auto-discarded |
| `temp:rlm:fatal_error_msg` | str | Fatal error message | job_builder | auto-discarded |
| `temp:parsed_blob` | dict | Parsed delegation blob | delegate_code_results | auto-discarded |

### Session-Scoped State Keys

These keys persist across invocations intentionally:

| Key | Type | Description | Set By | Cleared By |
|-----|------|-------------|--------|------------|
| `rlm:iteration` | int | Current loop iteration counter | delegate_code_results | **Preserved** |

### Legacy Keys (Migration)

During migration, readers use `get_rlm_state()` which tries `temp:rlm:*` first, falling back to `rlm:*`. Legacy `rlm:*` keys (e.g., `rlm:artifact_id`) are no longer written but may exist in older sessions. The pruning plugin clears both `temp:rlm:*` and `rlm:*` variants for safety.

## RLM Markers for Log Parsing

The executor wraps output in markers for reliable log parsing:

```
===RLM_EXEC_START artifact_id={id}===
<agent code output>
===RLM_EXEC_END artifact_id={id} status={success|failed}===
```

## Delta Table Schema

### artifact_registry Table

```sql
CREATE TABLE {catalog}.{schema}.artifact_registry (
    artifact_id STRING NOT NULL,
    session_id STRING NOT NULL,
    invocation_id STRING NOT NULL,
    iteration INT NOT NULL,
    artifact_type STRING NOT NULL,
    sublm_instruction STRING,
    code_artifact_key STRING,
    stdout_artifact_key STRING,
    stderr_artifact_key STRING,
    status STRING NOT NULL,
    metadata_json STRING,
    created_time TIMESTAMP NOT NULL,
    updated_time TIMESTAMP NOT NULL,
    consumed_time TIMESTAMP
) USING DELTA PARTITIONED BY (session_id)
```

## Future Enhancements

### Dynamic Job Parallelism

The `job_builder(BaseAgent)` design enables future parallel job execution:

```python
class ParallelJobBuilderAgent(BaseAgent):
    """
    Enhanced job_builder that can launch multiple executor jobs.
    - tool_context.state["rlm:parallel_artifacts"] = [artifact_id_1, ...]
    - Launches N jobs concurrently
    - Waits via asyncio.gather()
    """
```

### DeltaArtifactService (Production Persistence)

Replace `InMemoryArtifactService` with a custom implementation:

```python
class DeltaArtifactService(BaseArtifactService):
    """
    ArtifactService using Unity Catalog Volumes.
    Path: /Volumes/{catalog}/{schema}/artifacts/{app_name}/{user_id}/{session_id}/{filename}
    """
```

## Testing Checklist

### Unit Tests
- [ ] `test_docstring_parser.py` - Parse various blob formats
- [ ] `test_artifact_registry.py` - CRUD operations roundtrip
- [ ] `test_formatting_plugin.py` - Block vs allow scenarios
- [ ] `test_linting_plugin.py` - Syntax error detection

### Integration Tests
- [ ] `delegate_code_results()` → registry insert → state keys set
- [ ] Plugin chain: safety → format → lint → tool execution
- [ ] Context injection: artifact load → Content creation

### End-to-End Test (Databricks)
- [ ] Submit prompt to orchestrator
- [ ] Verify artifact registry row created
- [ ] Verify Job_B reads from registry by artifact_id
- [ ] Verify results_processor_agent receives injected context
- [ ] Verify state keys cleared after processing
