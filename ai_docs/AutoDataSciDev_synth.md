# AutoDataSciDev: Synthesized Architecture (v2)

**Synthesized from:** `chatGPT52_v1` (Governance/Platform) and `claudeO45_v1` (Mechanics/Implementation)
**Governing Doc:** `adk_fsm_synth.md`

---

## 1. Runtime Contract: Lakeflow Poller & RLM Loop

**Synthesis Strategy:**
*   **Outer Loop (Platform):** Adopts ChatGPT’s "Poll → Lease → Run" model to ensure safe concurrency and Databricks Jobs compatibility.
*   **Inner Loop (Mechanics):** Adopts Claude’s explicit `LOAD → PLAN → WRITE → EXEC → PARSE → MERGE → SAVE` cycle with a `SessionCapsule` to manage scarce LLM context.

### 1.1 Outer Control Plane (The "Wheel Task")

The system runs as a continuous Databricks Lakeflow Job (Python Wheel task) that wraps the ADK Runner.

1.  **Poll:** Periodically query the `jira_board_clone` table (Unity Catalog Delta) for `pending` or `processing` items.
2.  **Lease:** Attempt to acquire a distributed lease on a task.
    *   *Mechanism:* Optimistic `MERGE` into a `work_leases` table.
    *   *Constraint:* One active run per Jira task at a time.
3.  **Context Load:** If lease acquired, load the `run_state` and `SessionCapsule`.
4.  **Dispatch:**
    *   *DEV mode:* Enter the **RLM Orchestrator Loop**.
    *   *PROD mode:* Enter the **Deterministic FSM Runner**.

### 1.2 Inner DEV Loop (The RLM Cycle)

The `RLMOrchestratorAgent` (Custom Agent) drives the iteration. To protect the Base Agent's context window, we strictly enforce a **Session Capsule** pattern.

**The Cycle:**
1.  **LOAD:** Fetch `SessionCapsule` (compact summary of intent, progress, and recent errors) from Delta.
2.  **PLAN:** Base Agent views the Capsule and decides the next step (e.g., "Generate Data Loading Script").
3.  **WRITE:** Delegate to a specialized **CodeGeneratorAgent** to produce artifacts (Notebooks/Scripts).
    *   *Artifacts:* Saved to UC Volumes with paths like `runs/<run_id>/iter_<n>/notebook.py`.
4.  **EXEC:** Submit the generated notebook as a **Databricks Job Run** (or subordinate task).
    *   *Crucial:* Do not run `exec()` locally. Submit to the cluster to isolate side effects and capture full logs/MLflow traces.
5.  **PARSE:** Delegate raw logs/stdout to **Ephemeral Agents** (Stateless).
    *   *Role:* Digest 100k lines of Spark logs into a 5-line structured summary (Success/Fail, Error Type, Key Metrics).
6.  **MERGE:** Update the `SessionCapsule` with the parsed summary.
7.  **SAVE:** Persist the updated Capsule and `run_state` to Delta.

---

## 2. Deterministic FSM (PROD) & Governance

**Synthesis Strategy:**
*   **Skeleton:** Adopts Claude’s explicit `State | Event | Transition` class structure.
*   **Governance:** Enforces ChatGPT’s requirements for **pure transitions**, **separate effect recording**, and **schema-validated** tool outputs.

### 2.1 The FSM Contract

In Production, the system **is not** an infinite "Plan/Execute" loop. It is a finite state machine.

*   **States:** Explicit enum (e.g., `SCHEMA_VALIDATED`, `TRANSFORM_DEFINED`, `QUALITY_PASSED`).
*   **Transitions:** Pure functions: `(CurrentState, Event) -> NextState`.
*   **Effects:** Actions (like writing a table) happen *on entry* to a state, and their results emit **Events**.

### 2.2 Bounded Stochastic Executors (Exceptions Only)

LLMs are removed from the critical path. They exist only in **Exception Handlers**.

*   **Happy Path:** Deterministic Python code (promoted from DEV).
*   **Exception Path:** If a deterministic handler fails (e.g., `SchemaMismatchError`), the FSM invokes a **Bounded Remediation Agent**.
    *   *Constraints:* Strict budget (retries, tokens), read-only access to prod data, sandboxed proposal generation.
    *   *Outcome:* The agent produces a *fix* (e.g., a DDL patch), which is applied, then the FSM retries the deterministic step.

---

## 3. ADK Components: Plugin vs. Callback

**Synthesis Strategy:**
*   **Plugins (Global):** ChatGPT’s definition. Plugins apply to *every* agent and enforce system-wide invariants.
*   **Callbacks (Local):** Restricted to agent-specific logging or minor behavior tuning.

### 3.1 Required ADK Plugins

These plugins run "Above" the agents and cannot be overridden by agent prompts.

1.  **LeasePlugin:** Checks lease validity before every step. Aborts run if lease is lost.
2.  **ToolGatingPlugin:**
    *   *DEV:* Allow most tools (warn on destructive).
    *   *PROD:* Block all tools except whitelisted "safe" getters and registered handlers.
3.  **ContractPlugin:** Validates that every Agent response and Tool output conforms to its registered Pydantic schema *before* it can affect the state.
4.  **ObservabilityPlugin:** Emits structured events to a dedicated `agent_telemetry` table (separate from application logs).

---

## 4. Persistence Model: Tables & Capsules

**Synthesis Strategy:**
*   **Schema:** Adopts ChatGPT’s normalized table structure for governance.
*   **Content:** Embeds Claude’s rich `SessionCapsule` payload into the `run_state` table.

### 4.1 Unity Catalog Schema

**`silo_dev_rs.rlm_workspace`**

1.  **`jira_board_clone`**: (Source of Truth for Work)
    *   `task_id`, `status`, `priority`, `rlm_status` (pending/processing/done).
2.  **`work_leases`**: (Concurrency Control)
    *   `task_id`, `worker_id`, `lease_expires_at`.
3.  **`run_state`**: (The Active "Brain")
    *   `run_id`, `task_id`, `fsm_state`, `mode` (DEV/PROD).
    *   `session_capsule`: **Struct** (Nested content from Claude’s design: `current_objective`, `key_decisions`, `failed_attempts`, `artifact_refs`).
4.  **`artifact_registry`**: (File Tracking)
    *   `run_id`, `artifact_path` (Volumes URL), `hash`, `type` (notebook/script/model).
5.  **`fsm_registry`**: (Governance & Versioning)
    *   `workflow_name`, `version`, `is_active`, `definition_json`.

### 4.2 State Management Rules

*   **Updates:** Always use `MERGE` based on `run_id` to prevent race conditions.
*   **Source of Truth:** The Tables are authoritative. The Agent memory is just a cache of the Table state.

---

## 5. Promotion: DEV → PROD

**Synthesis Strategy:**
*   **Signals:** Uses Claude’s "Convergence Scoring" (code stability, output match rate).
*   **Gate:** Uses ChatGPT’s "Registry & Mechanical Gate" model.

### 5.1 The Promotion Pipeline

Promotion is **not automatic**. It is a system process triggered when convergence signals are met.

1.  **Signal Accumulation:**
    *   The RLM loop calculates a **Convergence Score** (0.0 - 1.0) based on:
        *   *Stability:* Has the generated code changed in the last 3 iterations?
        *   *Success:* Do tests pass consistently?
        *   *Determinism:* Do re-runs produce identical data checksums?
2.  **Recommendation:** When Score > Threshold (e.g., 0.9), the RLM proposes a promotion.
3.  **Mechanical Gate:**
    *   Extract the stable code blocks.
    *   Wrap them in the **FSM Skeleton** (from Section 2).
    *   Register this new FSM version in `fsm_registry` (State: `CANDIDATE`).
4.  **Validation:** A separate "Canary" job runs the Candidate FSM against a test dataset.
5.  **Activation:** If Canary passes, update `fsm_registry` (State: `ACTIVE`). The Poller (Section 1) will now route this Task Type to the new FSM.

---
---

# Appendix A: Detailed Implementation Spec

*The following sections are extracted directly from the source documents to serve as the canonical implementation reference.*

## A.1 RLM Loop Implementation
**Source:** `chatGPT52_v1` (Polling) & `claudeO45_v1` (Loop Logic)

### Poll Loop (Outermost)
> * **Poll** `jira_board_clone` (a UC Delta table that mirrors Jira issues/status/priority).
> * **Claim** an issue by writing a **lease** (optimistic update) to prevent duplicate work.
> * **Spawn/continue** a `run_id` with durable state (also in UC Delta).
> * **Execute** either:
>   * **DEV/RLM loop** (explore → generate → run → parse → fix), or
>   * **PROD/FSM runner** (deterministic state progression, idempotent actions).

### RLM Control Loop (Single Iteration)
```
┌────────────────────────────────────────────────────────────────────────────┐
│                         RLM ITERATION CYCLE                                │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐          │
│  │ 1. LOAD  │───▶│ 2. PLAN   │───▶│ 3. WRITE │─ ─▶│ 4. EXEC   │          │
│  │ CAPSULE  │     │          │     │ NOTEBOOK │     │ NOTEBOOK │          │
│  └──────────┘     └──────────┘     └──────────┘     └──────────┘          │
│       │                │                │                │                 │
│       ▼                ▼                ▼                ▼                 │
│  Load compact     Base agent       Wheel task       Lakeflow runs        │
│  state summary    decides next     writes/updates   notebook as          │
│  from Delta       computation      notebook code    downstream task      │
│                                                                            │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐          │
│  │ 8. SAVE  │◀───│ 7. MERGE  │◀───│ 6. PARSE │◀───│ 5. ROUTE │          │
│  │ CAPSULE  │     │          │     │          │     │ OUTPUT   │          │
│  └──────────┘     └──────────┘     └──────────┘     └──────────┘          │
│       │                │                │                │                 │
│       ▼                ▼                ▼                ▼                 │
│  Persist updated  Merge signals    Ephemeral        Short → Base agent  │
│  session capsule  into compact     agents parse     Long → Ephemeral    │
│  to Delta         state summary    long outputs     agents              │
└────────────────────────────────────────────────────────────────────────────┘
```

### Session Capsule Schema
```python
@dataclass
class SessionCapsule:
    """Compact state summary for Base agent (replaces full chat history)."""
    # Identity
    run_id: str
    jira_task_id: str
    iteration: int
    # Intent
    original_requirement: str
    current_objective: str
    constraints: list[str]
    # Progress
    completed_steps: list[str]
    current_step: str
    pending_steps: list[str]
    # Memory
    key_decisions: list[str]
    failed_approaches: list[str]
    discovered_schemas: dict
    # Artifacts
    generated_notebooks: list[str]
    generated_scripts: list[str]
    output_tables: list[str]
    # FSM Synthesis
    deterministic_code_blocks: list[str]
    stochastic_code_blocks: list[str]
    convergence_score: float
```

---

## A.2 PROD FSM & Governance
**Source:** `claudeO45_v1` (FSM Skeleton) & `chatGPT52_v1` (Governance)

### Generated FSM Code Structure (Skeleton)
```python
class PipelineState(Enum):
    INIT = auto()
    SCHEMA_LOADED = auto()
    DATA_VALIDATED = auto()
    # ... Error states ...
    SCHEMA_ERROR = auto()

class PipelineEvent(Enum):
    SCHEMA_LOADED = auto()
    VALIDATION_PASSED = auto()
    # ...

class CustomerChurnPipeline:
    TRANSITIONS = {
        (PipelineState.INIT, PipelineEvent.SCHEMA_LOADED): PipelineState.SCHEMA_LOADED,
        # ...
        # Error recovery transitions
        (PipelineState.VALIDATION_ERROR, PipelineEvent.RETRY_SUCCEEDED): PipelineState.DATA_VALIDATED,
    }

    def transition(self, event: PipelineEvent) -> PipelineState:
        """Execute deterministic state transition."""
        key = (self.ctx.current_state, event)
        if key not in self.TRANSITIONS:
            raise InvalidTransitionError(f"No transition for {key}")

        next_state = self.TRANSITIONS[key]
        # Log, Update externalized state, Execute entry action
        return next_state

    def _invoke_error_handler(self, handler_type: str):
        """Invoke bounded stochastic executor (ADK agent) for error recovery."""
        agent = self.error_handler(handler_type)
        # Agent is bounded: max_tokens, max_tool_calls, timeout
        result = agent.run(context=self.ctx, max_iterations=3, timeout_seconds=300)
```

### Promotion Rules
> A workflow (or a state handler) is promotable when:
> * it has passed tests N times across distinct runs,
> * its actions are idempotent (artifact paths + table writes are versioned),
> * it has explicit guards + invariants,
> * it no longer needs “freeform repair” except for known, bounded classes.

---

## A.3 ADK Plugin Architecture
**Source:** `chatGPT52_v1` (Definition) & `claudeO45_v1` (Implementation)

### Plugin Mandate
> **Required plugins**
> 1. **Lease + Concurrency Plugin:** Enforces “only one owner per run / issue”.
> 2. **Policy + Tool Gating Plugin:** DEV: “warn / allow”, PROD: “block unless whitelisted”.
> 3. **Schema/Contract Plugin:** Validates every tool output / artifact metadata / state payload.
> 4. **Observability Plugin:** Emits structured events for: transitions, tool calls, model calls, retries.
> 5. **Budget Plugin:** Caps iterations, token usage, job submissions.

### Plugin Code Example
```python
class PolicyEnforcementPlugin(PluginModule):
    """Global policy enforcement across all agents."""
    def __init__(self, mode: str):
        self.mode = mode
        self.blocked_tools = self._load_blocked_tools()

    def before_tool_callback(self, tool, context, args):
        """Enforce tool access policies."""
        if self.mode == "production":
            if tool.name in self.blocked_tools:
                raise PolicyViolationError(f"Tool {tool.name} blocked in production")
        self._audit_log(tool, context, args)

    def after_tool_callback(self, tool, context, result):
        """Validate tool outputs against contracts."""
        if hasattr(tool, "output_schema"):
            self._validate_output(result, tool.output_schema)
```

---

## A.4 Persistence Model
**Source:** `chatGPT52_v1` (Table Structure) & `claudeO45_v1` (Detailed Schema)

### Normalized Table Set
> 1. **`jira_board_clone`**: Issue_id, status, priority, labels, spec fields
> 2. **`work_leases`**: issue_id, lease_owner, lease_expires_at, heartbeat_ts
> 3. **`runs`**: (authoritative run state) run_id, issue_id, mode(dev/prod), current_state
> 4. **`iterations`**: (RLM loop history) run_id, iter_n, proposed_change_ref, outcome
> 5. **`artifacts_index`**: run_id, state, artifact_type, path, content_hash
> 6. **`fsm_registry`**: workflow_name, version, promotion_status

### Delta Table Definition (Example)
```sql
CREATE TABLE silo_dev_rs.rlm_workspace.run_state (
    run_id STRING NOT NULL,
    jira_task_id STRING NOT NULL,
    current_state STRING NOT NULL,
    -- ...
    inputs MAP<STRING, STRING>,
    checkpoints ARRAY<STRUCT<state: STRING, timestamp: TIMESTAMP, artifact_paths: ARRAY<STRING>>>,
    convergence_score FLOAT
)
USING DELTA
PARTITIONED BY (status)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

---

## A.5 Convergence & Promotion Logic
**Source:** `claudeO45_v1` (Scoring)

### Convergence Criteria Class
```python
class ConvergenceCriteria:
    """Determines when code is stable enough to synthesize into FSM."""
    MIN_STABLE_RUNS: int = 3
    MAX_CODE_DIFF_RATIO: float = 0.05
    ACCEPTANCE_CRITERIA_PASS_RATE: float = 1.0

    def is_converged(self, iterations: list[IterationResult]) -> bool:
        recent = iterations[-self.MIN_STABLE_RUNS:]
        return (
            len(recent) >= self.MIN_STABLE_RUNS
            and self._code_is_stable(recent)
            and self._outputs_match(recent)
            and self._acceptance_criteria_pass(recent)
        )
```

https://google.github.io/adk-docs/tools-custom/#tool-context

The ToolContext provides access to several key pieces of information and control levers:

state: State: Read and modify the current session's state. Changes made here are tracked and persisted.

actions: EventActions: Influence the agent's subsequent actions after the tool runs (e.g., skip summarization, transfer to another agent).

function_call_id: str: The unique identifier assigned by the framework to this specific invocation of the tool. Useful for tracking and correlating with authentication responses. This can also be helpful when multiple tools are called within a single model response.

function_call_event_id: str: This attribute provides the unique identifier of the event that triggered the current tool call. This can be useful for tracking and logging purposes.

auth_response: Any: Contains the authentication response/credentials if an authentication flow was completed before this tool call.

Access to Services: Methods to interact with configured services like Artifacts and Memory.

https://google.github.io/adk-docs/tools-custom/#controlling-agent-flow
