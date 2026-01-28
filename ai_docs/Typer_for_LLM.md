# Requirements: Building Typer-Based Tools for LLM Tool Calling

This document defines robust, production-oriented requirements for exposing **Typer** commands/functions as **LLM-callable tools** (OpenAI-compatible function/tool calling). It focuses on correctness, safety, and maintainability, not just a proof-of-concept.

---

## 0) Scope & Goals

### Goal
Enable an LLM to call selected capabilities from a Typer-based codebase safely and reliably by:
- Publishing **tool schemas** the model can use.
- Validating and executing tool calls.
- Returning tool results back into the conversation loop until a final answer is produced.

### Non-Goals
- Exposing an entire CLI verbatim (e.g., `--help` output, arbitrary subcommands) without curation.
- Allowing the LLM to run arbitrary code, arbitrary shell commands, or unbounded filesystem/network operations.

---

## 1) Terminology

- **Tool**: An LLM-callable function (OpenAI-style `type: "function"` tool).
- **Typer command**: A function registered with a Typer app.
- **Dispatcher / Router**: The component that maps tool calls → validated execution → tool result message.
- **Schema**: JSON Schema for the tool parameters (and optionally tool output).

---

## 2) Architectural Requirements (High-Level)

### R1 — Curated Tool Surface (Allowlist)
- Only explicitly allowlisted tools/commands may be exposed to the LLM.
- Tools MUST have stable names (no reflection-based exposure of all module functions).
- The allowlist MUST be the single source of truth for exposure.

**Acceptance criteria**
- There is a deterministic `available_tools` registry (e.g., dict or class registry).
- Unrecognized tool names are rejected with a structured error result (not executed).

---

## 3) Tool Definition Requirements

### R2 — Semantic Metadata (LLM Usability)
Each tool MUST include:
- A concise **description** (from docstring or explicit metadata).
- Parameter descriptions for every argument/option.
- Clear behavior notes: side effects, expected formats, units, constraints, and failure modes.

**Guidelines**
- Prefer `Annotated[..., typer.Option(help="...")]` / `typer.Argument(help="...")` to keep CLI help and tool schema descriptions aligned.
- Include examples in docstrings where ambiguity exists (dates, file paths, IDs, units).

**Acceptance criteria**
- Tool definitions contain descriptions sufficient for the model to choose and call the tool without guessing formats.

---

## 4) Schema Generation Requirements (Do Not Hand-Wave This)

### R3 — Explicit Schema Strategy
Typer is Click-based and does not inherently emit an OpenAI tool JSON schema. You MUST define and implement one of these strategies:

**Strategy A (recommended): Pydantic-first**
- Define a Pydantic model for tool inputs (and optionally outputs), or use Pydantic’s function validation (`validate_call`) to derive schema.
- Generate JSON Schema from Pydantic models and convert to OpenAI tool schema format.

**Strategy B: Typer/Click introspection**
- Introspect Typer command signatures + Click parameter metadata and map them into JSON Schema, including constraints (choices, bounds, patterns).
- This is more complex and MUST be tested thoroughly.

**Acceptance criteria**
- Schema generation is programmatic (not hand-written per tool, except for exceptional cases).
- Generated schema includes:
  - correct types
  - enums/choices where applicable
  - required vs optional fields consistent with runtime behavior
  - `additionalProperties` policy (see R7)

---

## 5) Parameter Handling Requirements

### R4 — Parameter Filtering (Typer Internals)
Tool schemas MUST NOT expose internal/injected parameters to the model, including:
- `ctx: typer.Context`
- `self` / bound instance parameters
- dependency-injection handles (e.g., `typer.Depends`)
- internal callbacks/global options unless explicitly designed for LLM use

If such parameters are needed at runtime, the dispatcher MUST inject them server-side.

**Acceptance criteria**
- The schema only contains user-provided parameters.
- Calls succeed without the model providing `ctx`/DI artifacts.

---

## 6) Validation Requirements (Authoritative Server-Side)

### R5 — Strict, Central Validation Before Execution
All tool calls MUST be validated server-side before execution:
- Parse tool arguments as JSON.
- Validate shape, types, and constraints using Pydantic (or equivalent).
- Decide and document coercion policy:
  - **Strict**: reject `"5"` for int.
  - **Coercive**: accept and coerce where safe.
- Apply defaults in code (unless you choose the “model must send defaults explicitly” policy).

**Acceptance criteria**
- No tool executes with unvalidated inputs.
- Validation errors produce structured tool error outputs.

---

## 7) “Strict Mode” / JSON Schema Policy Requirements

### R6 — Do Not Rely on Model-Side “Strictness”
If the API/provider supports a strict mode flag, you MAY enable it, but:
- Server-side validation remains the source of truth.
- Tool execution safety must not depend on the model complying perfectly.

**Acceptance criteria**
- Disabling provider strict mode does not compromise validation or safety.

### R7 — `additionalProperties` and Required/Optional Policy
You MUST define and enforce a consistent policy for:
- `additionalProperties` (recommended: `false` for tool parameter objects to reduce hallucinated keys).
- Required vs optional fields:
  - Optional fields MAY be omitted and defaulted in code.
  - Do NOT mark everything required “because strict mode”—that’s a policy choice, not a requirement.

**Acceptance criteria**
- Schema and runtime behavior match:
  - If a field is optional in runtime, it is not required in schema.
  - Unknown fields are rejected (when `additionalProperties: false` is used).

---

## 8) Dispatch/Execution Requirements

### R8 — Robust Router / Dispatcher
The dispatcher MUST:
1. Receive tool calls from the model response.
2. Parse and validate arguments.
3. Map tool name → function/command.
4. Execute the tool with injected dependencies/context as needed.
5. Capture outputs and errors into a stable tool result format.

**Acceptance criteria**
- Unknown tool name → structured error.
- Malformed JSON args → structured error.
- Validation failures → structured error.
- Tool exceptions → structured error (no crash).

### R9 — Stdout/Stderr Handling (Typer Reality)
Because Typer commands often print instead of returning values, you MUST implement one of:
- **Return-first rule**: LLM-exposed tools must return JSON-serializable results; printing is disallowed in tool layer.
- **Output capture**: wrap execution in stdout/stderr capture and return captured output.

**Acceptance criteria**
- Tool results presented back to the model contain the actual information (not only server logs).

### R10 — Exit/Termination Handling
Typer/Click may raise `typer.Exit` or `SystemExit`. Dispatcher MUST:
- Catch and convert these to structured tool results (success or error as appropriate).
- Prevent host process termination.

**Acceptance criteria**
- A tool cannot terminate the agent process.

### R11 — Async + Concurrency Support (if applicable)
If tools are I/O-bound or you expect multiple tool calls per turn:
- Support async tool functions and an async client, OR define a sync-only constraint.
- If the model requests multiple tool calls in one message, define:
  - sequential execution policy, or
  - bounded parallel execution policy (with limits)

**Acceptance criteria**
- Concurrency does not exceed configured limits.
- Tools respect timeouts.

---

## 9) Tool Result Contract Requirements

### R12 — Stable, Machine-Readable Tool Results
Tool outputs SHOULD be JSON-serializable objects with stable shape, e.g.:

```json
{ "status": "ok", "data": { ... }, "meta": { ... } }
```

Errors SHOULD be structured, e.g.:

```json
{ "status": "error", "error": { "type": "ValidationError", "message": "...", "details": {...} } }
```

If stdout capture is used, include it in a field (and cap size).

**Acceptance criteria**
- The model can reliably interpret results without scraping unstructured text.
- Large outputs are truncated with explicit markers and a note in `meta`.

---

## 10) Conversation Loop Requirements (Full Agent Loop)

### R13 — Multi-Turn Tool Loop (Round-Trip)
Implementation MUST support:
1. User message → model call with tool schemas.
2. If tool calls exist:
   - execute each tool call
   - append a tool message for each tool call (including correct `tool_call_id`)
3. Call model again with updated messages.
4. Repeat until model returns a normal assistant message.

**Acceptance criteria**
- Complex requests requiring multiple tool calls complete successfully.
- Tool outputs are always returned using the provider-required message format.

---

## 11) Security Requirements (Mandatory)

### R14 — Input Safety & Policy Controls
Even with type validation, semantic safety is required. You MUST implement:
- Allowlist of tools (R1).
- Guardrails for dangerous parameters:
  - file paths (path traversal, allowed roots)
  - URLs (SSRF protection; allowed domains)
  - command-like strings (no shell execution unless explicitly sandboxed)
- Max lengths for strings/lists; max numeric ranges where relevant.

**Acceptance criteria**
- Tools cannot access disallowed filesystem locations or internal network endpoints by default.
- Inputs exceeding limits are rejected.

### R15 — Secrets Management & Data Minimization
- Tools MUST use server-side credentials (env vars/secret manager).
- Tool outputs MUST NOT include secrets.
- Logs MUST redact sensitive values.

**Acceptance criteria**
- No raw secrets appear in tool outputs or logs.

### R16 — Resource Limits & Timeouts
- Per-tool timeout.
- Global request budget (max tool calls per user request).
- Output size caps (bytes/lines).
- Rate limiting to prevent runaway loops or abuse.

**Acceptance criteria**
- Misbehaving tools or repeated calls cannot exhaust CPU/memory or external quotas.

### R17 — Side Effects, Idempotency, and Confirmation
For any tool that mutates state (writes files, changes DB, sends emails, deletes resources):
- Tool description MUST declare side effects.
- Provide a `dry_run` option or preview mode where feasible.
- Require explicit confirmation step for destructive actions.
- Consider idempotency keys for external operations.

**Acceptance criteria**
- Destructive actions are not performed without explicit confirmation or policy approval.

---

## 12) Observability & Auditing Requirements

### R18 — Structured Logging and Tracing
Log at minimum:
- tool name
- request/correlation ID
- validated arguments (redacted)
- start/end timestamps, duration
- success/error status + error type
- output size (not necessarily full output)

**Acceptance criteria**
- Incidents can be diagnosed without reproducing with full raw data.

### R19 — Audit Trail for Side-Effecting Tools
Maintain an audit record for mutations:
- who/what initiated (user/session)
- what changed
- when
- outcome

**Acceptance criteria**
- State changes are attributable and reviewable.

---

## 13) Testing Requirements

### R20 — Unit Tests for Schema, Validation, Dispatch
You MUST test:
- Schema generation correctness (required/optional, enums, constraints, `additionalProperties`)
- Validation behavior (strict vs coercive)
- Unknown tool name handling
- Malformed JSON argument handling
- Exception and `typer.Exit` handling
- stdout capture behavior (if used)

**Acceptance criteria**
- CI runs these tests with deterministic fixtures.

### R21 — Integration Tests for Full Tool Loop
Test end-to-end:
- model → tool call → tool result message → model final answer
- multi-tool-call in one turn
- retries after validation errors (if supported)

**Acceptance criteria**
- The loop terminates and returns a final assistant response under normal conditions.

---

## 14) Compatibility Requirements

### R22 — Provider/API Contract Pinning + Adapters
OpenAI-compatible APIs differ in:
- tool call field shapes
- message formats for tool results
- strict-mode flags

You MUST:
- pin a target contract (provider + API version)
- implement adapter functions to parse tool calls and format tool result messages correctly

**Acceptance criteria**
- Swapping providers requires changing only adapter code, not tool definitions.

---

## 15) Recommended Implementation Conventions (Non-Mandatory but Strongly Suggested)

- Separate **tool layer** (pure functions returning structured data) from **CLI layer** (Typer commands that print).
- Prefer returning dicts/models; keep printed output for human CLI use only.
- Use Pydantic models for inputs/outputs to keep schema, validation, and docs aligned.
- Keep tool descriptions short but specific; add examples for formats.

---

## 16) Minimal Checklist (Quick Gate)

A Typer command/tool is “LLM-ready” only if:

- [ ] It’s explicitly allowlisted
- [ ] It has clear docstring + parameter descriptions
- [ ] Its parameter schema is generated programmatically and matches runtime behavior
- [ ] Inputs are validated server-side before execution
- [ ] It returns structured output (or stdout is captured)
- [ ] Exceptions and `typer.Exit`/`SystemExit` are safely handled
- [ ] The tool result is posted back to the model in a multi-turn loop
- [ ] Timeouts, rate limits, and output caps are enforced
- [ ] Secrets are never returned or logged
- [ ] Tests cover schema + dispatch + loop

---

If you want, I can also provide a reference folder layout (tool registry, schema generation module, dispatcher, adapters) and a template tool using Pydantic input/output models that can be used both as a Typer command and an LLM tool.