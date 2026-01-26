#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "databricks-sdk>=0.20.0",
#   "click>=8.1.0",
# ]
# ///
"""Generate RLM workflow flowcharts and structured logs from ADK telemetry.

This script queries silo_dev_rs.adk.events and silo_dev_rs.adk.llm_context_telemetry
to produce:
1. structlog-style JSONL timeline (one JSON object per step)
2. Markdown report with embedded Mermaid flowchart
3. Human-readable summary of what happened in an RLM invocation

Examples:
    # Generate flowchart for a specific session
    uv run scripts/databricks_rlm_flow.py --session-id test_level_16_1769446985

    # Generate for a specific invocation
    uv run scripts/databricks_rlm_flow.py --session-id test_level_16_1769446985 --invocation-id e-dba58506-...

    # Output to files
    uv run scripts/databricks_rlm_flow.py --session-id test_level_16_1769446985 -o report.md --jsonl timeline.jsonl

    # List recent sessions
    uv run scripts/databricks_rlm_flow.py list-sessions --limit 5
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import click

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_PROFILE = "rstanhope"

# =============================================================================
# Data Models
# =============================================================================

@dataclass
class FlowStep:
    """A single step in the RLM workflow timeline."""
    session_id: str
    invocation_id: str
    ts: str
    seq: int
    actor: str  # agent_name or author
    kind: str   # event_type: model_message, tool_call, tool_response, transfer, error, system
    name: Optional[str] = None  # tool name, agent name, etc.
    summary: str = ""
    state_delta: dict = field(default_factory=dict)  # {set: [...], deleted: [...]}
    artifacts: dict = field(default_factory=dict)     # artifact_id, run_url, etc.
    tokens: dict = field(default_factory=dict)        # prompt_token_count, etc.
    links: dict = field(default_factory=dict)         # snapshot paths, etc.
    error: Optional[str] = None
    raw: dict = field(default_factory=dict)           # original data for debugging


@dataclass 
class FlowSummary:
    """Summary statistics for an RLM flow."""
    session_id: str
    invocation_id: str
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    duration_seconds: float = 0.0
    total_events: int = 0
    total_llm_calls: int = 0
    total_tool_calls: int = 0
    total_errors: int = 0
    total_prompt_tokens: int = 0
    total_completion_tokens: int = 0
    agents_involved: list = field(default_factory=list)
    tools_used: list = field(default_factory=list)
    iterations: int = 0
    artifacts_created: list = field(default_factory=list)
    final_status: str = "unknown"


# =============================================================================
# SQL Execution (reused from execute_sql.py)
# =============================================================================

def get_sql_warehouse_id(client) -> str:
    """Get a SQL warehouse ID to execute statements."""
    warehouses = list(client.warehouses.list())
    
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING":
            logger.debug(f"Using SQL warehouse: {wh.name} (id: {wh.id})")
            return wh.id
    
    for wh in warehouses:
        if wh.state and wh.state.value == "STOPPED":
            logger.info(f"Starting SQL warehouse: {wh.name}")
            client.warehouses.start(wh.id)
            for _ in range(30):
                status = client.warehouses.get(wh.id)
                if status.state and status.state.value == "RUNNING":
                    return wh.id
                time.sleep(10)
    
    raise RuntimeError("No SQL warehouse available.")


def execute_sql_query(
    sql: str,
    profile: str = DEFAULT_PROFILE,
) -> tuple[list[str], list[list]]:
    """Execute SQL and return (columns, rows)."""
    from databricks.sdk import WorkspaceClient
    
    client = WorkspaceClient(profile=profile)
    warehouse_id = get_sql_warehouse_id(client)
    
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
    )
    
    if response.status and response.status.state.value == "SUCCEEDED":
        columns = []
        if response.manifest and response.manifest.schema:
            columns = [col.name for col in response.manifest.schema.columns]
        
        rows = []
        if response.result and response.result.data_array:
            rows = response.result.data_array
        
        return columns, rows
    else:
        error = response.status.error if response.status else "Unknown error"
        raise RuntimeError(f"SQL execution failed: {error}")


# =============================================================================
# Data Fetching
# =============================================================================

def fetch_events(
    session_id: str,
    invocation_id: Optional[str],
    profile: str,
) -> list[dict]:
    """Fetch events from silo_dev_rs.adk.events."""
    where_clause = f"session_id = '{session_id}'"
    if invocation_id:
        where_clause += f" AND invocation_id = '{invocation_id}'"
    
    sql = f"""
    SELECT
        session_id,
        invocation_id,
        event_id,
        author,
        event_timestamp,
        sequence_num,
        event_data_json,
        state_delta_json,
        has_state_delta
    FROM silo_dev_rs.adk.events
    WHERE {where_clause}
    ORDER BY sequence_num ASC
    """
    
    columns, rows = execute_sql_query(sql, profile)
    
    events = []
    for row in rows:
        event = dict(zip(columns, row))
        # Parse JSON fields
        if event.get("event_data_json"):
            try:
                event["event_data"] = json.loads(event["event_data_json"])
            except json.JSONDecodeError:
                event["event_data"] = {}
        if event.get("state_delta_json"):
            try:
                event["state_delta"] = json.loads(event["state_delta_json"])
            except json.JSONDecodeError:
                event["state_delta"] = {}
        events.append(event)
    
    return events


def fetch_llm_telemetry(
    session_id: str,
    invocation_id: Optional[str],
    profile: str,
) -> list[dict]:
    """Fetch LLM context telemetry from the view."""
    where_clause = f"session_id = '{session_id}'"
    if invocation_id:
        where_clause += f" AND invocation_id = '{invocation_id}'"
    
    sql = f"""
    SELECT
        session_id,
        invocation_id,
        agent_name,
        llm_call_index,
        model_name,
        ts_before,
        ts_after,
        ts_error,
        latency_ms,
        call_status,
        state_keys_count,
        state_token_estimate,
        state_token_estimate_persistable_only,
        prompt_token_count,
        candidates_token_count,
        request_last_message_role,
        request_last_message_token_estimate,
        SUBSTR(request_last_message_preview, 1, 200) AS request_last_message_preview,
        request_snapshot_path,
        response_snapshot_path,
        error_code,
        error_message
    FROM silo_dev_rs.adk.llm_context_telemetry
    WHERE {where_clause}
    ORDER BY ts_before ASC
    """
    
    columns, rows = execute_sql_query(sql, profile)
    
    telemetry = []
    for row in rows:
        record = dict(zip(columns, row))
        telemetry.append(record)
    
    return telemetry


def list_recent_sessions(profile: str, limit: int = 10) -> list[dict]:
    """List recent sessions with summary info."""
    sql = f"""
    SELECT
        session_id,
        invocation_id,
        MIN(event_timestamp) AS start_time,
        MAX(event_timestamp) AS end_time,
        COUNT(*) AS event_count,
        COUNT(DISTINCT author) AS agent_count
    FROM silo_dev_rs.adk.events
    GROUP BY session_id, invocation_id
    ORDER BY MAX(event_timestamp) DESC
    LIMIT {limit}
    """
    
    columns, rows = execute_sql_query(sql, profile)
    return [dict(zip(columns, row)) for row in rows]


# =============================================================================
# Event Parsing & Flow Step Generation
# =============================================================================

def parse_event_to_step(event: dict) -> FlowStep:
    """Convert an ADK event to a FlowStep."""
    event_data = event.get("event_data", {})
    content = event_data.get("content", {})
    actions = event_data.get("actions", {})
    parts = content.get("parts", [])
    
    # Determine event kind
    kind = "unknown"
    name = None
    summary = ""
    artifacts = {}
    
    # Check for function calls
    for part in parts:
        if part.get("functionCall"):
            kind = "tool_call"
            fc = part["functionCall"]
            name = fc.get("name", "unknown_tool")
            args_preview = str(fc.get("args", {}))[:100]
            summary = f"Called {name}({args_preview}...)"
            break
        elif part.get("functionResponse"):
            kind = "tool_response"
            fr = part["functionResponse"]
            name = fr.get("name", "unknown_tool")
            response = fr.get("response", {})
            status = response.get("status", "unknown")
            summary = f"{name} returned: {status}"
            
            # Extract artifact info
            if "artifact_id" in response:
                artifacts["artifact_id"] = response["artifact_id"]
            if "iteration" in response:
                artifacts["iteration"] = response["iteration"]
            break
        elif part.get("text"):
            text = part["text"]
            kind = "model_message"
            summary = text[:150] + ("..." if len(text) > 150 else "")
            break
    
    # Check for transfer
    if actions.get("transferToAgent"):
        kind = "transfer"
        name = actions["transferToAgent"]
        summary = f"Transfer to agent: {name}"
    
    # Check for escalate
    if actions.get("escalate"):
        kind = "escalate"
        summary = "Escalate (exit loop)"
    
    # Parse state delta
    state_delta_info = {"set": [], "deleted": []}
    raw_state_delta = event.get("state_delta", {})
    for key, value in raw_state_delta.items():
        if value is None:
            state_delta_info["deleted"].append(key)
        else:
            state_delta_info["set"].append(key)
    
    return FlowStep(
        session_id=event.get("session_id", ""),
        invocation_id=event.get("invocation_id", ""),
        ts=event.get("event_timestamp", ""),
        seq=int(event.get("sequence_num", 0)),
        actor=event.get("author", "unknown"),
        kind=kind,
        name=name,
        summary=summary,
        state_delta=state_delta_info,
        artifacts=artifacts,
        raw={"event_id": event.get("event_id")},
    )


def enrich_with_telemetry(steps: list[FlowStep], telemetry: list[dict]) -> list[FlowStep]:
    """Enrich flow steps with LLM telemetry data."""
    # Create lookup by (invocation_id, agent_name, approximate timestamp)
    telemetry_by_agent = {}
    for t in telemetry:
        key = (t.get("invocation_id"), t.get("agent_name"))
        if key not in telemetry_by_agent:
            telemetry_by_agent[key] = []
        telemetry_by_agent[key].append(t)
    
    # Match telemetry to steps
    for step in steps:
        key = (step.invocation_id, step.actor)
        if key in telemetry_by_agent:
            agent_telemetry = telemetry_by_agent[key]
            # Find closest telemetry record by timestamp
            for t in agent_telemetry:
                if t.get("ts_before") and step.ts:
                    # Simple heuristic: if timestamps are close, associate them
                    step.tokens = {
                        "prompt_token_count": t.get("prompt_token_count"),
                        "candidates_token_count": t.get("candidates_token_count"),
                        "state_token_estimate": t.get("state_token_estimate"),
                        "state_token_estimate_persistable_only": t.get("state_token_estimate_persistable_only"),
                        "latency_ms": t.get("latency_ms"),
                        "llm_call_index": t.get("llm_call_index"),
                    }
                    step.links = {
                        "request_snapshot_path": t.get("request_snapshot_path"),
                        "response_snapshot_path": t.get("response_snapshot_path"),
                    }
                    if t.get("error_code") or t.get("error_message"):
                        step.error = f"{t.get('error_code')}: {t.get('error_message')}"
                    break
    
    return steps


# =============================================================================
# Summary Generation
# =============================================================================

def generate_summary(steps: list[FlowStep], telemetry: list[dict]) -> FlowSummary:
    """Generate summary statistics from flow steps."""
    if not steps:
        return FlowSummary(session_id="", invocation_id="")
    
    summary = FlowSummary(
        session_id=steps[0].session_id,
        invocation_id=steps[0].invocation_id,
        start_time=steps[0].ts if steps else None,
        end_time=steps[-1].ts if steps else None,
        total_events=len(steps),
    )
    
    agents = set()
    tools = set()
    artifacts = []
    errors = 0
    
    for step in steps:
        agents.add(step.actor)
        if step.kind in ("tool_call", "tool_response") and step.name:
            tools.add(step.name)
        if step.artifacts.get("artifact_id"):
            artifacts.append(step.artifacts["artifact_id"])
        if step.error:
            errors += 1
    
    summary.agents_involved = sorted(agents)
    summary.tools_used = sorted(tools)
    summary.artifacts_created = artifacts
    summary.total_errors = errors
    summary.total_tool_calls = sum(1 for s in steps if s.kind == "tool_call")
    
    # Aggregate telemetry
    for t in telemetry:
        summary.total_llm_calls += 1
        if t.get("prompt_token_count"):
            try:
                summary.total_prompt_tokens += int(t["prompt_token_count"])
            except (ValueError, TypeError):
                pass
        if t.get("candidates_token_count"):
            try:
                summary.total_completion_tokens += int(t["candidates_token_count"])
            except (ValueError, TypeError):
                pass
    
    # Calculate duration
    if summary.start_time and summary.end_time:
        try:
            start = datetime.fromisoformat(summary.start_time.replace("Z", "+00:00"))
            end = datetime.fromisoformat(summary.end_time.replace("Z", "+00:00"))
            summary.duration_seconds = (end - start).total_seconds()
        except (ValueError, TypeError):
            pass
    
    # Determine iterations from artifact count
    summary.iterations = len(set(artifacts))
    
    # Determine final status
    has_exit = any(s.kind == "escalate" or (s.name == "exit_loop") for s in steps)
    summary.final_status = "completed" if has_exit else ("error" if errors > 0 else "in_progress")
    
    return summary


# =============================================================================
# Output Generation
# =============================================================================

def generate_jsonl(steps: list[FlowStep]) -> str:
    """Generate structlog-style JSONL output."""
    lines = []
    for step in steps:
        record = {
            "session_id": step.session_id,
            "invocation_id": step.invocation_id,
            "ts": step.ts,
            "seq": step.seq,
            "actor": step.actor,
            "kind": step.kind,
            "name": step.name,
            "summary": step.summary,
        }
        if step.state_delta.get("set") or step.state_delta.get("deleted"):
            record["state_delta"] = step.state_delta
        if step.artifacts:
            record["artifacts"] = step.artifacts
        if step.tokens:
            record["tokens"] = {k: v for k, v in step.tokens.items() if v is not None}
        if step.links:
            record["links"] = {k: v for k, v in step.links.items() if v}
        if step.error:
            record["error"] = step.error
        
        lines.append(json.dumps(record, default=str))
    
    return "\n".join(lines)


def generate_mermaid(steps: list[FlowStep], summary: FlowSummary) -> str:
    """Generate Mermaid flowchart from steps."""
    lines = ["```mermaid", "flowchart TD"]
    
    # Style definitions
    lines.append("    %% Style definitions")
    lines.append("    classDef agent fill:#e1f5fe,stroke:#01579b")
    lines.append("    classDef tool fill:#fff3e0,stroke:#e65100")
    lines.append("    classDef transfer fill:#f3e5f5,stroke:#7b1fa2")
    lines.append("    classDef error fill:#ffebee,stroke:#c62828")
    lines.append("    classDef artifact fill:#e8f5e9,stroke:#2e7d32")
    lines.append("")
    
    # Track nodes and edges
    node_id = 0
    prev_node = None
    agent_nodes = {}  # Track latest node per agent
    
    for step in steps:
        node_id += 1
        node_name = f"N{node_id}"
        
        # Determine node label and style
        if step.kind == "tool_call":
            label = f"🔧 {step.name}"
            style = "tool"
        elif step.kind == "tool_response":
            if step.artifacts.get("artifact_id"):
                label = f"📦 {step.artifacts['artifact_id'][:12]}"
                style = "artifact"
            else:
                label = f"✓ {step.name}"
                style = "tool"
        elif step.kind == "transfer":
            label = f"➡️ {step.name}"
            style = "transfer"
        elif step.kind == "escalate":
            label = "🏁 Exit Loop"
            style = "transfer"
        elif step.kind == "model_message":
            # Truncate summary for display
            short_summary = step.summary[:40] + "..." if len(step.summary) > 40 else step.summary
            # Escape special characters for Mermaid
            short_summary = short_summary.replace('"', "'").replace("\n", " ")
            label = f"💬 {step.actor}"
            style = "agent"
        else:
            label = f"{step.actor}: {step.kind}"
            style = "agent"
        
        # Check for errors
        if step.error:
            label = f"❌ {label}"
            style = "error"
        
        # Escape label for Mermaid
        label = label.replace('"', "'").replace("[", "(").replace("]", ")")
        
        # Add node
        lines.append(f"    {node_name}[\"{label}\"]:::{style}")
        
        # Add edge from previous node
        if prev_node:
            # Add token info on edges if available
            edge_label = ""
            if step.tokens.get("prompt_token_count"):
                edge_label = f"|{step.tokens['prompt_token_count']}t|"
            if edge_label:
                lines.append(f"    {prev_node} -->{edge_label} {node_name}")
            else:
                lines.append(f"    {prev_node} --> {node_name}")
        
        prev_node = node_name
        agent_nodes[step.actor] = node_name
    
    lines.append("```")
    return "\n".join(lines)


def generate_markdown_report(
    steps: list[FlowStep],
    summary: FlowSummary,
    telemetry: list[dict],
) -> str:
    """Generate full Markdown report."""
    lines = []
    
    # Header
    lines.append("# RLM Workflow Flow Report")
    lines.append("")
    lines.append(f"**Session ID:** `{summary.session_id}`")
    lines.append(f"**Invocation ID:** `{summary.invocation_id}`")
    lines.append(f"**Generated:** {datetime.now().isoformat()}")
    lines.append("")
    
    # Summary section
    lines.append("## Summary")
    lines.append("")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Duration | {summary.duration_seconds:.1f}s |")
    lines.append(f"| Total Events | {summary.total_events} |")
    lines.append(f"| LLM Calls | {summary.total_llm_calls} |")
    lines.append(f"| Tool Calls | {summary.total_tool_calls} |")
    lines.append(f"| Errors | {summary.total_errors} |")
    lines.append(f"| Prompt Tokens | {summary.total_prompt_tokens:,} |")
    lines.append(f"| Completion Tokens | {summary.total_completion_tokens:,} |")
    lines.append(f"| Iterations | {summary.iterations} |")
    lines.append(f"| Final Status | {summary.final_status} |")
    lines.append("")
    
    lines.append(f"**Agents:** {', '.join(summary.agents_involved)}")
    lines.append("")
    lines.append(f"**Tools Used:** {', '.join(summary.tools_used)}")
    lines.append("")
    if summary.artifacts_created:
        lines.append(f"**Artifacts:** {', '.join(summary.artifacts_created[:5])}")
        if len(summary.artifacts_created) > 5:
            lines.append(f"  ... and {len(summary.artifacts_created) - 5} more")
        lines.append("")
    
    # Flowchart
    lines.append("## Workflow Flowchart")
    lines.append("")
    lines.append(generate_mermaid(steps, summary))
    lines.append("")
    
    # LLM Call Details
    if telemetry:
        lines.append("## LLM Call Details")
        lines.append("")
        lines.append("| # | Agent | Model | Prompt Tokens | Completion | Latency | Status |")
        lines.append("|---|-------|-------|---------------|------------|---------|--------|")
        for t in telemetry:
            idx = t.get("llm_call_index", "?")
            agent = t.get("agent_name", "?")
            model = t.get("model_name", "?")
            prompt = t.get("prompt_token_count", "?")
            comp = t.get("candidates_token_count", "?")
            latency = t.get("latency_ms", "?")
            status = t.get("call_status", "?")
            lines.append(f"| {idx} | {agent} | {model} | {prompt} | {comp} | {latency}ms | {status} |")
        lines.append("")
    
    # Timeline
    lines.append("## Event Timeline")
    lines.append("")
    lines.append("| Time | Seq | Actor | Kind | Summary |")
    lines.append("|------|-----|-------|------|---------|")
    for step in steps[:50]:  # Limit to first 50 events
        ts_short = step.ts.split("T")[1][:12] if "T" in step.ts else step.ts[:12]
        summary_short = step.summary[:60].replace("|", "\\|").replace("\n", " ")
        if len(step.summary) > 60:
            summary_short += "..."
        lines.append(f"| {ts_short} | {step.seq} | {step.actor} | {step.kind} | {summary_short} |")
    
    if len(steps) > 50:
        lines.append(f"| ... | ... | ... | ... | *{len(steps) - 50} more events* |")
    lines.append("")
    
    # State Changes
    state_changes = [s for s in steps if s.state_delta.get("set") or s.state_delta.get("deleted")]
    if state_changes:
        lines.append("## State Changes")
        lines.append("")
        for step in state_changes[:20]:
            ts_short = step.ts.split("T")[1][:12] if "T" in step.ts else step.ts[:12]
            lines.append(f"**{ts_short}** ({step.actor}):")
            if step.state_delta.get("set"):
                lines.append(f"  - Set: `{', '.join(step.state_delta['set'])}`")
            if step.state_delta.get("deleted"):
                lines.append(f"  - Deleted: `{', '.join(step.state_delta['deleted'])}`")
            lines.append("")
    
    return "\n".join(lines)


# =============================================================================
# CLI
# =============================================================================

@click.group(invoke_without_command=True)
@click.option(
    "--session-id", "-s",
    help="Session ID to analyze.",
)
@click.option(
    "--invocation-id", "-i",
    help="Invocation ID to analyze (optional, defaults to all in session).",
)
@click.option(
    "--output", "-o",
    type=click.Path(path_type=Path),
    help="Output file for Markdown report.",
)
@click.option(
    "--jsonl",
    type=click.Path(path_type=Path),
    help="Output file for JSONL timeline.",
)
@click.option(
    "--profile", "-p",
    default=DEFAULT_PROFILE,
    show_default=True,
    envvar="DATABRICKS_PROFILE",
    help="Databricks CLI profile for authentication.",
)
@click.option(
    "--quiet", "-q",
    is_flag=True,
    help="Suppress console output (only write to files).",
)
@click.pass_context
def cli(
    ctx: click.Context,
    session_id: Optional[str],
    invocation_id: Optional[str],
    output: Optional[Path],
    jsonl: Optional[Path],
    profile: str,
    quiet: bool,
):
    """Generate RLM workflow flowcharts and structured logs.

    Analyzes ADK telemetry data from silo_dev_rs.adk.events and
    silo_dev_rs.adk.llm_context_telemetry to produce human-readable
    overviews of what happened in an RLM invocation.

    \b
    Examples:
        # Generate report for a session
        uv run scripts/databricks_rlm_flow.py -s test_level_16_1769446985

        # List recent sessions
        uv run scripts/databricks_rlm_flow.py list-sessions

        # Save outputs to files
        uv run scripts/databricks_rlm_flow.py -s my_session -o report.md --jsonl timeline.jsonl
    """
    ctx.ensure_object(dict)
    ctx.obj["profile"] = profile
    ctx.obj["quiet"] = quiet
    
    if ctx.invoked_subcommand is not None:
        return
    
    if not session_id:
        click.echo(ctx.get_help())
        click.echo("\n💡 Tip: Use 'list-sessions' to find recent sessions.")
        return
    
    # Fetch data
    if not quiet:
        click.secho(f"📊 Fetching data for session: {session_id}", fg="cyan")
    
    try:
        events = fetch_events(session_id, invocation_id, profile)
        telemetry = fetch_llm_telemetry(session_id, invocation_id, profile)
    except Exception as e:
        click.secho(f"❌ Error fetching data: {e}", fg="red")
        sys.exit(1)
    
    if not events:
        click.secho(f"⚠️  No events found for session: {session_id}", fg="yellow")
        sys.exit(1)
    
    if not quiet:
        click.secho(f"   Found {len(events)} events, {len(telemetry)} LLM calls", fg="green")
    
    # Process data
    steps = [parse_event_to_step(e) for e in events]
    steps = enrich_with_telemetry(steps, telemetry)
    summary = generate_summary(steps, telemetry)
    
    # Generate outputs
    if jsonl:
        jsonl_content = generate_jsonl(steps)
        jsonl.write_text(jsonl_content)
        if not quiet:
            click.secho(f"✅ JSONL timeline written to: {jsonl}", fg="green")
    
    report = generate_markdown_report(steps, summary, telemetry)
    
    if output:
        output.write_text(report)
        if not quiet:
            click.secho(f"✅ Markdown report written to: {output}", fg="green")
    
    if not quiet or (not output and not jsonl):
        click.echo("\n" + "=" * 70)
        click.echo(report)


@cli.command("list-sessions")
@click.option("--limit", "-l", default=10, show_default=True, help="Number of sessions to list.")
@click.pass_context
def list_sessions_cmd(ctx: click.Context, limit: int):
    """List recent sessions with summary info.

    \b
    Example:
        uv run scripts/databricks_rlm_flow.py list-sessions --limit 5
    """
    profile = ctx.obj.get("profile", DEFAULT_PROFILE)
    
    click.secho("📋 Recent Sessions:", fg="cyan", bold=True)
    click.echo("")
    
    try:
        sessions = list_recent_sessions(profile, limit)
    except Exception as e:
        click.secho(f"❌ Error listing sessions: {e}", fg="red")
        sys.exit(1)
    
    if not sessions:
        click.secho("No sessions found.", fg="yellow")
        return
    
    click.echo(f"{'Session ID':<35} {'Invocation ID':<40} {'Events':<8} {'Agents':<8} {'End Time'}")
    click.echo("-" * 120)
    
    for s in sessions:
        session_id = s.get("session_id", "?")[:34]
        invocation_id = s.get("invocation_id", "?")[:39]
        events = s.get("event_count", "?")
        agents = s.get("agent_count", "?")
        end_time = s.get("end_time", "?")
        if end_time and len(end_time) > 19:
            end_time = end_time[:19]
        click.echo(f"{session_id:<35} {invocation_id:<40} {events:<8} {agents:<8} {end_time}")
    
    click.echo("")
    click.echo("💡 Use: databricks_rlm_flow.py -s <session_id> to analyze a session")


@cli.command("telemetry")
@click.argument("session_id")
@click.option("--agent", "-a", help="Filter by agent name.")
@click.pass_context
def telemetry_cmd(ctx: click.Context, session_id: str, agent: Optional[str]):
    """Show LLM telemetry summary for a session.

    \b
    Example:
        uv run scripts/databricks_rlm_flow.py telemetry test_level_16_1769446985
        uv run scripts/databricks_rlm_flow.py telemetry test_level_16_1769446985 -a results_processor
    """
    profile = ctx.obj.get("profile", DEFAULT_PROFILE)
    
    try:
        telemetry = fetch_llm_telemetry(session_id, None, profile)
    except Exception as e:
        click.secho(f"❌ Error fetching telemetry: {e}", fg="red")
        sys.exit(1)
    
    if agent:
        telemetry = [t for t in telemetry if t.get("agent_name") == agent]
    
    if not telemetry:
        click.secho(f"No telemetry found for session: {session_id}", fg="yellow")
        return
    
    click.secho(f"📊 LLM Telemetry for: {session_id}", fg="cyan", bold=True)
    if agent:
        click.secho(f"   Filtered by agent: {agent}", fg="cyan")
    click.echo("")
    
    click.echo(f"{'#':<4} {'Agent':<20} {'Model':<20} {'Prompt':<10} {'Comp':<10} {'State':<10} {'Latency':<10} {'Status'}")
    click.echo("-" * 110)
    
    total_prompt = 0
    total_comp = 0
    
    for t in telemetry:
        idx = t.get("llm_call_index", "?")
        agent_name = (t.get("agent_name") or "?")[:19]
        model = (t.get("model_name") or "?")[:19]
        prompt = t.get("prompt_token_count") or "?"
        comp = t.get("candidates_token_count") or "?"
        state = t.get("state_token_estimate") or "?"
        latency = t.get("latency_ms") or "?"
        status = t.get("call_status") or "?"
        
        # Convert to int for totals (values come as strings from SQL)
        try:
            total_prompt += int(prompt)
        except (ValueError, TypeError):
            pass
        try:
            total_comp += int(comp)
        except (ValueError, TypeError):
            pass
        
        click.echo(f"{idx:<4} {agent_name:<20} {model:<20} {prompt:<10} {comp:<10} {state:<10} {latency:<10} {status}")
    
    click.echo("-" * 110)
    click.secho(f"{'Total':<4} {'':<20} {'':<20} {total_prompt:<10} {total_comp:<10}", fg="green", bold=True)


if __name__ == "__main__":
    cli()
