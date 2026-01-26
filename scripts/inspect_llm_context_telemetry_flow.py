#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "databricks-sdk>=0.20.0",
#   "click>=8.1.0",
# ]
# ///
"""
Inspect LLM context telemetry flow for agent sessions.

This script queries `silo_dev_rs.adk.llm_context_telemetry` to visualize
the invocation context state as it flows through the agent pipeline.

Key metrics tracked:
  - state_keys_count: Number of keys in session state
  - state_token_estimate: Estimated tokens for full state
  - state_token_estimate_persistable_only: Tokens excluding temp: keys
  - prompt_token_count: Actual tokens reported by LLM (authoritative)
  - request_last_message: The last message role/preview sent to LLM

Usage:
  uv run scripts/inspect_llm_context_telemetry_flow.py --latest
  uv run scripts/inspect_llm_context_telemetry_flow.py --session-id <SESSION_ID>
  uv run scripts/inspect_llm_context_telemetry_flow.py --session-id <SESSION_ID> --agent results_processor
  uv run scripts/inspect_llm_context_telemetry_flow.py --session-id <SESSION_ID> --export-csv flow.csv

Notes:
  - Uses the llm_context_telemetry view which pairs before/after model callbacks
  - State growth patterns help identify context bloat issues
  - The request_last_message_preview shows what triggered each LLM call
"""

from __future__ import annotations

import csv
import json
import logging
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import click

logger = logging.getLogger(__name__)

DEFAULT_PROFILE = "rstanhope"
DEFAULT_TABLE = "silo_dev_rs.adk.llm_context_telemetry"


def _get_sql_warehouse_id(client) -> str:
    """Get a running SQL warehouse ID."""
    warehouses = list(client.warehouses.list())
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING":
            logger.info("Using SQL warehouse: %s (id: %s)", wh.name, wh.id)
            return wh.id
    for wh in warehouses:
        if wh.state and wh.state.value == "STOPPED":
            logger.info("Starting SQL warehouse: %s", wh.name)
            client.warehouses.start(wh.id)
            for _ in range(30):
                status = client.warehouses.get(wh.id)
                if status.state and status.state.value == "RUNNING":
                    return wh.id
                time.sleep(10)
    raise RuntimeError("No SQL warehouse available. Please create or start one.")


def _run_sql(client, warehouse_id: str, sql: str) -> list[list[Any]]:
    """Execute SQL via Statement Execution API with polling."""
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
    )

    if not response.status:
        raise RuntimeError("SQL execution failed: missing status")

    statement_id = getattr(response, "statement_id", None)
    state = response.status.state.value

    # Poll if needed
    if state not in ("SUCCEEDED", "FAILED", "CANCELED"):
        if not statement_id:
            raise RuntimeError(f"SQL execution incomplete with no statement_id (state={state})")
        deadline = time.time() + 360.0
        while time.time() < deadline:
            status_resp = client.statement_execution.get_statement(statement_id)
            if not status_resp.status:
                break
            state = status_resp.status.state.value
            if state in ("SUCCEEDED", "FAILED", "CANCELED"):
                response = status_resp
                break
            time.sleep(1.5)

    if state != "SUCCEEDED":
        err = getattr(response.status, "error", None)
        msg = getattr(response.status, "error_message", None)
        raise RuntimeError(f"SQL execution failed: state={state} error={err!r} message={msg!r}")

    if response.result and response.result.data_array is not None:
        return response.result.data_array

    if statement_id:
        chunk = client.statement_execution.get_statement_result_chunk_n(statement_id, 0)
        if chunk and chunk.data_array is not None:
            return chunk.data_array

    return []


def _as_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None


def _as_str(x: Any) -> str:
    return "" if x is None else str(x)


@dataclass
class TelemetryRow:
    """Represents a row from llm_context_telemetry."""
    session_id: str
    invocation_id: str
    agent_name: str
    llm_call_index: int
    model_name: str
    ts_before: str
    ts_after: str
    latency_ms: Optional[int]
    state_keys_count: Optional[int]
    state_json_bytes: Optional[int]
    state_token_estimate: Optional[int]
    state_token_estimate_persistable_only: Optional[int]
    prompt_token_count: Optional[int]
    candidates_token_count: Optional[int]
    cached_content_token_count: Optional[int]
    state_overhead_tokens: Optional[int]
    request_last_message_role: str
    request_last_message_token_estimate: Optional[int]
    request_last_message_preview: str
    response_preview: str
    call_status: str


def _latest_session_sql(table: str) -> str:
    """Get the most recent session with telemetry."""
    return f"""
SELECT session_id, MAX(ts_before) AS latest_ts
FROM {table}
GROUP BY session_id
ORDER BY latest_ts DESC
LIMIT 1
""".strip()


def _telemetry_sql(table: str, session_id: str, agent_filter: Optional[str], limit: int) -> str:
    """Build SQL to fetch telemetry rows."""
    sid = session_id.replace("'", "''")
    where_clause = f"WHERE session_id = '{sid}'"
    if agent_filter:
        agent = agent_filter.replace("'", "''")
        where_clause += f" AND agent_name = '{agent}'"
    
    return f"""
SELECT
    session_id,
    invocation_id,
    agent_name,
    llm_call_index,
    model_name,
    CAST(ts_before AS STRING) AS ts_before,
    CAST(ts_after AS STRING) AS ts_after,
    latency_ms,
    state_keys_count,
    state_json_bytes,
    state_token_estimate,
    state_token_estimate_persistable_only,
    prompt_token_count,
    candidates_token_count,
    cached_content_token_count,
    state_overhead_tokens,
    request_last_message_role,
    request_last_message_token_estimate,
    substr(request_last_message_preview, 1, 500) AS request_last_message_preview,
    substr(response_preview, 1, 300) AS response_preview,
    call_status
FROM {table}
{where_clause}
ORDER BY llm_call_index ASC
LIMIT {int(limit)}
""".strip()


def _fetch_telemetry(
    *,
    profile: str,
    table: str,
    session_id: str,
    agent_filter: Optional[str],
    limit: int,
) -> list[TelemetryRow]:
    """Fetch telemetry rows from the view."""
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient(profile=profile)
    warehouse_id = _get_sql_warehouse_id(client)
    
    sql = _telemetry_sql(table, session_id, agent_filter, limit)
    rows = _run_sql(client, warehouse_id, sql)

    results: list[TelemetryRow] = []
    for r in rows:
        results.append(TelemetryRow(
            session_id=_as_str(r[0]),
            invocation_id=_as_str(r[1]),
            agent_name=_as_str(r[2]),
            llm_call_index=_as_int(r[3]) or 0,
            model_name=_as_str(r[4]),
            ts_before=_as_str(r[5]),
            ts_after=_as_str(r[6]),
            latency_ms=_as_int(r[7]),
            state_keys_count=_as_int(r[8]),
            state_json_bytes=_as_int(r[9]),
            state_token_estimate=_as_int(r[10]),
            state_token_estimate_persistable_only=_as_int(r[11]),
            prompt_token_count=_as_int(r[12]),
            candidates_token_count=_as_int(r[13]),
            cached_content_token_count=_as_int(r[14]),
            state_overhead_tokens=_as_int(r[15]),
            request_last_message_role=_as_str(r[16]),
            request_last_message_token_estimate=_as_int(r[17]),
            request_last_message_preview=_as_str(r[18]),
            response_preview=_as_str(r[19]),
            call_status=_as_str(r[20]),
        ))
    return results


def _resolve_session_id(*, profile: str, table: str) -> str:
    """Get the most recent session ID."""
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient(profile=profile)
    warehouse_id = _get_sql_warehouse_id(client)
    rows = _run_sql(client, warehouse_id, _latest_session_sql(table))
    if not rows or not rows[0] or not rows[0][0]:
        raise RuntimeError(f"No telemetry found in {table}")
    return str(rows[0][0])


def _print_flow_table(rows: list[TelemetryRow]) -> None:
    """Print the telemetry flow as a table."""
    click.echo("\n" + "=" * 120)
    click.echo("LLM Context Telemetry Flow")
    click.echo("=" * 120)
    
    # Header
    click.echo(
        f"{'Call':>4} │ {'Agent':<20} │ {'Keys':>4} │ {'State':>6} │ {'Prompt':>6} │ "
        f"{'Cached':>6} │ {'Latency':>7} │ {'Status':<10} │ Last Message"
    )
    click.echo("─" * 120)
    
    prev_state_tokens = None
    for row in rows:
        # Calculate state delta indicator
        state_delta = ""
        if prev_state_tokens is not None and row.state_token_estimate is not None:
            delta = row.state_token_estimate - prev_state_tokens
            if delta > 0:
                state_delta = f"↑{delta}"
            elif delta < 0:
                state_delta = f"↓{abs(delta)}"
        
        state_str = f"{row.state_token_estimate or '-':>6}"
        if state_delta:
            state_str = f"{state_str} ({state_delta})"
        
        # Truncate preview
        preview = row.request_last_message_preview[:50].replace("\n", " ")
        if len(row.request_last_message_preview) > 50:
            preview += "..."
        
        # Color status
        status = row.call_status
        if status == "completed":
            status_colored = click.style(status, fg="green")
        elif status == "error":
            status_colored = click.style(status, fg="red")
        else:
            status_colored = click.style(status, fg="yellow")
        
        # Highlight results_processor injections
        if "results_processor" in preview.lower() or preview.startswith("For context:"):
            preview = click.style(preview, fg="cyan")
        
        click.echo(
            f"{row.llm_call_index:>4} │ {row.agent_name:<20} │ "
            f"{row.state_keys_count or '-':>4} │ {state_str:<15} │ "
            f"{row.prompt_token_count or '-':>6} │ {row.cached_content_token_count or '-':>6} │ "
            f"{row.latency_ms or '-':>6}ms │ {status_colored:<10} │ {preview}"
        )
        
        if row.state_token_estimate is not None:
            prev_state_tokens = row.state_token_estimate


def _print_agent_summary(rows: list[TelemetryRow]) -> None:
    """Print summary statistics by agent."""
    click.echo("\n" + "=" * 80)
    click.echo("Agent Summary")
    click.echo("=" * 80)
    
    agent_stats: dict[str, dict] = {}
    for row in rows:
        if row.agent_name not in agent_stats:
            agent_stats[row.agent_name] = {
                "calls": 0,
                "total_latency": 0,
                "prompt_tokens": [],
                "state_tokens": [],
                "completed": 0,
                "errors": 0,
            }
        stats = agent_stats[row.agent_name]
        stats["calls"] += 1
        if row.latency_ms:
            stats["total_latency"] += row.latency_ms
        if row.prompt_token_count:
            stats["prompt_tokens"].append(row.prompt_token_count)
        if row.state_token_estimate:
            stats["state_tokens"].append(row.state_token_estimate)
        if row.call_status == "completed":
            stats["completed"] += 1
        elif row.call_status == "error":
            stats["errors"] += 1
    
    click.echo(f"{'Agent':<25} │ {'Calls':>5} │ {'Completed':>9} │ {'Errors':>6} │ {'Avg Latency':>11} │ {'Avg Prompt':>10} │ {'Max State':>9}")
    click.echo("─" * 95)
    
    for agent, stats in sorted(agent_stats.items(), key=lambda x: -x[1]["calls"]):
        avg_latency = stats["total_latency"] / stats["calls"] if stats["calls"] > 0 else 0
        avg_prompt = int(statistics.mean(stats["prompt_tokens"])) if stats["prompt_tokens"] else 0
        max_state = max(stats["state_tokens"]) if stats["state_tokens"] else 0
        
        click.echo(
            f"{agent:<25} │ {stats['calls']:>5} │ {stats['completed']:>9} │ "
            f"{stats['errors']:>6} │ {avg_latency:>9.0f}ms │ {avg_prompt:>10} │ {max_state:>9}"
        )


def _print_state_growth_analysis(rows: list[TelemetryRow]) -> None:
    """Analyze state growth patterns."""
    click.echo("\n" + "=" * 80)
    click.echo("State Growth Analysis")
    click.echo("=" * 80)
    
    state_tokens = [r.state_token_estimate for r in rows if r.state_token_estimate is not None]
    if not state_tokens:
        click.echo("  (No state token data available)")
        return
    
    click.echo(f"  Initial state tokens:  {state_tokens[0]:>8}")
    click.echo(f"  Final state tokens:    {state_tokens[-1]:>8}")
    click.echo(f"  Max state tokens:      {max(state_tokens):>8}")
    click.echo(f"  Min state tokens:      {min(state_tokens):>8}")
    click.echo(f"  Total growth:          {state_tokens[-1] - state_tokens[0]:>+8}")
    
    # Find largest jumps
    jumps: list[tuple[int, int, int]] = []
    for i in range(1, len(state_tokens)):
        delta = state_tokens[i] - state_tokens[i-1]
        if abs(delta) > 100:  # Only significant jumps
            jumps.append((i, delta, state_tokens[i]))
    
    if jumps:
        click.echo("\n  Significant state changes (>100 tokens):")
        for call_idx, delta, new_val in sorted(jumps, key=lambda x: -abs(x[1]))[:5]:
            row = rows[call_idx] if call_idx < len(rows) else None
            agent = row.agent_name if row else "?"
            sign = "+" if delta > 0 else ""
            click.echo(f"    Call {call_idx:>3} ({agent:<20}): {sign}{delta:>6} tokens → {new_val:>6} total")


def _print_injection_points(rows: list[TelemetryRow]) -> None:
    """Identify and display context injection points (e.g., results_processor)."""
    click.echo("\n" + "=" * 80)
    click.echo("Context Injection Points")
    click.echo("=" * 80)
    
    injection_rows = [
        r for r in rows 
        if "results_processor" in r.request_last_message_preview.lower()
        or r.request_last_message_preview.startswith("For context:")
    ]
    
    if not injection_rows:
        click.echo("  (No explicit results_processor injections found in this session)")
        return
    
    click.echo(f"  Found {len(injection_rows)} context injection point(s):\n")
    
    for row in injection_rows:
        click.echo(f"  Call {row.llm_call_index:>3} │ {row.agent_name:<20}")
        click.echo(f"         State: {row.state_token_estimate or '-'} tokens ({row.state_keys_count or '-'} keys)")
        click.echo(f"         Prompt: {row.prompt_token_count or '-'} tokens")
        
        # Extract artifact/iteration info from preview
        preview = row.request_last_message_preview
        if "Artifact ID:" in preview:
            start = preview.find("Artifact ID:")
            end = preview.find("\n", start) if "\n" in preview[start:] else start + 50
            artifact_line = preview[start:end].strip()
            click.echo(f"         {artifact_line}")
        if "Iteration:" in preview:
            start = preview.find("Iteration:")
            end = preview.find("\n", start) if "\n" in preview[start:] else start + 20
            iter_line = preview[start:end].strip()
            click.echo(f"         {iter_line}")
        click.echo()


def _export_csv(rows: list[TelemetryRow], path: Path) -> None:
    """Export telemetry rows to CSV."""
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "llm_call_index", "agent_name", "state_keys_count", "state_token_estimate",
            "state_persistable_only", "prompt_token_count", "candidates_token_count",
            "cached_content_token_count", "latency_ms", "call_status",
            "request_last_message_role", "request_last_message_preview"
        ])
        for row in rows:
            writer.writerow([
                row.llm_call_index,
                row.agent_name,
                row.state_keys_count or "",
                row.state_token_estimate or "",
                row.state_token_estimate_persistable_only or "",
                row.prompt_token_count or "",
                row.candidates_token_count or "",
                row.cached_content_token_count or "",
                row.latency_ms or "",
                row.call_status,
                row.request_last_message_role,
                row.request_last_message_preview[:200],
            ])
    click.echo(f"\n✓ Exported {len(rows)} rows to {path}")


@click.command()
@click.option("--profile", "-p", default=DEFAULT_PROFILE, show_default=True, envvar="DATABRICKS_PROFILE",
              help="Databricks CLI profile for authentication.")
@click.option("--table", "-t", default=DEFAULT_TABLE, show_default=True,
              help="Fully-qualified telemetry view name.")
@click.option("--session-id", "-s", help="Session ID to inspect.")
@click.option("--latest", is_flag=True, help="Use the most recent session with telemetry.")
@click.option("--agent", "-a", help="Filter by agent name (e.g., 'results_processor', 'databricks_analyst').")
@click.option("--limit", "-l", default=200, show_default=True, help="Maximum telemetry rows to fetch.")
@click.option("--export-csv", "csv_path", type=click.Path(path_type=Path),
              help="Export results to CSV file.")
@click.option("--verbose", "-v", is_flag=True, help="Show verbose output including full previews.")
def main(
    profile: str,
    table: str,
    session_id: str,
    latest: bool,
    agent: Optional[str],
    limit: int,
    csv_path: Optional[Path],
    verbose: bool,
) -> None:
    """
    Inspect LLM context telemetry flow for agent sessions.

    This tool visualizes how invocation context state evolves through
    the agent pipeline, helping identify:

    \b
      - State growth patterns and potential context bloat
      - Context injection points (e.g., results_processor messages)
      - Token usage trends per agent
      - LLM call latency distribution

    \b
    Examples:
        # Inspect most recent session
        uv run scripts/inspect_llm_context_telemetry_flow.py --latest

        # Inspect specific session
        uv run scripts/inspect_llm_context_telemetry_flow.py -s test_level_16_1769446985

        # Filter to results_processor calls only
        uv run scripts/inspect_llm_context_telemetry_flow.py -s <SESSION> --agent results_processor

        # Export to CSV for further analysis
        uv run scripts/inspect_llm_context_telemetry_flow.py -s <SESSION> --export-csv flow.csv
    """
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    if not session_id:
        if not latest:
            raise click.UsageError("Provide --session-id or use --latest.")
        session_id = _resolve_session_id(profile=profile, table=table)

    click.echo(f"Inspecting: table={table}")
    click.echo(f"Session ID: {session_id}")
    if agent:
        click.echo(f"Agent filter: {agent}")
    click.echo(f"Profile: {profile}")

    rows = _fetch_telemetry(
        profile=profile,
        table=table,
        session_id=session_id,
        agent_filter=agent,
        limit=limit,
    )

    if not rows:
        click.echo(click.style("\n✗ No telemetry found for that session.", fg="red"))
        sys.exit(1)

    click.echo(f"\nFound {len(rows)} telemetry row(s)")

    _print_flow_table(rows)
    _print_agent_summary(rows)
    _print_state_growth_analysis(rows)
    _print_injection_points(rows)

    if csv_path:
        _export_csv(rows, csv_path)


if __name__ == "__main__":
    main()
