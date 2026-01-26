#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "databricks-sdk>=0.20.0",
#   "click>=8.1.0",
# ]
# ///
"""
Inspect context flow in `silo_dev_rs.adk.events`.

Goal
- Determine whether content produced/injected for `results_processor` appears in:
  - persisted ADK state (the `state_delta_json` column), and/or
  - subsequent `databricks_analyst` activity (via text/tool args similarity).

Why this works
- `silo_dev_rs.adk.events.event_data_json` stores the serialized ADK Event, including:
  - content.parts[*].text / thought / functionCall
  - usageMetadata (promptTokenCount, cachedContentTokenCount, etc.)
- `state_delta_json` stores the persisted `event.actions.state_delta` (if any).

Usage
  uv run scripts/inspect_events_context_flow.py --latest
  uv run scripts/inspect_events_context_flow.py --session-id <SESSION_ID>
  uv run scripts/inspect_events_context_flow.py --session-id <SESSION_ID> --window 8

Notes
- We extract JSON fields in SQL using `get_json_object(...)` to avoid pulling huge blobs.
"""

from __future__ import annotations

import json
import logging
import re
import statistics
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Iterable, Optional

import click

logger = logging.getLogger(__name__)


DEFAULT_PROFILE = "rstanhope"
DEFAULT_TABLE = "silo_dev_rs.adk.events"


def _get_sql_warehouse_id(client) -> str:
    warehouses = list(client.warehouses.list())
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING":
            logger.info("Using SQL warehouse: %s (id: %s)", wh.name, wh.id)
            return wh.id
    for wh in warehouses:
        if wh.state and wh.state.value == "STOPPED":
            logger.info("Starting SQL warehouse: %s", wh.name)
            client.warehouses.start(wh.id)
            # Basic wait loop (mirrors scripts/execute_sql.py style)
            import time

            for _ in range(30):
                status = client.warehouses.get(wh.id)
                if status.state and status.state.value == "RUNNING":
                    return wh.id
                time.sleep(10)
    raise RuntimeError("No SQL warehouse available. Please create or start one.")


def _run_sql(client, warehouse_id: str, sql: str) -> list[list[Any]]:
    """
    Execute SQL via Statement Execution API with polling.

    The Databricks API can return RUNNING/PENDING after wait_timeout with no error;
    in that case we poll until a terminal state.
    """
    import time

    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
    )

    if not response.status:
        raise RuntimeError("SQL execution failed: missing status")

    statement_id = getattr(response, "statement_id", None)
    state = response.status.state.value

    # Poll if needed (covers RUNNING/PENDING after initial wait_timeout)
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

    # Inline results path
    if response.result and response.result.data_array is not None:
        return response.result.data_array

    # Chunked results fallback (some warehouses return chunks)
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


_WS_RE = re.compile(r"\s+")
_WORD_RE = re.compile(r"[A-Za-z0-9_]+")


def _normalize_text(s: str) -> str:
    s = s.replace("\u0000", "")
    s = _WS_RE.sub(" ", s).strip()
    return s


def _tokenize(s: str) -> list[str]:
    s = _normalize_text(s).lower()
    return _WORD_RE.findall(s)


def _jaccard(a_tokens: Iterable[str], b_tokens: Iterable[str]) -> float:
    a = set(a_tokens)
    b = set(b_tokens)
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _seq_ratio(a: str, b: str) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def _extract_json_string_field_by_markers(raw: str, field: str, end_marker: str) -> str:
    """
    Best-effort extraction for event_data_json strings that are *almost* JSON but
    may contain unescaped newlines (making them invalid JSON).

    We extract the first occurrence of `"field": "<value>"` and read until the
    next known marker (e.g., `, "thought":`).
    """
    if not raw:
        return ""
    start = raw.find(f"\"{field}\":")
    if start < 0:
        return ""
    i = start + len(field) + 3  # len('"') + field + len('":')
    while i < len(raw) and raw[i] in " \t\r\n":
        i += 1
    if i >= len(raw):
        return ""
    if raw.startswith("null", i):
        return ""
    if raw[i] != '"':
        return ""
    i += 1
    j = raw.find(end_marker, i)
    if j < 0:
        return ""
    val = raw[i:j]
    if val.endswith('"'):
        val = val[:-1]
    return val


def _extract_int_field(raw: str, field: str) -> Optional[int]:
    if not raw:
        return None
    m = re.search(rf"\"{re.escape(field)}\"\\s*:\\s*(\\d+)", raw)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _extract_function_call(raw: str) -> tuple[str, str]:
    """
    Extract the first functionCall name + args payload from the raw event string.
    Returns (tool_name, args_json_string) with best-effort parsing.
    """
    if not raw or "\"functionCall\"" not in raw:
        return "", ""
    fc_idx = raw.find("\"functionCall\":")
    if fc_idx < 0:
        return "", ""
    name = ""
    m_name = re.search(
        r"\"functionCall\"\\s*:\\s*\\{[\\s\\S]*?\"name\"\\s*:\\s*\"([^\"]+)\"",
        raw[fc_idx:],
    )
    if m_name:
        name = m_name.group(1)
    args = ""
    m_args = re.search(
        r"\"functionCall\"\\s*:\\s*\\{[\\s\\S]*?\"args\"\\s*:\\s*(\\{[\\s\\S]*?\\})\\s*,\\s*\"name\"",
        raw[fc_idx:],
    )
    if m_args:
        args = m_args.group(1)
    return name, args


def _extract_exec_sections(text: str) -> dict[str, str]:
    """
    Parse the injected EXECUTION RESULTS format produced by RlmContextInjectionPlugin.
    Returns best-effort sections: instruction/stdout/stderr/other/full.
    """
    out: dict[str, str] = {"full": _as_str(text)}
    t = _as_str(text)
    if "=== ANALYSIS INSTRUCTION ===" in t:
        after = t.split("=== ANALYSIS INSTRUCTION ===", 1)[1]
        block = after.split("===", 1)[0]
        out["instruction"] = block.strip()
    if "=== STDOUT ===" in t:
        after = t.split("=== STDOUT ===", 1)[1]
        block = after.split("===", 1)[0]
        out["stdout"] = block.strip()
    if "=== STDERR ===" in t:
        after = t.split("=== STDERR ===", 1)[1]
        block = after.split("===", 1)[0]
        out["stderr"] = block.strip()
    return out


@dataclass(frozen=True)
class EventRow:
    sequence_num: int
    author: str
    event_ts: str
    invocation_id: str
    text: str
    thought: str
    tool_name: str
    tool_args_json: str
    prompt_tokens: Optional[int]
    cached_tokens: Optional[int]
    has_state_delta: bool
    state_delta_json: str


def _coerce_bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    if x is None:
        return False
    s = str(x).strip().lower()
    return s in ("true", "1", "yes")


def _parse_state_delta_keys(state_delta_json: str) -> list[str]:
    if not state_delta_json:
        return []
    try:
        d = json.loads(state_delta_json)
    except Exception:
        return []
    if not isinstance(d, dict):
        return []
    return sorted(d.keys())


def _events_sql(table: str, session_id: str, limit: int) -> str:
    # Prefer pulling the raw JSON and parsing in Python.
    # Spark SQL JSON functions (get_json_object) can return NULL when the JSON
    # contains control characters that Python can still parse with strict=False.
    sid = session_id.replace("'", "''")
    return f"""
SELECT
  sequence_num,
  author,
  CAST(event_timestamp AS STRING) AS event_ts,
  invocation_id,
  -- guardrail: avoid pulling multi-megabyte blobs by accident
  substr(event_data_json, 1, 500000) AS event_data_json,
  has_state_delta,
  COALESCE(state_delta_json, '') AS state_delta_json
FROM {table}
WHERE session_id = '{sid}'
ORDER BY sequence_num ASC
LIMIT {int(limit)}
""".strip()


def _latest_session_sql(table: str) -> str:
    return f"""
SELECT session_id, CAST(event_timestamp AS STRING) AS event_ts
FROM {table}
WHERE author = 'results_processor'
ORDER BY event_timestamp DESC
LIMIT 1
""".strip()


def _fetch_events(
    *,
    profile: str,
    table: str,
    session_id: str,
    limit: int,
) -> list[EventRow]:
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient(profile=profile)
    warehouse_id = _get_sql_warehouse_id(client)
    rows = _run_sql(client, warehouse_id, _events_sql(table, session_id, limit))

    events: list[EventRow] = []
    for r in rows:
        # Column order matches SELECT in _events_sql
        seq = int(r[0])
        author = _as_str(r[1])
        event_ts = _as_str(r[2])
        invocation_id = _as_str(r[3])
        raw_event_data_json = _as_str(r[4])
        has_state_delta = _coerce_bool(r[5])
        state_delta_json = _as_str(r[6])

        # Best-effort parse of ADK event JSON
        text = ""
        thought = ""
        tool_name = ""
        tool_args_json = ""
        prompt_tokens: Optional[int] = None
        cached_tokens: Optional[int] = None
        if raw_event_data_json:
            cleaned = raw_event_data_json.replace("\u0000", "")
            try:
                ev = json.loads(cleaned, strict=False)
            except Exception:
                ev = {}

            # usageMetadata
            um = ev.get("usageMetadata") if isinstance(ev, dict) else None
            if isinstance(um, dict):
                prompt_tokens = _as_int(um.get("promptTokenCount"))
                cached_tokens = _as_int(um.get("cachedContentTokenCount"))

            # content.parts[*]
            content = ev.get("content") if isinstance(ev, dict) else None
            parts = None
            if isinstance(content, dict):
                parts = content.get("parts")
            if isinstance(parts, list):
                texts: list[str] = []
                thoughts: list[str] = []
                # Grab first tool call (if any)
                for p in parts:
                    if not isinstance(p, dict):
                        continue
                    if p.get("text"):
                        texts.append(str(p.get("text")))
                    if p.get("thought"):
                        thoughts.append(str(p.get("thought")))
                    fc = p.get("functionCall")
                    if tool_name == "" and isinstance(fc, dict) and fc.get("name"):
                        tool_name = str(fc.get("name"))
                        args = fc.get("args")
                        if args is None:
                            tool_args_json = ""
                        elif isinstance(args, str):
                            tool_args_json = args
                        else:
                            # Keep stable JSON for similarity comparisons
                            try:
                                tool_args_json = json.dumps(args, sort_keys=True)
                            except Exception:
                                tool_args_json = str(args)

                text = "\n".join(texts).strip()
                thought = "\n".join(thoughts).strip()

            # Fallback extraction when JSON parsing fails or Spark stored invalid JSON
            # (e.g., unescaped newlines inside the "text" value).
            if not text:
                text = _extract_json_string_field_by_markers(cleaned, "text", ", \"thought\":")
            if not thought:
                thought = _extract_json_string_field_by_markers(cleaned, "thought", ", \"thoughtSignature\":")
            if not tool_name:
                tool_name, tool_args_json = _extract_function_call(cleaned)
            if prompt_tokens is None:
                prompt_tokens = _extract_int_field(cleaned, "promptTokenCount")
            if cached_tokens is None:
                cached_tokens = _extract_int_field(cleaned, "cachedContentTokenCount")

        events.append(
            EventRow(
                sequence_num=seq,
                author=author,
                event_ts=event_ts,
                invocation_id=invocation_id,
                text=text,
                thought=thought,
                tool_name=tool_name,
                tool_args_json=tool_args_json,
                prompt_tokens=prompt_tokens,
                cached_tokens=cached_tokens,
                has_state_delta=has_state_delta,
                state_delta_json=state_delta_json,
            )
        )
    return events


def _resolve_session_id(*, profile: str, table: str) -> str:
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient(profile=profile)
    warehouse_id = _get_sql_warehouse_id(client)
    rows = _run_sql(client, warehouse_id, _latest_session_sql(table))
    if not rows or not rows[0] or not rows[0][0]:
        raise RuntimeError(f"No results_processor events found in {table}")
    return str(rows[0][0])


def _summarize_prompt_tokens(events: list[EventRow]) -> str:
    pts = [e.prompt_tokens for e in events if e.prompt_tokens is not None]
    if not pts:
        return "promptTokenCount: (no data)"
    return (
        f"promptTokenCount: min={min(pts)} max={max(pts)} "
        f"median={int(statistics.median(pts))} n={len(pts)}"
    )


def _print_event_table(events: list[EventRow]) -> None:
    counts: dict[str, int] = {}
    click.echo("\nEvent stream (sequence → author → promptTokens/cachedTokens → tool/text):")
    for e in events:
        counts[e.author] = counts.get(e.author, 0) + 1
        pt = "-" if e.prompt_tokens is None else str(e.prompt_tokens)
        ct = "-" if e.cached_tokens is None else str(e.cached_tokens)
        tool = e.tool_name.strip('"') if e.tool_name else ""
        text = _normalize_text(e.text.strip('"'))[:90]
        if tool:
            tail = f"tool={tool}"
        elif text:
            tail = f"text={text!r}"
        else:
            tail = "—"
        click.echo(f"  {e.sequence_num:>6}  {e.author:<18}  pt={pt:<6} ct={ct:<6}  {tail}")
    click.echo("\nCounts by author:")
    for author, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        click.echo(f"  {author:<18} {n}")
    click.echo(f"\n{_summarize_prompt_tokens(events)}")


def _analyze_state_deltas(events: list[EventRow]) -> None:
    keys_by_author: dict[str, set[str]] = {}
    any_delta = False
    for e in events:
        if not e.has_state_delta or not e.state_delta_json:
            continue
        any_delta = True
        keys = set(_parse_state_delta_keys(e.state_delta_json))
        if keys:
            keys_by_author.setdefault(e.author, set()).update(keys)

    click.echo("\nState deltas (from persisted `state_delta_json`):")
    if not any_delta:
        click.echo("  (No persisted state deltas in this session.)")
        return
    if not keys_by_author:
        click.echo("  (State deltas exist but keys could not be parsed.)")
        return
    for author, keys in sorted(keys_by_author.items(), key=lambda kv: kv[0]):
        keys_sorted = sorted(keys)
        click.echo(f"  {author}: {len(keys_sorted)} keys")
        for k in keys_sorted[:40]:
            click.echo(f"    - {k}")
        if len(keys_sorted) > 40:
            click.echo(f"    - … (+{len(keys_sorted) - 40} more)")


def _similarity_scan(
    *,
    events: list[EventRow],
    window: int,
    focus_from_author: str = "results_processor",
    focus_to_author: str = "databricks_analyst",
) -> None:
    """
    For each focus_from_author event, compare its chunks to the next N events from focus_to_author.
    Reports best match scores to identify what content seems to be "carried forward".
    """
    click.echo("\nSimilarity scan (results_processor → subsequent databricks_analyst events):")
    focus_idxs = [i for i, e in enumerate(events) if e.author == focus_from_author and e.text]
    if not focus_idxs:
        click.echo("  (No results_processor events with text found.)")
        return

    for idx in focus_idxs:
        src = events[idx]
        src_text = src.text.strip('"')
        sections = _extract_exec_sections(src_text)
        is_injection = src_text.startswith("=== EXECUTION RESULTS ===")
        label = "injection" if is_injection else "output"

        # Collect candidate targets in the following window
        targets: list[EventRow] = []
        for e in events[idx + 1 : idx + 1 + window * 3]:
            if e.author == focus_to_author:
                targets.append(e)
            if len(targets) >= window:
                break

        if not targets:
            continue

        click.echo(f"\n  Source {label}: seq={src.sequence_num} ts={src.event_ts} (chars={len(src_text)})")
        for sec_name, sec_text in sorted(sections.items(), key=lambda kv: kv[0] != "full"):
            if not sec_text:
                continue
            sec_norm = _normalize_text(sec_text)
            sec_tokens = _tokenize(sec_norm)
            click.echo(f"    Section {sec_name}: chars={len(sec_text)} uniq_tokens={len(set(sec_tokens))}")

        # Score each target against each section; keep the best section match per target
        for t in targets:
            tgt_blob_parts: list[str] = []
            if t.tool_name:
                tgt_blob_parts.append(t.tool_name.strip('"'))
            if t.tool_args_json:
                tgt_blob_parts.append(t.tool_args_json.strip('"'))
            if t.text:
                tgt_blob_parts.append(t.text.strip('"'))
            tgt_blob = _normalize_text(" ".join(p for p in tgt_blob_parts if p))

            best = {"sec": "", "seq_ratio": -1.0, "jaccard": -1.0}
            for sec_name, sec_text in sections.items():
                if not sec_text:
                    continue
                a = _normalize_text(sec_text)
                b = tgt_blob
                sr = _seq_ratio(a, b)
                jc = _jaccard(_tokenize(a), _tokenize(b))
                # rank by jaccard first, then seq ratio
                if (jc, sr) > (best["jaccard"], best["seq_ratio"]):
                    best = {"sec": sec_name, "seq_ratio": sr, "jaccard": jc}

            tool = t.tool_name.strip('"') if t.tool_name else ""
            pt = "-" if t.prompt_tokens is None else str(t.prompt_tokens)
            click.echo(
                f"    → target seq={t.sequence_num} pt={pt:<6} "
                f"tool={tool or '-':<24} best_section={best['sec']:<11} "
                f"jaccard={best['jaccard']:.3f} seq={best['seq_ratio']:.3f}"
            )


@click.command()
@click.option("--profile", "-p", default=DEFAULT_PROFILE, show_default=True, envvar="DATABRICKS_PROFILE")
@click.option("--table", "-t", default=DEFAULT_TABLE, show_default=True, help="Fully-qualified events table.")
@click.option("--session-id", help="Session ID to inspect.")
@click.option("--latest", is_flag=True, help="Automatically pick the most recent session that has results_processor events.")
@click.option("--limit", default=250, show_default=True, help="Max events to load for the session.")
@click.option("--window", default=8, show_default=True, help="How many subsequent databricks_analyst events to compare per results_processor event.")
def main(profile: str, table: str, session_id: str, latest: bool, limit: int, window: int) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    if not session_id:
        if not latest:
            raise click.UsageError("Provide --session-id or use --latest.")
        session_id = _resolve_session_id(profile=profile, table=table)

    click.echo(f"Inspecting: table={table} session_id={session_id} profile={profile}")
    events = _fetch_events(profile=profile, table=table, session_id=session_id, limit=limit)
    if not events:
        click.echo("No events found for that session_id.")
        return

    _print_event_table(events)
    _analyze_state_deltas(events)
    _similarity_scan(events=events, window=window)


if __name__ == "__main__":
    main()

