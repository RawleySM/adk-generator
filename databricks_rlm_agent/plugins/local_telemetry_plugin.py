"""Local Telemetry Plugin for ADK (Local Mode).

A custom ADK plugin that:
1. Preserves stdout print() logging for terminal observability (like UcDeltaTelemetryPlugin)
2. Persists callback-level telemetry to a local DuckDB table (adk_telemetry)
3. Supports special logging flags for blocked tool executions (safety plugin integration)
4. Captures invocation-context measurement for each LlmAgent context window

This is the local-mode equivalent of UcDeltaTelemetryPlugin, using DuckDB instead
of Spark/UC Delta tables.

Default database: .adk_local/adk.duckdb
Table: adk_telemetry
"""

from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Optional, TYPE_CHECKING

from google.genai import types
from google.adk.agents.base_agent import BaseAgent
from google.adk.agents.callback_context import CallbackContext
from google.adk.events.event import Event
from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse
from google.adk.tools.base_tool import BaseTool
from google.adk.tools.tool_context import ToolContext
from google.adk.plugins.base_plugin import BasePlugin

# Import shared helper functions from the UC Delta plugin
from .uc_delta_telemetry_plugin import (
    _canonical_json,
    _compute_sha256,
    _estimate_tokens,
    _get_token_estimation_metadata,
    _filter_persistable_state,
    _safe_state_to_dict,
    _compute_state_metrics,
    _compute_content_metrics,
    _get_llm_call_index_key,
    _build_request_snapshot,
    _save_request_snapshot,
    _build_request_preview,
    _build_response_snapshot,
    _save_response_snapshot,
)

if TYPE_CHECKING:
    from google.adk.agents.invocation_context import InvocationContext
    import duckdb

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_DB_PATH = ".adk_local/adk.duckdb"


def _get_duckdb_connection(db_path: str) -> "duckdb.DuckDBPyConnection":
    """Get a DuckDB connection, creating the database directory if needed.

    Args:
        db_path: Path to the DuckDB database file.

    Returns:
        DuckDB connection object.
    """
    import duckdb

    # Ensure the directory exists
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    return duckdb.connect(db_path)


def _ensure_local_telemetry_table(conn: "duckdb.DuckDBPyConnection") -> None:
    """Create the adk_telemetry table if it doesn't exist.

    Args:
        conn: DuckDB connection.
    """
    create_sql = """
        CREATE TABLE IF NOT EXISTS adk_telemetry (
            telemetry_id VARCHAR NOT NULL PRIMARY KEY,
            ts TIMESTAMP NOT NULL,
            app_name VARCHAR,
            user_id VARCHAR,
            session_id VARCHAR,
            invocation_id VARCHAR,
            branch VARCHAR,
            agent_name VARCHAR,
            callback_name VARCHAR NOT NULL,
            event_id VARCHAR,
            tool_name VARCHAR,
            function_call_id VARCHAR,
            model_name VARCHAR,
            tool_blocked BOOLEAN,
            blocked_reason VARCHAR,
            payload_json VARCHAR,
            created_time TIMESTAMP NOT NULL
        )
    """
    conn.execute(create_sql)
    logger.info("Local ADK telemetry table ready: adk_telemetry")


def _append_local_telemetry_row(
    conn: "duckdb.DuckDBPyConnection",
    callback_name: str,
    app_name: Optional[str] = None,
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    invocation_id: Optional[str] = None,
    branch: Optional[str] = None,
    agent_name: Optional[str] = None,
    event_id: Optional[str] = None,
    tool_name: Optional[str] = None,
    function_call_id: Optional[str] = None,
    model_name: Optional[str] = None,
    tool_blocked: Optional[bool] = None,
    blocked_reason: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
) -> str:
    """Append a telemetry row to the local adk_telemetry table.

    Uses parameterized queries for safe inserts.

    Args:
        conn: DuckDB connection.
        callback_name: Name of the callback that triggered this telemetry.
        app_name: Application name.
        user_id: User identifier.
        session_id: Session identifier.
        invocation_id: Invocation identifier.
        branch: Git branch or execution branch.
        agent_name: Name of the agent.
        event_id: Event identifier.
        tool_name: Name of the tool being executed.
        function_call_id: Function call identifier.
        model_name: Name of the LLM model.
        tool_blocked: Whether the tool was blocked.
        blocked_reason: Reason for blocking the tool.
        payload: Additional payload data as a dictionary.

    Returns:
        The telemetry_id of the inserted row.
    """
    telemetry_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc)
    created_time = ts

    # Serialize payload to JSON
    payload_json = json.dumps(payload) if payload else "{}"

    insert_sql = """
        INSERT INTO adk_telemetry (
            telemetry_id, ts, app_name, user_id, session_id,
            invocation_id, branch, agent_name, callback_name,
            event_id, tool_name, function_call_id, model_name,
            tool_blocked, blocked_reason, payload_json, created_time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    try:
        conn.execute(
            insert_sql,
            [
                telemetry_id,
                ts,
                app_name,
                user_id,
                session_id,
                invocation_id,
                branch,
                agent_name,
                callback_name,
                event_id,
                tool_name,
                function_call_id,
                model_name,
                tool_blocked,
                blocked_reason,
                payload_json,
                created_time,
            ],
        )
        logger.debug(f"Local ADK telemetry row inserted: {callback_name} ({telemetry_id})")
        return telemetry_id
    except Exception as e:
        logger.error(f"Failed to append local ADK telemetry row: {e}")
        raise


class LocalTelemetryPlugin(BasePlugin):
    """ADK plugin that logs to stdout and persists telemetry to local DuckDB.

    This plugin preserves the UcDeltaTelemetryPlugin's stdout behavior for terminal
    debugging while also persisting callback telemetry to a local DuckDB table
    for analytics and auditing in local development mode.

    Database: .adk_local/adk.duckdb (configurable via db_path parameter)
    Table: adk_telemetry

    Example:
        >>> plugin = LocalTelemetryPlugin()
        >>> runner = Runner(
        ...     agent=my_agent,
        ...     plugins=[plugin],
        ...     ...
        ... )

        >>> # With custom database path
        >>> plugin = LocalTelemetryPlugin(db_path="/path/to/custom.duckdb")
    """

    # Class-level lock for thread safety
    _lock = threading.RLock()

    def __init__(
        self,
        name: str = "local_telemetry_plugin",
        db_path: str = DEFAULT_DB_PATH,
        enable_stdout: bool = True,
    ):
        """Initialize the Local telemetry plugin.

        Args:
            name: Plugin instance name.
            db_path: Path to the DuckDB database file (default: .adk_local/adk.duckdb).
            enable_stdout: Whether to print logs to stdout (default True).
        """
        super().__init__(name)
        self._db_path = db_path
        self._enable_stdout = enable_stdout
        self._table_ensured = False
        self._conn: Optional["duckdb.DuckDBPyConnection"] = None

    def _get_conn(self) -> "duckdb.DuckDBPyConnection":
        """Get or cache DuckDB connection with thread safety."""
        if self._conn is None:
            with self._lock:
                # Double-check locking pattern
                if self._conn is None:
                    self._conn = _get_duckdb_connection(self._db_path)
        return self._conn

    def _ensure_table(self) -> None:
        """Ensure the telemetry table exists (called once) with thread safety."""
        if self._table_ensured:
            return
        with self._lock:
            # Double-check locking pattern
            if self._table_ensured:
                return
            try:
                _ensure_local_telemetry_table(self._get_conn())
                self._table_ensured = True
            except Exception as e:
                logger.warning(f"Could not ensure local telemetry table: {e}")

    async def close(self) -> None:
        """Clean up resources.

        Closes the DuckDB connection and resets the table ensured flag.
        Should be called when the plugin is no longer needed.
        """
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception as e:
                    logger.warning(f"Error closing DuckDB connection: {e}")
                self._conn = None
            self._table_ensured = False

    def _persist(
        self,
        callback_name: str,
        invocation_context: Optional["InvocationContext"] = None,
        callback_context: Optional[CallbackContext] = None,
        tool_context: Optional[ToolContext] = None,
        event: Optional[Event] = None,
        tool_name: Optional[str] = None,
        model_name: Optional[str] = None,
        tool_blocked: Optional[bool] = None,
        blocked_reason: Optional[str] = None,
        payload: Optional[dict[str, Any]] = None,
    ) -> None:
        """Persist telemetry row to local DuckDB."""
        self._ensure_table()

        # Extract identifiers from available contexts
        app_name: Optional[str] = None
        user_id: Optional[str] = None
        session_id: Optional[str] = None
        invocation_id: Optional[str] = None
        branch: Optional[str] = None
        agent_name: Optional[str] = None
        event_id: Optional[str] = None
        function_call_id: Optional[str] = None

        if invocation_context:
            invocation_id = invocation_context.invocation_id
            session_id = invocation_context.session.id if invocation_context.session else None
            user_id = invocation_context.user_id
            app_name = invocation_context.app_name
            branch = invocation_context.branch
            if hasattr(invocation_context.agent, "name"):
                agent_name = invocation_context.agent.name

        if callback_context:
            invocation_id = invocation_id or callback_context.invocation_id
            agent_name = agent_name or callback_context.agent_name
            # Extract more from underlying invocation context
            if hasattr(callback_context, "_invocation_context"):
                ic = callback_context._invocation_context
                session_id = session_id or (ic.session.id if ic.session else None)
                user_id = user_id or ic.user_id
                app_name = app_name or ic.app_name
                branch = branch or ic.branch

        if tool_context:
            agent_name = agent_name or tool_context.agent_name
            function_call_id = tool_context.function_call_id
            # Extract from underlying invocation context
            if hasattr(tool_context, "_invocation_context"):
                ic = tool_context._invocation_context
                invocation_id = invocation_id or ic.invocation_id
                session_id = session_id or (ic.session.id if ic.session else None)
                user_id = user_id or ic.user_id
                app_name = app_name or ic.app_name
                branch = branch or ic.branch

        if event:
            event_id = event.id

        try:
            _append_local_telemetry_row(
                conn=self._get_conn(),
                callback_name=callback_name,
                app_name=app_name,
                user_id=user_id,
                session_id=session_id,
                invocation_id=invocation_id,
                branch=branch,
                agent_name=agent_name,
                event_id=event_id,
                tool_name=tool_name,
                function_call_id=function_call_id,
                model_name=model_name,
                tool_blocked=tool_blocked,
                blocked_reason=blocked_reason,
                payload=payload,
            )
        except Exception as e:
            logger.error(f"Failed to persist telemetry for {callback_name}: {e}")

    def _log(self, message: str) -> None:
        """Print log message to stdout (grey text)."""
        if self._enable_stdout:
            formatted_message = f"\033[90m[{self.name}] {message}\033[0m"
            print(formatted_message)

    def _format_content(
        self, content: Optional[types.Content], max_length: int = 200
    ) -> str:
        """Format content for logging, truncating if too long."""
        if not content or not content.parts:
            return "None"

        parts = []
        for part in content.parts:
            if part.text:
                text = part.text.strip()
                if len(text) > max_length:
                    text = text[:max_length] + "..."
                parts.append(f"text: '{text}'")
            elif part.function_call:
                parts.append(f"function_call: {part.function_call.name}")
            elif part.function_response:
                parts.append(f"function_response: {part.function_response.name}")
            elif part.code_execution_result:
                parts.append("code_execution_result")
            else:
                parts.append("other_part")

        return " | ".join(parts)

    def _format_args(self, args: dict[str, Any], max_length: int = 300) -> str:
        """Format arguments dictionary for logging."""
        if not args:
            return "{}"

        formatted = str(args)
        if len(formatted) > max_length:
            formatted = formatted[:max_length] + "...}"
        return formatted

    def _content_to_dict(self, content: Optional[types.Content]) -> Optional[dict]:
        """Convert Content to a serializable dict for payload."""
        if not content or not content.parts:
            return None

        parts = []
        for part in content.parts:
            part_data: dict[str, Any] = {}
            if part.text:
                part_data["text"] = part.text
            if part.function_call:
                part_data["function_call"] = {
                    "name": part.function_call.name,
                    "args": dict(part.function_call.args) if part.function_call.args else {},
                }
            if part.function_response:
                part_data["function_response"] = {
                    "name": part.function_response.name,
                    "response": part.function_response.response,
                }
            parts.append(part_data)

        return {"role": content.role, "parts": parts}

    # -------------------------------------------------------------------------
    # Callback implementations
    # -------------------------------------------------------------------------

    async def on_user_message_callback(
        self,
        *,
        invocation_context: "InvocationContext",
        user_message: types.Content,
    ) -> Optional[types.Content]:
        """Log user message and invocation start."""
        self._log("USER MESSAGE RECEIVED")
        self._log(f"   Invocation ID: {invocation_context.invocation_id}")
        self._log(f"   Session ID: {invocation_context.session.id}")
        self._log(f"   User ID: {invocation_context.user_id}")
        self._log(f"   App Name: {invocation_context.app_name}")
        self._log(
            f"   Root Agent: {invocation_context.agent.name if hasattr(invocation_context.agent, 'name') else 'Unknown'}"
        )
        self._log(f"   User Content: {self._format_content(user_message)}")
        if invocation_context.branch:
            self._log(f"   Branch: {invocation_context.branch}")

        self._persist(
            callback_name="on_user_message_callback",
            invocation_context=invocation_context,
            payload={"user_content": self._content_to_dict(user_message)},
        )
        return None

    async def before_run_callback(
        self, *, invocation_context: "InvocationContext"
    ) -> Optional[types.Content]:
        """Log invocation start."""
        self._log("INVOCATION STARTING")
        self._log(f"   Invocation ID: {invocation_context.invocation_id}")
        self._log(
            f"   Starting Agent: {invocation_context.agent.name if hasattr(invocation_context.agent, 'name') else 'Unknown'}"
        )

        self._persist(
            callback_name="before_run_callback",
            invocation_context=invocation_context,
        )
        return None

    async def on_event_callback(
        self, *, invocation_context: "InvocationContext", event: Event
    ) -> Optional[Event]:
        """Log events yielded from the runner."""
        self._log("EVENT YIELDED")
        self._log(f"   Event ID: {event.id}")
        self._log(f"   Author: {event.author}")
        self._log(f"   Content: {self._format_content(event.content)}")
        self._log(f"   Final Response: {event.is_final_response()}")

        func_calls = None
        func_responses = None
        long_running_tools = None

        if event.get_function_calls():
            func_calls = [fc.name for fc in event.get_function_calls()]
            self._log(f"   Function Calls: {func_calls}")

        if event.get_function_responses():
            func_responses = [fr.name for fr in event.get_function_responses()]
            self._log(f"   Function Responses: {func_responses}")

        if event.long_running_tool_ids:
            long_running_tools = list(event.long_running_tool_ids)
            self._log(f"   Long Running Tools: {long_running_tools}")

        self._persist(
            callback_name="on_event_callback",
            invocation_context=invocation_context,
            event=event,
            payload={
                "author": event.author,
                "content": self._content_to_dict(event.content),
                "is_final_response": event.is_final_response(),
                "function_calls": func_calls,
                "function_responses": func_responses,
                "long_running_tool_ids": long_running_tools,
            },
        )
        return None

    async def after_run_callback(
        self, *, invocation_context: "InvocationContext"
    ) -> Optional[None]:
        """Log invocation completion."""
        self._log("INVOCATION COMPLETED")
        self._log(f"   Invocation ID: {invocation_context.invocation_id}")
        self._log(
            f"   Final Agent: {invocation_context.agent.name if hasattr(invocation_context.agent, 'name') else 'Unknown'}"
        )

        self._persist(
            callback_name="after_run_callback",
            invocation_context=invocation_context,
        )
        return None

    async def before_agent_callback(
        self, *, agent: BaseAgent, callback_context: CallbackContext
    ) -> Optional[types.Content]:
        """Log agent execution start."""
        self._log("AGENT STARTING")
        self._log(f"   Agent Name: {callback_context.agent_name}")
        self._log(f"   Invocation ID: {callback_context.invocation_id}")
        if callback_context._invocation_context.branch:
            self._log(f"   Branch: {callback_context._invocation_context.branch}")

        self._persist(
            callback_name="before_agent_callback",
            callback_context=callback_context,
        )
        return None

    async def after_agent_callback(
        self, *, agent: BaseAgent, callback_context: CallbackContext
    ) -> Optional[types.Content]:
        """Log agent execution completion."""
        self._log("AGENT COMPLETED")
        self._log(f"   Agent Name: {callback_context.agent_name}")
        self._log(f"   Invocation ID: {callback_context.invocation_id}")

        self._persist(
            callback_name="after_agent_callback",
            callback_context=callback_context,
        )
        return None

    async def before_model_callback(
        self, *, callback_context: CallbackContext, llm_request: LlmRequest
    ) -> Optional[LlmResponse]:
        """Log LLM request before sending to model.

        Captures invocation-context measurement metrics:
        - LLM call index (monotonic per invocation_id + agent_name)
        - State snapshot metrics (bytes, hash, token estimates)
        - Previous message metrics (role, token estimate, preview)
        """
        model_name = llm_request.model or "default"
        agent_name = callback_context.agent_name
        self._log("LLM REQUEST")
        self._log(f"   Model: {model_name}")
        self._log(f"   Agent: {agent_name}")

        # --- Phase 1: LLM call index tracking ---
        # Increment and store llm_call_index in temp: state
        index_key = _get_llm_call_index_key(agent_name)
        current_index = callback_context.state.get(index_key, 0)
        llm_call_index = current_index + 1
        callback_context.state[index_key] = llm_call_index
        self._log(f"   LLM Call Index: {llm_call_index}")

        # --- Phase 1: State snapshot metrics ---
        state_metrics = _compute_state_metrics(_safe_state_to_dict(callback_context.state))
        self._log(f"   State Keys: {state_metrics['state_keys_count']}")
        self._log(f"   State Bytes: {state_metrics['state_json_bytes']}")
        self._log(f"   State Token Estimate: {state_metrics['state_token_estimate']}")
        self._log(
            f"   State Token Estimate (persistable): {state_metrics['state_token_estimate_persistable_only']}"
        )

        # --- Phase 2: Previous message metrics ---
        prev_message_metrics: dict[str, Any] = {}
        if llm_request.contents and len(llm_request.contents) > 0:
            # Get the last message in the request
            last_content = llm_request.contents[-1]
            prev_message_metrics = _compute_content_metrics(last_content)
            if prev_message_metrics:
                self._log(f"   Prev Message Role: {prev_message_metrics.get('role')}")
                self._log(
                    f"   Prev Message Token Estimate: {prev_message_metrics.get('token_estimate')}"
                )

        # --- Existing telemetry ---
        sys_instruction = None
        if llm_request.config and llm_request.config.system_instruction:
            sys_instruction = llm_request.config.system_instruction[:200]
            if len(llm_request.config.system_instruction) > 200:
                sys_instruction += "..."
            self._log(f"   System Instruction: '{sys_instruction}'")

        tool_names = None
        if llm_request.tools_dict:
            tool_names = list(llm_request.tools_dict.keys())
            self._log(f"   Available Tools: {tool_names}")

        # --- Phase 3: Request snapshot (optional, if ADK_ARTIFACTS_PATH is set) ---
        request_preview = _build_request_preview(llm_request)
        request_snapshot_metadata: dict[str, Any] = {}

        # Get session_id for snapshot filename
        session_id = None
        if hasattr(callback_context, "_invocation_context"):
            ic = callback_context._invocation_context
            session_id = ic.session.id if ic.session else None

        # For local mode, use ADK_LOCAL_ARTIFACTS_PATH; fallback to ADK_ARTIFACTS_PATH
        artifacts_path = os.environ.get("ADK_LOCAL_ARTIFACTS_PATH") or os.environ.get("ADK_ARTIFACTS_PATH")
        if session_id and artifacts_path:
            # Build and save the full request snapshot
            snapshot = _build_request_snapshot(llm_request, callback_context)
            snapshot_result = _save_request_snapshot(
                snapshot=snapshot,
                session_id=session_id,
                invocation_id=callback_context.invocation_id,
                agent_name=agent_name,
                llm_call_index=llm_call_index,
            )
            if snapshot_result:
                request_snapshot_metadata = snapshot_result
                self._log(f"   Request Snapshot: {snapshot_result.get('request_snapshot_path')}")

        # Build enriched payload with all metrics
        payload: dict[str, Any] = {
            # LLM call tracking
            "llm_call": {
                "llm_call_index": llm_call_index,
                "model_name": model_name,
            },
            # State snapshot metrics
            "state_snapshot": state_metrics,
            # Token estimation metadata (method: tiktoken or heuristic)
            "token_estimation": _get_token_estimation_metadata(),
            # NEW: request_last_message - clarified semantics (last message in LlmRequest.contents)
            "request_last_message": {
                "role": prev_message_metrics.get("role"),
                "token_estimate": prev_message_metrics.get("token_estimate"),
                "preview": prev_message_metrics.get("preview"),
            } if prev_message_metrics else {},
            # DEPRECATED: prev_message - kept for backward compatibility
            # Will be removed in a future release. Use request_last_message instead.
            "prev_message": {
                "prev_message_role": prev_message_metrics.get("role"),
                "prev_message_token_estimate": prev_message_metrics.get("token_estimate"),
                "prev_message_preview": prev_message_metrics.get("preview"),
            } if prev_message_metrics else {},
            # Phase 3: Request sampling
            "request_sampling": {
                "request_preview": request_preview,
                **request_snapshot_metadata,
            },
            # Original telemetry fields
            "system_instruction_preview": sys_instruction,
            "available_tools": tool_names,
        }

        self._persist(
            callback_name="before_model_callback",
            callback_context=callback_context,
            model_name=model_name,
            payload=payload,
        )
        return None

    async def after_model_callback(
        self, *, callback_context: CallbackContext, llm_response: LlmResponse
    ) -> Optional[LlmResponse]:
        """Log LLM response after receiving from model.

        Includes the llm_call_index for pairing with before_model_callback
        and authoritative usage metadata from the model.
        """
        agent_name = callback_context.agent_name
        self._log("LLM RESPONSE")
        self._log(f"   Agent: {agent_name}")

        # --- Retrieve llm_call_index from temp: state ---
        index_key = _get_llm_call_index_key(agent_name)
        llm_call_index = callback_context.state.get(index_key, 0)
        self._log(f"   LLM Call Index: {llm_call_index}")

        payload: dict[str, Any] = {
            # LLM call tracking (for pairing with before_model_callback)
            "llm_call": {
                "llm_call_index": llm_call_index,
            },
        }

        if llm_response.error_code:
            self._log(f"   ERROR - Code: {llm_response.error_code}")
            self._log(f"   Error Message: {llm_response.error_message}")
            payload["error_code"] = llm_response.error_code
            payload["error_message"] = llm_response.error_message
        else:
            self._log(f"   Content: {self._format_content(llm_response.content)}")
            if llm_response.partial:
                self._log(f"   Partial: {llm_response.partial}")
            if llm_response.turn_complete is not None:
                self._log(f"   Turn Complete: {llm_response.turn_complete}")

            payload["content"] = self._content_to_dict(llm_response.content)
            payload["partial"] = llm_response.partial
            payload["turn_complete"] = llm_response.turn_complete

        # --- Authoritative usage metadata from the model ---
        if llm_response.usage_metadata:
            prompt_tokens = llm_response.usage_metadata.prompt_token_count
            candidates_tokens = llm_response.usage_metadata.candidates_token_count
            # Check for cached_content_token_count if available
            cached_tokens = getattr(
                llm_response.usage_metadata, "cached_content_token_count", None
            )

            self._log(
                f"   Token Usage - Input: {prompt_tokens}, Output: {candidates_tokens}"
            )
            if cached_tokens is not None:
                self._log(f"   Cached Content Tokens: {cached_tokens}")

            payload["usage_metadata"] = {
                "prompt_token_count": prompt_tokens,
                "candidates_token_count": candidates_tokens,
            }
            if cached_tokens is not None:
                payload["usage_metadata"]["cached_content_token_count"] = cached_tokens

        # --- Response sampling (preview for easy SQL browsing + optional full snapshot) ---
        response_preview = ""
        if llm_response.content and llm_response.content.parts:
            preview_parts = []
            for part in llm_response.content.parts:
                if part.text:
                    text = part.text[:500]
                    if len(part.text) > 500:
                        text += "..."
                    preview_parts.append(text)
                elif part.function_call:
                    preview_parts.append(f"[function_call: {part.function_call.name}]")
            response_preview = " | ".join(preview_parts)

        # --- Response snapshot (optional, for large responses when ADK_ARTIFACTS_PATH is set) ---
        response_snapshot_metadata: dict[str, Any] = {}

        # Get session_id for snapshot filename
        session_id = None
        if hasattr(callback_context, "_invocation_context"):
            ic = callback_context._invocation_context
            session_id = ic.session.id if ic.session else None

        # For local mode, use ADK_LOCAL_ARTIFACTS_PATH; fallback to ADK_ARTIFACTS_PATH
        artifacts_path_for_response = os.environ.get("ADK_LOCAL_ARTIFACTS_PATH") or os.environ.get("ADK_ARTIFACTS_PATH")
        if session_id and artifacts_path_for_response:
            # Build and save the full response snapshot (only if large enough)
            snapshot = _build_response_snapshot(llm_response, callback_context, llm_call_index)
            snapshot_result = _save_response_snapshot(
                snapshot=snapshot,
                session_id=session_id,
                invocation_id=callback_context.invocation_id,
                agent_name=agent_name,
                llm_call_index=llm_call_index,
            )
            if snapshot_result:
                response_snapshot_metadata = snapshot_result
                self._log(f"   Response Snapshot: {snapshot_result.get('response_snapshot_path')}")

        payload["response_sampling"] = {
            "response_preview": response_preview,
            **response_snapshot_metadata,
        }

        self._persist(
            callback_name="after_model_callback",
            callback_context=callback_context,
            payload=payload,
        )
        return None

    async def before_tool_callback(
        self,
        *,
        tool: BaseTool,
        tool_args: dict[str, Any],
        tool_context: ToolContext,
    ) -> Optional[dict]:
        """Log tool execution start."""
        self._log("TOOL STARTING")
        self._log(f"   Tool Name: {tool.name}")
        self._log(f"   Agent: {tool_context.agent_name}")
        self._log(f"   Function Call ID: {tool_context.function_call_id}")
        self._log(f"   Arguments: {self._format_args(tool_args)}")

        self._persist(
            callback_name="before_tool_callback",
            tool_context=tool_context,
            tool_name=tool.name,
            payload={"arguments": tool_args},
        )
        return None

    async def after_tool_callback(
        self,
        *,
        tool: BaseTool,
        tool_args: dict[str, Any],
        tool_context: ToolContext,
        result: dict,
    ) -> Optional[dict]:
        """Log tool execution completion."""
        self._log("TOOL COMPLETED")
        self._log(f"   Tool Name: {tool.name}")
        self._log(f"   Agent: {tool_context.agent_name}")
        self._log(f"   Function Call ID: {tool_context.function_call_id}")
        self._log(f"   Result: {self._format_args(result)}")

        self._persist(
            callback_name="after_tool_callback",
            tool_context=tool_context,
            tool_name=tool.name,
            payload={"arguments": tool_args, "result": result},
        )
        return None

    async def on_model_error_callback(
        self,
        *,
        callback_context: CallbackContext,
        llm_request: LlmRequest,
        error: Exception,
    ) -> Optional[LlmResponse]:
        """Log LLM error.

        Includes llm_call_index for pairing with before_model_callback rows,
        and a request preview for diagnosability.
        """
        model_name = llm_request.model or "default"
        agent_name = callback_context.agent_name
        self._log("LLM ERROR")
        self._log(f"   Agent: {agent_name}")
        self._log(f"   Error: {error}")

        # --- Retrieve llm_call_index from temp: state for pairing ---
        index_key = _get_llm_call_index_key(agent_name)
        llm_call_index = callback_context.state.get(index_key, 0)
        self._log(f"   LLM Call Index: {llm_call_index}")

        # --- Build request preview for diagnosability ---
        request_preview = _build_request_preview(llm_request, max_chars=500)

        self._persist(
            callback_name="on_model_error_callback",
            callback_context=callback_context,
            model_name=model_name,
            payload={
                # LLM call tracking (for pairing with before_model_callback)
                "llm_call": {
                    "llm_call_index": llm_call_index,
                },
                # Error details
                "error": str(error),
                "error_type": type(error).__name__,
                # Request preview for diagnosability
                "request_preview": request_preview,
            },
        )
        return None

    async def on_tool_error_callback(
        self,
        *,
        tool: BaseTool,
        tool_args: dict[str, Any],
        tool_context: ToolContext,
        error: Exception,
    ) -> Optional[dict]:
        """Log tool error."""
        self._log("TOOL ERROR")
        self._log(f"   Tool Name: {tool.name}")
        self._log(f"   Agent: {tool_context.agent_name}")
        self._log(f"   Function Call ID: {tool_context.function_call_id}")
        self._log(f"   Arguments: {self._format_args(tool_args)}")
        self._log(f"   Error: {error}")

        self._persist(
            callback_name="on_tool_error_callback",
            tool_context=tool_context,
            tool_name=tool.name,
            payload={
                "arguments": tool_args,
                "error": str(error),
                "error_type": type(error).__name__,
            },
        )
        return None
