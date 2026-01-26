"""UC Delta Telemetry Plugin for ADK.

A custom ADK plugin that:
1. Preserves stdout print() logging for terminal observability (like LoggingPlugin)
2. Persists callback-level telemetry to a Unity Catalog Delta table (adk_telemetry)
3. Supports special logging flags for blocked tool executions (safety plugin integration)
4. Captures invocation-context measurement for each LlmAgent context window:
   - State token estimates (including and excluding temp: keys)
   - Previous message token estimates
   - LLM call indexing per (invocation_id, agent_name)

Table: silo_dev_rs.adk.adk_telemetry (configurable via env vars)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
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

if TYPE_CHECKING:
    from google.adk.agents.invocation_context import InvocationContext
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper functions for state/message metrics (Phase 1 & 2)
# ---------------------------------------------------------------------------

def _canonical_json(obj: Any) -> str:
    """Serialize object to canonical JSON (sorted keys, no extra whitespace).

    This ensures consistent hashing across runs regardless of dict ordering.

    Args:
        obj: The object to serialize.

    Returns:
        Canonical JSON string.
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def _compute_sha256(data: str) -> str:
    """Compute SHA-256 hash of a string.

    Args:
        data: The string to hash.

    Returns:
        Hex digest of the SHA-256 hash.
    """
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Token estimation with tiktoken (with heuristic fallback)
# ---------------------------------------------------------------------------

# Module-level cache for tiktoken encoder (lazy loaded)
_tiktoken_encoder = None
_tiktoken_available = None


def _get_tiktoken_encoder():
    """Get or initialize the tiktoken encoder (lazy loaded).

    Uses cl100k_base encoding which is stable and works well for most models.
    Falls back gracefully if tiktoken is not available.

    Returns:
        Tuple of (encoder, is_available). Encoder is None if not available.
    """
    global _tiktoken_encoder, _tiktoken_available

    if _tiktoken_available is None:
        try:
            import tiktoken
            _tiktoken_encoder = tiktoken.get_encoding("cl100k_base")
            _tiktoken_available = True
            logger.debug("tiktoken encoder initialized (cl100k_base)")
        except ImportError:
            _tiktoken_encoder = None
            _tiktoken_available = False
            logger.info("tiktoken not available, using heuristic token estimation")
        except Exception as e:
            _tiktoken_encoder = None
            _tiktoken_available = False
            logger.warning(f"Failed to initialize tiktoken: {e}, using heuristic")

    return _tiktoken_encoder, _tiktoken_available


def _estimate_tokens(text: str, chars_per_token: float = 4.0) -> int:
    """Estimate token count using tiktoken if available, else heuristic.

    Uses tiktoken's cl100k_base encoding for accurate counts.
    Falls back to character-based heuristic (~4 chars/token) if tiktoken
    is not available or fails.

    Args:
        text: The text to estimate tokens for.
        chars_per_token: Average characters per token for fallback (default 4.0).

    Returns:
        Estimated token count.
    """
    if not text:
        return 0

    encoder, available = _get_tiktoken_encoder()
    if available and encoder is not None:
        try:
            return len(encoder.encode(text))
        except Exception:
            # Fall back to heuristic if encoding fails
            pass

    return max(1, int(len(text) / chars_per_token))


def _get_token_estimation_metadata() -> dict[str, Any]:
    """Get metadata about the token estimation method being used.

    Returns:
        Dictionary with estimation method metadata:
        - method: 'tiktoken' or 'heuristic'
        - encoding: encoding name if tiktoken (e.g., 'cl100k_base')
    """
    _, available = _get_tiktoken_encoder()
    if available:
        return {
            "method": "tiktoken",
            "encoding": "cl100k_base",
        }
    else:
        return {
            "method": "heuristic",
            "chars_per_token": 4.0,
        }


def _filter_persistable_state(state: dict[str, Any]) -> dict[str, Any]:
    """Filter out temp: keys from state (these are not persisted).

    Args:
        state: The full state dictionary.

    Returns:
        State dictionary with only persistable keys (no temp:* keys).
    """
    return {k: v for k, v in state.items() if not k.startswith("temp:")}


def _safe_state_to_dict(state_obj: Any) -> dict[str, Any]:
    """Safely convert ADK State object to dict.

    Workaround for State objects that implement __getitem__ but not __iter__,
    causing dict() to try sequence iteration (accessing index 0).
    """
    # Try direct _value access (fastest and confirmed by traceback to exist)
    if hasattr(state_obj, "_value") and isinstance(state_obj._value, dict):
        return dict(state_obj._value)

    # Try to_dict()
    if hasattr(state_obj, "to_dict") and callable(state_obj.to_dict):
        return state_obj.to_dict()

    # Try keys()
    if hasattr(state_obj, "keys") and callable(state_obj.keys):
        return {k: state_obj[k] for k in state_obj.keys()}

    # If it's already a dict
    if isinstance(state_obj, dict):
        return state_obj

    # Last resort: try dict(), keeping the original error if it fails
    return dict(state_obj)


def _compute_state_metrics(state: dict[str, Any]) -> dict[str, Any]:
    """Compute metrics for the current state.

    Args:
        state: The callback_context.state dictionary.

    Returns:
        Dictionary with state metrics:
        - state_keys_count: Number of keys
        - state_json_bytes: Size of serialized state
        - state_sha256: Hash of canonical JSON
        - state_token_estimate: Estimated tokens for full state
        - state_token_estimate_persistable_only: Tokens excluding temp: keys
    """
    # Full state metrics
    state_json = _canonical_json(dict(state))
    state_bytes = len(state_json.encode("utf-8"))
    state_hash = _compute_sha256(state_json)
    state_tokens = _estimate_tokens(state_json)

    # Persistable-only metrics (excluding temp: keys)
    persistable_state = _filter_persistable_state(state)
    persistable_json = _canonical_json(persistable_state)
    persistable_tokens = _estimate_tokens(persistable_json)

    return {
        "state_keys_count": len(state),
        "state_json_bytes": state_bytes,
        "state_sha256": state_hash,
        "state_token_estimate": state_tokens,
        "state_token_estimate_persistable_only": persistable_tokens,
    }


def _compute_content_metrics(
    content: Optional[types.Content],
    max_preview_chars: int = 500,
) -> dict[str, Any]:
    """Compute metrics for a Content object (e.g., previous message).

    Args:
        content: The Content object to analyze.
        max_preview_chars: Maximum characters for preview text.

    Returns:
        Dictionary with content metrics:
        - role: The content role (user, model, etc.)
        - token_estimate: Estimated tokens
        - preview: Truncated text preview
    """
    if not content or not content.parts:
        return {}

    role = content.role or "unknown"

    # Extract text from all parts
    text_parts = []
    for part in content.parts:
        if part.text:
            text_parts.append(part.text)
        elif part.function_call:
            text_parts.append(f"[function_call: {part.function_call.name}]")
        elif part.function_response:
            text_parts.append(f"[function_response: {part.function_response.name}]")

    full_text = " ".join(text_parts)
    token_estimate = _estimate_tokens(full_text)

    # Create preview
    preview = full_text[:max_preview_chars]
    if len(full_text) > max_preview_chars:
        preview += "..."

    return {
        "role": role,
        "token_estimate": token_estimate,
        "preview": preview,
    }


def _get_llm_call_index_key(agent_name: str) -> str:
    """Get the temp: state key for tracking LLM call index per agent.

    Args:
        agent_name: The agent name.

    Returns:
        State key in format 'temp:telemetry:llm_call_index:{agent_name}'.
    """
    return f"temp:telemetry:llm_call_index:{agent_name}"


def _build_request_snapshot(
    llm_request: "LlmRequest",
    callback_context: "CallbackContext",
) -> dict[str, Any]:
    """Build a full request snapshot for telemetry.

    Captures the complete request structure that enters the LLM:
    - System instruction
    - Tool schema names
    - Full message list with content

    Args:
        llm_request: The LlmRequest object.
        callback_context: The callback context.

    Returns:
        Dictionary with the complete request snapshot.
    """
    from google.adk.agents.callback_context import CallbackContext
    from google.adk.models.llm_request import LlmRequest

    snapshot: dict[str, Any] = {
        "agent_name": callback_context.agent_name,
        "invocation_id": callback_context.invocation_id,
        "model": llm_request.model,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # System instruction
    if llm_request.config and llm_request.config.system_instruction:
        snapshot["system_instruction"] = llm_request.config.system_instruction

    # Tool names (not full schemas to keep size manageable)
    if llm_request.tools_dict:
        snapshot["tool_names"] = list(llm_request.tools_dict.keys())

    # Full message list
    if llm_request.contents:
        messages = []
        for content in llm_request.contents:
            msg: dict[str, Any] = {"role": content.role}
            parts_data = []
            if content.parts:
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
                    if part_data:
                        parts_data.append(part_data)
            msg["parts"] = parts_data
            messages.append(msg)
        snapshot["messages"] = messages

    return snapshot


def _save_request_snapshot(
    snapshot: dict[str, Any],
    session_id: str,
    invocation_id: str,
    agent_name: str,
    llm_call_index: int,
    artifacts_path: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """Save request snapshot to UC Volumes and return pointer metadata.

    Args:
        snapshot: The request snapshot dictionary.
        session_id: Session ID.
        invocation_id: Invocation ID.
        agent_name: Agent name.
        llm_call_index: The LLM call index.
        artifacts_path: Path to artifacts directory (default from env).

    Returns:
        Dictionary with snapshot pointer metadata:
        - request_snapshot_path: Full path to saved snapshot
        - request_snapshot_sha256: Hash of the snapshot
        - request_snapshot_bytes: Size of the snapshot
        Or None if saving is disabled or fails.
    """
    if artifacts_path is None:
        artifacts_path = os.environ.get("ADK_ARTIFACTS_PATH")

    if not artifacts_path:
        # Snapshot saving is disabled
        return None

    try:
        # Serialize snapshot
        snapshot_json = _canonical_json(snapshot)
        snapshot_bytes = len(snapshot_json.encode("utf-8"))
        snapshot_hash = _compute_sha256(snapshot_json)

        # Build filename with identifiers for easy lookup
        safe_agent = re.sub(r'[^a-zA-Z0-9_-]', '_', agent_name)
        filename = f"request_snapshot_{session_id}_{invocation_id}_{safe_agent}_{llm_call_index}.json"

        # Use a telemetry subfolder to separate from other artifacts
        telemetry_path = os.path.join(artifacts_path, "telemetry", "request_snapshots")
        os.makedirs(telemetry_path, exist_ok=True)

        full_path = os.path.join(telemetry_path, filename)

        # Write the snapshot
        with open(full_path, "w") as f:
            f.write(snapshot_json)

        logger.debug(f"Request snapshot saved: {full_path}")

        return {
            "request_snapshot_path": full_path,
            "request_snapshot_sha256": snapshot_hash,
            "request_snapshot_bytes": snapshot_bytes,
        }

    except Exception as e:
        logger.warning(f"Failed to save request snapshot: {e}")
        return None


def _build_request_preview(
    llm_request: "LlmRequest",
    max_chars: int = 1000,
) -> str:
    """Build a small preview of the request for inline storage in telemetry.

    Args:
        llm_request: The LlmRequest object.
        max_chars: Maximum characters for the preview.

    Returns:
        String preview of the request.
    """
    preview_parts = []

    # Model
    preview_parts.append(f"model={llm_request.model or 'default'}")

    # System instruction preview
    if llm_request.config and llm_request.config.system_instruction:
        sys_instr = llm_request.config.system_instruction[:200]
        if len(llm_request.config.system_instruction) > 200:
            sys_instr += "..."
        preview_parts.append(f"system={sys_instr}")

    # Tool count
    if llm_request.tools_dict:
        preview_parts.append(f"tools={list(llm_request.tools_dict.keys())}")

    # Message count and last message preview
    if llm_request.contents:
        msg_count = len(llm_request.contents)
        preview_parts.append(f"messages={msg_count}")

        # Last message preview
        if llm_request.contents:
            last = llm_request.contents[-1]
            last_text = ""
            if last.parts:
                for part in last.parts:
                    if part.text:
                        last_text = part.text[:200]
                        if len(part.text) > 200:
                            last_text += "..."
                        break
            if last_text:
                preview_parts.append(f"last_msg({last.role})={last_text}")

    preview = " | ".join(preview_parts)
    if len(preview) > max_chars:
        preview = preview[:max_chars] + "..."

    return preview


def _build_response_snapshot(
    llm_response: "LlmResponse",
    callback_context: "CallbackContext",
    llm_call_index: int,
) -> dict[str, Any]:
    """Build a full response snapshot for telemetry.

    Captures the complete response structure from the LLM:
    - Full content (all parts)
    - Usage metadata
    - Streaming indicators

    Args:
        llm_response: The LlmResponse object.
        callback_context: The callback context.
        llm_call_index: The LLM call index for this response.

    Returns:
        Dictionary with the complete response snapshot.
    """
    from google.adk.agents.callback_context import CallbackContext
    from google.adk.models.llm_response import LlmResponse

    snapshot: dict[str, Any] = {
        "agent_name": callback_context.agent_name,
        "invocation_id": callback_context.invocation_id,
        "llm_call_index": llm_call_index,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Content
    if llm_response.content and llm_response.content.parts:
        content_parts = []
        for part in llm_response.content.parts:
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
            if part_data:
                content_parts.append(part_data)
        snapshot["content"] = {
            "role": llm_response.content.role,
            "parts": content_parts,
        }

    # Usage metadata
    if llm_response.usage_metadata:
        snapshot["usage_metadata"] = {
            "prompt_token_count": llm_response.usage_metadata.prompt_token_count,
            "candidates_token_count": llm_response.usage_metadata.candidates_token_count,
        }
        cached = getattr(llm_response.usage_metadata, "cached_content_token_count", None)
        if cached is not None:
            snapshot["usage_metadata"]["cached_content_token_count"] = cached

    # Streaming indicators
    snapshot["partial"] = llm_response.partial
    snapshot["turn_complete"] = llm_response.turn_complete

    # Error info if present
    if llm_response.error_code:
        snapshot["error_code"] = llm_response.error_code
        snapshot["error_message"] = llm_response.error_message

    return snapshot


def _save_response_snapshot(
    snapshot: dict[str, Any],
    session_id: str,
    invocation_id: str,
    agent_name: str,
    llm_call_index: int,
    artifacts_path: Optional[str] = None,
    size_threshold: int = 2000,
) -> Optional[dict[str, Any]]:
    """Save response snapshot to UC Volumes and return pointer metadata.

    Only saves if the response exceeds the size threshold (for large responses).

    Args:
        snapshot: The response snapshot dictionary.
        session_id: Session ID.
        invocation_id: Invocation ID.
        agent_name: Agent name.
        llm_call_index: The LLM call index.
        artifacts_path: Path to artifacts directory (default from env).
        size_threshold: Minimum bytes to trigger snapshot save (default 2000).

    Returns:
        Dictionary with snapshot pointer metadata:
        - response_snapshot_path: Full path to saved snapshot
        - response_snapshot_sha256: Hash of the snapshot
        - response_snapshot_bytes: Size of the snapshot
        Or None if saving is disabled, fails, or response is small.
    """
    if artifacts_path is None:
        artifacts_path = os.environ.get("ADK_ARTIFACTS_PATH")

    if not artifacts_path:
        # Snapshot saving is disabled
        return None

    try:
        # Serialize snapshot
        snapshot_json = _canonical_json(snapshot)
        snapshot_bytes = len(snapshot_json.encode("utf-8"))

        # Only save large responses (small ones are captured in response_preview)
        if snapshot_bytes < size_threshold:
            return None

        snapshot_hash = _compute_sha256(snapshot_json)

        # Build filename with identifiers for easy lookup
        safe_agent = re.sub(r'[^a-zA-Z0-9_-]', '_', agent_name)
        filename = f"response_snapshot_{session_id}_{invocation_id}_{safe_agent}_{llm_call_index}.json"

        # Use a telemetry subfolder to separate from other artifacts
        telemetry_path = os.path.join(artifacts_path, "telemetry", "response_snapshots")
        os.makedirs(telemetry_path, exist_ok=True)

        full_path = os.path.join(telemetry_path, filename)

        # Write the snapshot
        with open(full_path, "w") as f:
            f.write(snapshot_json)

        logger.debug(f"Response snapshot saved: {full_path}")

        return {
            "response_snapshot_path": full_path,
            "response_snapshot_sha256": snapshot_hash,
            "response_snapshot_bytes": snapshot_bytes,
        }

    except Exception as e:
        logger.warning(f"Failed to save response snapshot: {e}")
        return None


# Configuration from environment
ADK_DELTA_CATALOG = os.environ.get("ADK_DELTA_CATALOG", "silo_dev_rs")
ADK_DELTA_SCHEMA = os.environ.get("ADK_DELTA_SCHEMA", "adk")
ADK_AGENT_TELEMETRY_TABLE = os.environ.get("ADK_AGENT_TELEMETRY_TABLE", "adk_telemetry")


def _get_spark() -> "SparkSession":
    """Get or create SparkSession."""
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()


def _validate_identifier(name: str, identifier_type: str) -> str:
    """Validate SQL identifier to prevent injection.

    Args:
        name: The identifier name to validate.
        identifier_type: Type of identifier (e.g., 'catalog', 'schema', 'table').

    Returns:
        The validated identifier name.

    Raises:
        ValueError: If the identifier contains invalid characters.
    """
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        raise ValueError(f"Invalid {identifier_type} name: {name}")
    return name


def _get_telemetry_table_name(
    catalog: str = ADK_DELTA_CATALOG,
    schema: str = ADK_DELTA_SCHEMA,
    table: str = ADK_AGENT_TELEMETRY_TABLE,
) -> str:
    """Get fully qualified telemetry table name.

    Validates all identifiers to prevent SQL injection.

    Args:
        catalog: Unity Catalog name.
        schema: Schema name within the catalog.
        table: Table name for telemetry.

    Returns:
        Fully qualified table name in format 'catalog.schema.table'.

    Raises:
        ValueError: If any identifier contains invalid characters.
    """
    validated_catalog = _validate_identifier(catalog, "catalog")
    validated_schema = _validate_identifier(schema, "schema")
    validated_table = _validate_identifier(table, "table")
    return f"{validated_catalog}.{validated_schema}.{validated_table}"


def _ensure_adk_telemetry_table(
    spark: "SparkSession",
    catalog: str = ADK_DELTA_CATALOG,
    schema: str = ADK_DELTA_SCHEMA,
    table: str = ADK_AGENT_TELEMETRY_TABLE,
) -> None:
    """Create the adk_telemetry table if it doesn't exist."""
    table_name = _get_telemetry_table_name(catalog, schema, table)

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            telemetry_id STRING NOT NULL,
            ts TIMESTAMP NOT NULL,
            app_name STRING,
            user_id STRING,
            session_id STRING,
            invocation_id STRING,
            branch STRING,
            agent_name STRING,
            callback_name STRING NOT NULL,
            event_id STRING,
            tool_name STRING,
            function_call_id STRING,
            model_name STRING,
            tool_blocked BOOLEAN,
            blocked_reason STRING,
            payload_json STRING,
            created_time TIMESTAMP NOT NULL
        )
        USING DELTA
        PARTITIONED BY (app_name)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """

    try:
        spark.sql(create_sql)
        logger.info(f"ADK telemetry table ready: {table_name}")
    except Exception as e:
        logger.error(f"Failed to create ADK telemetry table: {e}")
        raise


def _append_telemetry_row(
    spark: "SparkSession",
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
    catalog: str = ADK_DELTA_CATALOG,
    schema: str = ADK_DELTA_SCHEMA,
    table: str = ADK_AGENT_TELEMETRY_TABLE,
) -> str:
    """Append a telemetry row to the adk_telemetry table.

    Uses PySpark DataFrame API for safe parameterized inserts,
    avoiding SQL injection vulnerabilities.

    Returns:
        The telemetry_id of the inserted row.
    """
    from pyspark.sql import Row
    from pyspark.sql.types import (
        BooleanType,
        StructType,
        StructField,
        StringType,
        TimestampType,
    )

    table_name = _get_telemetry_table_name(catalog, schema, table)

    telemetry_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc)
    created_time = ts

    # Serialize payload to JSON
    payload_json = json.dumps(payload) if payload else "{}"

    # Create a Row with all telemetry fields
    row = Row(
        telemetry_id=telemetry_id,
        ts=ts,
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
        invocation_id=invocation_id,
        branch=branch,
        agent_name=agent_name,
        callback_name=callback_name,
        event_id=event_id,
        tool_name=tool_name,
        function_call_id=function_call_id,
        model_name=model_name,
        tool_blocked=tool_blocked,
        blocked_reason=blocked_reason,
        payload_json=payload_json,
        created_time=created_time,
    )

    # Explicit schema to prevent inference errors on None fields
    telemetry_schema = StructType(
        [
            StructField("telemetry_id", StringType(), False),
            StructField("ts", TimestampType(), False),
            StructField("app_name", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("invocation_id", StringType(), True),
            StructField("branch", StringType(), True),
            StructField("agent_name", StringType(), True),
            StructField("callback_name", StringType(), False),
            StructField("event_id", StringType(), True),
            StructField("tool_name", StringType(), True),
            StructField("function_call_id", StringType(), True),
            StructField("model_name", StringType(), True),
            StructField("tool_blocked", BooleanType(), True),
            StructField("blocked_reason", StringType(), True),
            StructField("payload_json", StringType(), True),
            StructField("created_time", TimestampType(), False),
        ]
    )

    try:
        # Use DataFrame API for safe parameterized insert with explicit schema
        # Enable mergeSchema for schema evolution (e.g., new columns like tool_blocked)
        spark.createDataFrame([row], schema=telemetry_schema).write.mode("append").option("mergeSchema", "true").saveAsTable(table_name)
        logger.debug(f"ADK telemetry row inserted: {callback_name} ({telemetry_id})")
        return telemetry_id
    except Exception as e:
        logger.error(f"Failed to append ADK telemetry row: {e}")
        raise


class UcDeltaTelemetryPlugin(BasePlugin):
    """ADK plugin that logs to stdout and persists telemetry to UC Delta.

    This plugin preserves the LoggingPlugin's stdout behavior for terminal
    debugging while also persisting callback telemetry to a Unity Catalog
    Delta table for analytics and auditing.

    Table: silo_dev_rs.adk.adk_telemetry (configurable via environment)

    Environment Variables:
        ADK_DELTA_CATALOG: Catalog name (default: silo_dev_rs)
        ADK_DELTA_SCHEMA: Schema name (default: adk)
        ADK_AGENT_TELEMETRY_TABLE: Table name (default: adk_telemetry)

    Example:
        >>> plugin = UcDeltaTelemetryPlugin()
        >>> runner = Runner(
        ...     agent=my_agent,
        ...     plugins=[plugin],
        ...     ...
        ... )
    """

    # Class-level lock for thread safety (RLock allows re-entrant acquisition
    # so _ensure_table() can safely call _get_spark() while holding the lock)
    _lock = threading.RLock()

    def __init__(
        self,
        name: str = "uc_delta_telemetry_plugin",
        catalog: str = ADK_DELTA_CATALOG,
        schema: str = ADK_DELTA_SCHEMA,
        table: str = ADK_AGENT_TELEMETRY_TABLE,
        enable_stdout: bool = True,
    ):
        """Initialize the UC Delta telemetry plugin.

        Args:
            name: Plugin instance name.
            catalog: Unity Catalog name.
            schema: Schema name within the catalog.
            table: Table name for telemetry.
            enable_stdout: Whether to print logs to stdout (default True).
        """
        super().__init__(name)
        self._catalog = catalog
        self._schema = schema
        self._table = table
        self._enable_stdout = enable_stdout
        self._table_ensured = False
        self._spark: Optional["SparkSession"] = None

    def _get_spark(self) -> "SparkSession":
        """Get or cache SparkSession with thread safety."""
        if self._spark is None:
            with self._lock:
                # Double-check locking pattern
                if self._spark is None:
                    self._spark = _get_spark()
        return self._spark

    def _ensure_table(self) -> None:
        """Ensure the telemetry table exists (called once) with thread safety."""
        if self._table_ensured:
            return
        with self._lock:
            # Double-check locking pattern
            if self._table_ensured:
                return
            try:
                _ensure_adk_telemetry_table(
                    self._get_spark(),
                    self._catalog,
                    self._schema,
                    self._table,
                )
                self._table_ensured = True
            except Exception as e:
                logger.warning(f"Could not ensure telemetry table: {e}")

    async def close(self) -> None:
        """Clean up resources.

        Resets the SparkSession reference and table ensured flag.
        Should be called when the plugin is no longer needed.
        """
        with self._lock:
            self._spark = None
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
        """Persist telemetry row to UC Delta."""
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
            _append_telemetry_row(
                spark=self._get_spark(),
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
                catalog=self._catalog,
                schema=self._schema,
                table=self._table,
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

        if session_id and os.environ.get("ADK_ARTIFACTS_PATH"):
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

        if session_id and os.environ.get("ADK_ARTIFACTS_PATH"):
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
