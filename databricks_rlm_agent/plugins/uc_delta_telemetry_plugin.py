"""UC Delta Telemetry Plugin for ADK.

A custom ADK plugin that:
1. Preserves stdout print() logging for terminal observability (like LoggingPlugin)
2. Persists callback-level telemetry to a Unity Catalog Delta table (adk_telemetry)

Table: silo_dev_rs.adk.adk_telemetry (configurable via env vars)
"""

from __future__ import annotations

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
        payload_json=payload_json,
        created_time=created_time,
    )

    try:
        # Use DataFrame API for safe parameterized insert
        spark.createDataFrame([row]).write.mode("append").saveAsTable(table_name)
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

    # Class-level lock for thread safety
    _lock = threading.Lock()

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
        """Log LLM request before sending to model."""
        model_name = llm_request.model or "default"
        self._log("LLM REQUEST")
        self._log(f"   Model: {model_name}")
        self._log(f"   Agent: {callback_context.agent_name}")

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

        self._persist(
            callback_name="before_model_callback",
            callback_context=callback_context,
            model_name=model_name,
            payload={
                "system_instruction_preview": sys_instruction,
                "available_tools": tool_names,
            },
        )
        return None

    async def after_model_callback(
        self, *, callback_context: CallbackContext, llm_response: LlmResponse
    ) -> Optional[LlmResponse]:
        """Log LLM response after receiving from model."""
        self._log("LLM RESPONSE")
        self._log(f"   Agent: {callback_context.agent_name}")

        payload: dict[str, Any] = {}

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

        if llm_response.usage_metadata:
            self._log(
                f"   Token Usage - Input: {llm_response.usage_metadata.prompt_token_count}, "
                f"Output: {llm_response.usage_metadata.candidates_token_count}"
            )
            payload["usage_metadata"] = {
                "prompt_token_count": llm_response.usage_metadata.prompt_token_count,
                "candidates_token_count": llm_response.usage_metadata.candidates_token_count,
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
        """Log LLM error."""
        model_name = llm_request.model or "default"
        self._log("LLM ERROR")
        self._log(f"   Agent: {callback_context.agent_name}")
        self._log(f"   Error: {error}")

        self._persist(
            callback_name="on_model_error_callback",
            callback_context=callback_context,
            model_name=model_name,
            payload={"error": str(error), "error_type": type(error).__name__},
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
