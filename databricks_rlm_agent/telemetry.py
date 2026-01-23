"""Telemetry table for RLM Agent execution tracking.

This module provides functions to create and append to a Unity Catalog Delta
table that tracks orchestrator and executor events. This is separate from the
session events table managed by DeltaSessionService.

Table Schema:
    - event_id: Unique event identifier
    - event_type: Type of event (orchestrator_start, executor_complete, etc.)
    - component: Which component (orchestrator, executor)
    - run_id: The orchestrator's run identifier
    - iteration: Iteration number
    - timestamp: Event timestamp
    - metadata_json: JSON blob with additional event data
    - created_time: Row creation timestamp
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Table name constant
TELEMETRY_TABLE_NAME = "telemetry"


def _get_telemetry_table_name(catalog: str, schema: str) -> str:
    """Get fully qualified telemetry table name."""
    return f"{catalog}.{schema}.{TELEMETRY_TABLE_NAME}"


def ensure_telemetry_table(
    spark: "SparkSession",
    catalog: str,
    schema: str,
) -> None:
    """Create the telemetry table if it doesn't exist.
    
    Args:
        spark: Active SparkSession.
        catalog: Unity Catalog name.
        schema: Schema name within the catalog.
    """
    table_name = _get_telemetry_table_name(catalog, schema)
    
    logger.info(f"Ensuring telemetry table exists: {table_name}")
    
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            event_id STRING NOT NULL,
            event_type STRING NOT NULL,
            component STRING NOT NULL,
            run_id STRING,
            iteration INT,
            timestamp TIMESTAMP NOT NULL,
            metadata_json STRING,
            created_time TIMESTAMP NOT NULL
        )
        USING DELTA
        PARTITIONED BY (component)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """
    
    try:
        spark.sql(create_sql)
        logger.info(f"Telemetry table ready: {table_name}")
    except Exception as e:
        logger.error(f"Failed to create telemetry table: {e}")
        raise


def append_telemetry_event(
    spark: "SparkSession",
    catalog: str,
    schema: str,
    event_type: str,
    component: str,
    run_id: Optional[str] = None,
    iteration: Optional[int] = None,
    metadata: Optional[dict[str, Any]] = None,
    event_id: Optional[str] = None,
    timestamp: Optional[datetime] = None,
) -> str:
    """Append a telemetry event to the table.
    
    Args:
        spark: Active SparkSession.
        catalog: Unity Catalog name.
        schema: Schema name.
        event_type: Type of event (e.g., "orchestrator_start", "executor_complete").
        component: Component name (e.g., "orchestrator", "executor").
        run_id: Optional run identifier.
        iteration: Optional iteration number.
        metadata: Optional dictionary of additional event data.
        event_id: Optional event ID (generated if not provided).
        timestamp: Optional event timestamp (current time if not provided).
        
    Returns:
        The event_id of the inserted event.
    """
    table_name = _get_telemetry_table_name(catalog, schema)
    
    # Generate event_id if not provided
    if event_id is None:
        event_id = str(uuid.uuid4())
    
    # Use current time if not provided
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
    
    # Serialize metadata to JSON
    metadata_json = json.dumps(metadata) if metadata else "{}"
    
    # Current time for created_time
    created_time = datetime.now(timezone.utc)
    
    # Escape single quotes in metadata_json for SQL
    metadata_json_escaped = metadata_json.replace("'", "''")
    
    insert_sql = f"""
        INSERT INTO {table_name}
        (event_id, event_type, component, run_id, iteration, timestamp, metadata_json, created_time)
        VALUES (
            '{event_id}',
            '{event_type}',
            '{component}',
            {f"'{run_id}'" if run_id else "NULL"},
            {iteration if iteration is not None else "NULL"},
            TIMESTAMP '{timestamp.isoformat()}',
            '{metadata_json_escaped}',
            TIMESTAMP '{created_time.isoformat()}'
        )
    """
    
    try:
        spark.sql(insert_sql)
        logger.info(f"Telemetry event recorded: {event_type} ({event_id})")
        return event_id
    except Exception as e:
        logger.error(f"Failed to append telemetry event: {e}")
        raise


def query_telemetry(
    spark: "SparkSession",
    catalog: str,
    schema: str,
    run_id: Optional[str] = None,
    component: Optional[str] = None,
    event_type: Optional[str] = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Query telemetry events.
    
    Args:
        spark: Active SparkSession.
        catalog: Unity Catalog name.
        schema: Schema name.
        run_id: Optional filter by run_id.
        component: Optional filter by component.
        event_type: Optional filter by event_type.
        limit: Maximum number of results.
        
    Returns:
        List of telemetry event dictionaries.
    """
    table_name = _get_telemetry_table_name(catalog, schema)
    
    conditions = []
    if run_id:
        conditions.append(f"run_id = '{run_id}'")
    if component:
        conditions.append(f"component = '{component}'")
    if event_type:
        conditions.append(f"event_type = '{event_type}'")
    
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    query = f"""
        SELECT * FROM {table_name}
        WHERE {where_clause}
        ORDER BY timestamp DESC
        LIMIT {limit}
    """
    
    try:
        df = spark.sql(query)
        rows = df.collect()
        
        results = []
        for row in rows:
            event = {
                "event_id": row.event_id,
                "event_type": row.event_type,
                "component": row.component,
                "run_id": row.run_id,
                "iteration": row.iteration,
                "timestamp": row.timestamp.isoformat() if row.timestamp else None,
                "metadata": json.loads(row.metadata_json) if row.metadata_json else {},
                "created_time": row.created_time.isoformat() if row.created_time else None,
            }
            results.append(event)
        
        return results
        
    except Exception as e:
        logger.error(f"Failed to query telemetry: {e}")
        return []


def get_run_summary(
    spark: "SparkSession",
    catalog: str,
    schema: str,
    run_id: str,
) -> dict[str, Any]:
    """Get a summary of events for a specific run.
    
    Args:
        spark: Active SparkSession.
        catalog: Unity Catalog name.
        schema: Schema name.
        run_id: The run identifier.
        
    Returns:
        Summary dictionary with event counts and status.
    """
    events = query_telemetry(
        spark=spark,
        catalog=catalog,
        schema=schema,
        run_id=run_id,
        limit=1000,
    )
    
    summary = {
        "run_id": run_id,
        "total_events": len(events),
        "event_types": {},
        "components": {},
        "latest_event": None,
        "earliest_event": None,
        "iterations": set(),
    }
    
    for event in events:
        # Count by event type
        etype = event["event_type"]
        summary["event_types"][etype] = summary["event_types"].get(etype, 0) + 1
        
        # Count by component
        comp = event["component"]
        summary["components"][comp] = summary["components"].get(comp, 0) + 1
        
        # Track iterations
        if event["iteration"] is not None:
            summary["iterations"].add(event["iteration"])
    
    # Convert iterations set to sorted list
    summary["iterations"] = sorted(summary["iterations"])
    
    # Get earliest and latest events
    if events:
        summary["latest_event"] = events[0]  # Already sorted DESC
        summary["earliest_event"] = events[-1]
    
    return summary

