-- LLM Context Telemetry Analysis View (v2)
-- Pairs before_model_callback and after_model_callback telemetry rows
-- to provide comprehensive per-LLM-call metrics for post-run evaluation.
--
-- Table: silo_dev_rs.adk.adk_telemetry
-- This view enables analysis of:
--   - State token estimates (including and excluding temp: keys)
--   - Request last message token estimates (clarified semantics)
--   - Authoritative prompt token counts from model usage metadata
--   - Request/response snapshots and previews
--   - Model errors paired with their before_model_callback rows
--
-- v2 Changes:
--   - De-duplicates after_model_callback rows using ROW_NUMBER() window function
--   - Adds on_model_error_callback pairing via llm_call_index
--   - New request_last_message fields with fallback to deprecated prev_message
--   - Token estimation metadata (method: tiktoken or heuristic)
--
-- Usage:
--   SELECT * FROM llm_context_telemetry
--   WHERE session_id = '<your_session_id>'
--   ORDER BY ts_before ASC;

CREATE OR REPLACE VIEW silo_dev_rs.adk.llm_context_telemetry AS
WITH 
-- Extract before_model_callback rows with parsed payload
before_model AS (
    SELECT
        session_id,
        invocation_id,
        agent_name,
        ts AS ts_before,
        telemetry_id AS telemetry_id_before,
        model_name,
        -- LLM call tracking
        get_json_object(payload_json, '$.llm_call.llm_call_index') AS llm_call_index,
        -- State snapshot metrics
        get_json_object(payload_json, '$.state_snapshot.state_keys_count') AS state_keys_count,
        get_json_object(payload_json, '$.state_snapshot.state_json_bytes') AS state_json_bytes,
        get_json_object(payload_json, '$.state_snapshot.state_sha256') AS state_sha256,
        get_json_object(payload_json, '$.state_snapshot.state_token_estimate') AS state_token_estimate,
        get_json_object(payload_json, '$.state_snapshot.state_token_estimate_persistable_only') AS state_token_estimate_persistable_only,
        -- Token estimation metadata
        get_json_object(payload_json, '$.token_estimation.method') AS token_estimation_method,
        get_json_object(payload_json, '$.token_estimation.encoding') AS token_estimation_encoding,
        -- NEW: request_last_message fields (clarified semantics: last message in LlmRequest.contents)
        -- Falls back to deprecated prev_message fields for backward compatibility
        COALESCE(
            get_json_object(payload_json, '$.request_last_message.role'),
            get_json_object(payload_json, '$.prev_message.prev_message_role')
        ) AS request_last_message_role,
        COALESCE(
            get_json_object(payload_json, '$.request_last_message.token_estimate'),
            get_json_object(payload_json, '$.prev_message.prev_message_token_estimate')
        ) AS request_last_message_token_estimate,
        COALESCE(
            get_json_object(payload_json, '$.request_last_message.preview'),
            get_json_object(payload_json, '$.prev_message.prev_message_preview')
        ) AS request_last_message_preview,
        -- DEPRECATED: prev_message fields (kept for historical queries)
        get_json_object(payload_json, '$.prev_message.prev_message_role') AS prev_message_role,
        get_json_object(payload_json, '$.prev_message.prev_message_token_estimate') AS prev_message_token_estimate,
        get_json_object(payload_json, '$.prev_message.prev_message_preview') AS prev_message_preview,
        -- Request sampling
        get_json_object(payload_json, '$.request_sampling.request_preview') AS request_preview,
        get_json_object(payload_json, '$.request_sampling.request_snapshot_path') AS request_snapshot_path,
        get_json_object(payload_json, '$.request_sampling.request_snapshot_sha256') AS request_snapshot_sha256,
        get_json_object(payload_json, '$.request_sampling.request_snapshot_bytes') AS request_snapshot_bytes,
        -- Available tools
        get_json_object(payload_json, '$.available_tools') AS available_tools,
        get_json_object(payload_json, '$.system_instruction_preview') AS system_instruction_preview
    FROM silo_dev_rs.adk.adk_telemetry
    WHERE callback_name = 'before_model_callback'
),

-- Extract after_model_callback rows with parsed payload (raw, before de-duplication)
after_model_raw AS (
    SELECT
        session_id,
        invocation_id,
        agent_name,
        ts AS ts_after,
        telemetry_id AS telemetry_id_after,
        -- LLM call tracking
        get_json_object(payload_json, '$.llm_call.llm_call_index') AS llm_call_index,
        -- Streaming/partial indicators
        get_json_object(payload_json, '$.turn_complete') AS turn_complete,
        get_json_object(payload_json, '$.partial') AS partial,
        -- Usage metadata (authoritative from model)
        get_json_object(payload_json, '$.usage_metadata.prompt_token_count') AS prompt_token_count,
        get_json_object(payload_json, '$.usage_metadata.candidates_token_count') AS candidates_token_count,
        get_json_object(payload_json, '$.usage_metadata.cached_content_token_count') AS cached_content_token_count,
        -- Response sampling
        get_json_object(payload_json, '$.response_sampling.response_preview') AS response_preview,
        get_json_object(payload_json, '$.response_sampling.response_snapshot_path') AS response_snapshot_path,
        get_json_object(payload_json, '$.response_sampling.response_snapshot_sha256') AS response_snapshot_sha256,
        get_json_object(payload_json, '$.response_sampling.response_snapshot_bytes') AS response_snapshot_bytes,
        -- Error info (from after_model_callback, not on_model_error)
        get_json_object(payload_json, '$.error_code') AS error_code,
        get_json_object(payload_json, '$.error_message') AS error_message
    FROM silo_dev_rs.adk.adk_telemetry
    WHERE callback_name = 'after_model_callback'
),

-- De-duplicate after_model_callback rows: prefer turn_complete=true, then latest timestamp
-- This handles streaming/partial responses where multiple rows may exist per key
after_model_ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY session_id, invocation_id, agent_name, llm_call_index
            ORDER BY
                -- Prefer turn_complete = true (final response)
                CASE WHEN turn_complete = 'true' THEN 0 ELSE 1 END ASC,
                -- Then latest timestamp
                ts_after DESC,
                -- Tie-breaker: latest telemetry_id
                telemetry_id_after DESC
        ) AS rn
    FROM after_model_raw
),

-- Select one after_model_callback row per key
after_model AS (
    SELECT
        session_id,
        invocation_id,
        agent_name,
        ts_after,
        telemetry_id_after,
        llm_call_index,
        turn_complete,
        partial,
        prompt_token_count,
        candidates_token_count,
        cached_content_token_count,
        response_preview,
        response_snapshot_path,
        response_snapshot_sha256,
        response_snapshot_bytes,
        error_code,
        error_message
    FROM after_model_ranked
    WHERE rn = 1
),

-- Extract on_model_error_callback rows with parsed payload
-- These represent LLM errors that prevented a normal after_model_callback
model_error_raw AS (
    SELECT
        session_id,
        invocation_id,
        agent_name,
        ts AS ts_error,
        telemetry_id AS telemetry_id_error,
        model_name AS error_model_name,
        -- LLM call tracking (for pairing with before_model_callback)
        get_json_object(payload_json, '$.llm_call.llm_call_index') AS llm_call_index,
        -- Error details
        get_json_object(payload_json, '$.error') AS model_error,
        get_json_object(payload_json, '$.error_type') AS model_error_type,
        get_json_object(payload_json, '$.request_preview') AS error_request_preview
    FROM silo_dev_rs.adk.adk_telemetry
    WHERE callback_name = 'on_model_error_callback'
),

-- De-duplicate model errors (take latest per key)
model_error_ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY session_id, invocation_id, agent_name, llm_call_index
            ORDER BY ts_error DESC, telemetry_id_error DESC
        ) AS rn
    FROM model_error_raw
),

model_error AS (
    SELECT
        session_id,
        invocation_id,
        agent_name,
        ts_error,
        telemetry_id_error,
        llm_call_index,
        model_error,
        model_error_type,
        error_request_preview
    FROM model_error_ranked
    WHERE rn = 1
)

-- Join before, after, and error by (session_id, invocation_id, agent_name, llm_call_index)
SELECT
    b.session_id,
    b.invocation_id,
    b.agent_name,
    CAST(b.llm_call_index AS INT) AS llm_call_index,
    b.model_name,
    
    -- Timestamps
    b.ts_before,
    a.ts_after,
    e.ts_error,
    TIMESTAMPDIFF(MILLISECOND, b.ts_before, COALESCE(a.ts_after, e.ts_error)) AS latency_ms,
    
    -- State metrics (from before_model_callback)
    CAST(b.state_keys_count AS INT) AS state_keys_count,
    CAST(b.state_json_bytes AS INT) AS state_json_bytes,
    b.state_sha256,
    CAST(b.state_token_estimate AS INT) AS state_token_estimate,
    CAST(b.state_token_estimate_persistable_only AS INT) AS state_token_estimate_persistable_only,
    
    -- Token estimation metadata
    b.token_estimation_method,
    b.token_estimation_encoding,
    
    -- NEW: Request last message metrics (clarified semantics)
    b.request_last_message_role,
    CAST(b.request_last_message_token_estimate AS INT) AS request_last_message_token_estimate,
    b.request_last_message_preview,
    
    -- DEPRECATED: Previous message metrics (for backward compatibility)
    b.prev_message_role,
    CAST(b.prev_message_token_estimate AS INT) AS prev_message_token_estimate,
    b.prev_message_preview,
    
    -- Authoritative token usage (from after_model_callback)
    CAST(a.prompt_token_count AS INT) AS prompt_token_count,
    CAST(a.candidates_token_count AS INT) AS candidates_token_count,
    CAST(a.cached_content_token_count AS INT) AS cached_content_token_count,
    
    -- Token overhead analysis (estimate vs actual)
    CAST(a.prompt_token_count AS INT) - CAST(b.state_token_estimate AS INT) AS state_overhead_tokens,
    
    -- Request sampling
    b.request_preview,
    b.request_snapshot_path,
    b.request_snapshot_sha256,
    CAST(b.request_snapshot_bytes AS INT) AS request_snapshot_bytes,
    
    -- Response sampling
    a.response_preview,
    a.response_snapshot_path,
    a.response_snapshot_sha256,
    CAST(a.response_snapshot_bytes AS INT) AS response_snapshot_bytes,
    
    -- Streaming indicators (from after_model_callback)
    a.turn_complete,
    a.partial,
    
    -- Error info (COALESCE from after_model error fields and on_model_error)
    COALESCE(a.error_code, e.model_error_type) AS error_code,
    COALESCE(a.error_message, e.model_error) AS error_message,
    e.model_error_type,
    e.model_error,
    e.error_request_preview,
    
    -- Context
    b.available_tools,
    b.system_instruction_preview,
    
    -- Telemetry IDs for debugging
    b.telemetry_id_before,
    a.telemetry_id_after,
    e.telemetry_id_error,
    
    -- Completion status indicator
    CASE
        WHEN a.telemetry_id_after IS NOT NULL THEN 'completed'
        WHEN e.telemetry_id_error IS NOT NULL THEN 'error'
        ELSE 'pending_or_orphaned'
    END AS call_status

FROM before_model b
LEFT JOIN after_model a
    ON b.session_id = a.session_id
    AND b.invocation_id = a.invocation_id
    AND b.agent_name = a.agent_name
    AND b.llm_call_index = a.llm_call_index
LEFT JOIN model_error e
    ON b.session_id = e.session_id
    AND b.invocation_id = e.invocation_id
    AND b.agent_name = e.agent_name
    AND b.llm_call_index = e.llm_call_index;


-- Example queries for analysis:

-- 1. Token usage trends over LLM calls in a session
-- SELECT 
--     llm_call_index,
--     state_token_estimate,
--     state_token_estimate_persistable_only,
--     request_last_message_token_estimate,
--     prompt_token_count,
--     candidates_token_count,
--     latency_ms,
--     call_status
-- FROM silo_dev_rs.adk.llm_context_telemetry
-- WHERE session_id = '<session_id>'
-- ORDER BY llm_call_index;

-- 2. Compare estimated vs actual token usage
-- SELECT 
--     session_id,
--     AVG(state_token_estimate) AS avg_state_estimate,
--     AVG(prompt_token_count) AS avg_prompt_actual,
--     AVG(prompt_token_count - state_token_estimate) AS avg_overhead,
--     token_estimation_method
-- FROM silo_dev_rs.adk.llm_context_telemetry
-- WHERE ts_before >= CURRENT_DATE - INTERVAL 7 DAY
-- GROUP BY session_id, token_estimation_method;

-- 3. Find sessions with large state growth
-- SELECT 
--     session_id,
--     MAX(state_token_estimate) - MIN(state_token_estimate) AS state_growth,
--     MAX(llm_call_index) AS total_calls
-- FROM silo_dev_rs.adk.llm_context_telemetry
-- WHERE ts_before >= CURRENT_DATE - INTERVAL 1 DAY
-- GROUP BY session_id
-- HAVING state_growth > 1000
-- ORDER BY state_growth DESC;

-- 4. Find all errored LLM calls
-- SELECT 
--     session_id,
--     llm_call_index,
--     model_name,
--     error_code,
--     error_message,
--     model_error_type,
--     error_request_preview
-- FROM silo_dev_rs.adk.llm_context_telemetry
-- WHERE call_status = 'error'
-- ORDER BY ts_before DESC;

-- 5. Verify no duplicate rows per key (should return 0)
-- SELECT 
--     session_id, invocation_id, agent_name, llm_call_index, 
--     COUNT(*) AS cnt
-- FROM silo_dev_rs.adk.llm_context_telemetry
-- GROUP BY session_id, invocation_id, agent_name, llm_call_index
-- HAVING cnt > 1;
