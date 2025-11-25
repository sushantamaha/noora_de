{{ config(
    materialized='incremental',
    unique_key='audit_id' 
) }}

/*
  AUDIT LOG MODEL
  ---------------
  This model consolidates all data quality checks into a single table.
  It is incremental, meaning it preserves history of when errors were detected.
*/

WITH 
-- 1. DETECT DUPLICATES
check_duplicates AS (
    SELECT 
        message_uuid as related_uuid,
        'Duplicate Record' as check_name,
        'High' as severity,
        'Identical message found within ' || EXTRACT(EPOCH FROM time_diff) || ' seconds' as issue_details
    FROM (
        SELECT 
            message_uuid,
            inserted_at,
            masked_sender,
            content,
            direction,
            inserted_at - LAG(inserted_at) OVER (
                PARTITION BY masked_sender, content, direction 
                ORDER BY inserted_at
            ) as time_diff
        FROM {{ ref('stg_messages') }} -- Using source to catch them before aggregation if possible, or fct if preferred.
        WHERE content IS NOT NULL
        {% if is_incremental() %}
        -- To prevent re-scanning the entire table on each run
        AND inserted_at > (SELECT MAX(execution_at) FROM {{ this }})
        {% endif %}
    ) sub
    WHERE time_diff < INTERVAL '10 seconds'
),

-- 2. LOGICAL CONSISTENCY: READ BEFORE SENT
check_read_logic AS (
    SELECT 
        uuid as related_uuid,
        'Logical Error' as check_name,
        'Critical' as severity,
        'Message read ' || EXTRACT(EPOCH FROM (sent_at - read_at)) || 's before it was sent' as issue_details
    FROM {{ ref('int_messages_with_status_history') }}
    WHERE read_at < sent_at
    {% if is_incremental() %}
    -- To prevent re-scanning the entire table on each run
    AND message_created_at > (SELECT MAX(execution_at) FROM {{ this }})
    {% endif %}
),

-- 3. LOGICAL CONSISTENCY: DELIVERED BEFORE SENT
check_delivered_logic AS (
    SELECT 
        uuid as related_uuid,
        'Logical Error' as check_name,
        'Critical' as severity,
        'Message delivered ' || EXTRACT(EPOCH FROM (sent_at - delivered_at)) || 's before it was sent' as issue_details
    FROM {{ ref('int_messages_with_status_history') }}
    WHERE delivered_at < sent_at
    {% if is_incremental() %}
    -- To prevent re-scanning the entire table on each run
    AND message_created_at > (SELECT MAX(execution_at) FROM {{ this }})
    {% endif %}
),

-- 4. MISSING CRITICAL INFO
check_completeness AS (
    SELECT 
        uuid as related_uuid,
        'Missing Data' as check_name,
        'High' as severity,
        CASE 
            WHEN direction = 'outbound' AND (masked_addressees IS NULL OR masked_addressees = '') THEN 'Outbound message missing addressee'
            WHEN direction = 'inbound' AND (masked_sender IS NULL OR masked_sender = '') THEN 'Inbound message missing sender'
            ELSE 'Missing critical routing info'
        END as issue_details
    FROM {{ ref('int_messages_with_status_history') }}
    WHERE 
        (direction = 'outbound' AND (masked_addressees IS NULL OR masked_addressees = ''))
        OR (direction = 'inbound' AND (masked_sender IS NULL OR masked_sender = ''))
    {% if is_incremental() %}
    -- To prevent re-scanning the entire table on each run
    AND message_created_at > (SELECT MAX(execution_at) FROM {{ this }})
    {% endif %}
),

-- 5. INVALID ROUTING INFO
check_invalid_routing AS (
    SELECT 
        uuid as related_uuid,
        'Invalid Data' as check_name,
        'High' as severity,
        CASE 
            WHEN direction = 'outbound' AND masked_addressees = '' THEN 'Outbound message with empty addressee'
            WHEN direction = 'inbound' AND masked_sender = '' THEN 'Inbound message with empty sender'
            ELSE 'Invalid critical routing info'
        END as issue_details
    FROM {{ ref('int_messages_with_status_history') }}
    WHERE 
        (direction = 'outbound' AND masked_addressees = '')
        OR (direction = 'inbound' AND masked_sender = '')
    {% if is_incremental() %}
    -- To prevent re-scanning the entire table on each run
    AND message_created_at > (SELECT MAX(execution_at) FROM {{ this }})
    {% endif %}
),

-- UNION ALL CHECKS
all_issues AS (
    SELECT * FROM check_duplicates
    UNION ALL
    SELECT * FROM check_read_logic
    UNION ALL
    SELECT * FROM check_delivered_logic
    UNION ALL
    SELECT * FROM check_completeness
    UNION ALL
    SELECT * FROM check_invalid_routing
)

SELECT
    -- Generate a unique ID for this specific audit record
    {{ dbt_utils.generate_surrogate_key(['related_uuid', 'check_name']) }} as audit_id,
    CURRENT_TIMESTAMP as execution_at,
    related_uuid,
    check_name,
    severity,
    issue_details
FROM all_issues

{% if is_incremental() %}
  -- This condition prevents re-inserting the same errors on subsequent runs
  WHERE related_uuid NOT IN (SELECT related_uuid FROM {{ this }} WHERE check_name = all_issues.check_name)
{% endif %}
