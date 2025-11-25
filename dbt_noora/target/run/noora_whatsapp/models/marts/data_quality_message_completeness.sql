
  create view "whatsapp_communication"."public_marts"."data_quality_message_completeness__dbt_tmp"
    
    
  as (
    -- dbt_noora/models/marts/data_quality_message_completeness.sql


/*
Purpose: Check for missing critical fields and data completeness issues
Logic:
  - Validate that critical fields are populated appropriately
  - Check inbound messages have content
  - Check outbound messages have rendered_content
  - Identify messages missing expected fields
  - Flag data quality issues
*/

WITH message_validation AS (
    SELECT
        m.uuid AS message_uuid,
        m.id,
        m.direction,
        m.message_type,
        m.content,
        m.rendered_content,
        m.masked_from_addr,
        m.masked_addressees,
        m.external_timestamp,
        m.inserted_at,
        m.last_status,
        m.last_status_timestamp,
        -- Validation checks
        CASE 
            WHEN m.direction = 'inbound' AND (m.content IS NULL OR TRIM(m.content) = '')
            THEN true ELSE false
        END AS missing_inbound_content,
        CASE 
            WHEN m.direction = 'outbound' AND (m.rendered_content IS NULL OR TRIM(m.rendered_content) = '')
            THEN true ELSE false
        END AS missing_outbound_content,
        CASE 
            WHEN m.masked_from_addr IS NULL OR TRIM(m.masked_from_addr) = ''
            THEN true ELSE false
        END AS missing_sender,
        CASE 
            WHEN m.masked_addressees IS NULL OR TRIM(m.masked_addressees) = ''
            THEN true ELSE false
        END AS missing_addressee,
        CASE 
            WHEN m.external_timestamp IS NULL
            THEN true ELSE false
        END AS missing_external_timestamp,
        CASE 
            WHEN m.external_timestamp > m.inserted_at + INTERVAL '1 hour'
            THEN true ELSE false
        END AS future_external_timestamp,
        CASE
            WHEN m.last_status_timestamp IS NOT NULL 
                 AND m.external_timestamp IS NOT NULL
                 AND m.last_status_timestamp < m.external_timestamp
            THEN true ELSE false
        END AS invalid_status_timestamp,
        -- Check if message has any status records
        CASE 
            WHEN s.message_uuid IS NULL AND m.direction = 'outbound'
            THEN true ELSE false
        END AS missing_status_records
    FROM "whatsapp_communication"."public"."messages" m
    LEFT JOIN (
        SELECT DISTINCT message_uuid
        FROM "whatsapp_communication"."public"."statuses"
    ) s ON m.uuid = s.message_uuid
),

validation_summary AS (
    SELECT
        message_uuid,
        id,
        direction,
        message_type,
        inserted_at,
        external_timestamp,
        last_status,
        last_status_timestamp,
        -- Collect all issues
        ARRAY_REMOVE(ARRAY[
            CASE WHEN missing_inbound_content THEN 'MISSING_INBOUND_CONTENT' END,
            CASE WHEN missing_outbound_content THEN 'MISSING_OUTBOUND_CONTENT' END,
            CASE WHEN missing_sender THEN 'MISSING_SENDER' END,
            CASE WHEN missing_addressee THEN 'MISSING_ADDRESSEE' END,
            CASE WHEN missing_external_timestamp THEN 'MISSING_EXTERNAL_TIMESTAMP' END,
            CASE WHEN future_external_timestamp THEN 'FUTURE_EXTERNAL_TIMESTAMP' END,
            CASE WHEN invalid_status_timestamp THEN 'INVALID_STATUS_TIMESTAMP' END,
            CASE WHEN missing_status_records THEN 'MISSING_STATUS_RECORDS' END
        ], NULL) AS issues,
        -- Count of issues
        (missing_inbound_content::int + missing_outbound_content::int + 
         missing_sender::int + missing_addressee::int + 
         missing_external_timestamp::int + future_external_timestamp::int +
         invalid_status_timestamp::int + missing_status_records::int) AS issue_count
    FROM message_validation
)

SELECT
    message_uuid,
    id AS message_id,
    direction,
    message_type,
    inserted_at,
    external_timestamp,
    last_status,
    last_status_timestamp,
    issues,
    issue_count,
    CASE 
        WHEN issue_count = 0 THEN 'VALID'
        WHEN issue_count >= 3 THEN 'CRITICAL'
        WHEN issue_count >= 2 THEN 'WARNING'
        ELSE 'MINOR'
    END AS severity,
    ARRAY_TO_STRING(issues, ', ') AS issue_description,
    CURRENT_TIMESTAMP AS validation_timestamp
FROM validation_summary
WHERE issue_count > 0
ORDER BY 
    issue_count DESC,
    inserted_at DESC
  );