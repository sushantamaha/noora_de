
  
    

  create  table "whatsapp_communication"."public"."int_messages_with_status_history__dbt_tmp"
  
  
    as
  
  (
    

WITH unique_messages AS (
    -- Deduplicate messages: take the most recent version based on inserted_at
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY message_id ORDER BY inserted_at DESC) as row_num
        FROM "whatsapp_communication"."public"."stg_messages"
    ) m
    WHERE row_num = 1
),

pivoted_statuses AS (
    -- Flatten 1-to-many statuses into columns
    SELECT 
        message_id,
        -- Pivot statuses to timestamps
        MAX(CASE WHEN status = 'sent' THEN status_timestamp END) as sent_at,
        MAX(CASE WHEN status = 'delivered' THEN status_timestamp END) as delivered_at,
        MAX(CASE WHEN status = 'read' THEN status_timestamp END) as read_at,
        MAX(CASE WHEN status = 'failed' THEN status_timestamp END) as failed_at,
        MAX(CASE WHEN status = 'deleted' THEN 1 ELSE 0 END) as is_deleted_status
        
        -- Ensure comma is present before this line
        -- ARRAY_AGG(status_uuid) as status_uuids

FROM "whatsapp_communication"."public"."stg_statuses"
GROUP BY message_id
)

SELECT 
    m.message_uuid as uuid, -- Renaming back to generic uuid for the final table if desired
    m.message_id as original_id,
    m.content,
    m.rendered_content,
    m.message_type,
    m.direction,
    m.masked_addressees,
    m.masked_sender, -- updated from masked_from_addr
    m.inserted_at as message_created_at,
    
    -- Status timestamps
    s.sent_at,
    s.delivered_at,
    s.read_at,
    s.failed_at,
    -- s.status_uuids,
    
    -- Derived metrics
    
        (
        (
        (
        ((s.read_at)::date - (s.sent_at)::date)
     * 24 + date_part('hour', (s.read_at)::timestamp) - date_part('hour', (s.sent_at)::timestamp))
     * 60 + date_part('minute', (s.read_at)::timestamp) - date_part('minute', (s.sent_at)::timestamp))
     * 60 + floor(date_part('second', (s.read_at)::timestamp)) - floor(date_part('second', (s.sent_at)::timestamp)))
     as seconds_to_read,
    
    -- Final state
    COALESCE(s.is_deleted_status, 0) as has_delete_log

FROM unique_messages m
LEFT JOIN pivoted_statuses s ON m.message_id = s.message_id
  );
  