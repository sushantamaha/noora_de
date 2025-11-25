
  create view "whatsapp_communication"."public"."stg_whatsapp_messages__dbt_tmp"
    
    
  as (
    with raw as (
    select
        id as original_row_id,
        uuid,
        -- Cast external timestamp to standard timestamp format
        external_timestamp::timestamp as message_timestamp,
        -- Map author to sender
        masked_author as sender,
        -- Use rendered_content as the primary message body (falls back to content if needed)
        coalesce(rendered_content, content) as message,
        direction,
        message_type,
        is_deleted
    from "whatsapp_communication"."public"."messages"
)

select
    -- Generate a sequential ID for analytics, preserving the original UUID
    row_number() over (order by message_timestamp, original_row_id) as message_id,
    uuid as message_uuid,
    message_timestamp,
    sender,
    message,
    direction,
    message_type,
    is_deleted
from raw
-- Optional: Uncomment the line below if you only want active messages
-- where is_deleted = false
  );