select
    message_id,
    message_timestamp,
    sender,
    message,
    length(message) as message_length,
    case when message ilike '%<Media omitted>%' then true else false end as has_media
from "whatsapp_communication"."public"."stg_whatsapp_messages"