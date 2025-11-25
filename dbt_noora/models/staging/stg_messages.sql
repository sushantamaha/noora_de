{{ config(materialized='view') }}

select
    id as message_id,
    uuid as message_uuid,
    message_type,
    masked_addressees,           -- correct spelling
    masked_author,
    masked_from_addr as masked_sender,
    direction,
    source_type,
    external_id,
    external_timestamp,
    is_deleted,
    last_status,
    last_status_timestamp,
    content,
    author_type,
    rendered_content,
    inserted_at,
    updated_at
from {{ source('raw', 'messages') }}
