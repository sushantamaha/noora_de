-- models/staging/stg_statuses.sql
{{ config(materialized='view') }}

select
    s.id as status_id,
    s.uuid as status_uuid,
    s.message_uuid,
    s.message_id,
    s.number_id,
    s.status,
    s.updated_at,
    s.timestamp as status_timestamp,
    s.inserted_at as status_inserted_at,
    row_number() over (partition by s.message_uuid order by s.timestamp) as status_sequence
from {{ source('raw', 'statuses') }} s
where s.status is not null


