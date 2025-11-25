-- models/marts/core/dim_messages.sql
{{ config(materialized='table') }}

select * from {{ ref('int_messages_with_status_history') }}