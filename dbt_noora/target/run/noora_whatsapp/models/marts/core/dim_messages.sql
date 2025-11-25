
  
    

  create  table "whatsapp_communication"."public_marts"."dim_messages__dbt_tmp"
  
  
    as
  
  (
    -- models/marts/core/dim_messages.sql


select * from "whatsapp_communication"."public"."int_messages_with_status_history"
  );
  