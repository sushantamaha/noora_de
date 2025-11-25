
      
        
            delete from "whatsapp_communication"."public_marts"."fct_data_quality_audits"
            where (
                audit_id) in (
                select (audit_id)
                from "fct_data_quality_audits__dbt_tmp033622611318"
            );

        
    

    insert into "whatsapp_communication"."public_marts"."fct_data_quality_audits" ("audit_id", "execution_at", "related_uuid", "check_name", "severity", "issue_details")
    (
        select "audit_id", "execution_at", "related_uuid", "check_name", "severity", "issue_details"
        from "fct_data_quality_audits__dbt_tmp033622611318"
    )
  