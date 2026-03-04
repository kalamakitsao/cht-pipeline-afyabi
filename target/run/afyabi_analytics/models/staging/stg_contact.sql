
      
        
        
        delete from "echis"."afyabi"."stg_contact" as DBT_INTERNAL_DEST
        where (uuid) in (
            select distinct uuid
            from "stg_contact__dbt_tmp130404685090" as DBT_INTERNAL_SOURCE
        );

    

    insert into "echis"."afyabi"."stg_contact" ("uuid", "saved_timestamp", "reported", "parent_uuid", "name", "contact_type", "active", "muted")
    (
        select "uuid", "saved_timestamp", "reported", "parent_uuid", "name", "contact_type", "active", "muted"
        from "stg_contact__dbt_tmp130404685090"
    )
  