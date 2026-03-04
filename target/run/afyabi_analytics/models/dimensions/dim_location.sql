
      
        
        
        delete from "echis"."afyabi"."dim_location" as DBT_INTERNAL_DEST
        where (location_id) in (
            select distinct location_id
            from "dim_location__dbt_tmp130405979974" as DBT_INTERNAL_SOURCE
        );

    

    insert into "echis"."afyabi"."dim_location" ("location_id", "saved_timestamp", "name", "contact_type", "parent_uuid", "active", "muted")
    (
        select "location_id", "saved_timestamp", "name", "contact_type", "parent_uuid", "active", "muted"
        from "dim_location__dbt_tmp130405979974"
    )
  