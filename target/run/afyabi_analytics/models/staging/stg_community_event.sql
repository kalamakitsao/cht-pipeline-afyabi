
      
        
        
        delete from "echis"."afyabi"."stg_community_event" as DBT_INTERNAL_DEST
        where (uuid) in (
            select distinct uuid
            from "stg_community_event__dbt_tmp130404629726" as DBT_INTERNAL_SOURCE
        );

    

    insert into "echis"."afyabi"."stg_community_event" ("uuid", "saved_timestamp", "reported_by", "chp_area_id", "reported", "event_types", "event_date")
    (
        select "uuid", "saved_timestamp", "reported_by", "chp_area_id", "reported", "event_types", "event_date"
        from "stg_community_event__dbt_tmp130404629726"
    )
  