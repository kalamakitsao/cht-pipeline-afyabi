
      
        
        
        delete from "echis"."afyabi"."stg_sha_registration" as DBT_INTERNAL_DEST
        where (uuid) in (
            select distinct uuid
            from "stg_sha_registration__dbt_tmp130405946024" as DBT_INTERNAL_SOURCE
        );

    

    insert into "echis"."afyabi"."stg_sha_registration" ("uuid", "saved_timestamp", "reported", "chp_area_id", "member_uuid", "has_sha_registration")
    (
        select "uuid", "saved_timestamp", "reported", "chp_area_id", "member_uuid", "has_sha_registration"
        from "stg_sha_registration__dbt_tmp130405946024"
    )
  