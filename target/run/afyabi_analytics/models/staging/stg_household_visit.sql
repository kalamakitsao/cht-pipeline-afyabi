
      
        
        
        delete from "echis"."afyabi"."stg_household_visit" as DBT_INTERNAL_DEST
        where (uuid) in (
            select distinct uuid
            from "stg_household_visit__dbt_tmp130405180770" as DBT_INTERNAL_SOURCE
        );

    

    insert into "echis"."afyabi"."stg_household_visit" ("uuid", "saved_timestamp", "reported_by", "chp_area_id", "chu_area_id", "reported", "household", "chw")
    (
        select "uuid", "saved_timestamp", "reported_by", "chp_area_id", "chu_area_id", "reported", "household", "chw"
        from "stg_household_visit__dbt_tmp130405180770"
    )
  