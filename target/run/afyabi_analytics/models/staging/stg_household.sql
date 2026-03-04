
      
        
        
        delete from "echis"."afyabi"."stg_household" as DBT_INTERNAL_DEST
        where (household_id) in (
            select distinct household_id
            from "stg_household__dbt_tmp130404702837" as DBT_INTERNAL_SOURCE
        );

    

    insert into "echis"."afyabi"."stg_household" ("household_id", "saved_timestamp", "household_name", "reported", "chp_area_id", "chu_area_id")
    (
        select "household_id", "saved_timestamp", "household_name", "reported", "chp_area_id", "chu_area_id"
        from "stg_household__dbt_tmp130404702837"
    )
  