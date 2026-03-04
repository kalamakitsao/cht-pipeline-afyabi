
      
        
        
        delete from "echis"."afyabi"."stg_death_report" as DBT_INTERNAL_DEST
        where (uuid) in (
            select distinct uuid
            from "stg_death_report__dbt_tmp130406067944" as DBT_INTERNAL_SOURCE
        );

    

    insert into "echis"."afyabi"."stg_death_report" ("uuid", "saved_timestamp", "reported_by", "chp_area_id", "chu_area_id", "reported", "patient_id", "patient_sex", "patient_age_in_days", "date_of_death", "death_type")
    (
        select "uuid", "saved_timestamp", "reported_by", "chp_area_id", "chu_area_id", "reported", "patient_id", "patient_sex", "patient_age_in_days", "date_of_death", "death_type"
        from "stg_death_report__dbt_tmp130406067944"
    )
  