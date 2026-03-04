
      
        
        
        delete from "echis"."afyabi"."stg_pregnancy_home_visit" as DBT_INTERNAL_DEST
        where (uuid) in (
            select distinct uuid
            from "stg_pregnancy_home_visit__dbt_tmp130405811845" as DBT_INTERNAL_SOURCE
        );

    

    insert into "echis"."afyabi"."stg_pregnancy_home_visit" ("uuid", "saved_timestamp", "reported_by", "chp_area_id", "chu_area_id", "reported", "patient_id", "patient_age_in_years", "is_new_pregnancy", "is_currently_pregnant", "has_been_referred", "has_started_anc", "is_anc_upto_date", "is_counselled_anc", "effective_edd", "current_edd", "updated_edd")
    (
        select "uuid", "saved_timestamp", "reported_by", "chp_area_id", "chu_area_id", "reported", "patient_id", "patient_age_in_years", "is_new_pregnancy", "is_currently_pregnant", "has_been_referred", "has_started_anc", "is_anc_upto_date", "is_counselled_anc", "effective_edd", "current_edd", "updated_edd"
        from "stg_pregnancy_home_visit__dbt_tmp130405811845"
    )
  