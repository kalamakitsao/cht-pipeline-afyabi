
      
        
        
        delete from "echis"."afyabi"."stg_immunization_status" as DBT_INTERNAL_DEST
        where (uuid) in (
            select distinct uuid
            from "stg_immunization_status__dbt_tmp130406529590" as DBT_INTERNAL_SOURCE
        );

    

    insert into "echis"."afyabi"."stg_immunization_status" ("uuid", "saved_timestamp", "reported_by", "chp_area_id", "reported", "patient_id", "patient_sex", "has_measles_9", "needs_deworming_follow_up", "needs_growth_monitoring_referral", "needs_immunization_referral", "imm_schedule_upto_date")
    (
        select "uuid", "saved_timestamp", "reported_by", "chp_area_id", "reported", "patient_id", "patient_sex", "has_measles_9", "needs_deworming_follow_up", "needs_growth_monitoring_referral", "needs_immunization_referral", "imm_schedule_upto_date"
        from "stg_immunization_status__dbt_tmp130406529590"
    )
  