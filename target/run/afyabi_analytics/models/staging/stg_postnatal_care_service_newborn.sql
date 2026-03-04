
      
        
        
        delete from "echis"."afyabi"."stg_postnatal_care_service_newborn" as DBT_INTERNAL_DEST
        where (uuid) in (
            select distinct uuid
            from "stg_postnatal_care_service_newborn__dbt_tmp130405455957" as DBT_INTERNAL_SOURCE
        );

    

    insert into "echis"."afyabi"."stg_postnatal_care_service_newborn" ("uuid", "saved_timestamp", "reported_by", "chp_area_id", "chu_area_id", "reported", "patient_id", "pnc_service_count", "needs_danger_signs_follow_up", "needs_missed_visit_follow_up", "is_referred_for_pnc_services", "is_referred")
    (
        select "uuid", "saved_timestamp", "reported_by", "chp_area_id", "chu_area_id", "reported", "patient_id", "pnc_service_count", "needs_danger_signs_follow_up", "needs_missed_visit_follow_up", "is_referred_for_pnc_services", "is_referred"
        from "stg_postnatal_care_service_newborn__dbt_tmp130405455957"
    )
  