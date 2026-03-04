
      
        
        
        delete from "echis"."afyabi"."stg_postnatal_care_service" as DBT_INTERNAL_DEST
        where (uuid) in (
            select distinct uuid
            from "stg_postnatal_care_service__dbt_tmp130405395852" as DBT_INTERNAL_SOURCE
        );

    

    insert into "echis"."afyabi"."stg_postnatal_care_service" ("uuid", "saved_timestamp", "reported_by", "chp_area_id", "chu_area_id", "reported", "patient_id", "pregnancy_id", "date_of_delivery", "place_of_delivery", "pnc_service_count", "is_referred_for_pnc_services", "has_delivered")
    (
        select "uuid", "saved_timestamp", "reported_by", "chp_area_id", "chu_area_id", "reported", "patient_id", "pregnancy_id", "date_of_delivery", "place_of_delivery", "pnc_service_count", "is_referred_for_pnc_services", "has_delivered"
        from "stg_postnatal_care_service__dbt_tmp130405395852"
    )
  