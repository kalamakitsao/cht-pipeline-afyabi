
      
        
        
        delete from "echis"."afyabi"."stg_immunization" as DBT_INTERNAL_DEST
        where (uuid) in (
            select distinct uuid
            from "stg_immunization__dbt_tmp130406429031" as DBT_INTERNAL_SOURCE
        );

    

    insert into "echis"."afyabi"."stg_immunization" ("uuid", "saved_timestamp", "reported_by", "chp_area_id", "reported", "patient_id", "patient_sex", "is_dewormed", "is_referred_vitamin_a", "is_referred_immunization", "is_referred_growth_monitoring")
    (
        select "uuid", "saved_timestamp", "reported_by", "chp_area_id", "reported", "patient_id", "patient_sex", "is_dewormed", "is_referred_vitamin_a", "is_referred_immunization", "is_referred_growth_monitoring"
        from "stg_immunization__dbt_tmp130406429031"
    )
  