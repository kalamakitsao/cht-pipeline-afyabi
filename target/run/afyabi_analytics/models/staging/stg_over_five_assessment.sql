
      
        
        
        delete from "echis"."afyabi"."stg_over_five_assessment" as DBT_INTERNAL_DEST
        where (uuid) in (
            select distinct uuid
            from "stg_over_five_assessment__dbt_tmp130406703521" as DBT_INTERNAL_SOURCE
        );

    

    insert into "echis"."afyabi"."stg_over_five_assessment" ("uuid", "saved_timestamp", "reported_by", "chp_area_id", "chu_area_id", "reported", "patient_id", "patient_sex", "screened_for_diabetes", "is_referred_diabetes", "screened_for_hypertension", "is_referred_hypertension", "screened_for_mental_health", "is_referred_mental_health", "has_fever", "rdt_result", "repeat_rdt_result", "given_al", "has_been_referred")
    (
        select "uuid", "saved_timestamp", "reported_by", "chp_area_id", "chu_area_id", "reported", "patient_id", "patient_sex", "screened_for_diabetes", "is_referred_diabetes", "screened_for_hypertension", "is_referred_hypertension", "screened_for_mental_health", "is_referred_mental_health", "has_fever", "rdt_result", "repeat_rdt_result", "given_al", "has_been_referred"
        from "stg_over_five_assessment__dbt_tmp130406703521"
    )
  