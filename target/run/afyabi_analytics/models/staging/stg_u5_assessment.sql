
      
        
        
        delete from "echis"."afyabi"."stg_u5_assessment" as DBT_INTERNAL_DEST
        where (uuid) in (
            select distinct uuid
            from "stg_u5_assessment__dbt_tmp130406744084" as DBT_INTERNAL_SOURCE
        );

    

    insert into "echis"."afyabi"."stg_u5_assessment" ("uuid", "saved_timestamp", "reported_by", "chp_area_id", "chu_area_id", "reported", "patient_id", "patient_sex", "has_fever", "has_diarrhoea", "has_cough", "has_fast_breathing", "has_chest_indrawing", "muac_color", "rdt_result", "repeat_rdt_result", "gave_al", "gave_ors", "gave_zinc", "gave_amox", "is_exclusively_breastfeeding", "has_uptodate_growth_monitoring", "referred_for_development_milestones", "has_been_referred", "has_pneumonia", "has_malaria", "has_malnutrition")
    (
        select "uuid", "saved_timestamp", "reported_by", "chp_area_id", "chu_area_id", "reported", "patient_id", "patient_sex", "has_fever", "has_diarrhoea", "has_cough", "has_fast_breathing", "has_chest_indrawing", "muac_color", "rdt_result", "repeat_rdt_result", "gave_al", "gave_ors", "gave_zinc", "gave_amox", "is_exclusively_breastfeeding", "has_uptodate_growth_monitoring", "referred_for_development_milestones", "has_been_referred", "has_pneumonia", "has_malaria", "has_malnutrition"
        from "stg_u5_assessment__dbt_tmp130406744084"
    )
  