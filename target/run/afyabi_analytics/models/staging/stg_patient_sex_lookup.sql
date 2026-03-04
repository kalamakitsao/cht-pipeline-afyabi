
      
        
        
        delete from "echis"."afyabi"."stg_patient_sex_lookup" as DBT_INTERNAL_DEST
        where (patient_id) in (
            select distinct patient_id
            from "stg_patient_sex_lookup__dbt_tmp130405366736" as DBT_INTERNAL_SOURCE
        );

    

    insert into "echis"."afyabi"."stg_patient_sex_lookup" ("patient_id", "sex")
    (
        select "patient_id", "sex"
        from "stg_patient_sex_lookup__dbt_tmp130405366736"
    )
  