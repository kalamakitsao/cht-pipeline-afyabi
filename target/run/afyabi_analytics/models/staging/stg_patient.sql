
      
        
        
        delete from "echis"."afyabi"."stg_patient" as DBT_INTERNAL_DEST
        where (patient_id) in (
            select distinct patient_id
            from "stg_patient__dbt_tmp130405277444" as DBT_INTERNAL_SOURCE
        );

    

    insert into "echis"."afyabi"."stg_patient" ("patient_id", "saved_timestamp", "name", "sex", "date_of_birth", "household_id", "chp_area_id", "chu_area_id", "is_active", "muted")
    (
        select "patient_id", "saved_timestamp", "name", "sex", "date_of_birth", "household_id", "chp_area_id", "chu_area_id", "is_active", "muted"
        from "stg_patient__dbt_tmp130405277444"
    )
  