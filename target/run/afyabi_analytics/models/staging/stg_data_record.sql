
      
        
        
        delete from "echis"."afyabi"."stg_data_record" as DBT_INTERNAL_DEST
        where (uuid) in (
            select distinct uuid
            from "stg_data_record__dbt_tmp130404759873" as DBT_INTERNAL_SOURCE
        );

    

    insert into "echis"."afyabi"."stg_data_record" ("uuid", "saved_timestamp", "reported", "patient_id", "reported_by", "chp_area_id", "chu_area_id")
    (
        select "uuid", "saved_timestamp", "reported", "patient_id", "reported_by", "chp_area_id", "chu_area_id"
        from "stg_data_record__dbt_tmp130404759873"
    )
  