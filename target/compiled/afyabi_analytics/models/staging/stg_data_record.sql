

SELECT
    uuid,
    saved_timestamp,
    reported,
    patient_id,
    contact_uuid        AS reported_by,
    parent_uuid         AS chp_area_id,
    grandparent_uuid    AS chu_area_id
FROM "echis"."afyabi"."data_record"
WHERE patient_id IS NOT NULL

  AND saved_timestamp > (
      SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
      FROM "echis"."afyabi"."stg_data_record"
  )
