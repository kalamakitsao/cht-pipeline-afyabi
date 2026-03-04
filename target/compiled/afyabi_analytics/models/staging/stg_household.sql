

SELECT
    uuid            AS household_id,
    saved_timestamp,
    household_name,
    reported,
    chv_area_id     AS chp_area_id,
    chu_area_id
FROM "echis"."afyabi"."household"

WHERE saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM "echis"."afyabi"."stg_household"
)
