

SELECT
    uuid,
    saved_timestamp,
    reported_by,
    reported_by_parent          AS chp_area_id,
    reported_by_parent_parent   AS chu_area_id,
    reported,
    household,
    chw
FROM "echis"."afyabi"."household_visit"

WHERE saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM "echis"."afyabi"."stg_household_visit"
)
