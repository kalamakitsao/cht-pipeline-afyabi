

SELECT
    uuid,
    saved_timestamp,
    reported_by,
    reported_by_parent  AS chp_area_id,
    reported,
    event_types,
    event_date
FROM "echis"."afyabi"."community_event"

WHERE saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM "echis"."afyabi"."stg_community_event"
)
