

SELECT
    uuid,
    saved_timestamp,
    reported,
    reported_by_parent  AS chp_area_id,
    member_uuid,
    has_sha_registration
FROM "echis"."afyabi"."sha_registration"

WHERE saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM "echis"."afyabi"."stg_sha_registration"
)
