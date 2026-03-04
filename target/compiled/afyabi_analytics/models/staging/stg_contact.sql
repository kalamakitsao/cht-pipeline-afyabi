

SELECT
    uuid,
    saved_timestamp,
    reported,
    parent_uuid,
    name,
    contact_type,
    active,
    muted
FROM "echis"."afyabi"."contact"

WHERE saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM "echis"."afyabi"."stg_contact"
)
