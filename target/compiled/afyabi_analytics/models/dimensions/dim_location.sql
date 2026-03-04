

SELECT
    uuid            AS location_id,
    saved_timestamp,
    name,
    contact_type,
    parent_uuid,
    active,
    muted
FROM "echis"."afyabi"."stg_contact"
WHERE contact_type IN (
    'a_county',
    'b_sub_county',
    'c_community_health_unit',
    'd_community_health_volunteer_area'
)

  AND saved_timestamp > (SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp) FROM "echis"."afyabi"."dim_location")
