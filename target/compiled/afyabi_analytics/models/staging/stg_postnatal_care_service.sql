

SELECT
    uuid,
    saved_timestamp,
    reported_by,
    reported_by_parent          AS chp_area_id,
    reported_by_parent_parent   AS chu_area_id,
    reported,
    patient_id,
    pregnancy_id,
    date_of_delivery,
    place_of_delivery,
    pnc_service_count,
    is_referred_for_pnc_services,
    has_delivered
FROM "echis"."afyabi"."postnatal_care_service"

WHERE saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM "echis"."afyabi"."stg_postnatal_care_service"
)
