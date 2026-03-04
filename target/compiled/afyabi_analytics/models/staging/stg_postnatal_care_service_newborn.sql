

SELECT
    uuid,
    saved_timestamp,
    reported_by,
    reported_by_parent          AS chp_area_id,
    reported_by_parent_parent   AS chu_area_id,
    reported,
    patient_id,
    pnc_service_count,
    needs_danger_signs_follow_up,
    needs_missed_visit_follow_up,
    is_referred_for_pnc_services,
    is_referred
FROM "echis"."afyabi"."postnatal_care_service_newborn"

WHERE saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM "echis"."afyabi"."stg_postnatal_care_service_newborn"
)
