

SELECT
    uuid,
    saved_timestamp,
    reported_by,
    reported_by_parent          AS chp_area_id,
    reported_by_parent_parent   AS chu_area_id,
    reported,
    patient_id,
    patient_age_in_years,
    is_new_pregnancy,
    is_currently_pregnant,
    has_been_referred,
    has_started_anc,
    is_anc_upto_date,
    is_counselled_anc,
    COALESCE(updated_edd, current_edd) AS effective_edd,
    current_edd,
    updated_edd
FROM "echis"."afyabi"."pregnancy_home_visit"

WHERE saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM "echis"."afyabi"."stg_pregnancy_home_visit"
)
