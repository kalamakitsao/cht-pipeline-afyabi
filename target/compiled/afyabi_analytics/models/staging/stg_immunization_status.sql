

SELECT
    i.uuid,
    i.saved_timestamp,
    i.reported_by,
    i.reported_by_parent    AS chp_area_id,
    i.reported,
    i.patient_id,
    p.sex                   AS patient_sex,
    i.has_measles_9,
    i.needs_deworming_follow_up,
    i.needs_growth_monitoring_referral,
    i.needs_immunization_referral,
    i.imm_schedule_upto_date
FROM "echis"."afyabi"."immunization_status" i
LEFT JOIN "echis"."afyabi"."stg_patient_sex_lookup" p ON p.patient_id = i.patient_id

WHERE i.saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM "echis"."afyabi"."stg_immunization_status"
)
