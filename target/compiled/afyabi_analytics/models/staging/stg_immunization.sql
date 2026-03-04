

SELECT
    i.uuid,
    i.saved_timestamp,
    i.reported_by,
    i.reported_by_parent    AS chp_area_id,
    i.reported,
    i.patient_id,
    p.sex                   AS patient_sex,
    i.is_dewormed,
    i.is_referred_vitamin_a,
    i.is_referred_immunization,
    i.is_referred_growth_monitoring
FROM "echis"."afyabi"."immunization" i
LEFT JOIN "echis"."afyabi"."stg_patient_sex_lookup" p ON p.patient_id = i.patient_id

WHERE i.saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM "echis"."afyabi"."stg_immunization"
)
