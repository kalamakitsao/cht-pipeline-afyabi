

SELECT
    d.uuid,
    d.saved_timestamp,
    d.reported_by,
    d.reported_by_parent            AS chp_area_id,
    d.reported_by_parent_parent     AS chu_area_id,
    d.reported,
    d.patient_id,
    p.sex                           AS patient_sex,
    d.patient_age_in_days,
    d.date_of_death,
    d.death_type
FROM "echis"."afyabi"."death_report" d
LEFT JOIN "echis"."afyabi"."stg_patient_sex_lookup" p ON p.patient_id = d.patient_id

WHERE d.saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM "echis"."afyabi"."stg_death_report"
)
