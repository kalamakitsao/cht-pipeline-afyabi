

/*
  stg_over_five_assessment: Over-5 assessments with sex denormalized.
  Source: 30M rows, 13GB. Largest assessment table.
*/

SELECT
    a.uuid,
    a.saved_timestamp,
    a.reported_by,
    a.reported_by_parent            AS chp_area_id,
    a.reported_by_parent_parent     AS chu_area_id,
    a.reported,
    a.patient_id,
    p.sex                           AS patient_sex,
    a.screened_for_diabetes,
    a.is_referred_diabetes,
    a.screened_for_hypertension,
    a.is_referred_hypertension,
    a.screened_for_mental_health,
    a.is_referred_mental_health,
    a.has_fever,
    a.rdt_result,
    a.repeat_rdt_result,
    a.given_al,
    a.has_been_referred
FROM "echis"."afyabi"."over_five_assessment" a
LEFT JOIN "echis"."afyabi"."stg_patient_sex_lookup" p ON p.patient_id = a.patient_id

WHERE a.saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM "echis"."afyabi"."stg_over_five_assessment"
)
