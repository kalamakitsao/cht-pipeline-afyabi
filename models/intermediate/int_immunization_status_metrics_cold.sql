-- depends_on: {{ ref('stg_immunization_status') }}
-- depends_on: {{ ref('dim_period') }}
{{
  config(
    materialized = 'table',
    pre_hook = "SET LOCAL work_mem = '1GB'",
    indexes = [
      {'columns': ['chp_area_id', 'period_id']},
      {'columns': ['period_id']}
    ],
    tags = ['cadence_daily', 'intermediate']
  )
}}

SELECT
    s.chp_area_id,
    p.period_id,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_measles_9 = 'complete')                            AS under_1_immunised,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_measles_9 = 'complete' AND s.patient_sex = 'female') AS under_1_immunised_female,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_measles_9 = 'complete' AND s.patient_sex = 'male')   AS under_1_immunised_male,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.needs_deworming_follow_up = 'true' AND s.patient_sex = 'female') AS needs_deworming_follow_up_female,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.needs_deworming_follow_up = 'true' AND s.patient_sex = 'male')   AS needs_deworming_follow_up_male,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.needs_growth_monitoring_referral = 'true' AND s.patient_sex = 'female') AS referred_growth_monitoring_female,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.needs_growth_monitoring_referral = 'true' AND s.patient_sex = 'male')   AS referred_growth_monitoring_male,
    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.needs_immunization_referral = 'true'
          AND (s.imm_schedule_upto_date IS NULL OR s.imm_schedule_upto_date <> 'true')
    ) AS referred_missed_vaccine,
    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.needs_immunization_referral = 'true'
          AND (s.imm_schedule_upto_date IS NULL OR s.imm_schedule_upto_date <> 'true')
          AND s.patient_sex = 'female'
    ) AS referred_missed_vaccine_female,
    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.needs_immunization_referral = 'true'
          AND (s.imm_schedule_upto_date IS NULL OR s.imm_schedule_upto_date <> 'true')
          AND s.patient_sex = 'male'
    ) AS referred_missed_vaccine_male,
    NOW() AS last_updated
FROM {{ ref('stg_immunization_status') }} s
INNER JOIN {{ ref('dim_period') }} p
    ON s.reported >= p.start_date
   AND s.reported < (p.end_date + INTERVAL '1 day')
WHERE s.chp_area_id IS NOT NULL
  AND p.refresh_tier = 'cold'
GROUP BY s.chp_area_id, p.period_id
