-- depends_on: {{ ref('stg_immunization') }}
-- depends_on: {{ ref('dim_period') }}
{{
  config(
    materialized = 'table',
    pre_hook = "SET LOCAL work_mem = '256MB'",
    indexes = [
      {'columns': ['chp_area_id', 'period_id']},
      {'columns': ['period_id']}
    ],
    tags = ['cadence_hourly', 'intermediate']
  )
}}

SELECT
    s.chp_area_id,
    p.period_id,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_referred_immunization IS TRUE)                            AS referred_immunization,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_referred_immunization IS TRUE AND s.patient_sex = 'female') AS referred_immunization_female,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_referred_immunization IS TRUE AND s.patient_sex = 'male')   AS referred_immunization_male,
    NOW() AS last_updated
FROM {{ ref('stg_immunization') }} s
INNER JOIN {{ ref('dim_period') }} p
    ON s.reported >= p.start_date
   AND s.reported < (p.end_date + INTERVAL '1 day')
WHERE s.chp_area_id IS NOT NULL
  AND p.refresh_tier = 'hot'
GROUP BY s.chp_area_id, p.period_id
