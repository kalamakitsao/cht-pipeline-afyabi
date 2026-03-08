-- depends_on: {{ ref('stg_death_report') }}
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
    COUNT(s.uuid)                                                                                     AS total_deaths,
    COUNT(s.uuid) FILTER (WHERE s.death_type = 'maternal death')                                      AS maternal_deaths,
    COUNT(s.uuid) FILTER (WHERE s.patient_age_in_days < 29)                                           AS neonatal_deaths,
    COUNT(s.uuid) FILTER (WHERE s.patient_age_in_days BETWEEN 29 AND 1827)                            AS child_deaths,
    COUNT(s.uuid) FILTER (WHERE s.patient_age_in_days BETWEEN 29 AND 1827 AND s.patient_sex = 'female') AS child_deaths_female,
    COUNT(s.uuid) FILTER (WHERE s.patient_age_in_days BETWEEN 29 AND 1827 AND s.patient_sex = 'male')   AS child_deaths_male,
    COUNT(s.uuid) FILTER (WHERE s.patient_age_in_days > 1827 AND s.patient_sex = 'female')            AS over_5_deaths_female,
    COUNT(s.uuid) FILTER (WHERE s.patient_age_in_days > 1827 AND s.patient_sex = 'male')              AS over_5_deaths_male,
    NOW() AS last_updated
FROM {{ ref('stg_death_report') }} s
INNER JOIN {{ ref('dim_period') }} p
    ON s.reported >= p.start_date
   AND s.reported < (p.end_date + INTERVAL '1 day')
WHERE s.chp_area_id IS NOT NULL
  AND p.refresh_tier = 'cold'
GROUP BY s.chp_area_id, p.period_id
