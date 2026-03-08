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

/*
  int_population_age_dependent: Age-based population metrics per period.
  CROSS JOIN required — a child's age relative to each period's end_date differs.
  Source: stg_patient (30M) × dim_period (15) = 450M virtual rows.
  Output: ~75K rows (5K chp_areas × 15 periods).

  This is the heaviest query in the pipeline. 1GB work_mem keeps
  the hash aggregation in memory.
*/

SELECT
    pt.chp_area_id,
    p.period_id,
    COUNT(pt.patient_id) FILTER (
        WHERE pt.date_of_birth > p.end_date - INTERVAL '5 years'
    ) AS under_5_population,
    COUNT(pt.patient_id) FILTER (
        WHERE pt.sex = 'female' AND pt.date_of_birth > p.end_date - INTERVAL '5 years'
    ) AS population_under_5_female,
    COUNT(pt.patient_id) FILTER (
        WHERE pt.sex = 'male' AND pt.date_of_birth > p.end_date - INTERVAL '5 years'
    ) AS population_under_5_male,
    COUNT(pt.patient_id) FILTER (
        WHERE age(p.end_date, pt.date_of_birth) >= INTERVAL '11 months'
          AND age(p.end_date, pt.date_of_birth) <  INTERVAL '1 year'
    ) AS children_turning_one,
    COUNT(pt.patient_id) FILTER (
        WHERE pt.sex = 'female'
          AND age(p.end_date, pt.date_of_birth) >= INTERVAL '11 months'
          AND age(p.end_date, pt.date_of_birth) <  INTERVAL '1 year'
    ) AS female_turning_one,
    COUNT(pt.patient_id) FILTER (
        WHERE pt.sex = 'male'
          AND age(p.end_date, pt.date_of_birth) >= INTERVAL '11 months'
          AND age(p.end_date, pt.date_of_birth) <  INTERVAL '1 year'
    ) AS male_turning_one
FROM {{ ref('stg_patient') }} pt
CROSS JOIN {{ ref('dim_period') }} p
WHERE pt.is_active IS TRUE
  AND pt.chp_area_id IS NOT NULL
  AND pt.date_of_birth IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM {{ ref('stg_death_report') }} d
      WHERE d.patient_id = pt.patient_id
  )
GROUP BY pt.chp_area_id, p.period_id
