{{
  config(
    materialized = 'table',
    pre_hook = "SET LOCAL work_mem = '512MB'",
    indexes = [
      {'columns': ['chp_area_id'], 'unique': true}
    ],
    tags = ['cadence_daily', 'intermediate']
  )
}}

/*
  int_population_static: Active, non-deceased population by CHP area.
  Period-independent — same values across all 15 periods.
  Source: stg_patient (30M) anti-joined with stg_death_report (153K).
  Output: ~5K rows (one per CHP area).
*/

SELECT
    pt.chp_area_id,
    COUNT(pt.patient_id)                               AS population,
    COUNT(pt.patient_id) FILTER (WHERE pt.sex = 'female') AS population_female,
    COUNT(pt.patient_id) FILTER (WHERE pt.sex = 'male')   AS population_male
FROM {{ ref('stg_patient') }} pt
WHERE pt.is_active IS TRUE
  AND pt.chp_area_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM {{ ref('stg_death_report') }} d
      WHERE d.patient_id = pt.patient_id
  )
GROUP BY pt.chp_area_id
