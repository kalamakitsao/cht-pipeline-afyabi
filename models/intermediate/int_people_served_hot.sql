-- depends_on: {{ ref('stg_data_record') }}
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

/*
  int_people_served_hot: Distinct patients served by CHP area × period.
  Source: stg_data_record (57M rows).
*/

SELECT
    s.chp_area_id,
    p.period_id,
    COUNT(DISTINCT s.patient_id) AS people_served
FROM {{ ref('stg_data_record') }} s
INNER JOIN {{ ref('dim_period') }} p
    ON s.reported >= p.start_date
   AND s.reported < (p.end_date + INTERVAL '1 day')
WHERE s.chp_area_id IS NOT NULL
  AND p.refresh_tier = 'hot'
GROUP BY s.chp_area_id, p.period_id
