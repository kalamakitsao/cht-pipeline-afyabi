-- depends_on: {{ ref('stg_household_visit') }}
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
    COUNT(DISTINCT s.household)  AS hh_visited,
    1                            AS chps_reporting,
    NOW()                        AS last_updated
FROM {{ ref('stg_household_visit') }} s
INNER JOIN {{ ref('dim_period') }} p
    ON s.reported >= p.start_date
   AND s.reported < (p.end_date + INTERVAL '1 day')
WHERE s.chp_area_id IS NOT NULL
  AND p.refresh_tier = 'cold'
GROUP BY s.chp_area_id, p.period_id
