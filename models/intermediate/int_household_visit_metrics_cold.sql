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

/*
  int_household_visit_metrics_cold: Households visited by CHP area x period.
  
  Queries the PARTITIONED source table directly instead of stg_household_visit.
  Source: household_visit partitioned by RANGE(reported), 78 partitions.
  Column mapping: reported_by_parent = chp_area_id, household = household
*/

SELECT
    s.reported_by_parent    AS chp_area_id,
    p.period_id,
    COUNT(DISTINCT s.household) AS hh_visited,
    1                           AS chps_reporting,
    NOW()                       AS last_updated
FROM {{ source(var('source_schema'), 'household_visit') }} s
INNER JOIN {{ ref('dim_period') }} p
    ON s.reported >= p.start_date
   AND s.reported < (p.end_date + INTERVAL '1 day')
WHERE s.reported_by_parent IS NOT NULL
  AND p.refresh_tier = 'cold'
GROUP BY s.reported_by_parent, p.period_id
