-- depends_on: {{ ref('stg_household_visit') }}
-- depends_on: {{ ref('dim_period') }}
{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['chp_area_id', 'period_id']},
      {'columns': ['period_id']}
    ],
    tags = ['cadence_hourly', 'intermediate']
  )
}}

/*
  int_household_visit_metrics: 118M source rows — the bottleneck.
  
  PARTITION-FRIENDLY: UNION ALL with literal dates per period.
  Each branch triggers static partition pruning on stg_household_visit.
  
  Non-partitioned fallback: each branch uses index range scan on (reported).
*/

{% set periods_query %}
    SELECT period_id, start_date, (end_date + INTERVAL '1 day')::date AS end_excl
    FROM {{ ref('dim_period') }} ORDER BY period_id
{% endset %}

{% if execute %}
  {% set period_rows = run_query(periods_query) %}
{% else %}
  {% set period_rows = [] %}
{% endif %}

{% for row in period_rows %}
{% if not loop.first %}UNION ALL{% endif %}

SELECT
    chp_area_id,
    {{ row['period_id'] }}::bigint  AS period_id,
    COUNT(DISTINCT household)       AS hh_visited,
    1                               AS chps_reporting,
    NOW()                           AS last_updated
FROM {{ ref('stg_household_visit') }}
WHERE reported >= '{{ row['start_date'] }}'::date
  AND reported <  '{{ row['end_excl'] }}'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id
{% endfor %}
