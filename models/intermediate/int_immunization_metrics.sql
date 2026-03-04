-- depends_on: {{ ref('stg_immunization') }}
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

/* int_immunization_metrics: 20M rows. Partition-friendly. */

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
    {{ row['period_id'] }}::bigint AS period_id,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_immunization IS TRUE)                              AS referred_immunization,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_immunization IS TRUE AND patient_sex = 'female')   AS referred_immunization_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_immunization IS TRUE AND patient_sex = 'male')     AS referred_immunization_male,
    NOW() AS last_updated
FROM {{ ref('stg_immunization') }}
WHERE reported >= '{{ row['start_date'] }}'::date
  AND reported <  '{{ row['end_excl'] }}'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id
{% endfor %}
