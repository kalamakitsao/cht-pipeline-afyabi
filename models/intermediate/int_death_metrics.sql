-- depends_on: {{ ref('stg_death_report') }}
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

/* int_death_metrics: 148K rows. Partition-friendly. */

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
    COUNT(uuid)                                                                                     AS total_deaths,
    COUNT(uuid) FILTER (WHERE death_type = 'maternal death')                                        AS maternal_deaths,
    COUNT(uuid) FILTER (WHERE patient_age_in_days < 29)                                             AS neonatal_deaths,
    COUNT(uuid) FILTER (WHERE patient_age_in_days BETWEEN 29 AND 1827)                              AS child_deaths,
    COUNT(uuid) FILTER (WHERE patient_age_in_days BETWEEN 29 AND 1827 AND patient_sex = 'female')   AS child_deaths_female,
    COUNT(uuid) FILTER (WHERE patient_age_in_days BETWEEN 29 AND 1827 AND patient_sex = 'male')     AS child_deaths_male,
    COUNT(uuid) FILTER (WHERE patient_age_in_days > 1827 AND patient_sex = 'female')                AS over_5_deaths_female,
    COUNT(uuid) FILTER (WHERE patient_age_in_days > 1827 AND patient_sex = 'male')                  AS over_5_deaths_male,
    NOW() AS last_updated
FROM {{ ref('stg_death_report') }}
WHERE reported >= '{{ row['start_date'] }}'::date
  AND reported <  '{{ row['end_excl'] }}'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id
{% endfor %}
