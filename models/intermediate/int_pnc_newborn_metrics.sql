-- depends_on: {{ ref('stg_postnatal_care_service_newborn') }}
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

/* int_pnc_newborn_metrics: 56K rows. Partition-friendly. */

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
    COUNT(DISTINCT patient_id)                                                                       AS newborns,
    COUNT(DISTINCT patient_id) FILTER (WHERE needs_danger_signs_follow_up IS TRUE)                   AS newborns_needing_follow_up_danger_signs,
    COUNT(DISTINCT patient_id) FILTER (WHERE needs_missed_visit_follow_up IS TRUE)                   AS newborns_needing_follow_up_missed_visit,
    COUNT(DISTINCT patient_id) FILTER (
        WHERE needs_danger_signs_follow_up IS TRUE OR needs_missed_visit_follow_up IS TRUE
    ) AS newborns_needing_follow_up,
    COUNT(DISTINCT patient_id) FILTER (WHERE needs_danger_signs_follow_up IS TRUE)                   AS needs_follow_up_danger_signs,
    COUNT(DISTINCT patient_id) FILTER (WHERE needs_missed_visit_follow_up IS TRUE)                   AS needs_follow_up_missed_visit,
    NOW() AS last_updated
FROM {{ ref('stg_postnatal_care_service_newborn') }}
WHERE reported >= '{{ row['start_date'] }}'::date
  AND reported <  '{{ row['end_excl'] }}'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id
{% endfor %}
