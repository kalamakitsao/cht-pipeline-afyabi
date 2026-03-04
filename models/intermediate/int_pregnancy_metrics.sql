-- depends_on: {{ ref('stg_pregnancy_home_visit') }}
-- depends_on: {{ ref('stg_postnatal_care_service') }}
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

/* int_pregnancy_metrics: 876K rows. Partition-friendly. */

{% set periods_query %}
    SELECT period_id, start_date, end_date, (end_date + INTERVAL '1 day')::date AS end_excl
    FROM {{ ref('dim_period') }} ORDER BY period_id
{% endset %}

{% if execute %}
  {% set period_rows = run_query(periods_query) %}
{% else %}
  {% set period_rows = [] %}
{% endif %}

-- Pre-compute latest deliveries (used for repeat_pregnancies)
WITH latest_deliveries AS (
    SELECT patient_id, MAX(date_of_delivery) AS last_delivery_date
    FROM {{ ref('stg_postnatal_care_service') }}
    WHERE date_of_delivery IS NOT NULL
    GROUP BY patient_id
)

{% for row in period_rows %}
{% if not loop.first %}UNION ALL{% endif %}

SELECT
    s.chp_area_id,
    {{ row['period_id'] }}::bigint AS period_id,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_currently_pregnant IS TRUE OR s.is_new_pregnancy IS TRUE
    ) AS actively_pregnant_women,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.effective_edd >= '{{ row['end_date'] }}'::date
          AND s.effective_edd <= '{{ row['end_date'] }}'::date + INTERVAL '9 months'
    ) AS currently_pregnant,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_new_pregnancy IS TRUE)              AS new_pregnancies,
    COUNT(DISTINCT s.patient_id)                                                         AS pregnant_women_visited,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE)              AS pregnant_women_referred,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.has_been_referred IS TRUE AND s.is_anc_upto_date IS FALSE
    ) AS pregnant_women_referred_missed_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE AND s.has_started_anc IS FALSE
    ) AS new_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE (s.is_new_pregnancy IS TRUE OR s.is_currently_pregnant IS TRUE)
          AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE
          AND s.has_started_anc IS FALSE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.current_edd IS NOT NULL
          AND (((s.current_edd::date - '{{ row['end_date'] }}'::date) / 7.0 - 40) * -1) BETWEEN 0 AND 12
    ) AS first_trimester_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE
          AND ld.last_delivery_date IS NOT NULL
          AND s.reported > ld.last_delivery_date
          AND s.reported <= ld.last_delivery_date + INTERVAL '12 months'
    ) AS repeat_pregnancies,

    NOW() AS last_updated

FROM {{ ref('stg_pregnancy_home_visit') }} s
LEFT JOIN latest_deliveries ld ON s.patient_id = ld.patient_id
WHERE s.reported >= '{{ row['start_date'] }}'::date
  AND s.reported <  '{{ row['end_excl'] }}'::date
  AND s.chp_area_id IS NOT NULL
GROUP BY s.chp_area_id
{% endfor %}
