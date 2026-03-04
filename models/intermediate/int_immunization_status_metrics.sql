-- depends_on: {{ ref('stg_immunization_status') }}
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

/* int_immunization_status_metrics: 12.7M rows. Partition-friendly. */

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

    COUNT(DISTINCT patient_id) FILTER (WHERE has_measles_9 = 'complete')                              AS under_1_immunised,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_measles_9 = 'complete' AND patient_sex = 'female')   AS under_1_immunised_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_measles_9 = 'complete' AND patient_sex = 'male')     AS under_1_immunised_male,

    COUNT(DISTINCT patient_id) FILTER (WHERE needs_deworming_follow_up = 'true' AND patient_sex = 'female') AS needs_deworming_follow_up_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE needs_deworming_follow_up = 'true' AND patient_sex = 'male')   AS needs_deworming_follow_up_male,

    COUNT(DISTINCT patient_id) FILTER (WHERE needs_growth_monitoring_referral = 'true' AND patient_sex = 'female') AS referred_growth_monitoring_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE needs_growth_monitoring_referral = 'true' AND patient_sex = 'male')   AS referred_growth_monitoring_male,

    COUNT(DISTINCT patient_id) FILTER (
        WHERE needs_immunization_referral = 'true'
          AND (imm_schedule_upto_date IS NULL OR imm_schedule_upto_date <> 'true')
    ) AS referred_missed_vaccine,
    COUNT(DISTINCT patient_id) FILTER (
        WHERE needs_immunization_referral = 'true'
          AND (imm_schedule_upto_date IS NULL OR imm_schedule_upto_date <> 'true')
          AND patient_sex = 'female'
    ) AS referred_missed_vaccine_female,
    COUNT(DISTINCT patient_id) FILTER (
        WHERE needs_immunization_referral = 'true'
          AND (imm_schedule_upto_date IS NULL OR imm_schedule_upto_date <> 'true')
          AND patient_sex = 'male'
    ) AS referred_missed_vaccine_male,

    NOW() AS last_updated
FROM {{ ref('stg_immunization_status') }}
WHERE reported >= '{{ row['start_date'] }}'::date
  AND reported <  '{{ row['end_excl'] }}'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id
{% endfor %}
