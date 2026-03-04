-- depends_on: {{ ref('stg_over_five_assessment') }}
-- depends_on: {{ ref('dim_period') }}
{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['chp_area_id', 'period_id']},
      {'columns': ['period_id']},
      {'columns': ['chp_area_id']}
    ],
    tags = ['cadence_hourly', 'intermediate']
  )
}}

/* int_over_five_metrics: 30M rows, 33 metrics. Partition-friendly. */

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

    COUNT(uuid)                                                                                 AS over_5_assessments,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE)                         AS over_5_referred,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'female') AS over_5_referred_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'male')   AS over_5_referred_male,

    -- Diabetes
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE)                     AS screened_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female') AS screened_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')   AS screened_diabetes_male,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE)                                    AS screenings_diabetes,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female')         AS screenings_diabetes_female,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')           AS screenings_diabetes_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE)                      AS referred_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'female') AS referred_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'male')   AS referred_diabetes_male,

    -- Hypertension
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE)                 AS screened_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female') AS screened_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')   AS screened_hypertension_male,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE)                                AS screenings_hypertension,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female')     AS screenings_hypertension_female,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')       AS screenings_hypertension_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE)                  AS referred_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'female') AS referred_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'male')   AS referred_hypertension_male,

    -- Mental health
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE)                AS screened_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female') AS screened_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')   AS screened_mental_health_male,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE)                               AS screenings_mental_health,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female')    AS screenings_mental_health_female,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')      AS screenings_mental_health_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE)                 AS referred_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'female') AS referred_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'male')   AS referred_mental_health_male,

    NOW() AS last_updated

FROM {{ ref('stg_over_five_assessment') }}
WHERE reported >= '{{ row['start_date'] }}'::date
  AND reported <  '{{ row['end_excl'] }}'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id
{% endfor %}
