-- depends_on: {{ ref('stg_u5_assessment') }}
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

/*
  int_u5_metrics: 12.5M source rows, 35 metrics.
  Sex pre-joined in staging. Partition-friendly UNION ALL pattern.
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
    {{ row['period_id'] }}::bigint AS period_id,

    -- Patient-level distinct
    COUNT(DISTINCT patient_id)                                                              AS u5_assessed,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE)                     AS u5_referred,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'female') AS u5_referred_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'male')   AS u5_referred_male,

    -- Assessed for conditions
    COUNT(DISTINCT patient_id) FILTER (WHERE has_diarrhoea IS NOT NULL)                     AS u5_assessed_diarrhoea,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_diarrhoea IS NOT NULL AND patient_sex = 'female') AS u5_assessed_diarrhoea_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_diarrhoea IS NOT NULL AND patient_sex = 'male')   AS u5_assessed_diarrhoea_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_malaria IS NOT NULL)                       AS u5_assessed_malaria,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_malaria IS NOT NULL AND patient_sex = 'female')   AS u5_assessed_malaria_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_malaria IS NOT NULL AND patient_sex = 'male')     AS u5_assessed_malaria_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_malnutrition IS NOT NULL)                  AS u5_assessed_malnutrition,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_malnutrition IS NOT NULL AND patient_sex = 'female') AS u5_assessed_malnutrition_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_malnutrition IS NOT NULL AND patient_sex = 'male')   AS u5_assessed_malnutrition_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_pneumonia IS NOT NULL)                     AS u5_assessed_pneumonia,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_pneumonia IS NOT NULL AND patient_sex = 'female')  AS u5_assessed_pneumonia_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_pneumonia IS NOT NULL AND patient_sex = 'male')    AS u5_assessed_pneumonia_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_exclusively_breastfeeding IS NOT NULL)      AS u5_assessed_exclusive_breastfeeding,

    -- Event-level
    COUNT(uuid) FILTER (WHERE has_malaria IS TRUE)                                          AS u5_confirmed_malaria_cases,
    COUNT(uuid) FILTER (WHERE has_diarrhoea IS TRUE)                                        AS u5_diarrhea_cases,
    COUNT(uuid) FILTER (WHERE has_malnutrition IS TRUE)                                     AS u5_malnutrition_cases,
    COUNT(uuid) FILTER (WHERE has_malnutrition IS TRUE AND patient_sex = 'female')          AS u5_malnutrition_female,
    COUNT(uuid) FILTER (WHERE has_malnutrition IS TRUE AND patient_sex = 'male')            AS u5_malnutrition_male,
    COUNT(uuid) FILTER (WHERE has_pneumonia IS TRUE)                                        AS u5_pneumonia_cases,

    -- Treatment
    COUNT(DISTINCT patient_id) FILTER (WHERE gave_al IS TRUE)                               AS u5_treated_malaria,
    COUNT(DISTINCT patient_id) FILTER (WHERE gave_ors IS TRUE OR gave_zinc IS TRUE)         AS u5_treated_diarrhoea,
    COUNT(DISTINCT patient_id) FILTER (WHERE gave_amox IS TRUE AND has_pneumonia IS TRUE)   AS u5_treated_pneumonia,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_exclusively_breastfeeding IS TRUE)          AS u5_exclusive_breastfeeding,

    -- Testing / suspected
    COUNT(DISTINCT patient_id) FILTER (WHERE rdt_result IS NOT NULL AND rdt_result <> 'not_done') AS u5_tested_malaria,
    COUNT(DISTINCT patient_id) FILTER (
        WHERE has_fever IS TRUE AND has_malaria IS NOT TRUE
          AND (has_pneumonia IS NOT TRUE OR has_diarrhoea IS NOT TRUE)
    ) AS u5_suspected_malaria_cases,

    -- Referrals by condition
    COUNT(DISTINCT patient_id) FILTER (WHERE has_diarrhoea IS TRUE AND has_been_referred IS TRUE)    AS referred_for_diarrhoea,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_fever IS TRUE AND has_been_referred IS TRUE)        AS referred_for_malaria,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_malnutrition IS TRUE AND has_been_referred IS TRUE) AS referred_for_malnutrition,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_pneumonia IS TRUE AND has_been_referred IS TRUE)    AS referred_for_pneumonia,

    -- Development milestones
    COUNT(DISTINCT patient_id) FILTER (WHERE referred_for_development_milestones IS TRUE)                              AS referred_for_development_milestones,
    COUNT(DISTINCT patient_id) FILTER (WHERE referred_for_development_milestones IS TRUE AND patient_sex = 'female')   AS female_referred_for_development_milestones,
    COUNT(DISTINCT patient_id) FILTER (WHERE referred_for_development_milestones IS TRUE AND patient_sex = 'male')     AS male_referred_for_development_milestones,

    NOW() AS last_updated

FROM {{ ref('stg_u5_assessment') }}
WHERE reported >= '{{ row['start_date'] }}'::date
  AND reported <  '{{ row['end_excl'] }}'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id
{% endfor %}
