-- depends_on: {{ ref('stg_over_five_assessment') }}
-- depends_on: {{ ref('dim_period') }}
{{
  config(
    materialized = 'table',
    pre_hook = "SET LOCAL work_mem = '1GB'",
    indexes = [
      {'columns': ['chp_area_id', 'period_id']},
      {'columns': ['period_id']},
      {'columns': ['chp_area_id']}
    ],
    tags = ['cadence_daily', 'intermediate']
  )
}}

SELECT
    s.chp_area_id,
    p.period_id,

    COUNT(s.uuid)                                                                                 AS over_5_assessments,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE)                       AS over_5_referred,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE AND s.patient_sex = 'female') AS over_5_referred_female,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE AND s.patient_sex = 'male')   AS over_5_referred_male,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.screened_for_diabetes IS TRUE)                   AS screened_diabetes,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.screened_for_diabetes IS TRUE AND s.patient_sex = 'female') AS screened_diabetes_female,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.screened_for_diabetes IS TRUE AND s.patient_sex = 'male')   AS screened_diabetes_male,
    COUNT(s.uuid) FILTER (WHERE s.screened_for_diabetes IS TRUE)                                  AS screenings_diabetes,
    COUNT(s.uuid) FILTER (WHERE s.screened_for_diabetes IS TRUE AND s.patient_sex = 'female')     AS screenings_diabetes_female,
    COUNT(s.uuid) FILTER (WHERE s.screened_for_diabetes IS TRUE AND s.patient_sex = 'male')       AS screenings_diabetes_male,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_referred_diabetes IS TRUE)                    AS referred_diabetes,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_referred_diabetes IS TRUE AND s.patient_sex = 'female') AS referred_diabetes_female,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_referred_diabetes IS TRUE AND s.patient_sex = 'male')   AS referred_diabetes_male,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.screened_for_hypertension IS TRUE)               AS screened_hypertension,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.screened_for_hypertension IS TRUE AND s.patient_sex = 'female') AS screened_hypertension_female,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.screened_for_hypertension IS TRUE AND s.patient_sex = 'male')   AS screened_hypertension_male,
    COUNT(s.uuid) FILTER (WHERE s.screened_for_hypertension IS TRUE)                              AS screenings_hypertension,
    COUNT(s.uuid) FILTER (WHERE s.screened_for_hypertension IS TRUE AND s.patient_sex = 'female') AS screenings_hypertension_female,
    COUNT(s.uuid) FILTER (WHERE s.screened_for_hypertension IS TRUE AND s.patient_sex = 'male')   AS screenings_hypertension_male,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_referred_hypertension IS TRUE)                AS referred_hypertension,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_referred_hypertension IS TRUE AND s.patient_sex = 'female') AS referred_hypertension_female,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_referred_hypertension IS TRUE AND s.patient_sex = 'male')   AS referred_hypertension_male,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.screened_for_mental_health IS TRUE)              AS screened_mental_health,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.screened_for_mental_health IS TRUE AND s.patient_sex = 'female') AS screened_mental_health_female,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.screened_for_mental_health IS TRUE AND s.patient_sex = 'male')   AS screened_mental_health_male,
    COUNT(s.uuid) FILTER (WHERE s.screened_for_mental_health IS TRUE)                             AS screenings_mental_health,
    COUNT(s.uuid) FILTER (WHERE s.screened_for_mental_health IS TRUE AND s.patient_sex = 'female') AS screenings_mental_health_female,
    COUNT(s.uuid) FILTER (WHERE s.screened_for_mental_health IS TRUE AND s.patient_sex = 'male')   AS screenings_mental_health_male,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_referred_mental_health IS TRUE)               AS referred_mental_health,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_referred_mental_health IS TRUE AND s.patient_sex = 'female') AS referred_mental_health_female,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_referred_mental_health IS TRUE AND s.patient_sex = 'male')   AS referred_mental_health_male,

    NOW() AS last_updated
FROM {{ ref('stg_over_five_assessment') }} s
INNER JOIN {{ ref('dim_period') }} p
    ON s.reported >= p.start_date
   AND s.reported < (p.end_date + INTERVAL '1 day')
WHERE s.chp_area_id IS NOT NULL
  AND p.refresh_tier = 'cold'
GROUP BY s.chp_area_id, p.period_id
