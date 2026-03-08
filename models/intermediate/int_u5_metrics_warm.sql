-- depends_on: {{ ref('stg_u5_assessment') }}
-- depends_on: {{ ref('dim_period') }}
{{
  config(
    materialized = 'table',
    pre_hook = "SET LOCAL work_mem = '512MB'",
    indexes = [
      {'columns': ['chp_area_id', 'period_id']},
      {'columns': ['period_id']},
      {'columns': ['chp_area_id']}
    ],
    tags = ['cadence_6h', 'intermediate']
  )
}}

SELECT
    s.chp_area_id,
    p.period_id,

    COUNT(DISTINCT s.patient_id)                                                              AS u5_assessed,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE)                   AS u5_referred,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE AND s.patient_sex = 'female') AS u5_referred_female,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE AND s.patient_sex = 'male')   AS u5_referred_male,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_diarrhoea IS NOT NULL)                   AS u5_assessed_diarrhoea,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_diarrhoea IS NOT NULL AND s.patient_sex = 'female') AS u5_assessed_diarrhoea_female,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_diarrhoea IS NOT NULL AND s.patient_sex = 'male')   AS u5_assessed_diarrhoea_male,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_malaria IS NOT NULL)                     AS u5_assessed_malaria,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_malaria IS NOT NULL AND s.patient_sex = 'female')   AS u5_assessed_malaria_female,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_malaria IS NOT NULL AND s.patient_sex = 'male')     AS u5_assessed_malaria_male,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_malnutrition IS NOT NULL)                AS u5_assessed_malnutrition,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_malnutrition IS NOT NULL AND s.patient_sex = 'female') AS u5_assessed_malnutrition_female,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_malnutrition IS NOT NULL AND s.patient_sex = 'male')   AS u5_assessed_malnutrition_male,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_pneumonia IS NOT NULL)                   AS u5_assessed_pneumonia,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_pneumonia IS NOT NULL AND s.patient_sex = 'female')  AS u5_assessed_pneumonia_female,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_pneumonia IS NOT NULL AND s.patient_sex = 'male')    AS u5_assessed_pneumonia_male,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_exclusively_breastfeeding IS NOT NULL)    AS u5_assessed_exclusive_breastfeeding,

    COUNT(s.uuid) FILTER (WHERE s.has_malaria IS TRUE)                                        AS u5_confirmed_malaria_cases,
    COUNT(s.uuid) FILTER (WHERE s.has_diarrhoea IS TRUE)                                      AS u5_diarrhea_cases,
    COUNT(s.uuid) FILTER (WHERE s.has_malnutrition IS TRUE)                                   AS u5_malnutrition_cases,
    COUNT(s.uuid) FILTER (WHERE s.has_malnutrition IS TRUE AND s.patient_sex = 'female')      AS u5_malnutrition_female,
    COUNT(s.uuid) FILTER (WHERE s.has_malnutrition IS TRUE AND s.patient_sex = 'male')        AS u5_malnutrition_male,
    COUNT(s.uuid) FILTER (WHERE s.has_pneumonia IS TRUE)                                      AS u5_pneumonia_cases,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.gave_al IS TRUE)                             AS u5_treated_malaria,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.gave_ors IS TRUE OR s.gave_zinc IS TRUE)     AS u5_treated_diarrhoea,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.gave_amox IS TRUE AND s.has_pneumonia IS TRUE) AS u5_treated_pneumonia,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_exclusively_breastfeeding IS TRUE)        AS u5_exclusive_breastfeeding,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.rdt_result IS NOT NULL AND s.rdt_result <> 'not_done') AS u5_tested_malaria,
    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.has_fever IS TRUE AND s.has_malaria IS NOT TRUE
          AND (s.has_pneumonia IS NOT TRUE OR s.has_diarrhoea IS NOT TRUE)
    ) AS u5_suspected_malaria_cases,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_diarrhoea IS TRUE AND s.has_been_referred IS TRUE)    AS referred_for_diarrhoea,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_fever IS TRUE AND s.has_been_referred IS TRUE)        AS referred_for_malaria,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_malnutrition IS TRUE AND s.has_been_referred IS TRUE) AS referred_for_malnutrition,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_pneumonia IS TRUE AND s.has_been_referred IS TRUE)    AS referred_for_pneumonia,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.referred_for_development_milestones IS TRUE)                            AS referred_for_development_milestones,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.referred_for_development_milestones IS TRUE AND s.patient_sex = 'female') AS female_referred_for_development_milestones,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.referred_for_development_milestones IS TRUE AND s.patient_sex = 'male')   AS male_referred_for_development_milestones,

    NOW() AS last_updated
FROM {{ ref('stg_u5_assessment') }} s
INNER JOIN {{ ref('dim_period') }} p
    ON s.reported >= p.start_date
   AND s.reported < (p.end_date + INTERVAL '1 day')
WHERE s.chp_area_id IS NOT NULL
  AND p.refresh_tier = 'warm'
GROUP BY s.chp_area_id, p.period_id
