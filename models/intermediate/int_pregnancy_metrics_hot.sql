-- depends_on: {{ ref('stg_pregnancy_home_visit') }}
-- depends_on: {{ ref('dim_period') }}
-- depends_on: {{ ref('stg_postnatal_care_service') }}
{{
  config(
    materialized = 'table',
    pre_hook = "SET LOCAL work_mem = '256MB'",
    indexes = [
      {'columns': ['chp_area_id', 'period_id']},
      {'columns': ['period_id']}
    ],
    tags = ['cadence_hourly', 'intermediate']
  )
}}

WITH latest_deliveries AS (
    SELECT patient_id, MAX(date_of_delivery) AS last_delivery_date
    FROM {{ ref('stg_postnatal_care_service') }}
    WHERE date_of_delivery IS NOT NULL
    GROUP BY patient_id
)

SELECT
    s.chp_area_id,
    p.period_id,
    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_currently_pregnant IS TRUE OR s.is_new_pregnancy IS TRUE
    ) AS actively_pregnant_women,
    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.effective_edd >= p.end_date
          AND s.effective_edd <= p.end_date + INTERVAL '9 months'
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
          AND (((s.current_edd::date - p.end_date) / 7.0 - 40) * -1) BETWEEN 0 AND 12
    ) AS first_trimester_pregnancies,
    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE
          AND ld.last_delivery_date IS NOT NULL
          AND s.reported > ld.last_delivery_date
          AND s.reported <= ld.last_delivery_date + INTERVAL '12 months'
    ) AS repeat_pregnancies,
    NOW() AS last_updated
FROM {{ ref('stg_pregnancy_home_visit') }} s
INNER JOIN {{ ref('dim_period') }} p
    ON s.reported >= p.start_date
   AND s.reported < (p.end_date + INTERVAL '1 day')
LEFT JOIN latest_deliveries ld ON s.patient_id = ld.patient_id
WHERE s.chp_area_id IS NOT NULL
  AND p.refresh_tier = 'hot'
GROUP BY s.chp_area_id, p.period_id
