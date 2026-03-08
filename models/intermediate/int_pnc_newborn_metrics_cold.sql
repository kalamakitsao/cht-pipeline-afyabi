-- depends_on: {{ ref('stg_postnatal_care_service_newborn') }}
-- depends_on: {{ ref('dim_period') }}
{{
  config(
    materialized = 'table',
    pre_hook = "SET LOCAL work_mem = '1GB'",
    indexes = [
      {'columns': ['chp_area_id', 'period_id']},
      {'columns': ['period_id']}
    ],
    tags = ['cadence_daily', 'intermediate']
  )
}}

SELECT
    s.chp_area_id,
    p.period_id,
    COUNT(DISTINCT s.patient_id)                                                                       AS newborns,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.needs_danger_signs_follow_up IS TRUE)                 AS newborns_needing_follow_up_danger_signs,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.needs_missed_visit_follow_up IS TRUE)                 AS newborns_needing_follow_up_missed_visit,
    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.needs_danger_signs_follow_up IS TRUE OR s.needs_missed_visit_follow_up IS TRUE
    ) AS newborns_needing_follow_up,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.needs_danger_signs_follow_up IS TRUE)                 AS needs_follow_up_danger_signs,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.needs_missed_visit_follow_up IS TRUE)                 AS needs_follow_up_missed_visit,
    NOW() AS last_updated
FROM {{ ref('stg_postnatal_care_service_newborn') }} s
INNER JOIN {{ ref('dim_period') }} p
    ON s.reported >= p.start_date
   AND s.reported < (p.end_date + INTERVAL '1 day')
WHERE s.chp_area_id IS NOT NULL
  AND p.refresh_tier = 'cold'
GROUP BY s.chp_area_id, p.period_id
