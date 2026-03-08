-- depends_on: {{ ref('stg_postnatal_care_service') }}
-- depends_on: {{ ref('dim_period') }}
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

SELECT
    s.chp_area_id,
    p.period_id,
    COUNT(s.uuid) FILTER (WHERE s.pnc_service_count = 1)                              AS deliveries,
    COUNT(s.uuid) FILTER (WHERE s.place_of_delivery = 'health_facility')               AS skilled_birth_attendance,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_referred_for_pnc_services IS TRUE) AS referred_for_pnc,
    NOW() AS last_updated
FROM {{ ref('stg_postnatal_care_service') }} s
INNER JOIN {{ ref('dim_period') }} p
    ON s.reported >= p.start_date
   AND s.reported < (p.end_date + INTERVAL '1 day')
WHERE s.chp_area_id IS NOT NULL
  AND p.refresh_tier = 'hot'
GROUP BY s.chp_area_id, p.period_id
