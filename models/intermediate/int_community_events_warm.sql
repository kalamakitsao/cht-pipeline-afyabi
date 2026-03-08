-- depends_on: {{ ref('stg_community_event') }}
-- depends_on: {{ ref('dim_period') }}
{{
  config(
    materialized = 'table',
    pre_hook = "SET LOCAL work_mem = '512MB'",
    indexes = [
      {'columns': ['chp_area_id', 'period_id']},
      {'columns': ['period_id']}
    ],
    tags = ['cadence_6h', 'intermediate']
  )
}}

/*
  int_community_events_warm: Community event counts by CHP area × period.
  Source: stg_community_event (28K rows — tiny).
*/

SELECT
    s.chp_area_id,
    p.period_id,
    COUNT(s.uuid) FILTER (WHERE s.event_types ILIKE '%monthly_cu_meetings%') AS monthly_cu_meetings,
    COUNT(s.uuid) FILTER (WHERE s.event_types != 'monthly_cu_meetings')       AS other_community_events
FROM {{ ref('stg_community_event') }} s
INNER JOIN {{ ref('dim_period') }} p
    ON s.reported >= p.start_date
   AND s.reported < (p.end_date + INTERVAL '1 day')
WHERE s.chp_area_id IS NOT NULL
  AND p.refresh_tier = 'warm'
GROUP BY s.chp_area_id, p.period_id
