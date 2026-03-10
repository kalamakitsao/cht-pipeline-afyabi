{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['chp_area_id', 'period_id']},
      {'columns': ['period_id']}
    ],
    tags = ['cadence_daily', 'intermediate']
  )
}}

/*
  int_chps_meeting_target_cold: 1 if (hh_visited / total_households) >= 0.33.
  Reads from pre-computed int_household_visit_metrics_cold and int_households.
  No raw table scans — just small pre-aggregated tables.
*/

SELECT
    hv.chp_area_id,
    hv.period_id,
    CASE
        WHEN hh.households_registered > 0
         AND (hv.hh_visited::numeric / hh.households_registered) >= 0.33
        THEN 1 ELSE 0
    END AS chps_meeting_target,
    NOW() AS last_updated
FROM {{ ref('int_household_visit_metrics_cold') }} hv
INNER JOIN {{ ref('int_households') }} hh ON hh.chp_area_id = hv.chp_area_id
