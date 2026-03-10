{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['chp_area_id'], 'unique': true}
    ],
    tags = ['cadence_daily', 'intermediate']
  )
}}

/*
  int_chp_metrics: Period-independent CHP-area indicators.
  - chps_enrolled: 1 for every CHP area in the hierarchy
  - chps_with_hholds: 1 if the CHP area has any households
*/

SELECT
    a.chp_area_id,
    1 AS chps_enrolled,
    CASE WHEN hh.households_registered > 0 THEN 1 ELSE 0 END AS chps_with_hholds,
    NOW() AS last_updated
FROM {{ ref('mv_location_hierarchy') }} a
LEFT JOIN {{ ref('int_households') }} hh ON hh.chp_area_id = a.chp_area_id
