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
  int_households: Registered household count by CHP area.
  Period-independent. Source: stg_household (9.1M).
  Output: ~5K rows.
*/

SELECT
    chp_area_id,
    COUNT(household_id) AS households_registered
FROM {{ ref('stg_household') }}
WHERE chp_area_id IS NOT NULL
GROUP BY chp_area_id
