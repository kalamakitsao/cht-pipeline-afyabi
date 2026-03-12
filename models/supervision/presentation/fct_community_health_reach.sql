{{ config(materialized='table', tags=['supervision','reach','cadence_hourly']) }}

SELECT
    county_id,
    county,
    households_registered,
    households_served_90d,
    pct_households_served_90d AS community_health_reach_pct
FROM {{ ref('fct_supervision_county') }}
