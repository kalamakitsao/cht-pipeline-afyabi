{{ config(materialized='table', tags=['supervision','shock_metrics','cadence_hourly']) }}

SELECT
    county_id,
    county,
    households_per_chp_area,
    pct_households_zero_members,
    pct_households_no_service_90d,
    ROUND(100.0 - pct_active_chp_areas_30d, 1) AS pct_inactive_chp_areas_30d,
    avg_people_per_household
FROM {{ ref('fct_supervision_county') }}
