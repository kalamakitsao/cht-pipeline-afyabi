{{ config(materialized='table', tags=['supervision','scorecard','cadence_hourly']) }}

SELECT
    county_id,
    county,
    pct_households_served_90d,
    pct_active_chp_areas_30d,
    households_per_chp_area,
    avg_people_per_household,
    pct_households_zero_members,
    pct_households_registered_on_sha,
    pct_households_no_service_90d,
    pct_chp_areas_zero_households,
    pct_people_missing_household_link
FROM {{ ref('fct_supervision_county') }}
