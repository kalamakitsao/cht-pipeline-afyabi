{{ config(materialized='table', tags=['supervision','operational','cadence_hourly']) }}

SELECT
    county_id,
    county,
    households_registered,
    households_served_90d,
    pct_households_served_90d,
    forms_submitted_90d,
    distinct_services_provided_90d,
    people_served_90d,
    pct_active_chp_areas_30d,
    households_per_chp_area,
    households_no_service_90d,
    pct_households_no_service_90d,
    households_registered_on_sha,
    pct_households_registered_on_sha
FROM {{ ref('fct_supervision_county') }}
