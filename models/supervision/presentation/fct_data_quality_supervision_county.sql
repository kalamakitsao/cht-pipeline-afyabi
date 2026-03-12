{{ config(materialized='table', tags=['supervision','data_quality','cadence_daily']) }}

SELECT
    county_id,
    county,
    households_registered,
    households_zero_members,
    pct_households_zero_members,
    households_one_member,
    pct_households_one_member,
    households_gt_15_members,
    pct_households_gt_15_members,
    avg_people_per_household,
    chp_areas_zero_households,
    pct_chp_areas_zero_households,
    total_people,
    people_missing_household_link,
    pct_people_missing_household_link
FROM {{ ref('fct_supervision_county') }}
