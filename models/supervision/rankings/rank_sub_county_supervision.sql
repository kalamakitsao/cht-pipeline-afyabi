{{ config(materialized='table', tags=['supervision','ranking','cadence_hourly']) }}

WITH scored AS (
    SELECT
        *,
        ROUND(
            COALESCE(pct_households_served_90d, 0) * 0.40 +
            COALESCE(pct_active_chp_areas_30d, 0) * 0.20 +
            GREATEST(0, 100 - COALESCE(pct_households_zero_members, 0) * 10) * 0.15 +
            LEAST(100, COALESCE(avg_people_per_household, 0) * 20) * 0.10 +
            COALESCE(pct_households_registered_on_sha, 0) * 0.10 +
            GREATEST(0, 100 - COALESCE(pct_people_missing_household_link, 0) * 20) * 0.05
        , 1) AS supervision_score
    FROM {{ ref('fct_supervision_sub_county') }}
)
SELECT
    DENSE_RANK() OVER (ORDER BY supervision_score DESC) AS rank_no,
    *
FROM scored
