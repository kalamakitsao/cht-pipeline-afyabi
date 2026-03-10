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
  int_population_household_metrics: Assembler model.
  
  Driver: mv_location_hierarchy × dim_period — guarantees every CHP area
  gets a row for every period, even if it has no patients (but has households).
  
  Previous bug: using pop_static as driver dropped CHP areas with households
  but no active patients, causing fact_aggregate to undercount.
*/

WITH all_chp AS (
    SELECT chp_area_id
    FROM {{ ref('mv_location_hierarchy') }}
),

periods AS (
    SELECT period_id FROM {{ ref('dim_period') }}
),

ce AS (
    SELECT * FROM {{ ref('int_community_events_hot') }}
    UNION ALL
    SELECT * FROM {{ ref('int_community_events_warm') }}
    UNION ALL
    SELECT * FROM {{ ref('int_community_events_cold') }}
),

ps AS (
    SELECT * FROM {{ ref('int_people_served_hot') }}
    UNION ALL
    SELECT * FROM {{ ref('int_people_served_warm') }}
    UNION ALL
    SELECT * FROM {{ ref('int_people_served_cold') }}
)

SELECT
    a.chp_area_id,
    pr.period_id,
    COALESCE(pop.population, 0)                  AS population,
    COALESCE(pop.population_female, 0)           AS population_female,
    COALESCE(pop.population_male, 0)             AS population_male,
    COALESCE(age_dep.under_5_population, 0)      AS under_5_population,
    COALESCE(age_dep.population_under_5_female, 0) AS population_under_5_female,
    COALESCE(age_dep.population_under_5_male, 0) AS population_under_5_male,
    COALESCE(age_dep.children_turning_one, 0)    AS children_turning_one,
    COALESCE(age_dep.female_turning_one, 0)      AS female_turning_one,
    COALESCE(age_dep.male_turning_one, 0)        AS male_turning_one,
    COALESCE(hh.households_registered, 0)        AS households_registered,
    COALESCE(sha.households_assessed_sha, 0)     AS households_assessed_sha,
    COALESCE(sha.households_registered_on_sha, 0) AS households_registered_on_sha,
    COALESCE(sha.households_registered_on_sha, 0) AS households_with_sha,
    COALESCE(ce.monthly_cu_meetings, 0)          AS monthly_cu_meetings,
    COALESCE(ce.other_community_events, 0)       AS other_community_events,
    COALESCE(ps.people_served, 0)                AS people_served,
    NOW() AS last_updated
FROM all_chp a
CROSS JOIN periods pr
LEFT JOIN {{ ref('int_population_static') }} pop
    ON pop.chp_area_id = a.chp_area_id
LEFT JOIN {{ ref('int_population_age_dependent') }} age_dep
    ON age_dep.chp_area_id = a.chp_area_id AND age_dep.period_id = pr.period_id
LEFT JOIN {{ ref('int_households') }} hh
    ON hh.chp_area_id = a.chp_area_id
LEFT JOIN {{ ref('int_sha') }} sha
    ON sha.chp_area_id = a.chp_area_id
LEFT JOIN ce
    ON ce.chp_area_id = a.chp_area_id AND ce.period_id = pr.period_id
LEFT JOIN ps
    ON ps.chp_area_id = a.chp_area_id AND ps.period_id = pr.period_id