-- depends_on: {{ ref('stg_patient') }}
-- depends_on: {{ ref('stg_household') }}
-- depends_on: {{ ref('stg_sha_registration') }}
-- depends_on: {{ ref('stg_community_event') }}
-- depends_on: {{ ref('stg_data_record') }}
-- depends_on: {{ ref('stg_death_report') }}
-- depends_on: {{ ref('dim_period') }}
{{
  config(
    materialized = 'table',
    pre_hook = "SET LOCAL work_mem = '256MB'",
    indexes = [
      {'columns': ['chp_area_id', 'period_id']},
      {'columns': ['period_id']}
    ],
    tags = ['cadence_hourly', 'intermediate']
  )
}}

WITH periods AS (
    SELECT period_id, start_date, end_date,
           (end_date + INTERVAL '1 day')::date AS end_excl
    FROM {{ ref('dim_period') }}
    WHERE refresh_tier = 'hot'
),

deceased_patients AS (
    SELECT DISTINCT patient_id
    FROM {{ ref('stg_death_report') }}
    WHERE patient_id IS NOT NULL
),

pop_static AS (
    SELECT
        pt.chp_area_id,
        COUNT(pt.patient_id)                               AS population,
        COUNT(pt.patient_id) FILTER (WHERE pt.sex = 'female') AS population_female,
        COUNT(pt.patient_id) FILTER (WHERE pt.sex = 'male')   AS population_male
    FROM {{ ref('stg_patient') }} pt
    LEFT JOIN deceased_patients d ON d.patient_id = pt.patient_id
    WHERE pt.is_active IS TRUE
      AND pt.chp_area_id IS NOT NULL
      AND d.patient_id IS NULL
    GROUP BY pt.chp_area_id
),

pop_age_dependent AS (
    SELECT
        pt.chp_area_id,
        p.period_id,
        COUNT(pt.patient_id) FILTER (
            WHERE pt.date_of_birth > p.end_date - INTERVAL '5 years'
        ) AS under_5_population,
        COUNT(pt.patient_id) FILTER (
            WHERE pt.sex = 'female' AND pt.date_of_birth > p.end_date - INTERVAL '5 years'
        ) AS population_under_5_female,
        COUNT(pt.patient_id) FILTER (
            WHERE pt.sex = 'male' AND pt.date_of_birth > p.end_date - INTERVAL '5 years'
        ) AS population_under_5_male,
        COUNT(pt.patient_id) FILTER (
            WHERE age(p.end_date, pt.date_of_birth) >= INTERVAL '11 months'
              AND age(p.end_date, pt.date_of_birth) <  INTERVAL '1 year'
        ) AS children_turning_one,
        COUNT(pt.patient_id) FILTER (
            WHERE pt.sex = 'female'
              AND age(p.end_date, pt.date_of_birth) >= INTERVAL '11 months'
              AND age(p.end_date, pt.date_of_birth) <  INTERVAL '1 year'
        ) AS female_turning_one,
        COUNT(pt.patient_id) FILTER (
            WHERE pt.sex = 'male'
              AND age(p.end_date, pt.date_of_birth) >= INTERVAL '11 months'
              AND age(p.end_date, pt.date_of_birth) <  INTERVAL '1 year'
        ) AS male_turning_one
    FROM {{ ref('stg_patient') }} pt
    CROSS JOIN periods p
    LEFT JOIN deceased_patients d ON d.patient_id = pt.patient_id
    WHERE pt.is_active IS TRUE
      AND pt.chp_area_id IS NOT NULL
      AND pt.date_of_birth IS NOT NULL
      AND d.patient_id IS NULL
    GROUP BY pt.chp_area_id, p.period_id
),

hh AS (
    SELECT chp_area_id, COUNT(household_id) AS households_registered
    FROM {{ ref('stg_household') }}
    WHERE chp_area_id IS NOT NULL
    GROUP BY chp_area_id
),

sha AS (
    SELECT
        pt.chp_area_id,
        COUNT(DISTINCT pt.household_id)                                               AS households_assessed_sha,
        COUNT(DISTINCT pt.household_id) FILTER (WHERE r.has_sha_registration IS TRUE) AS households_registered_on_sha
    FROM {{ ref('stg_sha_registration') }} r
    INNER JOIN {{ ref('stg_patient') }} pt ON pt.patient_id = r.member_uuid
    WHERE pt.chp_area_id IS NOT NULL
    GROUP BY pt.chp_area_id
),

ce AS (
    SELECT
        s.chp_area_id,
        p.period_id,
        COUNT(s.uuid) FILTER (WHERE s.event_types ILIKE '%monthly_cu_meetings%') AS monthly_cu_meetings,
        COUNT(s.uuid) FILTER (WHERE s.event_types != 'monthly_cu_meetings')       AS other_community_events
    FROM {{ ref('stg_community_event') }} s
    INNER JOIN periods p
        ON s.reported >= p.start_date
       AND s.reported < p.end_excl
    WHERE s.chp_area_id IS NOT NULL
    GROUP BY s.chp_area_id, p.period_id
),

ps AS (
    SELECT
        s.chp_area_id,
        p.period_id,
        COUNT(DISTINCT s.patient_id) AS people_served
    FROM {{ ref('stg_data_record') }} s
    INNER JOIN periods p
        ON s.reported >= p.start_date
       AND s.reported < p.end_excl
    WHERE s.chp_area_id IS NOT NULL
    GROUP BY s.chp_area_id, p.period_id
)

SELECT
    ps2.chp_area_id,
    pr.period_id,
    ps2.population,
    ps2.population_female,
    ps2.population_male,
    COALESCE(pad.under_5_population, 0)          AS under_5_population,
    COALESCE(pad.population_under_5_female, 0)   AS population_under_5_female,
    COALESCE(pad.population_under_5_male, 0)     AS population_under_5_male,
    COALESCE(pad.children_turning_one, 0)        AS children_turning_one,
    COALESCE(pad.female_turning_one, 0)          AS female_turning_one,
    COALESCE(pad.male_turning_one, 0)            AS male_turning_one,
    COALESCE(hh.households_registered, 0)        AS households_registered,
    COALESCE(sha.households_assessed_sha, 0)     AS households_assessed_sha,
    COALESCE(sha.households_registered_on_sha, 0) AS households_registered_on_sha,
    COALESCE(sha.households_registered_on_sha, 0) AS households_with_sha,
    COALESCE(ce.monthly_cu_meetings, 0)          AS monthly_cu_meetings,
    COALESCE(ce.other_community_events, 0)       AS other_community_events,
    COALESCE(ps.people_served, 0)                AS people_served,
    NOW() AS last_updated
FROM pop_static ps2
CROSS JOIN periods pr
LEFT JOIN pop_age_dependent pad ON pad.chp_area_id = ps2.chp_area_id AND pad.period_id = pr.period_id
LEFT JOIN hh ON hh.chp_area_id = ps2.chp_area_id
LEFT JOIN sha ON sha.chp_area_id = ps2.chp_area_id
LEFT JOIN ce ON ce.chp_area_id = ps2.chp_area_id AND ce.period_id = pr.period_id
LEFT JOIN ps ON ps.chp_area_id = ps2.chp_area_id AND ps.period_id = pr.period_id
