
  
    

  create  table "echis"."afyabi"."int_population_household_metrics__dbt_tmp"
  
  
    as
  
  (
    -- depends_on: "echis"."afyabi"."stg_patient"
-- depends_on: "echis"."afyabi"."stg_household"
-- depends_on: "echis"."afyabi"."stg_sha_registration"
-- depends_on: "echis"."afyabi"."stg_community_event"
-- depends_on: "echis"."afyabi"."stg_data_record"
-- depends_on: "echis"."afyabi"."stg_death_report"
-- depends_on: "echis"."afyabi"."dim_period"


/*
  int_population_household_metrics: Hybrid approach.

  Driver: pop_static (from stg_patient directly) — NOT mv_location_hierarchy,
  which may be empty if contact_type values don't match.

  - pop_static: no period dependency, computed once (driver)
  - pop_age_dependent: CROSS JOIN with periods (age depends on end_date)
  - ce/ps: UNION ALL with literal dates for partition pruning
  - hh/sha: cumulative, no period filter
*/




  


WITH periods AS (
    SELECT period_id, start_date, end_date
    FROM "echis"."afyabi"."dim_period"
),

deceased_patients AS (
    SELECT DISTINCT patient_id
    FROM "echis"."afyabi"."stg_death_report"
    WHERE patient_id IS NOT NULL
),

pop_static AS (
    SELECT
        pt.chp_area_id,
        COUNT(pt.patient_id)                               AS population,
        COUNT(pt.patient_id) FILTER (WHERE pt.sex = 'female') AS population_female,
        COUNT(pt.patient_id) FILTER (WHERE pt.sex = 'male')   AS population_male
    FROM "echis"."afyabi"."stg_patient" pt
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
    FROM "echis"."afyabi"."stg_patient" pt
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
    FROM "echis"."afyabi"."stg_household"
    WHERE chp_area_id IS NOT NULL
    GROUP BY chp_area_id
),

sha AS (
    SELECT
        pt.chp_area_id,
        COUNT(DISTINCT pt.household_id)                                               AS households_assessed_sha,
        COUNT(DISTINCT pt.household_id) FILTER (WHERE r.has_sha_registration IS TRUE) AS households_registered_on_sha
    FROM "echis"."afyabi"."stg_sha_registration" r
    INNER JOIN "echis"."afyabi"."stg_patient" pt ON pt.patient_id = r.member_uuid
    WHERE pt.chp_area_id IS NOT NULL
    GROUP BY pt.chp_area_id
),

-- Community events: partition-friendly with literal dates
ce AS (
    
    
    SELECT
        chp_area_id,
        1::bigint AS period_id,
        COUNT(uuid) FILTER (WHERE event_types ILIKE '%monthly_cu_meetings%') AS monthly_cu_meetings,
        COUNT(uuid) FILTER (WHERE event_types != 'monthly_cu_meetings')       AS other_community_events
    FROM "echis"."afyabi"."stg_community_event"
    WHERE reported >= '2026-03-01'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        2::bigint AS period_id,
        COUNT(uuid) FILTER (WHERE event_types ILIKE '%monthly_cu_meetings%') AS monthly_cu_meetings,
        COUNT(uuid) FILTER (WHERE event_types != 'monthly_cu_meetings')       AS other_community_events
    FROM "echis"."afyabi"."stg_community_event"
    WHERE reported >= '2026-02-28'::date
      AND reported <  '2026-03-01'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        3::bigint AS period_id,
        COUNT(uuid) FILTER (WHERE event_types ILIKE '%monthly_cu_meetings%') AS monthly_cu_meetings,
        COUNT(uuid) FILTER (WHERE event_types != 'monthly_cu_meetings')       AS other_community_events
    FROM "echis"."afyabi"."stg_community_event"
    WHERE reported >= '2026-02-22'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        4::bigint AS period_id,
        COUNT(uuid) FILTER (WHERE event_types ILIKE '%monthly_cu_meetings%') AS monthly_cu_meetings,
        COUNT(uuid) FILTER (WHERE event_types != 'monthly_cu_meetings')       AS other_community_events
    FROM "echis"."afyabi"."stg_community_event"
    WHERE reported >= '2026-02-01'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        5::bigint AS period_id,
        COUNT(uuid) FILTER (WHERE event_types ILIKE '%monthly_cu_meetings%') AS monthly_cu_meetings,
        COUNT(uuid) FILTER (WHERE event_types != 'monthly_cu_meetings')       AS other_community_events
    FROM "echis"."afyabi"."stg_community_event"
    WHERE reported >= '2025-12-01'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        6::bigint AS period_id,
        COUNT(uuid) FILTER (WHERE event_types ILIKE '%monthly_cu_meetings%') AS monthly_cu_meetings,
        COUNT(uuid) FILTER (WHERE event_types != 'monthly_cu_meetings')       AS other_community_events
    FROM "echis"."afyabi"."stg_community_event"
    WHERE reported >= '2025-09-01'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        7::bigint AS period_id,
        COUNT(uuid) FILTER (WHERE event_types ILIKE '%monthly_cu_meetings%') AS monthly_cu_meetings,
        COUNT(uuid) FILTER (WHERE event_types != 'monthly_cu_meetings')       AS other_community_events
    FROM "echis"."afyabi"."stg_community_event"
    WHERE reported >= '2025-03-01'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        8::bigint AS period_id,
        COUNT(uuid) FILTER (WHERE event_types ILIKE '%monthly_cu_meetings%') AS monthly_cu_meetings,
        COUNT(uuid) FILTER (WHERE event_types != 'monthly_cu_meetings')       AS other_community_events
    FROM "echis"."afyabi"."stg_community_event"
    WHERE reported >= '2026-02-23'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        9::bigint AS period_id,
        COUNT(uuid) FILTER (WHERE event_types ILIKE '%monthly_cu_meetings%') AS monthly_cu_meetings,
        COUNT(uuid) FILTER (WHERE event_types != 'monthly_cu_meetings')       AS other_community_events
    FROM "echis"."afyabi"."stg_community_event"
    WHERE reported >= '2026-02-16'::date
      AND reported <  '2026-02-23'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        10::bigint AS period_id,
        COUNT(uuid) FILTER (WHERE event_types ILIKE '%monthly_cu_meetings%') AS monthly_cu_meetings,
        COUNT(uuid) FILTER (WHERE event_types != 'monthly_cu_meetings')       AS other_community_events
    FROM "echis"."afyabi"."stg_community_event"
    WHERE reported >= '2026-03-01'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        11::bigint AS period_id,
        COUNT(uuid) FILTER (WHERE event_types ILIKE '%monthly_cu_meetings%') AS monthly_cu_meetings,
        COUNT(uuid) FILTER (WHERE event_types != 'monthly_cu_meetings')       AS other_community_events
    FROM "echis"."afyabi"."stg_community_event"
    WHERE reported >= '2026-02-01'::date
      AND reported <  '2026-03-01'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        12::bigint AS period_id,
        COUNT(uuid) FILTER (WHERE event_types ILIKE '%monthly_cu_meetings%') AS monthly_cu_meetings,
        COUNT(uuid) FILTER (WHERE event_types != 'monthly_cu_meetings')       AS other_community_events
    FROM "echis"."afyabi"."stg_community_event"
    WHERE reported >= '2026-01-01'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        13::bigint AS period_id,
        COUNT(uuid) FILTER (WHERE event_types ILIKE '%monthly_cu_meetings%') AS monthly_cu_meetings,
        COUNT(uuid) FILTER (WHERE event_types != 'monthly_cu_meetings')       AS other_community_events
    FROM "echis"."afyabi"."stg_community_event"
    WHERE reported >= '2025-10-01'::date
      AND reported <  '2026-01-01'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        14::bigint AS period_id,
        COUNT(uuid) FILTER (WHERE event_types ILIKE '%monthly_cu_meetings%') AS monthly_cu_meetings,
        COUNT(uuid) FILTER (WHERE event_types != 'monthly_cu_meetings')       AS other_community_events
    FROM "echis"."afyabi"."stg_community_event"
    WHERE reported >= '2026-01-01'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        15::bigint AS period_id,
        COUNT(uuid) FILTER (WHERE event_types ILIKE '%monthly_cu_meetings%') AS monthly_cu_meetings,
        COUNT(uuid) FILTER (WHERE event_types != 'monthly_cu_meetings')       AS other_community_events
    FROM "echis"."afyabi"."stg_community_event"
    WHERE reported >= '2020-01-01'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
),

-- People served: partition-friendly with literal dates
ps AS (
    
    
    SELECT
        chp_area_id,
        1::bigint AS period_id,
        COUNT(DISTINCT patient_id)      AS people_served
    FROM "echis"."afyabi"."stg_data_record"
    WHERE reported >= '2026-03-01'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        2::bigint AS period_id,
        COUNT(DISTINCT patient_id)      AS people_served
    FROM "echis"."afyabi"."stg_data_record"
    WHERE reported >= '2026-02-28'::date
      AND reported <  '2026-03-01'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        3::bigint AS period_id,
        COUNT(DISTINCT patient_id)      AS people_served
    FROM "echis"."afyabi"."stg_data_record"
    WHERE reported >= '2026-02-22'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        4::bigint AS period_id,
        COUNT(DISTINCT patient_id)      AS people_served
    FROM "echis"."afyabi"."stg_data_record"
    WHERE reported >= '2026-02-01'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        5::bigint AS period_id,
        COUNT(DISTINCT patient_id)      AS people_served
    FROM "echis"."afyabi"."stg_data_record"
    WHERE reported >= '2025-12-01'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        6::bigint AS period_id,
        COUNT(DISTINCT patient_id)      AS people_served
    FROM "echis"."afyabi"."stg_data_record"
    WHERE reported >= '2025-09-01'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        7::bigint AS period_id,
        COUNT(DISTINCT patient_id)      AS people_served
    FROM "echis"."afyabi"."stg_data_record"
    WHERE reported >= '2025-03-01'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        8::bigint AS period_id,
        COUNT(DISTINCT patient_id)      AS people_served
    FROM "echis"."afyabi"."stg_data_record"
    WHERE reported >= '2026-02-23'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        9::bigint AS period_id,
        COUNT(DISTINCT patient_id)      AS people_served
    FROM "echis"."afyabi"."stg_data_record"
    WHERE reported >= '2026-02-16'::date
      AND reported <  '2026-02-23'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        10::bigint AS period_id,
        COUNT(DISTINCT patient_id)      AS people_served
    FROM "echis"."afyabi"."stg_data_record"
    WHERE reported >= '2026-03-01'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        11::bigint AS period_id,
        COUNT(DISTINCT patient_id)      AS people_served
    FROM "echis"."afyabi"."stg_data_record"
    WHERE reported >= '2026-02-01'::date
      AND reported <  '2026-03-01'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        12::bigint AS period_id,
        COUNT(DISTINCT patient_id)      AS people_served
    FROM "echis"."afyabi"."stg_data_record"
    WHERE reported >= '2026-01-01'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        13::bigint AS period_id,
        COUNT(DISTINCT patient_id)      AS people_served
    FROM "echis"."afyabi"."stg_data_record"
    WHERE reported >= '2025-10-01'::date
      AND reported <  '2026-01-01'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        14::bigint AS period_id,
        COUNT(DISTINCT patient_id)      AS people_served
    FROM "echis"."afyabi"."stg_data_record"
    WHERE reported >= '2026-01-01'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
    UNION ALL
    SELECT
        chp_area_id,
        15::bigint AS period_id,
        COUNT(DISTINCT patient_id)      AS people_served
    FROM "echis"."afyabi"."stg_data_record"
    WHERE reported >= '2020-01-01'::date
      AND reported <  '2026-03-02'::date
      AND chp_area_id IS NOT NULL
    GROUP BY chp_area_id
    
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
  );
  