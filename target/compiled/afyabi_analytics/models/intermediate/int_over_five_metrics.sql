-- depends_on: "echis"."afyabi"."stg_over_five_assessment"
-- depends_on: "echis"."afyabi"."dim_period"


/* int_over_five_metrics: 30M rows, 33 metrics. Partition-friendly. */




  





SELECT
    chp_area_id,
    1::bigint AS period_id,

    COUNT(uuid)                                                                                 AS over_5_assessments,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE)                         AS over_5_referred,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'female') AS over_5_referred_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'male')   AS over_5_referred_male,

    -- Diabetes
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE)                     AS screened_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female') AS screened_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')   AS screened_diabetes_male,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE)                                    AS screenings_diabetes,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female')         AS screenings_diabetes_female,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')           AS screenings_diabetes_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE)                      AS referred_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'female') AS referred_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'male')   AS referred_diabetes_male,

    -- Hypertension
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE)                 AS screened_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female') AS screened_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')   AS screened_hypertension_male,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE)                                AS screenings_hypertension,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female')     AS screenings_hypertension_female,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')       AS screenings_hypertension_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE)                  AS referred_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'female') AS referred_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'male')   AS referred_hypertension_male,

    -- Mental health
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE)                AS screened_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female') AS screened_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')   AS screened_mental_health_male,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE)                               AS screenings_mental_health,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female')    AS screenings_mental_health_female,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')      AS screenings_mental_health_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE)                 AS referred_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'female') AS referred_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'male')   AS referred_mental_health_male,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_over_five_assessment"
WHERE reported >= '2026-03-01'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    2::bigint AS period_id,

    COUNT(uuid)                                                                                 AS over_5_assessments,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE)                         AS over_5_referred,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'female') AS over_5_referred_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'male')   AS over_5_referred_male,

    -- Diabetes
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE)                     AS screened_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female') AS screened_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')   AS screened_diabetes_male,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE)                                    AS screenings_diabetes,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female')         AS screenings_diabetes_female,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')           AS screenings_diabetes_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE)                      AS referred_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'female') AS referred_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'male')   AS referred_diabetes_male,

    -- Hypertension
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE)                 AS screened_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female') AS screened_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')   AS screened_hypertension_male,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE)                                AS screenings_hypertension,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female')     AS screenings_hypertension_female,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')       AS screenings_hypertension_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE)                  AS referred_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'female') AS referred_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'male')   AS referred_hypertension_male,

    -- Mental health
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE)                AS screened_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female') AS screened_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')   AS screened_mental_health_male,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE)                               AS screenings_mental_health,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female')    AS screenings_mental_health_female,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')      AS screenings_mental_health_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE)                 AS referred_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'female') AS referred_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'male')   AS referred_mental_health_male,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_over_five_assessment"
WHERE reported >= '2026-02-28'::date
  AND reported <  '2026-03-01'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    3::bigint AS period_id,

    COUNT(uuid)                                                                                 AS over_5_assessments,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE)                         AS over_5_referred,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'female') AS over_5_referred_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'male')   AS over_5_referred_male,

    -- Diabetes
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE)                     AS screened_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female') AS screened_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')   AS screened_diabetes_male,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE)                                    AS screenings_diabetes,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female')         AS screenings_diabetes_female,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')           AS screenings_diabetes_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE)                      AS referred_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'female') AS referred_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'male')   AS referred_diabetes_male,

    -- Hypertension
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE)                 AS screened_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female') AS screened_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')   AS screened_hypertension_male,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE)                                AS screenings_hypertension,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female')     AS screenings_hypertension_female,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')       AS screenings_hypertension_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE)                  AS referred_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'female') AS referred_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'male')   AS referred_hypertension_male,

    -- Mental health
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE)                AS screened_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female') AS screened_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')   AS screened_mental_health_male,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE)                               AS screenings_mental_health,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female')    AS screenings_mental_health_female,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')      AS screenings_mental_health_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE)                 AS referred_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'female') AS referred_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'male')   AS referred_mental_health_male,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_over_five_assessment"
WHERE reported >= '2026-02-22'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    4::bigint AS period_id,

    COUNT(uuid)                                                                                 AS over_5_assessments,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE)                         AS over_5_referred,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'female') AS over_5_referred_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'male')   AS over_5_referred_male,

    -- Diabetes
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE)                     AS screened_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female') AS screened_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')   AS screened_diabetes_male,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE)                                    AS screenings_diabetes,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female')         AS screenings_diabetes_female,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')           AS screenings_diabetes_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE)                      AS referred_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'female') AS referred_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'male')   AS referred_diabetes_male,

    -- Hypertension
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE)                 AS screened_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female') AS screened_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')   AS screened_hypertension_male,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE)                                AS screenings_hypertension,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female')     AS screenings_hypertension_female,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')       AS screenings_hypertension_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE)                  AS referred_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'female') AS referred_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'male')   AS referred_hypertension_male,

    -- Mental health
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE)                AS screened_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female') AS screened_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')   AS screened_mental_health_male,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE)                               AS screenings_mental_health,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female')    AS screenings_mental_health_female,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')      AS screenings_mental_health_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE)                 AS referred_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'female') AS referred_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'male')   AS referred_mental_health_male,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_over_five_assessment"
WHERE reported >= '2026-02-01'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    5::bigint AS period_id,

    COUNT(uuid)                                                                                 AS over_5_assessments,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE)                         AS over_5_referred,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'female') AS over_5_referred_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'male')   AS over_5_referred_male,

    -- Diabetes
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE)                     AS screened_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female') AS screened_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')   AS screened_diabetes_male,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE)                                    AS screenings_diabetes,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female')         AS screenings_diabetes_female,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')           AS screenings_diabetes_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE)                      AS referred_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'female') AS referred_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'male')   AS referred_diabetes_male,

    -- Hypertension
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE)                 AS screened_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female') AS screened_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')   AS screened_hypertension_male,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE)                                AS screenings_hypertension,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female')     AS screenings_hypertension_female,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')       AS screenings_hypertension_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE)                  AS referred_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'female') AS referred_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'male')   AS referred_hypertension_male,

    -- Mental health
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE)                AS screened_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female') AS screened_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')   AS screened_mental_health_male,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE)                               AS screenings_mental_health,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female')    AS screenings_mental_health_female,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')      AS screenings_mental_health_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE)                 AS referred_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'female') AS referred_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'male')   AS referred_mental_health_male,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_over_five_assessment"
WHERE reported >= '2025-12-01'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    6::bigint AS period_id,

    COUNT(uuid)                                                                                 AS over_5_assessments,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE)                         AS over_5_referred,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'female') AS over_5_referred_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'male')   AS over_5_referred_male,

    -- Diabetes
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE)                     AS screened_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female') AS screened_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')   AS screened_diabetes_male,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE)                                    AS screenings_diabetes,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female')         AS screenings_diabetes_female,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')           AS screenings_diabetes_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE)                      AS referred_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'female') AS referred_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'male')   AS referred_diabetes_male,

    -- Hypertension
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE)                 AS screened_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female') AS screened_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')   AS screened_hypertension_male,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE)                                AS screenings_hypertension,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female')     AS screenings_hypertension_female,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')       AS screenings_hypertension_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE)                  AS referred_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'female') AS referred_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'male')   AS referred_hypertension_male,

    -- Mental health
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE)                AS screened_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female') AS screened_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')   AS screened_mental_health_male,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE)                               AS screenings_mental_health,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female')    AS screenings_mental_health_female,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')      AS screenings_mental_health_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE)                 AS referred_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'female') AS referred_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'male')   AS referred_mental_health_male,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_over_five_assessment"
WHERE reported >= '2025-09-01'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    7::bigint AS period_id,

    COUNT(uuid)                                                                                 AS over_5_assessments,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE)                         AS over_5_referred,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'female') AS over_5_referred_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'male')   AS over_5_referred_male,

    -- Diabetes
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE)                     AS screened_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female') AS screened_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')   AS screened_diabetes_male,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE)                                    AS screenings_diabetes,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female')         AS screenings_diabetes_female,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')           AS screenings_diabetes_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE)                      AS referred_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'female') AS referred_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'male')   AS referred_diabetes_male,

    -- Hypertension
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE)                 AS screened_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female') AS screened_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')   AS screened_hypertension_male,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE)                                AS screenings_hypertension,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female')     AS screenings_hypertension_female,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')       AS screenings_hypertension_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE)                  AS referred_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'female') AS referred_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'male')   AS referred_hypertension_male,

    -- Mental health
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE)                AS screened_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female') AS screened_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')   AS screened_mental_health_male,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE)                               AS screenings_mental_health,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female')    AS screenings_mental_health_female,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')      AS screenings_mental_health_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE)                 AS referred_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'female') AS referred_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'male')   AS referred_mental_health_male,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_over_five_assessment"
WHERE reported >= '2025-03-01'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    8::bigint AS period_id,

    COUNT(uuid)                                                                                 AS over_5_assessments,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE)                         AS over_5_referred,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'female') AS over_5_referred_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'male')   AS over_5_referred_male,

    -- Diabetes
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE)                     AS screened_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female') AS screened_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')   AS screened_diabetes_male,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE)                                    AS screenings_diabetes,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female')         AS screenings_diabetes_female,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')           AS screenings_diabetes_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE)                      AS referred_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'female') AS referred_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'male')   AS referred_diabetes_male,

    -- Hypertension
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE)                 AS screened_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female') AS screened_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')   AS screened_hypertension_male,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE)                                AS screenings_hypertension,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female')     AS screenings_hypertension_female,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')       AS screenings_hypertension_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE)                  AS referred_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'female') AS referred_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'male')   AS referred_hypertension_male,

    -- Mental health
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE)                AS screened_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female') AS screened_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')   AS screened_mental_health_male,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE)                               AS screenings_mental_health,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female')    AS screenings_mental_health_female,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')      AS screenings_mental_health_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE)                 AS referred_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'female') AS referred_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'male')   AS referred_mental_health_male,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_over_five_assessment"
WHERE reported >= '2026-02-23'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    9::bigint AS period_id,

    COUNT(uuid)                                                                                 AS over_5_assessments,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE)                         AS over_5_referred,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'female') AS over_5_referred_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'male')   AS over_5_referred_male,

    -- Diabetes
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE)                     AS screened_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female') AS screened_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')   AS screened_diabetes_male,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE)                                    AS screenings_diabetes,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female')         AS screenings_diabetes_female,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')           AS screenings_diabetes_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE)                      AS referred_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'female') AS referred_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'male')   AS referred_diabetes_male,

    -- Hypertension
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE)                 AS screened_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female') AS screened_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')   AS screened_hypertension_male,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE)                                AS screenings_hypertension,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female')     AS screenings_hypertension_female,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')       AS screenings_hypertension_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE)                  AS referred_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'female') AS referred_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'male')   AS referred_hypertension_male,

    -- Mental health
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE)                AS screened_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female') AS screened_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')   AS screened_mental_health_male,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE)                               AS screenings_mental_health,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female')    AS screenings_mental_health_female,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')      AS screenings_mental_health_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE)                 AS referred_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'female') AS referred_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'male')   AS referred_mental_health_male,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_over_five_assessment"
WHERE reported >= '2026-02-16'::date
  AND reported <  '2026-02-23'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    10::bigint AS period_id,

    COUNT(uuid)                                                                                 AS over_5_assessments,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE)                         AS over_5_referred,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'female') AS over_5_referred_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'male')   AS over_5_referred_male,

    -- Diabetes
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE)                     AS screened_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female') AS screened_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')   AS screened_diabetes_male,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE)                                    AS screenings_diabetes,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female')         AS screenings_diabetes_female,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')           AS screenings_diabetes_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE)                      AS referred_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'female') AS referred_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'male')   AS referred_diabetes_male,

    -- Hypertension
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE)                 AS screened_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female') AS screened_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')   AS screened_hypertension_male,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE)                                AS screenings_hypertension,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female')     AS screenings_hypertension_female,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')       AS screenings_hypertension_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE)                  AS referred_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'female') AS referred_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'male')   AS referred_hypertension_male,

    -- Mental health
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE)                AS screened_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female') AS screened_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')   AS screened_mental_health_male,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE)                               AS screenings_mental_health,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female')    AS screenings_mental_health_female,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')      AS screenings_mental_health_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE)                 AS referred_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'female') AS referred_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'male')   AS referred_mental_health_male,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_over_five_assessment"
WHERE reported >= '2026-03-01'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    11::bigint AS period_id,

    COUNT(uuid)                                                                                 AS over_5_assessments,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE)                         AS over_5_referred,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'female') AS over_5_referred_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'male')   AS over_5_referred_male,

    -- Diabetes
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE)                     AS screened_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female') AS screened_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')   AS screened_diabetes_male,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE)                                    AS screenings_diabetes,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female')         AS screenings_diabetes_female,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')           AS screenings_diabetes_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE)                      AS referred_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'female') AS referred_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'male')   AS referred_diabetes_male,

    -- Hypertension
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE)                 AS screened_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female') AS screened_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')   AS screened_hypertension_male,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE)                                AS screenings_hypertension,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female')     AS screenings_hypertension_female,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')       AS screenings_hypertension_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE)                  AS referred_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'female') AS referred_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'male')   AS referred_hypertension_male,

    -- Mental health
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE)                AS screened_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female') AS screened_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')   AS screened_mental_health_male,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE)                               AS screenings_mental_health,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female')    AS screenings_mental_health_female,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')      AS screenings_mental_health_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE)                 AS referred_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'female') AS referred_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'male')   AS referred_mental_health_male,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_over_five_assessment"
WHERE reported >= '2026-02-01'::date
  AND reported <  '2026-03-01'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    12::bigint AS period_id,

    COUNT(uuid)                                                                                 AS over_5_assessments,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE)                         AS over_5_referred,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'female') AS over_5_referred_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'male')   AS over_5_referred_male,

    -- Diabetes
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE)                     AS screened_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female') AS screened_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')   AS screened_diabetes_male,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE)                                    AS screenings_diabetes,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female')         AS screenings_diabetes_female,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')           AS screenings_diabetes_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE)                      AS referred_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'female') AS referred_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'male')   AS referred_diabetes_male,

    -- Hypertension
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE)                 AS screened_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female') AS screened_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')   AS screened_hypertension_male,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE)                                AS screenings_hypertension,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female')     AS screenings_hypertension_female,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')       AS screenings_hypertension_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE)                  AS referred_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'female') AS referred_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'male')   AS referred_hypertension_male,

    -- Mental health
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE)                AS screened_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female') AS screened_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')   AS screened_mental_health_male,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE)                               AS screenings_mental_health,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female')    AS screenings_mental_health_female,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')      AS screenings_mental_health_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE)                 AS referred_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'female') AS referred_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'male')   AS referred_mental_health_male,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_over_five_assessment"
WHERE reported >= '2026-01-01'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    13::bigint AS period_id,

    COUNT(uuid)                                                                                 AS over_5_assessments,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE)                         AS over_5_referred,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'female') AS over_5_referred_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'male')   AS over_5_referred_male,

    -- Diabetes
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE)                     AS screened_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female') AS screened_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')   AS screened_diabetes_male,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE)                                    AS screenings_diabetes,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female')         AS screenings_diabetes_female,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')           AS screenings_diabetes_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE)                      AS referred_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'female') AS referred_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'male')   AS referred_diabetes_male,

    -- Hypertension
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE)                 AS screened_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female') AS screened_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')   AS screened_hypertension_male,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE)                                AS screenings_hypertension,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female')     AS screenings_hypertension_female,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')       AS screenings_hypertension_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE)                  AS referred_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'female') AS referred_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'male')   AS referred_hypertension_male,

    -- Mental health
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE)                AS screened_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female') AS screened_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')   AS screened_mental_health_male,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE)                               AS screenings_mental_health,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female')    AS screenings_mental_health_female,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')      AS screenings_mental_health_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE)                 AS referred_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'female') AS referred_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'male')   AS referred_mental_health_male,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_over_five_assessment"
WHERE reported >= '2025-10-01'::date
  AND reported <  '2026-01-01'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    14::bigint AS period_id,

    COUNT(uuid)                                                                                 AS over_5_assessments,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE)                         AS over_5_referred,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'female') AS over_5_referred_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'male')   AS over_5_referred_male,

    -- Diabetes
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE)                     AS screened_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female') AS screened_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')   AS screened_diabetes_male,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE)                                    AS screenings_diabetes,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female')         AS screenings_diabetes_female,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')           AS screenings_diabetes_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE)                      AS referred_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'female') AS referred_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'male')   AS referred_diabetes_male,

    -- Hypertension
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE)                 AS screened_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female') AS screened_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')   AS screened_hypertension_male,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE)                                AS screenings_hypertension,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female')     AS screenings_hypertension_female,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')       AS screenings_hypertension_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE)                  AS referred_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'female') AS referred_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'male')   AS referred_hypertension_male,

    -- Mental health
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE)                AS screened_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female') AS screened_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')   AS screened_mental_health_male,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE)                               AS screenings_mental_health,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female')    AS screenings_mental_health_female,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')      AS screenings_mental_health_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE)                 AS referred_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'female') AS referred_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'male')   AS referred_mental_health_male,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_over_five_assessment"
WHERE reported >= '2026-01-01'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    15::bigint AS period_id,

    COUNT(uuid)                                                                                 AS over_5_assessments,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE)                         AS over_5_referred,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'female') AS over_5_referred_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE has_been_referred IS TRUE AND patient_sex = 'male')   AS over_5_referred_male,

    -- Diabetes
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE)                     AS screened_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female') AS screened_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')   AS screened_diabetes_male,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE)                                    AS screenings_diabetes,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'female')         AS screenings_diabetes_female,
    COUNT(uuid) FILTER (WHERE screened_for_diabetes IS TRUE AND patient_sex = 'male')           AS screenings_diabetes_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE)                      AS referred_diabetes,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'female') AS referred_diabetes_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_diabetes IS TRUE AND patient_sex = 'male')   AS referred_diabetes_male,

    -- Hypertension
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE)                 AS screened_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female') AS screened_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')   AS screened_hypertension_male,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE)                                AS screenings_hypertension,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'female')     AS screenings_hypertension_female,
    COUNT(uuid) FILTER (WHERE screened_for_hypertension IS TRUE AND patient_sex = 'male')       AS screenings_hypertension_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE)                  AS referred_hypertension,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'female') AS referred_hypertension_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_hypertension IS TRUE AND patient_sex = 'male')   AS referred_hypertension_male,

    -- Mental health
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE)                AS screened_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female') AS screened_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')   AS screened_mental_health_male,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE)                               AS screenings_mental_health,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'female')    AS screenings_mental_health_female,
    COUNT(uuid) FILTER (WHERE screened_for_mental_health IS TRUE AND patient_sex = 'male')      AS screenings_mental_health_male,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE)                 AS referred_mental_health,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'female') AS referred_mental_health_female,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_mental_health IS TRUE AND patient_sex = 'male')   AS referred_mental_health_male,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_over_five_assessment"
WHERE reported >= '2020-01-01'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id
