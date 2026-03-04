
  
    

  create  table "echis"."afyabi"."int_pnc_metrics__dbt_tmp"
  
  
    as
  
  (
    -- depends_on: "echis"."afyabi"."stg_postnatal_care_service"
-- depends_on: "echis"."afyabi"."dim_period"


/* int_pnc_metrics: 454K rows. Partition-friendly. */




  





SELECT
    chp_area_id,
    1::bigint AS period_id,
    COUNT(uuid) FILTER (WHERE pnc_service_count = 1)                                        AS deliveries,
    COUNT(uuid) FILTER (WHERE place_of_delivery = 'health_facility')                         AS skilled_birth_attendance,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_for_pnc_services IS TRUE)           AS referred_for_pnc,
    NOW() AS last_updated
FROM "echis"."afyabi"."stg_postnatal_care_service"
WHERE reported >= '2026-03-01'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    2::bigint AS period_id,
    COUNT(uuid) FILTER (WHERE pnc_service_count = 1)                                        AS deliveries,
    COUNT(uuid) FILTER (WHERE place_of_delivery = 'health_facility')                         AS skilled_birth_attendance,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_for_pnc_services IS TRUE)           AS referred_for_pnc,
    NOW() AS last_updated
FROM "echis"."afyabi"."stg_postnatal_care_service"
WHERE reported >= '2026-02-28'::date
  AND reported <  '2026-03-01'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    3::bigint AS period_id,
    COUNT(uuid) FILTER (WHERE pnc_service_count = 1)                                        AS deliveries,
    COUNT(uuid) FILTER (WHERE place_of_delivery = 'health_facility')                         AS skilled_birth_attendance,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_for_pnc_services IS TRUE)           AS referred_for_pnc,
    NOW() AS last_updated
FROM "echis"."afyabi"."stg_postnatal_care_service"
WHERE reported >= '2026-02-22'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    4::bigint AS period_id,
    COUNT(uuid) FILTER (WHERE pnc_service_count = 1)                                        AS deliveries,
    COUNT(uuid) FILTER (WHERE place_of_delivery = 'health_facility')                         AS skilled_birth_attendance,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_for_pnc_services IS TRUE)           AS referred_for_pnc,
    NOW() AS last_updated
FROM "echis"."afyabi"."stg_postnatal_care_service"
WHERE reported >= '2026-02-01'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    5::bigint AS period_id,
    COUNT(uuid) FILTER (WHERE pnc_service_count = 1)                                        AS deliveries,
    COUNT(uuid) FILTER (WHERE place_of_delivery = 'health_facility')                         AS skilled_birth_attendance,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_for_pnc_services IS TRUE)           AS referred_for_pnc,
    NOW() AS last_updated
FROM "echis"."afyabi"."stg_postnatal_care_service"
WHERE reported >= '2025-12-01'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    6::bigint AS period_id,
    COUNT(uuid) FILTER (WHERE pnc_service_count = 1)                                        AS deliveries,
    COUNT(uuid) FILTER (WHERE place_of_delivery = 'health_facility')                         AS skilled_birth_attendance,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_for_pnc_services IS TRUE)           AS referred_for_pnc,
    NOW() AS last_updated
FROM "echis"."afyabi"."stg_postnatal_care_service"
WHERE reported >= '2025-09-01'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    7::bigint AS period_id,
    COUNT(uuid) FILTER (WHERE pnc_service_count = 1)                                        AS deliveries,
    COUNT(uuid) FILTER (WHERE place_of_delivery = 'health_facility')                         AS skilled_birth_attendance,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_for_pnc_services IS TRUE)           AS referred_for_pnc,
    NOW() AS last_updated
FROM "echis"."afyabi"."stg_postnatal_care_service"
WHERE reported >= '2025-03-01'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    8::bigint AS period_id,
    COUNT(uuid) FILTER (WHERE pnc_service_count = 1)                                        AS deliveries,
    COUNT(uuid) FILTER (WHERE place_of_delivery = 'health_facility')                         AS skilled_birth_attendance,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_for_pnc_services IS TRUE)           AS referred_for_pnc,
    NOW() AS last_updated
FROM "echis"."afyabi"."stg_postnatal_care_service"
WHERE reported >= '2026-02-23'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    9::bigint AS period_id,
    COUNT(uuid) FILTER (WHERE pnc_service_count = 1)                                        AS deliveries,
    COUNT(uuid) FILTER (WHERE place_of_delivery = 'health_facility')                         AS skilled_birth_attendance,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_for_pnc_services IS TRUE)           AS referred_for_pnc,
    NOW() AS last_updated
FROM "echis"."afyabi"."stg_postnatal_care_service"
WHERE reported >= '2026-02-16'::date
  AND reported <  '2026-02-23'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    10::bigint AS period_id,
    COUNT(uuid) FILTER (WHERE pnc_service_count = 1)                                        AS deliveries,
    COUNT(uuid) FILTER (WHERE place_of_delivery = 'health_facility')                         AS skilled_birth_attendance,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_for_pnc_services IS TRUE)           AS referred_for_pnc,
    NOW() AS last_updated
FROM "echis"."afyabi"."stg_postnatal_care_service"
WHERE reported >= '2026-03-01'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    11::bigint AS period_id,
    COUNT(uuid) FILTER (WHERE pnc_service_count = 1)                                        AS deliveries,
    COUNT(uuid) FILTER (WHERE place_of_delivery = 'health_facility')                         AS skilled_birth_attendance,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_for_pnc_services IS TRUE)           AS referred_for_pnc,
    NOW() AS last_updated
FROM "echis"."afyabi"."stg_postnatal_care_service"
WHERE reported >= '2026-02-01'::date
  AND reported <  '2026-03-01'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    12::bigint AS period_id,
    COUNT(uuid) FILTER (WHERE pnc_service_count = 1)                                        AS deliveries,
    COUNT(uuid) FILTER (WHERE place_of_delivery = 'health_facility')                         AS skilled_birth_attendance,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_for_pnc_services IS TRUE)           AS referred_for_pnc,
    NOW() AS last_updated
FROM "echis"."afyabi"."stg_postnatal_care_service"
WHERE reported >= '2026-01-01'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    13::bigint AS period_id,
    COUNT(uuid) FILTER (WHERE pnc_service_count = 1)                                        AS deliveries,
    COUNT(uuid) FILTER (WHERE place_of_delivery = 'health_facility')                         AS skilled_birth_attendance,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_for_pnc_services IS TRUE)           AS referred_for_pnc,
    NOW() AS last_updated
FROM "echis"."afyabi"."stg_postnatal_care_service"
WHERE reported >= '2025-10-01'::date
  AND reported <  '2026-01-01'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    14::bigint AS period_id,
    COUNT(uuid) FILTER (WHERE pnc_service_count = 1)                                        AS deliveries,
    COUNT(uuid) FILTER (WHERE place_of_delivery = 'health_facility')                         AS skilled_birth_attendance,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_for_pnc_services IS TRUE)           AS referred_for_pnc,
    NOW() AS last_updated
FROM "echis"."afyabi"."stg_postnatal_care_service"
WHERE reported >= '2026-01-01'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

UNION ALL

SELECT
    chp_area_id,
    15::bigint AS period_id,
    COUNT(uuid) FILTER (WHERE pnc_service_count = 1)                                        AS deliveries,
    COUNT(uuid) FILTER (WHERE place_of_delivery = 'health_facility')                         AS skilled_birth_attendance,
    COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_for_pnc_services IS TRUE)           AS referred_for_pnc,
    NOW() AS last_updated
FROM "echis"."afyabi"."stg_postnatal_care_service"
WHERE reported >= '2020-01-01'::date
  AND reported <  '2026-03-02'::date
  AND chp_area_id IS NOT NULL
GROUP BY chp_area_id

  );
  