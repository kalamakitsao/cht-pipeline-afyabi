-- depends_on: "echis"."afyabi"."stg_pregnancy_home_visit"
-- depends_on: "echis"."afyabi"."stg_postnatal_care_service"
-- depends_on: "echis"."afyabi"."dim_period"


/* int_pregnancy_metrics: 876K rows. Partition-friendly. */




  


-- Pre-compute latest deliveries (used for repeat_pregnancies)
WITH latest_deliveries AS (
    SELECT patient_id, MAX(date_of_delivery) AS last_delivery_date
    FROM "echis"."afyabi"."stg_postnatal_care_service"
    WHERE date_of_delivery IS NOT NULL
    GROUP BY patient_id
)




SELECT
    s.chp_area_id,
    1::bigint AS period_id,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_currently_pregnant IS TRUE OR s.is_new_pregnancy IS TRUE
    ) AS actively_pregnant_women,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.effective_edd >= '2026-03-01'::date
          AND s.effective_edd <= '2026-03-01'::date + INTERVAL '9 months'
    ) AS currently_pregnant,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_new_pregnancy IS TRUE)              AS new_pregnancies,
    COUNT(DISTINCT s.patient_id)                                                         AS pregnant_women_visited,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE)              AS pregnant_women_referred,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.has_been_referred IS TRUE AND s.is_anc_upto_date IS FALSE
    ) AS pregnant_women_referred_missed_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE AND s.has_started_anc IS FALSE
    ) AS new_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE (s.is_new_pregnancy IS TRUE OR s.is_currently_pregnant IS TRUE)
          AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE
          AND s.has_started_anc IS FALSE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.current_edd IS NOT NULL
          AND (((s.current_edd::date - '2026-03-01'::date) / 7.0 - 40) * -1) BETWEEN 0 AND 12
    ) AS first_trimester_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE
          AND ld.last_delivery_date IS NOT NULL
          AND s.reported > ld.last_delivery_date
          AND s.reported <= ld.last_delivery_date + INTERVAL '12 months'
    ) AS repeat_pregnancies,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_pregnancy_home_visit" s
LEFT JOIN latest_deliveries ld ON s.patient_id = ld.patient_id
WHERE s.reported >= '2026-03-01'::date
  AND s.reported <  '2026-03-02'::date
  AND s.chp_area_id IS NOT NULL
GROUP BY s.chp_area_id

UNION ALL

SELECT
    s.chp_area_id,
    2::bigint AS period_id,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_currently_pregnant IS TRUE OR s.is_new_pregnancy IS TRUE
    ) AS actively_pregnant_women,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.effective_edd >= '2026-02-28'::date
          AND s.effective_edd <= '2026-02-28'::date + INTERVAL '9 months'
    ) AS currently_pregnant,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_new_pregnancy IS TRUE)              AS new_pregnancies,
    COUNT(DISTINCT s.patient_id)                                                         AS pregnant_women_visited,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE)              AS pregnant_women_referred,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.has_been_referred IS TRUE AND s.is_anc_upto_date IS FALSE
    ) AS pregnant_women_referred_missed_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE AND s.has_started_anc IS FALSE
    ) AS new_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE (s.is_new_pregnancy IS TRUE OR s.is_currently_pregnant IS TRUE)
          AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE
          AND s.has_started_anc IS FALSE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.current_edd IS NOT NULL
          AND (((s.current_edd::date - '2026-02-28'::date) / 7.0 - 40) * -1) BETWEEN 0 AND 12
    ) AS first_trimester_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE
          AND ld.last_delivery_date IS NOT NULL
          AND s.reported > ld.last_delivery_date
          AND s.reported <= ld.last_delivery_date + INTERVAL '12 months'
    ) AS repeat_pregnancies,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_pregnancy_home_visit" s
LEFT JOIN latest_deliveries ld ON s.patient_id = ld.patient_id
WHERE s.reported >= '2026-02-28'::date
  AND s.reported <  '2026-03-01'::date
  AND s.chp_area_id IS NOT NULL
GROUP BY s.chp_area_id

UNION ALL

SELECT
    s.chp_area_id,
    3::bigint AS period_id,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_currently_pregnant IS TRUE OR s.is_new_pregnancy IS TRUE
    ) AS actively_pregnant_women,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.effective_edd >= '2026-03-01'::date
          AND s.effective_edd <= '2026-03-01'::date + INTERVAL '9 months'
    ) AS currently_pregnant,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_new_pregnancy IS TRUE)              AS new_pregnancies,
    COUNT(DISTINCT s.patient_id)                                                         AS pregnant_women_visited,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE)              AS pregnant_women_referred,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.has_been_referred IS TRUE AND s.is_anc_upto_date IS FALSE
    ) AS pregnant_women_referred_missed_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE AND s.has_started_anc IS FALSE
    ) AS new_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE (s.is_new_pregnancy IS TRUE OR s.is_currently_pregnant IS TRUE)
          AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE
          AND s.has_started_anc IS FALSE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.current_edd IS NOT NULL
          AND (((s.current_edd::date - '2026-03-01'::date) / 7.0 - 40) * -1) BETWEEN 0 AND 12
    ) AS first_trimester_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE
          AND ld.last_delivery_date IS NOT NULL
          AND s.reported > ld.last_delivery_date
          AND s.reported <= ld.last_delivery_date + INTERVAL '12 months'
    ) AS repeat_pregnancies,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_pregnancy_home_visit" s
LEFT JOIN latest_deliveries ld ON s.patient_id = ld.patient_id
WHERE s.reported >= '2026-02-22'::date
  AND s.reported <  '2026-03-02'::date
  AND s.chp_area_id IS NOT NULL
GROUP BY s.chp_area_id

UNION ALL

SELECT
    s.chp_area_id,
    4::bigint AS period_id,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_currently_pregnant IS TRUE OR s.is_new_pregnancy IS TRUE
    ) AS actively_pregnant_women,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.effective_edd >= '2026-03-01'::date
          AND s.effective_edd <= '2026-03-01'::date + INTERVAL '9 months'
    ) AS currently_pregnant,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_new_pregnancy IS TRUE)              AS new_pregnancies,
    COUNT(DISTINCT s.patient_id)                                                         AS pregnant_women_visited,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE)              AS pregnant_women_referred,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.has_been_referred IS TRUE AND s.is_anc_upto_date IS FALSE
    ) AS pregnant_women_referred_missed_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE AND s.has_started_anc IS FALSE
    ) AS new_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE (s.is_new_pregnancy IS TRUE OR s.is_currently_pregnant IS TRUE)
          AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE
          AND s.has_started_anc IS FALSE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.current_edd IS NOT NULL
          AND (((s.current_edd::date - '2026-03-01'::date) / 7.0 - 40) * -1) BETWEEN 0 AND 12
    ) AS first_trimester_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE
          AND ld.last_delivery_date IS NOT NULL
          AND s.reported > ld.last_delivery_date
          AND s.reported <= ld.last_delivery_date + INTERVAL '12 months'
    ) AS repeat_pregnancies,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_pregnancy_home_visit" s
LEFT JOIN latest_deliveries ld ON s.patient_id = ld.patient_id
WHERE s.reported >= '2026-02-01'::date
  AND s.reported <  '2026-03-02'::date
  AND s.chp_area_id IS NOT NULL
GROUP BY s.chp_area_id

UNION ALL

SELECT
    s.chp_area_id,
    5::bigint AS period_id,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_currently_pregnant IS TRUE OR s.is_new_pregnancy IS TRUE
    ) AS actively_pregnant_women,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.effective_edd >= '2026-03-01'::date
          AND s.effective_edd <= '2026-03-01'::date + INTERVAL '9 months'
    ) AS currently_pregnant,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_new_pregnancy IS TRUE)              AS new_pregnancies,
    COUNT(DISTINCT s.patient_id)                                                         AS pregnant_women_visited,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE)              AS pregnant_women_referred,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.has_been_referred IS TRUE AND s.is_anc_upto_date IS FALSE
    ) AS pregnant_women_referred_missed_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE AND s.has_started_anc IS FALSE
    ) AS new_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE (s.is_new_pregnancy IS TRUE OR s.is_currently_pregnant IS TRUE)
          AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE
          AND s.has_started_anc IS FALSE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.current_edd IS NOT NULL
          AND (((s.current_edd::date - '2026-03-01'::date) / 7.0 - 40) * -1) BETWEEN 0 AND 12
    ) AS first_trimester_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE
          AND ld.last_delivery_date IS NOT NULL
          AND s.reported > ld.last_delivery_date
          AND s.reported <= ld.last_delivery_date + INTERVAL '12 months'
    ) AS repeat_pregnancies,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_pregnancy_home_visit" s
LEFT JOIN latest_deliveries ld ON s.patient_id = ld.patient_id
WHERE s.reported >= '2025-12-01'::date
  AND s.reported <  '2026-03-02'::date
  AND s.chp_area_id IS NOT NULL
GROUP BY s.chp_area_id

UNION ALL

SELECT
    s.chp_area_id,
    6::bigint AS period_id,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_currently_pregnant IS TRUE OR s.is_new_pregnancy IS TRUE
    ) AS actively_pregnant_women,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.effective_edd >= '2026-03-01'::date
          AND s.effective_edd <= '2026-03-01'::date + INTERVAL '9 months'
    ) AS currently_pregnant,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_new_pregnancy IS TRUE)              AS new_pregnancies,
    COUNT(DISTINCT s.patient_id)                                                         AS pregnant_women_visited,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE)              AS pregnant_women_referred,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.has_been_referred IS TRUE AND s.is_anc_upto_date IS FALSE
    ) AS pregnant_women_referred_missed_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE AND s.has_started_anc IS FALSE
    ) AS new_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE (s.is_new_pregnancy IS TRUE OR s.is_currently_pregnant IS TRUE)
          AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE
          AND s.has_started_anc IS FALSE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.current_edd IS NOT NULL
          AND (((s.current_edd::date - '2026-03-01'::date) / 7.0 - 40) * -1) BETWEEN 0 AND 12
    ) AS first_trimester_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE
          AND ld.last_delivery_date IS NOT NULL
          AND s.reported > ld.last_delivery_date
          AND s.reported <= ld.last_delivery_date + INTERVAL '12 months'
    ) AS repeat_pregnancies,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_pregnancy_home_visit" s
LEFT JOIN latest_deliveries ld ON s.patient_id = ld.patient_id
WHERE s.reported >= '2025-09-01'::date
  AND s.reported <  '2026-03-02'::date
  AND s.chp_area_id IS NOT NULL
GROUP BY s.chp_area_id

UNION ALL

SELECT
    s.chp_area_id,
    7::bigint AS period_id,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_currently_pregnant IS TRUE OR s.is_new_pregnancy IS TRUE
    ) AS actively_pregnant_women,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.effective_edd >= '2026-03-01'::date
          AND s.effective_edd <= '2026-03-01'::date + INTERVAL '9 months'
    ) AS currently_pregnant,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_new_pregnancy IS TRUE)              AS new_pregnancies,
    COUNT(DISTINCT s.patient_id)                                                         AS pregnant_women_visited,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE)              AS pregnant_women_referred,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.has_been_referred IS TRUE AND s.is_anc_upto_date IS FALSE
    ) AS pregnant_women_referred_missed_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE AND s.has_started_anc IS FALSE
    ) AS new_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE (s.is_new_pregnancy IS TRUE OR s.is_currently_pregnant IS TRUE)
          AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE
          AND s.has_started_anc IS FALSE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.current_edd IS NOT NULL
          AND (((s.current_edd::date - '2026-03-01'::date) / 7.0 - 40) * -1) BETWEEN 0 AND 12
    ) AS first_trimester_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE
          AND ld.last_delivery_date IS NOT NULL
          AND s.reported > ld.last_delivery_date
          AND s.reported <= ld.last_delivery_date + INTERVAL '12 months'
    ) AS repeat_pregnancies,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_pregnancy_home_visit" s
LEFT JOIN latest_deliveries ld ON s.patient_id = ld.patient_id
WHERE s.reported >= '2025-03-01'::date
  AND s.reported <  '2026-03-02'::date
  AND s.chp_area_id IS NOT NULL
GROUP BY s.chp_area_id

UNION ALL

SELECT
    s.chp_area_id,
    8::bigint AS period_id,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_currently_pregnant IS TRUE OR s.is_new_pregnancy IS TRUE
    ) AS actively_pregnant_women,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.effective_edd >= '2026-03-01'::date
          AND s.effective_edd <= '2026-03-01'::date + INTERVAL '9 months'
    ) AS currently_pregnant,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_new_pregnancy IS TRUE)              AS new_pregnancies,
    COUNT(DISTINCT s.patient_id)                                                         AS pregnant_women_visited,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE)              AS pregnant_women_referred,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.has_been_referred IS TRUE AND s.is_anc_upto_date IS FALSE
    ) AS pregnant_women_referred_missed_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE AND s.has_started_anc IS FALSE
    ) AS new_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE (s.is_new_pregnancy IS TRUE OR s.is_currently_pregnant IS TRUE)
          AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE
          AND s.has_started_anc IS FALSE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.current_edd IS NOT NULL
          AND (((s.current_edd::date - '2026-03-01'::date) / 7.0 - 40) * -1) BETWEEN 0 AND 12
    ) AS first_trimester_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE
          AND ld.last_delivery_date IS NOT NULL
          AND s.reported > ld.last_delivery_date
          AND s.reported <= ld.last_delivery_date + INTERVAL '12 months'
    ) AS repeat_pregnancies,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_pregnancy_home_visit" s
LEFT JOIN latest_deliveries ld ON s.patient_id = ld.patient_id
WHERE s.reported >= '2026-02-23'::date
  AND s.reported <  '2026-03-02'::date
  AND s.chp_area_id IS NOT NULL
GROUP BY s.chp_area_id

UNION ALL

SELECT
    s.chp_area_id,
    9::bigint AS period_id,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_currently_pregnant IS TRUE OR s.is_new_pregnancy IS TRUE
    ) AS actively_pregnant_women,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.effective_edd >= '2026-02-22'::date
          AND s.effective_edd <= '2026-02-22'::date + INTERVAL '9 months'
    ) AS currently_pregnant,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_new_pregnancy IS TRUE)              AS new_pregnancies,
    COUNT(DISTINCT s.patient_id)                                                         AS pregnant_women_visited,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE)              AS pregnant_women_referred,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.has_been_referred IS TRUE AND s.is_anc_upto_date IS FALSE
    ) AS pregnant_women_referred_missed_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE AND s.has_started_anc IS FALSE
    ) AS new_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE (s.is_new_pregnancy IS TRUE OR s.is_currently_pregnant IS TRUE)
          AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE
          AND s.has_started_anc IS FALSE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.current_edd IS NOT NULL
          AND (((s.current_edd::date - '2026-02-22'::date) / 7.0 - 40) * -1) BETWEEN 0 AND 12
    ) AS first_trimester_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE
          AND ld.last_delivery_date IS NOT NULL
          AND s.reported > ld.last_delivery_date
          AND s.reported <= ld.last_delivery_date + INTERVAL '12 months'
    ) AS repeat_pregnancies,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_pregnancy_home_visit" s
LEFT JOIN latest_deliveries ld ON s.patient_id = ld.patient_id
WHERE s.reported >= '2026-02-16'::date
  AND s.reported <  '2026-02-23'::date
  AND s.chp_area_id IS NOT NULL
GROUP BY s.chp_area_id

UNION ALL

SELECT
    s.chp_area_id,
    10::bigint AS period_id,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_currently_pregnant IS TRUE OR s.is_new_pregnancy IS TRUE
    ) AS actively_pregnant_women,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.effective_edd >= '2026-03-01'::date
          AND s.effective_edd <= '2026-03-01'::date + INTERVAL '9 months'
    ) AS currently_pregnant,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_new_pregnancy IS TRUE)              AS new_pregnancies,
    COUNT(DISTINCT s.patient_id)                                                         AS pregnant_women_visited,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE)              AS pregnant_women_referred,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.has_been_referred IS TRUE AND s.is_anc_upto_date IS FALSE
    ) AS pregnant_women_referred_missed_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE AND s.has_started_anc IS FALSE
    ) AS new_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE (s.is_new_pregnancy IS TRUE OR s.is_currently_pregnant IS TRUE)
          AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE
          AND s.has_started_anc IS FALSE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.current_edd IS NOT NULL
          AND (((s.current_edd::date - '2026-03-01'::date) / 7.0 - 40) * -1) BETWEEN 0 AND 12
    ) AS first_trimester_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE
          AND ld.last_delivery_date IS NOT NULL
          AND s.reported > ld.last_delivery_date
          AND s.reported <= ld.last_delivery_date + INTERVAL '12 months'
    ) AS repeat_pregnancies,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_pregnancy_home_visit" s
LEFT JOIN latest_deliveries ld ON s.patient_id = ld.patient_id
WHERE s.reported >= '2026-03-01'::date
  AND s.reported <  '2026-03-02'::date
  AND s.chp_area_id IS NOT NULL
GROUP BY s.chp_area_id

UNION ALL

SELECT
    s.chp_area_id,
    11::bigint AS period_id,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_currently_pregnant IS TRUE OR s.is_new_pregnancy IS TRUE
    ) AS actively_pregnant_women,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.effective_edd >= '2026-02-28'::date
          AND s.effective_edd <= '2026-02-28'::date + INTERVAL '9 months'
    ) AS currently_pregnant,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_new_pregnancy IS TRUE)              AS new_pregnancies,
    COUNT(DISTINCT s.patient_id)                                                         AS pregnant_women_visited,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE)              AS pregnant_women_referred,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.has_been_referred IS TRUE AND s.is_anc_upto_date IS FALSE
    ) AS pregnant_women_referred_missed_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE AND s.has_started_anc IS FALSE
    ) AS new_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE (s.is_new_pregnancy IS TRUE OR s.is_currently_pregnant IS TRUE)
          AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE
          AND s.has_started_anc IS FALSE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.current_edd IS NOT NULL
          AND (((s.current_edd::date - '2026-02-28'::date) / 7.0 - 40) * -1) BETWEEN 0 AND 12
    ) AS first_trimester_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE
          AND ld.last_delivery_date IS NOT NULL
          AND s.reported > ld.last_delivery_date
          AND s.reported <= ld.last_delivery_date + INTERVAL '12 months'
    ) AS repeat_pregnancies,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_pregnancy_home_visit" s
LEFT JOIN latest_deliveries ld ON s.patient_id = ld.patient_id
WHERE s.reported >= '2026-02-01'::date
  AND s.reported <  '2026-03-01'::date
  AND s.chp_area_id IS NOT NULL
GROUP BY s.chp_area_id

UNION ALL

SELECT
    s.chp_area_id,
    12::bigint AS period_id,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_currently_pregnant IS TRUE OR s.is_new_pregnancy IS TRUE
    ) AS actively_pregnant_women,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.effective_edd >= '2026-03-01'::date
          AND s.effective_edd <= '2026-03-01'::date + INTERVAL '9 months'
    ) AS currently_pregnant,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_new_pregnancy IS TRUE)              AS new_pregnancies,
    COUNT(DISTINCT s.patient_id)                                                         AS pregnant_women_visited,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE)              AS pregnant_women_referred,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.has_been_referred IS TRUE AND s.is_anc_upto_date IS FALSE
    ) AS pregnant_women_referred_missed_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE AND s.has_started_anc IS FALSE
    ) AS new_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE (s.is_new_pregnancy IS TRUE OR s.is_currently_pregnant IS TRUE)
          AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE
          AND s.has_started_anc IS FALSE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.current_edd IS NOT NULL
          AND (((s.current_edd::date - '2026-03-01'::date) / 7.0 - 40) * -1) BETWEEN 0 AND 12
    ) AS first_trimester_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE
          AND ld.last_delivery_date IS NOT NULL
          AND s.reported > ld.last_delivery_date
          AND s.reported <= ld.last_delivery_date + INTERVAL '12 months'
    ) AS repeat_pregnancies,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_pregnancy_home_visit" s
LEFT JOIN latest_deliveries ld ON s.patient_id = ld.patient_id
WHERE s.reported >= '2026-01-01'::date
  AND s.reported <  '2026-03-02'::date
  AND s.chp_area_id IS NOT NULL
GROUP BY s.chp_area_id

UNION ALL

SELECT
    s.chp_area_id,
    13::bigint AS period_id,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_currently_pregnant IS TRUE OR s.is_new_pregnancy IS TRUE
    ) AS actively_pregnant_women,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.effective_edd >= '2025-12-31'::date
          AND s.effective_edd <= '2025-12-31'::date + INTERVAL '9 months'
    ) AS currently_pregnant,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_new_pregnancy IS TRUE)              AS new_pregnancies,
    COUNT(DISTINCT s.patient_id)                                                         AS pregnant_women_visited,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE)              AS pregnant_women_referred,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.has_been_referred IS TRUE AND s.is_anc_upto_date IS FALSE
    ) AS pregnant_women_referred_missed_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE AND s.has_started_anc IS FALSE
    ) AS new_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE (s.is_new_pregnancy IS TRUE OR s.is_currently_pregnant IS TRUE)
          AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE
          AND s.has_started_anc IS FALSE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.current_edd IS NOT NULL
          AND (((s.current_edd::date - '2025-12-31'::date) / 7.0 - 40) * -1) BETWEEN 0 AND 12
    ) AS first_trimester_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE
          AND ld.last_delivery_date IS NOT NULL
          AND s.reported > ld.last_delivery_date
          AND s.reported <= ld.last_delivery_date + INTERVAL '12 months'
    ) AS repeat_pregnancies,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_pregnancy_home_visit" s
LEFT JOIN latest_deliveries ld ON s.patient_id = ld.patient_id
WHERE s.reported >= '2025-10-01'::date
  AND s.reported <  '2026-01-01'::date
  AND s.chp_area_id IS NOT NULL
GROUP BY s.chp_area_id

UNION ALL

SELECT
    s.chp_area_id,
    14::bigint AS period_id,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_currently_pregnant IS TRUE OR s.is_new_pregnancy IS TRUE
    ) AS actively_pregnant_women,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.effective_edd >= '2026-03-01'::date
          AND s.effective_edd <= '2026-03-01'::date + INTERVAL '9 months'
    ) AS currently_pregnant,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_new_pregnancy IS TRUE)              AS new_pregnancies,
    COUNT(DISTINCT s.patient_id)                                                         AS pregnant_women_visited,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE)              AS pregnant_women_referred,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.has_been_referred IS TRUE AND s.is_anc_upto_date IS FALSE
    ) AS pregnant_women_referred_missed_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE AND s.has_started_anc IS FALSE
    ) AS new_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE (s.is_new_pregnancy IS TRUE OR s.is_currently_pregnant IS TRUE)
          AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE
          AND s.has_started_anc IS FALSE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.current_edd IS NOT NULL
          AND (((s.current_edd::date - '2026-03-01'::date) / 7.0 - 40) * -1) BETWEEN 0 AND 12
    ) AS first_trimester_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE
          AND ld.last_delivery_date IS NOT NULL
          AND s.reported > ld.last_delivery_date
          AND s.reported <= ld.last_delivery_date + INTERVAL '12 months'
    ) AS repeat_pregnancies,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_pregnancy_home_visit" s
LEFT JOIN latest_deliveries ld ON s.patient_id = ld.patient_id
WHERE s.reported >= '2026-01-01'::date
  AND s.reported <  '2026-03-02'::date
  AND s.chp_area_id IS NOT NULL
GROUP BY s.chp_area_id

UNION ALL

SELECT
    s.chp_area_id,
    15::bigint AS period_id,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_currently_pregnant IS TRUE OR s.is_new_pregnancy IS TRUE
    ) AS actively_pregnant_women,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.effective_edd >= '2026-03-01'::date
          AND s.effective_edd <= '2026-03-01'::date + INTERVAL '9 months'
    ) AS currently_pregnant,

    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.is_new_pregnancy IS TRUE)              AS new_pregnancies,
    COUNT(DISTINCT s.patient_id)                                                         AS pregnant_women_visited,
    COUNT(DISTINCT s.patient_id) FILTER (WHERE s.has_been_referred IS TRUE)              AS pregnant_women_referred,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.has_been_referred IS TRUE AND s.is_anc_upto_date IS FALSE
    ) AS pregnant_women_referred_missed_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE AND s.has_started_anc IS FALSE
    ) AS new_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE (s.is_new_pregnancy IS TRUE OR s.is_currently_pregnant IS TRUE)
          AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.has_been_referred IS TRUE
          AND s.has_started_anc IS FALSE AND s.patient_age_in_years BETWEEN 10 AND 19
    ) AS new_teen_pregnant_women_referred_anc,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE AND s.current_edd IS NOT NULL
          AND (((s.current_edd::date - '2026-03-01'::date) / 7.0 - 40) * -1) BETWEEN 0 AND 12
    ) AS first_trimester_pregnancies,

    COUNT(DISTINCT s.patient_id) FILTER (
        WHERE s.is_new_pregnancy IS TRUE
          AND ld.last_delivery_date IS NOT NULL
          AND s.reported > ld.last_delivery_date
          AND s.reported <= ld.last_delivery_date + INTERVAL '12 months'
    ) AS repeat_pregnancies,

    NOW() AS last_updated

FROM "echis"."afyabi"."stg_pregnancy_home_visit" s
LEFT JOIN latest_deliveries ld ON s.patient_id = ld.patient_id
WHERE s.reported >= '2020-01-01'::date
  AND s.reported <  '2026-03-02'::date
  AND s.chp_area_id IS NOT NULL
GROUP BY s.chp_area_id
