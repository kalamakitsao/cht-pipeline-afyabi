{{ config(
    materialized='table',
    indexes=[
      {'columns':['chp_area_id'], 'unique': true},
      {'columns':['community_unit_id']},
      {'columns':['sub_county_id']},
      {'columns':['county_id']}
    ],
    tags=['supervision','scorecard','cadence_hourly']
) }}

WITH svc_90 AS (
    SELECT
        s.chp_area_id,
        COUNT(DISTINCT s.household_id) AS households_served_90d,
        COUNT(s.service_uuid) AS forms_submitted_90d,
        COUNT(DISTINCT s.form) AS distinct_services_provided_90d,
        COUNT(DISTINCT s.patient_id) AS people_served_90d
    FROM {{ ref('int_supervision_services_90d') }} s
    GROUP BY s.chp_area_id
),

svc_30 AS (
    SELECT
        s.chp_area_id,
        1 AS active_chp_area_30d
    FROM {{ ref('int_supervision_services_90d') }} s
    WHERE s.reported >= NOW() - interval '30 days'
    GROUP BY s.chp_area_id
),

person_quality AS (
    SELECT
        pl.chp_area_id,
        COUNT(*) AS total_people,
        COUNT(*) FILTER (WHERE pl.missing_household_link = 1) AS people_missing_household_link
    FROM {{ ref('int_supervision_person_linkage') }} pl
    WHERE pl.chp_area_id IS NOT NULL
    GROUP BY pl.chp_area_id
)

SELECT
    lh.chp_area_id,
    lh.chp_area,
    lh.community_unit_id,
    lh.community_unit,
    lh.sub_county_id,
    lh.sub_county,
    lh.county_id,
    lh.county,

    COALESCE(reg.households_registered, 0) AS households_registered,
    COALESCE(s90.households_served_90d, 0) AS households_served_90d,
    ROUND(100.0 * COALESCE(s90.households_served_90d, 0) / NULLIF(reg.households_registered, 0), 1) AS pct_households_served_90d,

    COALESCE(s90.forms_submitted_90d, 0) AS forms_submitted_90d,
    COALESCE(s90.distinct_services_provided_90d, 0) AS distinct_services_provided_90d,
    COALESCE(s90.people_served_90d, 0) AS people_served_90d,

    COALESCE(reg.households_zero_members, 0) AS households_zero_members,
    ROUND(100.0 * COALESCE(reg.households_zero_members, 0) / NULLIF(reg.households_registered, 0), 1) AS pct_households_zero_members,

    COALESCE(reg.households_one_member, 0) AS households_one_member,
    ROUND(100.0 * COALESCE(reg.households_one_member, 0) / NULLIF(reg.households_registered, 0), 1) AS pct_households_one_member,

    COALESCE(reg.households_gt_15_members, 0) AS households_gt_15_members,
    ROUND(100.0 * COALESCE(reg.households_gt_15_members, 0) / NULLIF(reg.households_registered, 0), 1) AS pct_households_gt_15_members,

    COALESCE(reg.avg_people_per_household, 0) AS avg_people_per_household,

    COALESCE(sha.households_registered_on_sha, 0) AS households_registered_on_sha,
    ROUND(100.0 * COALESCE(sha.households_registered_on_sha, 0) / NULLIF(reg.households_registered, 0), 1) AS pct_households_registered_on_sha,

    CASE WHEN COALESCE(s30.active_chp_area_30d, 0) = 1 THEN 1 ELSE 0 END AS active_chp_area_30d,

    GREATEST(COALESCE(reg.households_registered, 0) - COALESCE(s90.households_served_90d, 0), 0) AS households_no_service_90d,
    ROUND(100.0 * GREATEST(COALESCE(reg.households_registered, 0) - COALESCE(s90.households_served_90d, 0), 0) / NULLIF(reg.households_registered, 0), 1) AS pct_households_no_service_90d,

    CASE WHEN COALESCE(reg.households_registered, 0) = 0 THEN 1 ELSE 0 END AS has_zero_households,

    COALESCE(pq.total_people, 0) AS total_people,
    COALESCE(pq.people_missing_household_link, 0) AS people_missing_household_link,
    ROUND(100.0 * COALESCE(pq.people_missing_household_link, 0) / NULLIF(pq.total_people, 0), 2) AS pct_people_missing_household_link

FROM {{ ref('mv_location_hierarchy') }} lh
LEFT JOIN {{ ref('int_supervision_chp_area_registry') }} reg
    ON reg.chp_area_id = lh.chp_area_id
LEFT JOIN svc_90 s90
    ON s90.chp_area_id = lh.chp_area_id
LEFT JOIN svc_30 s30
    ON s30.chp_area_id = lh.chp_area_id
LEFT JOIN {{ ref('int_sha') }} sha
    ON sha.chp_area_id = lh.chp_area_id
LEFT JOIN person_quality pq
    ON pq.chp_area_id = lh.chp_area_id
