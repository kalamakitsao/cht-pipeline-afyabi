{{ config(
    materialized='table',
    indexes=[
      {'columns':['household_id'], 'unique': true},
      {'columns':['chp_area_id']},
      {'columns':['member_count']},
      {'columns':['has_zero_members']}
    ],
    tags=['supervision','data_quality','cadence_daily','intermediate']
) }}

WITH households AS (
    SELECT
        h.uuid AS household_id,
        h.chv_area_id AS chp_area_id
    FROM {{ source(env_var('POSTGRES_SCHEMA'), 'household') }} h
    WHERE h.uuid IS NOT NULL
      AND h.chv_area_id IS NOT NULL
),

members AS (
    SELECT
        p.household_id,
        COUNT(DISTINCT p.uuid) AS member_count
    FROM {{ source(env_var('POSTGRES_SCHEMA'), 'patient_f_client') }} p
    WHERE p.household_id IS NOT NULL
    GROUP BY p.household_id
)

SELECT
    h.household_id,
    h.chp_area_id,
    COALESCE(m.member_count, 0) AS member_count,
    CASE WHEN COALESCE(m.member_count, 0) = 0 THEN 1 ELSE 0 END AS has_zero_members,
    CASE WHEN COALESCE(m.member_count, 0) = 1 THEN 1 ELSE 0 END AS has_one_member,
    CASE WHEN COALESCE(m.member_count, 0) > 15 THEN 1 ELSE 0 END AS has_gt_15_members
FROM households h
LEFT JOIN members m
    ON m.household_id = h.household_id
