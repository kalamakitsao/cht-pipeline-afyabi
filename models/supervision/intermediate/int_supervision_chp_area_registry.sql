{{ config(
    materialized='table',
    indexes=[
      {'columns':['chp_area_id'], 'unique': true},
      {'columns':['households_registered']},
      {'columns':['households_zero_members']}
    ],
    tags=['supervision','data_quality','cadence_daily','intermediate']
) }}

SELECT
    hm.chp_area_id,
    COUNT(*) AS households_registered,
    COUNT(*) FILTER (WHERE hm.has_zero_members = 1) AS households_zero_members,
    COUNT(*) FILTER (WHERE hm.has_one_member = 1) AS households_one_member,
    COUNT(*) FILTER (WHERE hm.has_gt_15_members = 1) AS households_gt_15_members,
    ROUND(AVG(hm.member_count::numeric), 2) AS avg_people_per_household
FROM {{ ref('int_supervision_household_members') }} hm
GROUP BY hm.chp_area_id
