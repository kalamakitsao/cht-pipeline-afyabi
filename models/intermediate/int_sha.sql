{{
  config(
    materialized = 'table',
    pre_hook = "SET LOCAL work_mem = '512MB'",
    indexes = [
      {'columns': ['chp_area_id'], 'unique': true}
    ],
    tags = ['cadence_daily', 'intermediate']
  )
}}

/*
  int_sha: SHA household assessment/registration by CHP area.
  Period-independent. Joins stg_sha_registration (5.3M) with stg_patient (30M)
  to resolve chp_area_id from patient records.
  Output: ~5K rows.
*/

SELECT
    pt.chp_area_id,
    COUNT(DISTINCT pt.household_id)                                               AS households_assessed_sha,
    COUNT(DISTINCT pt.household_id) FILTER (WHERE r.has_sha_registration IS TRUE) AS households_registered_on_sha
FROM {{ ref('stg_sha_registration') }} r
INNER JOIN {{ ref('stg_patient') }} pt ON pt.patient_id = r.member_uuid
WHERE pt.chp_area_id IS NOT NULL
GROUP BY pt.chp_area_id
