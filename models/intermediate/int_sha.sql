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
  
  Optimization: Drive from the smaller stg_sha_registration (5.3M rows),
  look up chp_area_id and household_id from stg_patient via index.
  With idx_stg_patient_id_chp_hh covering index, the join becomes
  index-only scans — no 30M-row hash join needed.
*/

SELECT
    pt.chp_area_id,
    COUNT(DISTINCT pt.household_id)                                               AS households_assessed_sha,
    COUNT(DISTINCT pt.household_id) FILTER (WHERE r.has_sha_registration IS TRUE) AS households_registered_on_sha
FROM {{ ref('stg_sha_registration') }} r
INNER JOIN {{ ref('stg_patient') }} pt ON pt.patient_id = r.member_uuid
WHERE pt.chp_area_id IS NOT NULL
  AND pt.household_id IS NOT NULL
GROUP BY pt.chp_area_id