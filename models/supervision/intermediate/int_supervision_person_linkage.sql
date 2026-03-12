{{ config(
    materialized='table',
    indexes=[
      {'columns':['person_id'], 'unique': true},
      {'columns':['chp_area_id']},
      {'columns':['missing_household_link']}
    ],
    tags=['supervision','data_quality','cadence_daily','intermediate']
) }}

SELECT
    p.uuid AS person_id,
    p.chp_area_id,
    p.household_id,
    CASE WHEN p.household_id IS NULL THEN 1 ELSE 0 END AS missing_household_link
FROM {{ source(env_var('POSTGRES_SCHEMA'), 'patient_f_client') }} p
WHERE p.uuid IS NOT NULL
