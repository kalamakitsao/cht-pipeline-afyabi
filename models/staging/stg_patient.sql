{{
  config(
    materialized = 'incremental',
    unique_key = 'patient_id',
    on_schema_change = 'append_new_columns',
    indexes = [
      {'columns': ['patient_id'], 'unique': true},
      {'columns': ['chp_area_id']},
      {'columns': ['sex']},
      {'columns': ['date_of_birth']},
      {'columns': ['household_id']},
      {'columns': ['saved_timestamp']}
    ],
    tags = ['cadence_hourly', 'staging']
  )
}}

/*
  stg_patient: Core demographic dimension.
  
  This table is used DIRECTLY only for:
  1. Population metrics (point-in-time counts by sex, age, muted status)
  2. SHA metrics (household_id lookup)
  3. Providing sex values to other staging models via stg_patient_sex_lookup
  
  It is NOT joined at intermediate aggregation time — sex is denormalized
  into domain staging tables instead.
*/

SELECT
    uuid                    AS patient_id,
    saved_timestamp,
    name,
    sex,
    date_of_birth,
    household_id,
    chp_area_id                  AS chp_area_id,
    chu_id                  AS chu_area_id,
    (muted IS NULL)         AS is_active,
    muted
FROM {{ source(var('source_schema'), 'patient_f_client') }}
{% if is_incremental() %}
WHERE saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
