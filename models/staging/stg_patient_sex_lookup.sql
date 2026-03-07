{{
  config(
    materialized = 'incremental',
    unique_key = 'patient_id',
    incremental_strategy = 'delete+insert',
    on_schema_change = 'sync_all_columns',
    indexes = [
      {'columns': ['patient_id'], 'unique': true}
    ],
    tags = ['cadence_hourly', 'staging']
  )
}}

/*
  stg_patient_sex_lookup: Minimal table for denormalizing sex
  into domain staging tables at staging time.

  saved_timestamp is included solely for incremental detection.
  The unique_key = patient_id ensures upserts if sex changes.
*/

SELECT
    uuid              AS patient_id,
    sex,
    saved_timestamp
FROM {{ source(var('source_schema'), 'patient_f_client') }}
WHERE uuid IS NOT NULL
{% if is_incremental() %}
  AND saved_timestamp > (SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp) FROM {{ this }})
{% endif %}