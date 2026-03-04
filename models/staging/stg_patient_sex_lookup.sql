{{
  config(
    materialized = 'incremental',
    unique_key = 'patient_id',
    on_schema_change = 'sync_all_columns',
    indexes = [
      {'columns': ['patient_id'], 'unique': true}
    ],
    tags = ['cadence_hourly', 'staging']
  )
}}

/*
  stg_patient_sex_lookup: Minimal 2-column table for denormalizing sex
  into domain staging tables.
  
  Why separate from stg_patient?
  - stg_patient is ~13GB with 67 columns
  - This table is ~2 columns × 30M rows ≈ ~1GB
  - Index-only scans on (patient_id) → sex are extremely fast
  - Joined ONCE at staging time, never at aggregation time
*/

SELECT
    uuid    AS patient_id,
    sex
FROM {{ source(var('source_schema'), 'patient_f_client') }}
WHERE uuid IS NOT NULL
{% if is_incremental() %}
  AND uuid NOT IN (SELECT patient_id FROM {{ this }})
{% endif %}
