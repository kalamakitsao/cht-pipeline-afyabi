{{
  config(
    materialized = 'incremental',
    unique_key = 'uuid',
    incremental_strategy = 'delete+insert',
    on_schema_change = 'append_new_columns',
    indexes = [
      {'columns': ['uuid'], 'unique': true},
      {'columns': ['chp_area_id']},
      {'columns': ['reported']},
      {'columns': ['saved_timestamp']}
    ],
    tags = ['cadence_hourly', 'staging']
  )
}}

SELECT
    i.uuid,
    i.saved_timestamp,
    i.reported_by,
    i.reported_by_parent    AS chp_area_id,
    i.reported,
    i.patient_id,
    p.sex                   AS patient_sex,
    i.is_dewormed,
    i.is_referred_vitamin_a,
    i.is_referred_immunization,
    i.is_referred_growth_monitoring
FROM {{ source(var('source_schema'), 'immunization') }} i
LEFT JOIN {{ ref('stg_patient_sex_lookup') }} p ON p.patient_id = i.patient_id
{% if is_incremental() %}
WHERE i.saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
