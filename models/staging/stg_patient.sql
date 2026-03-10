{{
  config(
    materialized = 'incremental',
    unique_key = 'patient_id',
    incremental_strategy = 'delete+insert',
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

SELECT
    p.uuid                    AS patient_id,
    p.saved_timestamp,
    p.name,
    p.sex,
    p.date_of_birth,
    p.household_id,
    COALESCE(p.chp_area_id, h.chp_area_id) AS chp_area_id,
    p.chu_id                  AS chu_area_id,
    (p.muted IS NULL)         AS is_active,
    p.muted
FROM {{ source(var('source_schema'), 'patient_f_client') }} p
LEFT JOIN {{ ref('stg_household') }} h ON h.household_id = p.household_id
WHERE COALESCE(p.chp_area_id, h.chp_area_id) IS NOT NULL
{% if is_incremental() %}
  AND p.saved_timestamp > (
      SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
      FROM {{ this }}
  )
{% endif %}