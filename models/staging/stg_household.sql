{{
  config(
    materialized = 'incremental',
    unique_key = 'household_id',
    incremental_strategy = 'delete+insert',
    on_schema_change = 'append_new_columns',
    indexes = [
      {'columns': ['household_id'], 'unique': true},
      {'columns': ['chp_area_id']},
      {'columns': ['saved_timestamp']}
    ],
    tags = ['cadence_hourly', 'staging']
  )
}}

SELECT
    uuid            AS household_id,
    saved_timestamp,
    household_name,
    reported,
    chv_area_id     AS chp_area_id,
    chu_area_id
FROM {{ source(var('source_schema'), 'household') }}
{% if is_incremental() %}
WHERE saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
