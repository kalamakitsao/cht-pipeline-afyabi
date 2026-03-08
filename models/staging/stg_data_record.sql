{{
  config(
    materialized = 'incremental',
    unique_key = 'uuid',
    incremental_strategy = 'delete+insert',
    on_schema_change = 'append_new_columns',
    indexes = [
      {'columns': ['uuid'], 'unique': true},
      {'columns': ['chp_area_id']},
      {'columns': ['patient_id']},
      {'columns': ['reported']},
      {'columns': ['saved_timestamp']}
    ],
    tags = ['cadence_hourly', 'staging']
  )
}}

SELECT DISTINCT ON (uuid)
    uuid,
    saved_timestamp,
    reported,
    patient_id,
    contact_uuid        AS reported_by,
    parent_uuid         AS chp_area_id,
    grandparent_uuid    AS chu_area_id
FROM {{ source(var('source_schema'), 'data_record') }}
WHERE patient_id IS NOT NULL
{% if is_incremental() %}
  AND saved_timestamp > (
      SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
      FROM {{ this }}
  )
{% endif %}
ORDER BY uuid, saved_timestamp DESC