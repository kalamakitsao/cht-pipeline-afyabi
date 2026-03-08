{{
  config(
    materialized = 'incremental',
    unique_key = 'uuid',
    incremental_strategy = 'delete+insert',
    on_schema_change = 'append_new_columns',
    indexes = [
      {'columns': ['uuid'], 'unique': true},
      {'columns': ['chp_area_id']},
      {'columns': ['member_uuid']},
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
    reported_by_parent  AS chp_area_id,
    member_uuid,
    has_sha_registration
FROM {{ source(var('source_schema'), 'sha_registration') }}
{% if is_incremental() %}
WHERE saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
ORDER BY uuid, saved_timestamp DESC
