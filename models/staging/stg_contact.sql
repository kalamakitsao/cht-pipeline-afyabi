{{
  config(
    materialized = 'incremental',
    unique_key = 'uuid',
    on_schema_change = 'append_new_columns',
    indexes = [
      {'columns': ['uuid'], 'unique': true},
      {'columns': ['contact_type']},
      {'columns': ['parent_uuid']},
      {'columns': ['saved_timestamp']}
    ],
    tags = ['cadence_hourly', 'staging']
  )
}}

SELECT
    uuid,
    saved_timestamp,
    reported,
    parent_uuid,
    name,
    contact_type,
    active,
    muted
FROM {{ source(var('source_schema'), 'contact') }}
{% if is_incremental() %}
WHERE saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
