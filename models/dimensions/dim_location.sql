{{
  config(
    materialized = 'incremental',
    unique_key = 'location_id',
    indexes = [
      {'columns': ['location_id'], 'unique': true},
      {'columns': ['contact_type']},
      {'columns': ['parent_uuid']}
    ],
    tags = ['cadence_hourly']
  )
}}

SELECT
    uuid            AS location_id,
    saved_timestamp,
    name,
    contact_type,
    parent_uuid,
    active,
    muted
FROM {{ ref('stg_contact') }}
WHERE contact_type IN (
    'a_county',
    'b_sub_county',
    'c_community_health_unit',
    'd_community_health_volunteer_area'
)
{% if is_incremental() %}
  AND saved_timestamp > (SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp) FROM {{ this }})
{% endif %}
