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
    uuid,
    saved_timestamp,
    reported_by,
    reported_by_parent          AS chp_area_id,
    reported_by_parent_parent   AS chu_area_id,
    reported,
    patient_id,
    pregnancy_id,
    date_of_delivery,
    place_of_delivery,
    pnc_service_count,
    is_referred_for_pnc_services,
    has_delivered
FROM {{ source(var('source_schema'), 'postnatal_care_service') }}
{% if is_incremental() %}
WHERE saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
