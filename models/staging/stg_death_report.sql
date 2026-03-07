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
    d.uuid,
    d.saved_timestamp,
    d.reported_by,
    d.reported_by_parent            AS chp_area_id,
    d.reported_by_parent_parent     AS chu_area_id,
    d.reported,
    d.patient_id,
    p.sex                           AS patient_sex,
    d.patient_age_in_days,
    d.date_of_death,
    d.death_type
FROM {{ source(var('source_schema'), 'death_report') }} d
LEFT JOIN {{ ref('stg_patient_sex_lookup') }} p ON p.patient_id = d.patient_id
{% if is_incremental() %}
WHERE d.saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
