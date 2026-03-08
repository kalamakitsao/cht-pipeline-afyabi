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

WITH deduped AS (
    SELECT DISTINCT ON (uuid)
        uuid,
        saved_timestamp,
        reported_by,
        reported_by_parent,
        reported_by_parent_parent,
        reported,
        patient_id,
        screened_for_diabetes,
        is_referred_diabetes,
        screened_for_hypertension,
        is_referred_hypertension,
        screened_for_mental_health,
        is_referred_mental_health,
        has_fever,
        rdt_result,
        repeat_rdt_result,
        given_al,
        has_been_referred
    FROM {{ source(var('source_schema'), 'over_five_assessment') }}
    {% if is_incremental() %}
    WHERE saved_timestamp > (
        SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
        FROM {{ this }}
    )
    {% endif %}
    ORDER BY uuid, saved_timestamp DESC
)

SELECT
    a.uuid,
    a.saved_timestamp,
    a.reported_by,
    a.reported_by_parent            AS chp_area_id,
    a.reported_by_parent_parent     AS chu_area_id,
    a.reported,
    a.patient_id,
    p.sex                           AS patient_sex,
    a.screened_for_diabetes,
    a.is_referred_diabetes,
    a.screened_for_hypertension,
    a.is_referred_hypertension,
    a.screened_for_mental_health,
    a.is_referred_mental_health,
    a.has_fever,
    a.rdt_result,
    a.repeat_rdt_result,
    a.given_al,
    a.has_been_referred
FROM deduped a
LEFT JOIN {{ ref('stg_patient_sex_lookup') }} p ON p.patient_id = a.patient_id