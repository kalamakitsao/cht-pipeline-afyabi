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
        has_fever,
        has_diarrhoea,
        has_cough,
        has_fast_breathing,
        has_chest_indrawing,
        muac_color,
        rdt_result,
        repeat_rdt_result,
        gave_al,
        gave_ors,
        gave_zinc,
        gave_amox,
        is_exclusively_breastfeeding,
        has_uptodate_growth_monitoring,
        referred_for_development_milestones,
        has_been_referred
    FROM {{ source(var('source_schema'), 'u5_assessment') }}
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
    a.has_fever,
    a.has_diarrhoea,
    a.has_cough,
    a.has_fast_breathing,
    a.has_chest_indrawing,
    a.muac_color,
    a.rdt_result,
    a.repeat_rdt_result,
    a.gave_al,
    a.gave_ors,
    a.gave_zinc,
    a.gave_amox,
    a.is_exclusively_breastfeeding,
    a.has_uptodate_growth_monitoring,
    a.referred_for_development_milestones,
    a.has_been_referred,
    (a.has_fast_breathing IS TRUE OR a.has_chest_indrawing IS TRUE)  AS has_pneumonia,
    (a.rdt_result = 'positive' OR a.repeat_rdt_result = 'positive') AS has_malaria,
    (a.muac_color IN ('red', 'yellow'))                              AS has_malnutrition
FROM deduped a
LEFT JOIN {{ ref('stg_patient_sex_lookup') }} p ON p.patient_id = a.patient_id
