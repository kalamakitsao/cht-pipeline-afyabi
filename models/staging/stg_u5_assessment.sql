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

/*
  stg_u5_assessment: Under-5 assessments with derived condition flags
  and sex denormalized from patient_f_client.
  
  The sex JOIN happens HERE (once, incremental) rather than in the
  intermediate aggregation layer (which would re-join 30M rows every run).
  
  Source: 12.5M rows, 6.7GB. Incremental picks up ~hourly deltas.
*/

SELECT
    a.uuid,
    a.saved_timestamp,
    a.reported_by,
    a.reported_by_parent            AS chp_area_id,
    a.reported_by_parent_parent     AS chu_area_id,
    a.reported,
    a.patient_id,
    -- Denormalized from patient_f_client (the key optimization)
    p.sex                           AS patient_sex,
    -- Raw assessment fields
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
    -- Derived condition flags (computed once at staging, reused everywhere)
    (a.has_fast_breathing IS TRUE OR a.has_chest_indrawing IS TRUE)      AS has_pneumonia,
    (a.rdt_result = 'positive' OR a.repeat_rdt_result = 'positive')     AS has_malaria,
    (a.muac_color IN ('red', 'yellow'))                                  AS has_malnutrition
FROM {{ source(var('source_schema'), 'u5_assessment') }} a
LEFT JOIN {{ ref('stg_patient_sex_lookup') }} p ON p.patient_id = a.patient_id
{% if is_incremental() %}
WHERE a.saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
