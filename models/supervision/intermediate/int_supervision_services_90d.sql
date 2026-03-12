{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='service_uuid',
    on_schema_change='ignore',
    indexes=[
      {'columns':['service_uuid'], 'unique': true},
      {'columns':['reported']},
      {'columns':['chp_area_id']},
      {'columns':['household_id']},
      {'columns':['patient_id']},
      {'columns':['form']},
      {'columns':['chp_area_id','reported']}
    ],
    tags=['supervision','cadence_hourly','intermediate']
) }}

WITH filtered AS (
    SELECT
        dr.uuid        AS service_uuid,
        dr.reported,
        dr.form,
        dr.patient_id,
        dr.place_id,
        dr.parent_uuid AS chp_area_id
    FROM {{ source(env_var('POSTGRES_SCHEMA'), 'data_record') }} dr
    WHERE COALESCE(dr._deleted, false) = false
      AND dr.form IS NOT NULL
      AND dr.reported IS NOT NULL
      AND dr.parent_uuid IS NOT NULL
      AND dr.reported >= NOW() - interval '90 days'
      {% if is_incremental() %}
      AND dr.reported >= (
          SELECT COALESCE(MAX(reported) - interval '7 days', NOW() - interval '90 days')
          FROM {{ this }}
      )
      {% endif %}
),

person_services AS (
    SELECT
        f.service_uuid,
        f.reported,
        f.form,
        f.chp_area_id,
        p.household_id,
        p.uuid AS patient_id,
        'person'::text AS service_level
    FROM filtered f
    INNER JOIN {{ source(env_var('POSTGRES_SCHEMA'), 'patient_f_client') }} p
        ON p.uuid = f.patient_id
    WHERE f.patient_id IS NOT NULL
      AND p.household_id IS NOT NULL
),

household_services AS (
    SELECT
        f.service_uuid,
        f.reported,
        f.form,
        f.chp_area_id,
        h.uuid AS household_id,
        NULL::text AS patient_id,
        'household'::text AS service_level
    FROM filtered f
    INNER JOIN {{ source(env_var('POSTGRES_SCHEMA'), 'household') }} h
        ON h.uuid = f.place_id
    WHERE f.place_id IS NOT NULL
),

unioned AS (
    SELECT * FROM person_services
    UNION ALL
    SELECT * FROM household_services
),

deduped AS (
    SELECT DISTINCT ON (service_uuid)
        service_uuid,
        reported,
        form,
        chp_area_id,
        household_id,
        patient_id,
        service_level
    FROM unioned
    ORDER BY service_uuid, service_level DESC
)

SELECT
    service_uuid,
    reported,
    form,
    chp_area_id,
    household_id,
    patient_id,
    service_level
FROM deduped
