{{ config(
  materialized = 'table',
  indexes = [
    {'columns': ['chp_area_id'], 'unique': true},
    {'columns': ['community_unit_id']},
    {'columns': ['sub_county_id']},
    {'columns': ['county_id']}
  ],
  tags = ['cadence_hourly']
) }}

/*
  Denormalized location hierarchy with canonical latest names.

  - One row per chp_area_uuid
  - community_unit name is taken from the latest chu_name seen for that chu_uuid
  - sub_county and county IDs still come from dim_location lineage
*/

WITH latest_cu_name AS (
    SELECT DISTINCT ON (h.chu_uuid)
        h.chu_uuid AS community_unit_id,
        h.chu_name AS community_unit
    FROM {{ source(var('source_schema'), 'chp_hierarchy') }} h
    WHERE h.chu_uuid IS NOT NULL
      AND h.chu_name IS NOT NULL
    ORDER BY h.chu_uuid, h.saved_timestamp DESC
),

base AS (
    SELECT DISTINCT ON (h.chp_area_uuid)
        h.chp_area_uuid  AS chp_area_id,
        h.chp_area_name  AS chp_area,
        h.chu_uuid       AS community_unit_id
    FROM {{ source(var('source_schema'), 'chp_hierarchy') }} h
    WHERE h.chp_muted IS NULL
      AND h.chp_area_uuid IS NOT NULL
    ORDER BY h.chp_area_uuid, h.saved_timestamp DESC
)

SELECT
    b.chp_area_id,
    b.chp_area,
    b.community_unit_id,
    cu_latest.community_unit,
    sc.location_id AS sub_county_id,
    sc.name        AS sub_county,
    co.location_id AS county_id,
    co.name        AS county
FROM base b
LEFT JOIN latest_cu_name cu_latest
    ON cu_latest.community_unit_id = b.community_unit_id
LEFT JOIN {{ ref('dim_location') }} cu
    ON cu.location_id = b.community_unit_id
LEFT JOIN {{ ref('dim_location') }} sc
    ON sc.location_id = cu.parent_uuid
LEFT JOIN {{ ref('dim_location') }} co
    ON co.location_id = sc.parent_uuid
