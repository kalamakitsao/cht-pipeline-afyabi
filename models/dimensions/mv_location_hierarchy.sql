{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['chp_area_id'], 'unique': true},
      {'columns': ['community_unit_id']},
      {'columns': ['sub_county_id']},
      {'columns': ['county_id']}
    ],
    tags = ['cadence_hourly']
  )
}}

/*
  Denormalized location hierarchy.
  Base: chp_hierarchy (pre-flattened).
  IDs: dim_location provides sub_county_id and county_id.
  DISTINCT ON: ensures one row per chp_area_uuid in case chp_hierarchy
  has duplicates (multiple records per CHP area).
*/

SELECT DISTINCT ON (h.chp_area_uuid)
    h.chp_area_uuid     AS chp_area_id,
    h.chp_area_name      AS chp_area,
    h.chu_uuid           AS community_unit_id,
    h.chu_name           AS community_unit,
    sc.location_id       AS sub_county_id,
    h.sub_county_name    AS sub_county,
    co.location_id       AS county_id,
    h.county_name        AS county
FROM {{ source(var('source_schema'), 'chp_hierarchy') }} h
LEFT JOIN {{ ref('dim_location') }} cu ON cu.location_id = h.chu_uuid
LEFT JOIN {{ ref('dim_location') }} sc ON sc.location_id = cu.parent_uuid
LEFT JOIN {{ ref('dim_location') }} co ON co.location_id = sc.parent_uuid
WHERE h.chp_muted IS NULL
  AND h.chp_area_uuid IS NOT NULL
ORDER BY h.chp_area_uuid, h.saved_timestamp DESC