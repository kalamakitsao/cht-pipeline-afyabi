
  
    

  create  table "echis"."afyabi"."mv_location_hierarchy__dbt_tmp"
  
  
    as
  
  (
    

/*
  Denormalized location hierarchy built by self-joining dim_location
  4 levels deep via parent_uuid.
  
  Hierarchy: county → sub_county → community_unit → chp_area
  
  contact_type values:
    a_]county                              = County
    b_sub_county                           = Sub-county
    c_community_health_unit                = Community Unit (CU)
    d_community_health_volunteer_area      = CHP Area (finest grain)
*/

SELECT
    chp.location_id     AS chp_area_id,
    chp.name            AS chp_area,
    cu.location_id      AS community_unit_id,
    cu.name             AS community_unit,
    sc.location_id      AS sub_county_id,
    sc.name             AS sub_county,
    co.location_id      AS county_id,
    co.name             AS county
FROM "echis"."afyabi"."dim_location" chp
LEFT JOIN "echis"."afyabi"."dim_location" cu  ON cu.location_id  = chp.parent_uuid
LEFT JOIN "echis"."afyabi"."dim_location" sc  ON sc.location_id  = cu.parent_uuid
LEFT JOIN "echis"."afyabi"."dim_location" co  ON co.location_id  = sc.parent_uuid
WHERE chp.contact_type = 'd_community_health_volunteer_area'
  AND chp.muted IS NULL
  );
  