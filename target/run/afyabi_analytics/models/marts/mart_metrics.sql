
  create view "echis"."afyabi"."mart_metrics__dbt_tmp"
    
    
  as (
    

/*
  mart_metrics: Primary dashboard query interface.
  
  Joins fact_aggregate with location hierarchy and period dimensions.
  This is a VIEW so it always reflects the latest fact_aggregate data
  with zero rebuild time.
*/

SELECT
    f.location_id   AS chp_area_id,
    f.period_id,
    f.metric_id,
    f.value,
    f.last_updated,
    lh.chp_area,
    lh.community_unit_id,
    lh.community_unit,
    lh.sub_county_id,
    lh.sub_county,
    lh.county_id,
    lh.county,
    dp.label         AS period_label,
    dp.period_type,
    dp.start_date    AS period_start,
    dp.end_date      AS period_end,
    dp.period_id_name
FROM "echis"."afyabi"."fact_aggregate" f
INNER JOIN "echis"."afyabi"."mv_location_hierarchy" lh ON lh.chp_area_id = f.location_id
INNER JOIN "echis"."afyabi"."dim_period" dp ON dp.period_id = f.period_id
  );