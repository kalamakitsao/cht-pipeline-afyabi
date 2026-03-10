{{ config(
    materialized = 'table',
    schema = 'aggregates',
    indexes = [
      {"columns": ["location_id", "period_id", "metric_id"], "unique": true},
      {"columns": ["chp_area_id"]},
      {"columns": ["county_id"]},
      {"columns": ["period_label"]},
      {"columns": ["metric_group"]},
      {"columns": ["last_updated"]}
    ],
    tags = ['cadence_daily', 'mart']
) }}

/*
  CHP-area level detail — no aggregation needed, just enrichment
  of fact_aggregate with location hierarchy, period, and metric labels.
*/

SELECT
    'chp area'               AS level,
    lh.chp_area_id,
    lh.chp_area,
    lh.community_unit_id,
    lh.community_unit,
    lh.sub_county_id,
    lh.sub_county,
    lh.county_id,
    lh.county,
    dp.start_date            AS period_start,
    dp.end_date              AS period_end,
    dp.label                 AS period_label,
    dm.group_name            AS metric_group,
    dm.metric_group_id,
    dm.name                  AS metric,
    fa.value,
    fa.location_id,
    fa.period_id,
    fa.metric_id,
    fa.last_updated
FROM {{ ref('fact_aggregate') }} fa
INNER JOIN {{ ref('mv_location_hierarchy') }} lh ON lh.chp_area_id = fa.location_id
INNER JOIN {{ ref('dim_period') }} dp ON dp.period_id = fa.period_id
INNER JOIN {{ source(var('source_schema'), 'dim_metric') }} dm ON dm.metric_id = fa.metric_id