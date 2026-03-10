{{ config(
    materialized = 'table',
    indexes = [
      {"columns": ["county_id", "sub_county_id", "community_unit_id", "period_id", "metric_id"], "unique": true},
      {"columns": ["period_label"]},
      {"columns": ["metric_group"]},
      {"columns": ["last_updated"]}
    ],
    tags = ['cadence_daily', 'mart']
) }}

SELECT
    'community unit'         AS level,
    lh.county_id,
    lh.county,
    lh.sub_county_id,
    lh.sub_county,
    lh.community_unit_id,
    lh.community_unit,
    dp.start_date            AS period_start,
    dp.end_date              AS period_end,
    dp.label                 AS period_label,
    dm.group_name            AS metric_group,
    dm.metric_group_id,
    dm.name                  AS metric,
    SUM(fa.value)            AS value,
    fa.period_id,
    fa.metric_id,
    MAX(fa.last_updated)     AS last_updated
FROM {{ ref('fact_aggregate') }} fa
INNER JOIN {{ ref('mv_location_hierarchy') }} lh ON lh.chp_area_id = fa.location_id
INNER JOIN {{ ref('dim_period') }} dp ON dp.period_id = fa.period_id
INNER JOIN {{ ref('dim_metric') }} dm ON dm.metric_id = fa.metric_id
GROUP BY
    lh.county_id, lh.county,
    lh.sub_county_id, lh.sub_county,
    lh.community_unit_id, lh.community_unit,
    dp.start_date, dp.end_date, dp.label,
    dm.group_name, dm.metric_group_id, dm.name,
    fa.period_id, fa.metric_id
