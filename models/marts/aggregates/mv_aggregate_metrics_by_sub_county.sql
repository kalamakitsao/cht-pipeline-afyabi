{{ config(
    materialized = 'table',
    indexes = [
      {"columns": ["county_id", "sub_county_id", "period_id", "metric_id"], "unique": true},
      {"columns": ["period_label"]},
      {"columns": ["metric_group"]},
      {"columns": ["last_updated"]}
    ],
    tags = ['cadence_daily', 'mart']
) }}

/*
  Sub-county level aggregation.
  Group by the true grain only and project labels with MAX() to avoid
  duplicate-key failures caused by descriptive-field drift.
*/

SELECT
    'sub county'             AS level,
    lh.county_id,
    MAX(lh.county)           AS county,
    lh.sub_county_id,
    MAX(lh.sub_county)       AS sub_county,
    MAX(dp.start_date)       AS period_start,
    MAX(dp.end_date)         AS period_end,
    MAX(dp.label)            AS period_label,
    MAX(dm.group_name)       AS metric_group,
    MAX(dm.metric_group_id)  AS metric_group_id,
    MAX(dm.name)             AS metric,
    SUM(fa.value)            AS value,
    fa.period_id,
    fa.metric_id,
    MAX(fa.last_updated)     AS last_updated
FROM {{ ref('fact_aggregate') }} fa
INNER JOIN {{ ref('mv_location_hierarchy') }} lh
    ON lh.chp_area_id = fa.location_id
INNER JOIN {{ ref('dim_period') }} dp
    ON dp.period_id = fa.period_id
INNER JOIN {{ source(var('source_schema'), 'dim_metric') }} dm
    ON dm.metric_id = fa.metric_id
GROUP BY
    lh.county_id,
    lh.sub_county_id,
    fa.period_id,
    fa.metric_id
