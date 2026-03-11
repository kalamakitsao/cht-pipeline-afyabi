{{ config(
    materialized = 'table',
    indexes = [
      {"columns": ["period_id", "metric_id"], "unique": true},
      {"columns": ["period_label"]},
      {"columns": ["metric_group"]},
      {"columns": ["last_updated"]}
    ],
    tags = ['cadence_hourly', 'mart']
) }}

/*
  National-level aggregation.
  Group by the true grain only: (period_id, metric_id).
  Descriptive columns are projected with MAX() so label drift never creates
  duplicate business keys.
*/

SELECT
    'national'               AS level,
    'Kenya'                  AS name,
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
INNER JOIN {{ ref('dim_period') }} dp
    ON dp.period_id = fa.period_id
INNER JOIN {{ source(var('source_schema'), 'dim_metric') }} dm
    ON dm.metric_id = fa.metric_id
GROUP BY
    fa.period_id,
    fa.metric_id
