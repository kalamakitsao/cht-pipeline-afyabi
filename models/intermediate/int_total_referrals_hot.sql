{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['chp_area_id', 'period_id']},
      {'columns': ['period_id']}
    ],
    tags = ['cadence_hourly', 'intermediate']
  )
}}

/*
  int_total_referrals_hot: u5_referred + over_5_referred.
  Reads from pre-computed tiered models — no raw table scans.
*/

SELECT
    COALESCE(u5.chp_area_id, o5.chp_area_id) AS chp_area_id,
    COALESCE(u5.period_id, o5.period_id)       AS period_id,
    COALESCE(u5.u5_referred, 0) + COALESCE(o5.over_5_referred, 0) AS total_referrals,
    NOW() AS last_updated
FROM {{ ref('int_u5_metrics_hot') }} u5
FULL OUTER JOIN {{ ref('int_over_five_metrics_hot') }} o5
    ON u5.chp_area_id = o5.chp_area_id AND u5.period_id = o5.period_id
