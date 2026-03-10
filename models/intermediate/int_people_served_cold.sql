-- depends_on: {{ ref('dim_period') }}
{{
  config(
    materialized = 'table',
    pre_hook = "SET LOCAL work_mem = '1GB'",
    indexes = [
      {'columns': ['chp_area_id', 'period_id']},
      {'columns': ['period_id']}
    ],
    tags = ['cadence_daily', 'intermediate']
  )
}}

/*
  int_people_served_cold: Distinct patients served by CHP area x period.
  
  Queries the PARTITIONED source table directly instead of stg_data_record.
  The source (data_record) is partitioned by RANGE(reported) with 77+ partitions.
  PostgreSQL prunes to only the relevant partitions for each period's date range.
  
  Column mapping: parent_uuid = chp_area_id, patient_id = patient_id
*/

SELECT
    s.parent_uuid       AS chp_area_id,
    p.period_id,
    COUNT(DISTINCT s.patient_id) AS people_served
FROM {{ source(var('source_schema'), 'data_record') }} s
INNER JOIN {{ ref('dim_period') }} p
    ON s.reported >= p.start_date
   AND s.reported < (p.end_date + INTERVAL '1 day')
WHERE s.parent_uuid IS NOT NULL
  AND s.patient_id IS NOT NULL
  AND p.refresh_tier = 'cold'
GROUP BY s.parent_uuid, p.period_id
