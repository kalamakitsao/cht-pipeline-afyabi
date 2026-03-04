/*
  macro: aggregate_by_period

  PROBLEM WITH PARTITIONED TABLES:
  ================================
  PostgreSQL partition pruning requires the planner to know partition key
  bounds at plan time (static pruning) or at least at execution time per
  loop iteration (dynamic pruning via nested loop).

  The pattern:
    FROM staging s INNER JOIN periods p ON s.reported >= p.start_date AND s.reported < p.end_excl
  
  Often results in a hash join that scans ALL partitions, then filters.
  The planner doesn't propagate the period bounds into partition constraints.

  SOLUTION:
  =========
  Generate a UNION ALL of per-period queries, each with literal date
  bounds in WHERE clauses. PostgreSQL's static partition pruning works
  perfectly with literal values:

    SELECT ... FROM staging WHERE reported >= '2026-02-22' AND reported < '2026-03-02'  -- Last 7 Days
    UNION ALL
    SELECT ... FROM staging WHERE reported >= '2026-03-01' AND reported < '2026-03-02'  -- Today
    UNION ALL
    ...

  Each UNION ALL branch prunes to exactly the needed partitions.

  TRADE-OFF:
  The staging table is referenced 15 times (once per period), but each
  reference only hits the relevant partitions. For the biggest table
  (household_visit, 118M rows partitioned monthly):
  - "Today" scans 1 partition (~3-4M rows)
  - "Last 7 Days" scans 1-2 partitions
  - "All Time" scans all partitions (unavoidable)
  
  Total I/O is comparable to a single full scan but with much better
  parallelism opportunity and memory usage per branch.

  USAGE:
  Call this macro from intermediate models. It takes the staging ref,
  the grouping column, and a list of aggregate expressions.
*/

{% macro aggregate_by_period(staging_ref, group_col, agg_expressions, where_clause='') %}

{#
  We read dim_period at compile time to generate literal dates.
  Since dim_period is a small static table rebuilt daily, this is safe.
  The Jinja loop generates one SELECT per period with literal bounds.
#}

{% set periods_query %}
    SELECT period_id, start_date, end_date, end_date + INTERVAL '1 day' AS end_excl
    FROM {{ ref('dim_period') }}
{% endset %}

{% if execute %}
  {% set period_rows = run_query(periods_query) %}
{% else %}
  {% set period_rows = [] %}
{% endif %}

{% for row in period_rows %}
{% if not loop.first %}
UNION ALL
{% endif %}
SELECT
    {{ group_col }},
    {{ row['period_id'] }} AS period_id,
    {{ agg_expressions }}
FROM {{ staging_ref }}
WHERE reported >= '{{ row['start_date'] }}'::date
  AND reported <  '{{ row['end_excl'] }}'::date
  AND {{ group_col }} IS NOT NULL
  {% if where_clause %}AND {{ where_clause }}{% endif %}
GROUP BY {{ group_col }}
{% endfor %}

{% endmacro %}
