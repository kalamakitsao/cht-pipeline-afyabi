/*
  macro: generate_period_agg

  The core challenge: COUNT(DISTINCT patient_id) cannot be summed across days.
  A patient seen Monday and Tuesday = 1 distinct patient for the week, not 2.

  Strategy: For each intermediate model, we query the staging table ONCE and
  compute all 15 periods simultaneously using conditional aggregation with
  FILTER (WHERE ...) clauses. PostgreSQL evaluates FILTER clauses very
  efficiently as they are bitmask operations on the same scan.

  This means ONE scan of the staging table produces all 15 period results.
  No CROSS JOIN with dim_period. No repeated scans.

  The reported timestamp is compared against period boundaries passed in
  as literal values from dim_period.
*/

{% macro period_filter(reported_col, period_alias) %}
  FILTER (WHERE {{ reported_col }} >= {{ period_alias }}_start
            AND {{ reported_col }} <  {{ period_alias }}_end + INTERVAL '1 day')
{% endmacro %}
