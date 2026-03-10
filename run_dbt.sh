#!/bin/bash
# =============================================================================
# AfyaBi dbt run script — sequential tier execution
# =============================================================================
#
# DO NOT run `dbt run` with all 62 models at once.
# 33 threads × multi-GB scans = I/O collapse.
#
# Instead, run in phases:
#
# Phase 1: Staging (incremental, fast) + dim_period
# Phase 2: Hot tier intermediates (3 periods, fast)  
# Phase 3: Warm tier intermediates (7 periods, medium)
# Phase 4: Cold tier intermediates (5 periods, slow but no contention)
# Phase 5: Population sub-models (heavy, run alone)
# Phase 6: Assembler + marts
# =============================================================================

set -e

echo "=== Phase 1: Staging + dim_period ==="
dbt run --select tag:staging dim_period --threads 6
# ~3-5 min. 6 threads is enough — staging is incremental, mostly fast.

echo "=== Phase 2: Hot tier ==="
dbt run --select tag:cadence_hourly --exclude tag:staging tag:mart dim_period --threads 6
# ~2 min. Today/Yesterday/This Week — tiny date ranges.

echo "=== Phase 3: Warm tier ==="
dbt run --select tag:cadence_6h --threads 4
# ~5-10 min. Last 7 Days through This Quarter.

echo "=== Phase 4: Cold tier (excluding population sub-models) ==="
dbt run --select tag:cadence_daily --exclude int_population_static int_population_age_dependent int_households int_sha int_population_household_metrics --threads 4
# ~15-20 min. Last 6 Months through All Time.

echo "=== Phase 5: Population sub-models (run with minimal contention) ==="
# These are the heaviest — run with low thread count so they don't compete
dbt run --select int_population_static int_population_age_dependent int_households int_sha int_people_served_cold --threads 3
# ~10-15 min. The big ones: 30M patient scan, 57M data_record scan.

echo "=== Phase 6: Assembler + marts ==="
dbt run --select int_population_household_metrics fact_aggregate mart_metrics --threads 2
# ~1 min. Joins pre-aggregated small tables.

echo "=== Done ==="
