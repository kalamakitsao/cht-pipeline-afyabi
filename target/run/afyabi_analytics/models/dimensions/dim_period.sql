
  
    

  create  table "echis"."afyabi"."dim_period__dbt_tmp"
  
  
    as
  
  (
    

WITH periods AS (
    SELECT 1 AS period_id, 'Today' AS label, 'daily' AS period_type,
           CURRENT_DATE AS start_date, CURRENT_DATE AS end_date, 'today' AS period_id_name
    UNION ALL SELECT 2, 'Yesterday', 'daily',
           CURRENT_DATE - 1, CURRENT_DATE - 1, 'yesterday'
    UNION ALL SELECT 3, 'Last 7 Days', 'daily',
           CURRENT_DATE - 7, CURRENT_DATE, 'last_7_days'
    UNION ALL SELECT 4, 'Last 1 Month', 'monthly',
           (CURRENT_DATE - INTERVAL '1 month')::date, CURRENT_DATE, 'last_1_month'
    UNION ALL SELECT 5, 'Last 3 Months', 'monthly',
           (CURRENT_DATE - INTERVAL '3 months')::date, CURRENT_DATE, 'last_3_months'
    UNION ALL SELECT 6, 'Last 6 Months', 'monthly',
           (CURRENT_DATE - INTERVAL '6 months')::date, CURRENT_DATE, 'last_6_months'
    UNION ALL SELECT 7, 'Last 1 Year', 'yearly',
           (CURRENT_DATE - INTERVAL '1 year')::date, CURRENT_DATE, 'last_1_yr'
    UNION ALL SELECT 8, 'This Week', 'weekly',
           DATE_TRUNC('week', CURRENT_DATE)::date, CURRENT_DATE, 'this_week'
    UNION ALL SELECT 9, 'Last Week', 'weekly',
           DATE_TRUNC('week', CURRENT_DATE - INTERVAL '7 days')::date,
           (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 day')::date, 'last_week'
    UNION ALL SELECT 10, 'This Month', 'monthly',
           DATE_TRUNC('month', CURRENT_DATE)::date, CURRENT_DATE, 'this_month'
    UNION ALL SELECT 11, 'Last Month', 'monthly',
           DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')::date,
           (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 day')::date, 'last_month'
    UNION ALL SELECT 12, 'This Quarter', 'quarterly',
           DATE_TRUNC('quarter', CURRENT_DATE)::date, CURRENT_DATE, 'this_quarter'
    UNION ALL SELECT 13, 'Last Quarter', 'quarterly',
           DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '3 months')::date,
           (DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day')::date, 'last_quarter'
    UNION ALL SELECT 14, 'Year to Date (YTD)', 'yearly',
           DATE_TRUNC('year', CURRENT_DATE)::date, CURRENT_DATE, 'ytd'
    UNION ALL SELECT 15, 'All Time', 'all_time',
           DATE '2020-01-01', CURRENT_DATE, 'all_time'
)
SELECT * FROM periods
  );
  