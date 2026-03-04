
  
    

  create  table "echis"."afyabi"."fact_aggregate__dbt_tmp"
  
  
    as
  
  (
    

/*
  fact_aggregate: Central EAV fact table (full rebuild).
  
  All intermediate models are materialized='table' (full rebuilds)
  to guarantee exact COUNT(DISTINCT) across all period ranges.
  This unpivot reads those small result tables and produces the final output.
  
  Output size: ~N_chp_areas × 15_periods × ~132_metrics
  With ~5K CHP areas: ~5000 × 15 × 132 = ~10M rows (very manageable)
*/








SELECT
    chp_area_id     AS location_id,
    period_id,
    'actively_pregnant_women'  AS metric_id,
    COALESCE(actively_pregnant_women, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pregnancy_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'currently_pregnant'  AS metric_id,
    COALESCE(currently_pregnant, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pregnancy_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'new_pregnancies'  AS metric_id,
    COALESCE(new_pregnancies, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pregnancy_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'pregnant_women_visited'  AS metric_id,
    COALESCE(pregnant_women_visited, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pregnancy_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'pregnant_women_referred'  AS metric_id,
    COALESCE(pregnant_women_referred, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pregnancy_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'pregnant_women_referred_missed_anc'  AS metric_id,
    COALESCE(pregnant_women_referred_missed_anc, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pregnancy_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'new_pregnant_women_referred_anc'  AS metric_id,
    COALESCE(new_pregnant_women_referred_anc, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pregnancy_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'teen_pregnancies'  AS metric_id,
    COALESCE(teen_pregnancies, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pregnancy_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'new_teen_pregnancies'  AS metric_id,
    COALESCE(new_teen_pregnancies, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pregnancy_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'new_teen_pregnant_women_referred_anc'  AS metric_id,
    COALESCE(new_teen_pregnant_women_referred_anc, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pregnancy_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'first_trimester_pregnancies'  AS metric_id,
    COALESCE(first_trimester_pregnancies, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pregnancy_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'repeat_pregnancies'  AS metric_id,
    COALESCE(repeat_pregnancies, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pregnancy_metrics"




UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_assessed'  AS metric_id,
    COALESCE(u5_assessed, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_referred'  AS metric_id,
    COALESCE(u5_referred, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_referred_female'  AS metric_id,
    COALESCE(u5_referred_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_referred_male'  AS metric_id,
    COALESCE(u5_referred_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_assessed_diarrhoea'  AS metric_id,
    COALESCE(u5_assessed_diarrhoea, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_assessed_diarrhoea_female'  AS metric_id,
    COALESCE(u5_assessed_diarrhoea_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_assessed_diarrhoea_male'  AS metric_id,
    COALESCE(u5_assessed_diarrhoea_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_assessed_malaria'  AS metric_id,
    COALESCE(u5_assessed_malaria, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_assessed_malaria_female'  AS metric_id,
    COALESCE(u5_assessed_malaria_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_assessed_malaria_male'  AS metric_id,
    COALESCE(u5_assessed_malaria_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_assessed_malnutrition'  AS metric_id,
    COALESCE(u5_assessed_malnutrition, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_assessed_malnutrition_female'  AS metric_id,
    COALESCE(u5_assessed_malnutrition_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_assessed_malnutrition_male'  AS metric_id,
    COALESCE(u5_assessed_malnutrition_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_assessed_pneumonia'  AS metric_id,
    COALESCE(u5_assessed_pneumonia, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_assessed_pneumonia_female'  AS metric_id,
    COALESCE(u5_assessed_pneumonia_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_assessed_pneumonia_male'  AS metric_id,
    COALESCE(u5_assessed_pneumonia_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_assessed_exclusive_breastfeeding'  AS metric_id,
    COALESCE(u5_assessed_exclusive_breastfeeding, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_confirmed_malaria_cases'  AS metric_id,
    COALESCE(u5_confirmed_malaria_cases, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_diarrhea_cases'  AS metric_id,
    COALESCE(u5_diarrhea_cases, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_malnutrition_cases'  AS metric_id,
    COALESCE(u5_malnutrition_cases, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_malnutrition_female'  AS metric_id,
    COALESCE(u5_malnutrition_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_malnutrition_male'  AS metric_id,
    COALESCE(u5_malnutrition_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_pneumonia_cases'  AS metric_id,
    COALESCE(u5_pneumonia_cases, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_treated_malaria'  AS metric_id,
    COALESCE(u5_treated_malaria, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_treated_diarrhoea'  AS metric_id,
    COALESCE(u5_treated_diarrhoea, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_treated_pneumonia'  AS metric_id,
    COALESCE(u5_treated_pneumonia, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_exclusive_breastfeeding'  AS metric_id,
    COALESCE(u5_exclusive_breastfeeding, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_tested_malaria'  AS metric_id,
    COALESCE(u5_tested_malaria, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'u5_suspected_malaria_cases'  AS metric_id,
    COALESCE(u5_suspected_malaria_cases, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_for_diarrhoea'  AS metric_id,
    COALESCE(referred_for_diarrhoea, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_for_malaria'  AS metric_id,
    COALESCE(referred_for_malaria, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_for_malnutrition'  AS metric_id,
    COALESCE(referred_for_malnutrition, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_for_pneumonia'  AS metric_id,
    COALESCE(referred_for_pneumonia, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_for_development_milestones'  AS metric_id,
    COALESCE(referred_for_development_milestones, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'female_referred_for_development_milestones'  AS metric_id,
    COALESCE(female_referred_for_development_milestones, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'male_referred_for_development_milestones'  AS metric_id,
    COALESCE(male_referred_for_development_milestones, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"




UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'over_5_assessments'  AS metric_id,
    COALESCE(over_5_assessments, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'over_5_referred'  AS metric_id,
    COALESCE(over_5_referred, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'over_5_referred_female'  AS metric_id,
    COALESCE(over_5_referred_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'over_5_referred_male'  AS metric_id,
    COALESCE(over_5_referred_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'screened_diabetes'  AS metric_id,
    COALESCE(screened_diabetes, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'screened_diabetes_female'  AS metric_id,
    COALESCE(screened_diabetes_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'screened_diabetes_male'  AS metric_id,
    COALESCE(screened_diabetes_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'screenings_diabetes'  AS metric_id,
    COALESCE(screenings_diabetes, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'screenings_diabetes_female'  AS metric_id,
    COALESCE(screenings_diabetes_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'screenings_diabetes_male'  AS metric_id,
    COALESCE(screenings_diabetes_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_diabetes'  AS metric_id,
    COALESCE(referred_diabetes, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_diabetes_female'  AS metric_id,
    COALESCE(referred_diabetes_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_diabetes_male'  AS metric_id,
    COALESCE(referred_diabetes_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'screened_hypertension'  AS metric_id,
    COALESCE(screened_hypertension, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'screened_hypertension_female'  AS metric_id,
    COALESCE(screened_hypertension_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'screened_hypertension_male'  AS metric_id,
    COALESCE(screened_hypertension_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'screenings_hypertension'  AS metric_id,
    COALESCE(screenings_hypertension, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'screenings_hypertension_female'  AS metric_id,
    COALESCE(screenings_hypertension_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'screenings_hypertension_male'  AS metric_id,
    COALESCE(screenings_hypertension_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_hypertension'  AS metric_id,
    COALESCE(referred_hypertension, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_hypertension_female'  AS metric_id,
    COALESCE(referred_hypertension_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_hypertension_male'  AS metric_id,
    COALESCE(referred_hypertension_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'screened_mental_health'  AS metric_id,
    COALESCE(screened_mental_health, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'screened_mental_health_female'  AS metric_id,
    COALESCE(screened_mental_health_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'screened_mental_health_male'  AS metric_id,
    COALESCE(screened_mental_health_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'screenings_mental_health'  AS metric_id,
    COALESCE(screenings_mental_health, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'screenings_mental_health_female'  AS metric_id,
    COALESCE(screenings_mental_health_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'screenings_mental_health_male'  AS metric_id,
    COALESCE(screenings_mental_health_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_mental_health'  AS metric_id,
    COALESCE(referred_mental_health, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_mental_health_female'  AS metric_id,
    COALESCE(referred_mental_health_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_mental_health_male'  AS metric_id,
    COALESCE(referred_mental_health_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_over_five_metrics"




UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'total_deaths'  AS metric_id,
    COALESCE(total_deaths, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_death_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'maternal_deaths'  AS metric_id,
    COALESCE(maternal_deaths, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_death_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'neonatal_deaths'  AS metric_id,
    COALESCE(neonatal_deaths, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_death_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'child_deaths'  AS metric_id,
    COALESCE(child_deaths, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_death_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'child_deaths_female'  AS metric_id,
    COALESCE(child_deaths_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_death_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'child_deaths_male'  AS metric_id,
    COALESCE(child_deaths_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_death_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'over_5_deaths_female'  AS metric_id,
    COALESCE(over_5_deaths_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_death_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'over_5_deaths_male'  AS metric_id,
    COALESCE(over_5_deaths_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_death_metrics"




UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'deliveries'  AS metric_id,
    COALESCE(deliveries, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pnc_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'skilled_birth_attendance'  AS metric_id,
    COALESCE(skilled_birth_attendance, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pnc_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_for_pnc'  AS metric_id,
    COALESCE(referred_for_pnc, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pnc_metrics"




UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'newborns'  AS metric_id,
    COALESCE(newborns, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pnc_newborn_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'newborns_needing_follow_up'  AS metric_id,
    COALESCE(newborns_needing_follow_up, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pnc_newborn_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'newborns_needing_follow_up_danger_signs'  AS metric_id,
    COALESCE(newborns_needing_follow_up_danger_signs, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pnc_newborn_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'newborns_needing_follow_up_missed_visit'  AS metric_id,
    COALESCE(newborns_needing_follow_up_missed_visit, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pnc_newborn_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'needs_follow_up_danger_signs'  AS metric_id,
    COALESCE(needs_follow_up_danger_signs, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pnc_newborn_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'needs_follow_up_missed_visit'  AS metric_id,
    COALESCE(needs_follow_up_missed_visit, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_pnc_newborn_metrics"




UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_immunization'  AS metric_id,
    COALESCE(referred_immunization, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_immunization_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_immunization_female'  AS metric_id,
    COALESCE(referred_immunization_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_immunization_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_immunization_male'  AS metric_id,
    COALESCE(referred_immunization_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_immunization_metrics"




UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'under_1_immunised'  AS metric_id,
    COALESCE(under_1_immunised, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_immunization_status_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'under_1_immunised_female'  AS metric_id,
    COALESCE(under_1_immunised_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_immunization_status_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'under_1_immunised_male'  AS metric_id,
    COALESCE(under_1_immunised_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_immunization_status_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'needs_deworming_follow_up_female'  AS metric_id,
    COALESCE(needs_deworming_follow_up_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_immunization_status_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'needs_deworming_follow_up_male'  AS metric_id,
    COALESCE(needs_deworming_follow_up_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_immunization_status_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_growth_monitoring_female'  AS metric_id,
    COALESCE(referred_growth_monitoring_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_immunization_status_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_growth_monitoring_male'  AS metric_id,
    COALESCE(referred_growth_monitoring_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_immunization_status_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_missed_vaccine'  AS metric_id,
    COALESCE(referred_missed_vaccine, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_immunization_status_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_missed_vaccine_female'  AS metric_id,
    COALESCE(referred_missed_vaccine_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_immunization_status_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'referred_missed_vaccine_male'  AS metric_id,
    COALESCE(referred_missed_vaccine_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_immunization_status_metrics"




UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'hh_visited'  AS metric_id,
    COALESCE(hh_visited, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_household_visit_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'chps_reporting'  AS metric_id,
    COALESCE(chps_reporting, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_household_visit_metrics"




UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'population'  AS metric_id,
    COALESCE(population, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_population_household_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'population_female'  AS metric_id,
    COALESCE(population_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_population_household_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'population_male'  AS metric_id,
    COALESCE(population_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_population_household_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'under_5_population'  AS metric_id,
    COALESCE(under_5_population, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_population_household_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'population_under_5_female'  AS metric_id,
    COALESCE(population_under_5_female, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_population_household_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'population_under_5_male'  AS metric_id,
    COALESCE(population_under_5_male, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_population_household_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'children_turning_one'  AS metric_id,
    COALESCE(children_turning_one, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_population_household_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'female_turning_one'  AS metric_id,
    COALESCE(female_turning_one, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_population_household_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'male_turning_one'  AS metric_id,
    COALESCE(male_turning_one, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_population_household_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'households_registered'  AS metric_id,
    COALESCE(households_registered, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_population_household_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'households_assessed_sha'  AS metric_id,
    COALESCE(households_assessed_sha, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_population_household_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'households_registered_on_sha'  AS metric_id,
    COALESCE(households_registered_on_sha, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_population_household_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'households_with_sha'  AS metric_id,
    COALESCE(households_with_sha, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_population_household_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'monthly_cu_meetings'  AS metric_id,
    COALESCE(monthly_cu_meetings, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_population_household_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'other_community_events'  AS metric_id,
    COALESCE(other_community_events, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_population_household_metrics"


UNION ALL


SELECT
    chp_area_id     AS location_id,
    period_id,
    'people_served'  AS metric_id,
    COALESCE(people_served, 0)::bigint AS value,
    last_updated
FROM "echis"."afyabi"."int_population_household_metrics"



-- Composite: total_referrals = U5 + over-5 referred
UNION ALL
SELECT
    COALESCE(u5.chp_area_id, o5.chp_area_id),
    COALESCE(u5.period_id, o5.period_id),
    'total_referrals',
    (COALESCE(u5.u5_referred, 0) + COALESCE(o5.over_5_referred, 0))::bigint,
    GREATEST(u5.last_updated, o5.last_updated)
FROM "echis"."afyabi"."int_u5_metrics" u5
FULL OUTER JOIN "echis"."afyabi"."int_over_five_metrics" o5
    ON u5.chp_area_id = o5.chp_area_id AND u5.period_id = o5.period_id

-- Alias: referred_for_developmental_delays = referred_for_development_milestones
UNION ALL
SELECT
    chp_area_id, period_id,
    'referred_for_developmental_delays',
    COALESCE(referred_for_development_milestones, 0)::bigint,
    last_updated
FROM "echis"."afyabi"."int_u5_metrics"

-- Structural: chps_enrolled (1 per CHP area)
UNION ALL
SELECT
    lh.chp_area_id, pr.period_id, 'chps_enrolled', 1::bigint, NOW()
FROM "echis"."afyabi"."mv_location_hierarchy" lh
CROSS JOIN "echis"."afyabi"."dim_period" pr

-- Structural: chps_with_hholds
UNION ALL
SELECT
    h.chp_area_id, pr.period_id, 'chps_with_hholds', 1::bigint, NOW()
FROM (
    SELECT DISTINCT h.chp_area_id
    FROM "echis"."afyabi"."stg_household" h
    INNER JOIN "echis"."afyabi"."stg_contact" c ON c.uuid = h.household_id AND c.muted IS NULL
    WHERE h.chp_area_id IS NOT NULL
) h
CROSS JOIN "echis"."afyabi"."dim_period" pr

-- Derived: chps_meeting_target
UNION ALL
SELECT
    hvm.chp_area_id,
    hvm.period_id,
    'chps_meeting_target',
    CASE WHEN hvm.hh_visited::numeric / NULLIF(hh.households_registered, 0) >= 0.33
         THEN 1 ELSE 0 END::bigint,
    NOW()
FROM "echis"."afyabi"."int_household_visit_metrics" hvm
LEFT JOIN (
    SELECT chp_area_id, COUNT(household_id) AS households_registered
    FROM "echis"."afyabi"."stg_household"
    GROUP BY chp_area_id
) hh ON hh.chp_area_id = hvm.chp_area_id
  );
  