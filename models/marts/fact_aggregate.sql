{{
  config(
    materialized = 'table',
    indexes = [
      {'columns': ['location_id', 'period_id', 'metric_id'], 'unique': true},
      {'columns': ['metric_id']},
      {'columns': ['period_id']},
      {'columns': ['location_id']}
    ],
    tags = ['cadence_hourly', 'mart']
  )
}}

/*
  fact_aggregate: Central EAV fact table.
  9 domains are tiered (hot/warm/cold).
  1 domain (population_household) is single (not tiered — scans all patients regardless).
*/

{% set tiered_domains = [
    {
        'base': 'int_u5_metrics',
        'metrics': [
            'u5_assessed', 'u5_referred', 'u5_referred_female', 'u5_referred_male',
            'u5_assessed_diarrhoea', 'u5_assessed_diarrhoea_female', 'u5_assessed_diarrhoea_male',
            'u5_assessed_malaria', 'u5_assessed_malaria_female', 'u5_assessed_malaria_male',
            'u5_assessed_malnutrition', 'u5_assessed_malnutrition_female', 'u5_assessed_malnutrition_male',
            'u5_assessed_pneumonia', 'u5_assessed_pneumonia_female', 'u5_assessed_pneumonia_male',
            'u5_assessed_exclusive_breastfeeding',
            'u5_confirmed_malaria_cases', 'u5_diarrhea_cases',
            'u5_malnutrition_cases', 'u5_malnutrition_female', 'u5_malnutrition_male',
            'u5_pneumonia_cases',
            'u5_treated_malaria', 'u5_treated_diarrhoea', 'u5_treated_pneumonia',
            'u5_exclusive_breastfeeding', 'u5_tested_malaria', 'u5_suspected_malaria_cases',
            'referred_for_diarrhoea', 'referred_for_malaria', 'referred_for_malnutrition',
            'referred_for_pneumonia',
            'referred_for_development_milestones',
            'female_referred_for_development_milestones',
            'male_referred_for_development_milestones'
        ]
    },
    {
        'base': 'int_over_five_metrics',
        'metrics': [
            'over_5_assessments', 'over_5_referred', 'over_5_referred_female', 'over_5_referred_male',
            'screened_diabetes', 'screened_diabetes_female', 'screened_diabetes_male',
            'screenings_diabetes', 'screenings_diabetes_female', 'screenings_diabetes_male',
            'referred_diabetes', 'referred_diabetes_female', 'referred_diabetes_male',
            'screened_hypertension', 'screened_hypertension_female', 'screened_hypertension_male',
            'screenings_hypertension', 'screenings_hypertension_female', 'screenings_hypertension_male',
            'referred_hypertension', 'referred_hypertension_female', 'referred_hypertension_male',
            'screened_mental_health', 'screened_mental_health_female', 'screened_mental_health_male',
            'screenings_mental_health', 'screenings_mental_health_female', 'screenings_mental_health_male',
            'referred_mental_health', 'referred_mental_health_female', 'referred_mental_health_male'
        ]
    },
    {
        'base': 'int_household_visit_metrics',
        'metrics': ['hh_visited', 'chps_reporting']
    },
    {
        'base': 'int_pregnancy_metrics',
        'metrics': [
            'actively_pregnant_women', 'currently_pregnant', 'new_pregnancies',
            'pregnant_women_visited', 'pregnant_women_referred',
            'pregnant_women_referred_missed_anc', 'new_pregnant_women_referred_anc',
            'teen_pregnancies', 'new_teen_pregnancies',
            'new_teen_pregnant_women_referred_anc', 'first_trimester_pregnancies',
            'repeat_pregnancies'
        ]
    },
    {
        'base': 'int_death_metrics',
        'metrics': [
            'total_deaths', 'maternal_deaths', 'neonatal_deaths',
            'child_deaths', 'child_deaths_female', 'child_deaths_male',
            'over_5_deaths_female', 'over_5_deaths_male'
        ]
    },
    {
        'base': 'int_pnc_metrics',
        'metrics': ['deliveries', 'skilled_birth_attendance', 'referred_for_pnc']
    },
    {
        'base': 'int_pnc_newborn_metrics',
        'metrics': [
            'newborns', 'newborns_needing_follow_up',
            'newborns_needing_follow_up_danger_signs', 'newborns_needing_follow_up_missed_visit',
            'needs_follow_up_danger_signs', 'needs_follow_up_missed_visit'
        ]
    },
    {
        'base': 'int_immunization_metrics',
        'metrics': ['referred_immunization', 'referred_immunization_female', 'referred_immunization_male']
    },
    {
        'base': 'int_immunization_status_metrics',
        'metrics': [
            'under_1_immunised', 'under_1_immunised_female', 'under_1_immunised_male',
            'needs_deworming_follow_up_female', 'needs_deworming_follow_up_male',
            'referred_growth_monitoring_female', 'referred_growth_monitoring_male',
            'referred_missed_vaccine', 'referred_missed_vaccine_female', 'referred_missed_vaccine_male'
        ]
    }
] %}

{% set single_domains = [
    {
        'ref': 'int_population_household_metrics',
        'metrics': [
            'population', 'population_female', 'population_male',
            'under_5_population', 'population_under_5_female', 'population_under_5_male',
            'children_turning_one', 'female_turning_one', 'male_turning_one',
            'households_registered',
            'households_assessed_sha', 'households_registered_on_sha', 'households_with_sha',
            'monthly_cu_meetings', 'other_community_events',
            'people_served'
        ]
    }
] %}

{% set tiers = ['hot', 'warm', 'cold'] %}

{% set is_first = [true] %}

{# ── Tiered domains ── #}
{% for domain in tiered_domains %}
{% for metric in domain['metrics'] %}
{% for tier in tiers %}
{% if not is_first[0] %}
UNION ALL
{% endif %}
{% if is_first[0] %}{% do is_first.pop() %}{% do is_first.append(false) %}{% endif %}
SELECT
    chp_area_id     AS location_id,
    period_id,
    '{{ metric }}'  AS metric_id,
    COALESCE({{ metric }}, 0)::bigint AS value,
    last_updated
FROM {{ ref(domain['base'] ~ '_' ~ tier) }}
{% endfor %}
{% endfor %}
{% endfor %}

{# ── Single (non-tiered) domains ── #}
{% for domain in single_domains %}
{% for metric in domain['metrics'] %}
UNION ALL
SELECT
    chp_area_id     AS location_id,
    period_id,
    '{{ metric }}'  AS metric_id,
    COALESCE({{ metric }}, 0)::bigint AS value,
    last_updated
FROM {{ ref(domain['ref']) }}
{% endfor %}
{% endfor %}
