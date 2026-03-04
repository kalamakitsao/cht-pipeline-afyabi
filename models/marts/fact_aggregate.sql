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
  fact_aggregate: Central EAV fact table (full rebuild).
  
  All intermediate models are materialized='table' (full rebuilds)
  to guarantee exact COUNT(DISTINCT) across all period ranges.
  This unpivot reads those small result tables and produces the final output.
  
  Output size: ~N_chp_areas × 15_periods × ~132_metrics
  With ~5K CHP areas: ~5000 × 15 × 132 = ~10M rows (very manageable)
*/

{% set domains = [
    {
        'ref': 'int_pregnancy_metrics',
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
        'ref': 'int_u5_metrics',
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
        'ref': 'int_over_five_metrics',
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
        'ref': 'int_death_metrics',
        'metrics': [
            'total_deaths', 'maternal_deaths', 'neonatal_deaths',
            'child_deaths', 'child_deaths_female', 'child_deaths_male',
            'over_5_deaths_female', 'over_5_deaths_male'
        ]
    },
    {
        'ref': 'int_pnc_metrics',
        'metrics': ['deliveries', 'skilled_birth_attendance', 'referred_for_pnc']
    },
    {
        'ref': 'int_pnc_newborn_metrics',
        'metrics': [
            'newborns', 'newborns_needing_follow_up',
            'newborns_needing_follow_up_danger_signs', 'newborns_needing_follow_up_missed_visit',
            'needs_follow_up_danger_signs', 'needs_follow_up_missed_visit'
        ]
    },
    {
        'ref': 'int_immunization_metrics',
        'metrics': ['referred_immunization', 'referred_immunization_female', 'referred_immunization_male']
    },
    {
        'ref': 'int_immunization_status_metrics',
        'metrics': [
            'under_1_immunised', 'under_1_immunised_female', 'under_1_immunised_male',
            'needs_deworming_follow_up_female', 'needs_deworming_follow_up_male',
            'referred_growth_monitoring_female', 'referred_growth_monitoring_male',
            'referred_missed_vaccine', 'referred_missed_vaccine_female', 'referred_missed_vaccine_male'
        ]
    },
    {
        'ref': 'int_household_visit_metrics',
        'metrics': ['hh_visited', 'chps_reporting']
    },
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

{% set is_first = [true] %}
{% for domain in domains %}
{% for metric in domain['metrics'] %}
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
FROM {{ ref(domain['ref']) }}
{% endfor %}
{% endfor %}

-- Composite: total_referrals = U5 + over-5 referred
UNION ALL
SELECT
    COALESCE(u5.chp_area_id, o5.chp_area_id),
    COALESCE(u5.period_id, o5.period_id),
    'total_referrals',
    (COALESCE(u5.u5_referred, 0) + COALESCE(o5.over_5_referred, 0))::bigint,
    GREATEST(u5.last_updated, o5.last_updated)
FROM {{ ref('int_u5_metrics') }} u5
FULL OUTER JOIN {{ ref('int_over_five_metrics') }} o5
    ON u5.chp_area_id = o5.chp_area_id AND u5.period_id = o5.period_id

-- Alias: referred_for_developmental_delays = referred_for_development_milestones
UNION ALL
SELECT
    chp_area_id, period_id,
    'referred_for_developmental_delays',
    COALESCE(referred_for_development_milestones, 0)::bigint,
    last_updated
FROM {{ ref('int_u5_metrics') }}

-- Structural: chps_enrolled (1 per CHP area)
UNION ALL
SELECT
    lh.chp_area_id, pr.period_id, 'chps_enrolled', 1::bigint, NOW()
FROM {{ ref('mv_location_hierarchy') }} lh
CROSS JOIN {{ ref('dim_period') }} pr

-- Structural: chps_with_hholds
UNION ALL
SELECT
    h.chp_area_id, pr.period_id, 'chps_with_hholds', 1::bigint, NOW()
FROM (
    SELECT DISTINCT h.chp_area_id
    FROM {{ ref('stg_household') }} h
    INNER JOIN {{ ref('stg_contact') }} c ON c.uuid = h.household_id AND c.muted IS NULL
    WHERE h.chp_area_id IS NOT NULL
) h
CROSS JOIN {{ ref('dim_period') }} pr

-- Derived: chps_meeting_target
UNION ALL
SELECT
    hvm.chp_area_id,
    hvm.period_id,
    'chps_meeting_target',
    CASE WHEN hvm.hh_visited::numeric / NULLIF(hh.households_registered, 0) >= 0.33
         THEN 1 ELSE 0 END::bigint,
    NOW()
FROM {{ ref('int_household_visit_metrics') }} hvm
LEFT JOIN (
    SELECT chp_area_id, COUNT(household_id) AS households_registered
    FROM {{ ref('stg_household') }}
    GROUP BY chp_area_id
) hh ON hh.chp_area_id = hvm.chp_area_id