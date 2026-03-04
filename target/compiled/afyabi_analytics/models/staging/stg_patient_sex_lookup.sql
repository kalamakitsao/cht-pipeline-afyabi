

/*
  stg_patient_sex_lookup: Minimal 2-column table for denormalizing sex
  into domain staging tables.
  
  Why separate from stg_patient?
  - stg_patient is ~13GB with 67 columns
  - This table is ~2 columns × 30M rows ≈ ~1GB
  - Index-only scans on (patient_id) → sex are extremely fast
  - Joined ONCE at staging time, never at aggregation time
*/

SELECT
    uuid    AS patient_id,
    sex
FROM "echis"."afyabi"."patient_f_client"
WHERE uuid IS NOT NULL

  AND uuid NOT IN (SELECT patient_id FROM "echis"."afyabi"."stg_patient_sex_lookup")
