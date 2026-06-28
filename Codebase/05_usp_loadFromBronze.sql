/*
================================================================================
05_usp_loadFromBronze.sql
================================================================================
silver.usp_loadFromBronze
-------------------------
Reads from bronze.encounters and does two things in one pass:

  1. VALIDATES — seven cross-field semantic rules (comorbidity sums,
     timestamp sequence, LOS, fraud signals, HEDIS gate, PDSA linkage,
     NT-proBNP plausibility). Violations are stored in _validation_flag;
     the row is still inserted into Silver so analysts can investigate it.
     NULL _validation_flag = clean row.

  2. ENGINEERS 28 features — severity tiers, glycemic tiers, burnout
     composites, throughput minutes, financial ratios, supply chain tiers,
     and temporal features — all computed with CASE WHEN, DATEDIFF, and
     arithmetic expressions.

Both steps run inside CTEs so the INSERT touches Silver exactly once.

================================================================================
*/
USE HCWarehouse_N2;
GO

CREATE OR ALTER PROCEDURE silver.usp_loadFromBronze
    @batch_id       NVARCHAR(20),   -- e.g. '20260622_143201'
    @rows_loaded    INT             = 0 OUTPUT,
    @rows_rejected  INT             = 0 OUTPUT,  -- unused; kept for interface parity
    @rows_flagged   INT             = 0 OUTPUT,
    @rows_clean     BIGINT          = 0 OUTPUT,
    @duration_ms    INT             = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = SYSUTCDATETIME();

    IF EXISTS (
        SELECT 1 FROM silver.encounters_clean WHERE _load_batch_id = @batch_id
    )
    BEGIN
        PRINT '  [SKIP] Batch ' + @batch_id
              + ' already present in silver.encounters_clean — no rows inserted.';
        PRINT '         To reprocess: EXEC dbo.usp_deleteBatch '''
              + @batch_id + ''';';
        SET @rows_loaded   = 0;
        SET @rows_rejected = 0;
        SET @rows_flagged  = 0;
        SET @rows_clean    = 0;
        SET @duration_ms   = DATEDIFF(MILLISECOND, @start_time, SYSUTCDATETIME());
        RETURN;
    END;

    -- TRUNCATE TABLE silver.encounters_clean;

    -- ── CTE 1: Cross-field validation ─────────────────────────────────────────
    -- Evaluates 7 rules. First match wins; NULL means the row is clean.
    -- To add all non valid codes to the same cell
    WITH validated AS (
        SELECT
            b.*,
            CASE
                -- Rule 1: comorbidity_count must equal the sum of three flags
                WHEN b.comorbidity_count <>
                     (b.has_diabetes + b.has_hypertension + b.has_chf)
                THEN 'COMORBIDITY_MISMATCH'

                -- Rule 2: timestamps must be in chronological order
                WHEN b.triage_timestamp  > b.admit_timestamp      THEN 'TRIAGE_AFTER_ADMIT'
                WHEN b.admit_timestamp   > b.bed_request_time     THEN 'BED_REQUEST_BEFORE_ADMIT'
                WHEN b.bed_request_time  > b.bed_assign_time      THEN 'BED_ASSIGN_BEFORE_REQUEST'
                WHEN b.or_start          > b.or_end               THEN 'OR_END_BEFORE_OR_START'
                WHEN b.or_end            > b.discharge_timestamp  THEN 'DISCHARGE_BEFORE_OR_END'

                -- Rule 3: LOS must match the date difference within ±1 day
                WHEN ABS(b.los_days -
                     DATEDIFF(DAY, b.admit_timestamp, b.discharge_timestamp)) > 1
                THEN 'LOS_INCONSISTENT'

                -- Rule 4: submitted charge must exceed allowed amount
                WHEN b.submitted_charge_usd < b.allowed_amount_usd
                THEN 'CHARGE_BELOW_ALLOWED'
                WHEN b.fraud_upcoding_flag = 1
                     AND b.submitted_charge_usd / NULLIF(b.allowed_amount_usd, 0) < 1.4
                THEN 'UPCODING_SIGNAL_WEAK'

                -- Rule 5: HEDIS poor control requires diabetes + tested + HbA1c > 9
                WHEN b.hedis_hba1c_poor_control = 1
                     AND NOT (b.has_diabetes = 1
                              AND b.hedis_hba1c_tested = 1
                              AND b.hba1c_pct > 9.0)
                THEN 'HEDIS_GATE_VIOLATION'

                -- Rule 6: every safety incident must have a PDSA cycle ID
                WHEN b.safety_incident_flag = 1
                     AND b.pdsa_cycle_id IS NULL
                THEN 'PDSA_MISSING_FOR_INCIDENT'

                -- Rule 7: CHF patients should have elevated NT-proBNP
                WHEN b.has_chf = 1
                     AND b.nt_probnp_pg_ml < 900
                THEN 'NTPROBNP_CHF_IMPLAUSIBLE'

                ELSE NULL   -- clean row
            END AS flag_code
        FROM bronze.encounters AS b
        WHERE b._load_batch_id = @batch_id
    ),

    -- ── CTE 2: Feature engineering ────────────────────────────────────────────
    -- Adds 28 derived columns on top of the validated rows.
    -- LEFT JOINs to reference tables supply benchmark values.
    engineered AS (
        SELECT
            v.*,

            -- Clinical risk tiers
            CASE
                WHEN v.severity_index < 0.25 THEN 'Low'
                WHEN v.severity_index < 0.50 THEN 'Moderate'
                WHEN v.severity_index < 0.75 THEN 'High'
                ELSE 'Critical'
            END AS severity_tier,

            CASE
                WHEN v.has_diabetes = 0    THEN NULL
                WHEN v.hba1c_pct < 7.0     THEN 'Controlled'
                WHEN v.hba1c_pct <= 9.0    THEN 'Borderline'
                ELSE 'Poor'
            END AS glycemic_control_tier,

            CASE
                WHEN v.bmi < 18.5  THEN 'Underweight'
                WHEN v.bmi < 25.0  THEN 'Normal'
                WHEN v.bmi < 30.0  THEN 'Overweight'
                WHEN v.bmi < 35.0  THEN 'Obese-I'
                WHEN v.bmi < 40.0  THEN 'Obese-II'
                ELSE 'Obese-III'
            END AS obesity_class,

            CASE
                WHEN v.sex = 'male'   AND v.creatinine_mg_dl > 1.2 THEN 1
                WHEN v.sex = 'female' AND v.creatinine_mg_dl > 1.0 THEN 1
                ELSE 0
            END AS ckd_risk_flag,

            CAST(CASE WHEN v.temperature_f >= 100.4       THEN 1 ELSE 0 END AS TINYINT) AS fever_flag,
            CAST(CASE WHEN v.heart_rate_bpm > 100         THEN 1 ELSE 0 END AS TINYINT) AS tachycardia_flag,
            CAST(CASE WHEN v.spo2_pct < 90                THEN 1 ELSE 0 END AS TINYINT) AS hypoxia_flag,
            CAST(CASE WHEN v.medication_adherence_pdc >= 0.80 THEN 1 ELSE 0 END AS TINYINT)
                AS medication_adherent_flag,

            -- Operational throughput (all in minutes)
            DATEDIFF(MINUTE, v.triage_timestamp,  v.admit_timestamp)  AS door_to_admit_minutes,
            DATEDIFF(MINUTE, v.bed_request_time,  v.bed_assign_time)  AS bed_wait_minutes,
            DATEDIFF(MINUTE, v.admit_timestamp,   v.or_start)         AS admit_to_or_minutes,

            CAST(
                v.actual_or_minutes /
                NULLIF(CAST(pb.expected_or_minutes AS DECIMAL(10,4)), 0)
            AS DECIMAL(10,4)) AS or_efficiency_ratio,   -- >1.0 = case ran over time

            CAST(CASE WHEN v.bed_occupancy_pct > 85 THEN 1 ELSE 0 END AS TINYINT) AS high_occupancy_flag,
            CAST(DATEADD(DAY, 30, v.discharge_timestamp) AS DATE)                 AS readmission_window_end,

            -- Financial analytics
            CASE
                WHEN v.payer IN ('Medicare', 'Medicaid') THEN 'Public'
                WHEN v.payer = 'Self-Pay'                THEN 'Self-Pay'
                ELSE 'Commercial'
            END AS payer_tier,

            CAST(v.submitted_charge_usd / NULLIF(v.allowed_amount_usd, 0)
                AS DECIMAL(10,4)) AS charge_inflation_ratio,   -- >1.4 on upcoding rows

            CAST(CASE WHEN v.fraud_upcoding_flag = 1
                           OR v.fraud_duplicate_flag = 1
                           OR v.fraud_unbundling_flag = 1
                 THEN 1 ELSE 0 END AS TINYINT) AS any_fraud_flag,

            CAST(ISNULL(cb.base_rate_usd, 6031.90) * v.drg_weight
                AS DECIMAL(12,2)) AS expected_reimbursement_usd,

            CAST(v.allowed_amount_usd - v.kit_unit_cost_usd
                AS DECIMAL(12,2)) AS margin_estimate_usd,

            -- HR and workforce
            CAST(
                (v.burnout_exhaustion_mbi
                 + v.burnout_cynicism_mbi
                 + (6.0 - v.burnout_personal_accomplishment_mbi)) / 3.0
            AS DECIMAL(8,4)) AS burnout_composite,
 
            CASE
                WHEN (v.burnout_exhaustion_mbi
                      + v.burnout_cynicism_mbi
                      + (6.0 - v.burnout_personal_accomplishment_mbi)) / 3.0 < 2.0
                     THEN 'Low'
                WHEN (v.burnout_exhaustion_mbi
                      + v.burnout_cynicism_mbi
                      + (6.0 - v.burnout_personal_accomplishment_mbi)) / 3.0 < 4.0
                     THEN 'Moderate'
                ELSE 'High'
            END AS burnout_tier,

            CASE
                WHEN v.nurse_tenure_years < 2.0  THEN 'New'
                WHEN v.nurse_tenure_years < 10.0 THEN 'Mid'
                ELSE 'Senior'
            END AS experience_tier,

            CAST(CASE WHEN v.turnover_risk_index > 0.65 THEN 1 ELSE 0 END AS TINYINT)
                AS high_turnover_risk_flag,
            CAST(CASE WHEN v.nurse_unit <> v.unit_assigned THEN 1 ELSE 0 END AS TINYINT)
                AS float_nurse_flag,    -- nurse pulled to a different unit

            -- Supply chain
            CASE
                WHEN v.days_of_supply < 7.0  THEN 'Critical'
                WHEN v.days_of_supply < 14.0 THEN 'Warning'
                ELSE 'Safe'
            END AS days_of_supply_tier,

            DATEDIFF(DAY, CAST(SYSUTCDATETIME() AS DATE), v.kit_expiration_date)
                AS days_to_expiry,
            CAST(CASE
                WHEN DATEDIFF(DAY, CAST(SYSUTCDATETIME() AS DATE), v.kit_expiration_date) < 30
                THEN 1 ELSE 0 END AS TINYINT) AS near_expiry_flag,

            -- Demographic and temporal (pre-computed for query performance)
            CASE
                WHEN v.age BETWEEN 18 AND 40 THEN '18-40'
                WHEN v.age BETWEEN 41 AND 60 THEN '41-60'
                WHEN v.age BETWEEN 61 AND 75 THEN '61-75'
                ELSE '76+'
            END AS age_group,

            CAST(DATEPART(HOUR, v.triage_timestamp)    AS TINYINT)  AS encounter_hour,
            DATENAME(WEEKDAY, v.triage_timestamp)                     AS encounter_day_of_week,
            CAST(YEAR(v.triage_timestamp)              AS SMALLINT)  AS encounter_year

        FROM validated AS v
        LEFT JOIN dbo.ref_procedure_benchmarks AS pb ON pb.cpt_code = v.cpt_code
        LEFT JOIN dbo.ref_cms_base_rates       AS cb ON cb.fiscal_year = YEAR(v.triage_timestamp)
    )

    -- ── Insert all rows into Silver ───────────────────────────────────────────
    -- Flagged rows are included — _validation_flag tells analysts which rule failed.
    -- Clean rows have _validation_flag = NULL.
    INSERT INTO silver.encounters_clean (
        _load_batch_id, _validation_flag,
        patient_id, birth_year, age, sex, race_ethnicity, state,
        encounter_id, icd10_code, diagnosis_display, diagnosis_category,
        severity_index, comorbidity_count, has_diabetes, has_hypertension, has_chf,
        sbp_mmhg, dbp_mmhg, heart_rate_bpm, spo2_pct, temperature_f, bmi,
        respiratory_rate, hba1c_pct, glucose_mg_dl, creatinine_mg_dl,
        wbc_10e3_ul, nt_probnp_pg_ml, medication_adherence_pdc, readmission_30d_flag,
        triage_timestamp, admit_timestamp, bed_request_time, bed_assign_time,
        unit_assigned, bed_occupancy_pct, cpt_code, procedure_display,
        or_start, or_end, actual_or_minutes, or_turnover_minutes,
        los_days, discharge_timestamp, safety_incident_flag, incident_type, incident_severity,
        claim_id, payer, npi_billing, drg_weight,
        submitted_charge_usd, allowed_amount_usd, patient_responsibility_usd,
        fraud_upcoding_flag, fraud_duplicate_flag, fraud_unbundling_flag,
        nurse_emp_id, nurse_role, nurse_unit, nurse_tenure_years, nurse_fte,
        surgeon_emp_id, surgeon_specialty, shift_hours, patients_per_nurse_ratio,
        overtime_hours, burnout_exhaustion_mbi, burnout_cynicism_mbi,
        burnout_personal_accomplishment_mbi,  -- ++ ADDED
        turnover_risk_index,
        cahps_nurse_communication, cahps_doctor_communication, cahps_responsiveness,
        cahps_pain_management, cahps_discharge_info, cahps_care_transition,
        cahps_cleanliness, cahps_quietness,
        surgical_kit_id, kit_name, kit_unit_cost_usd, kit_current_stock,
        kit_reorder_point, kit_lead_time_days, kit_expiration_date, stockout_risk_flag,
        weekly_procedure_volume, projected_demand_4wk, days_of_supply,
        hedis_hba1c_tested, hedis_hba1c_poor_control, pdsa_cycle_id, himss_emram_stage,
        severity_tier, glycemic_control_tier, obesity_class, ckd_risk_flag,
        fever_flag, tachycardia_flag, hypoxia_flag, medication_adherent_flag,
        door_to_admit_minutes, bed_wait_minutes, admit_to_or_minutes,
        or_efficiency_ratio, high_occupancy_flag, readmission_window_end,
        payer_tier, charge_inflation_ratio, any_fraud_flag,
        expected_reimbursement_usd, margin_estimate_usd,
        burnout_composite, burnout_tier, experience_tier,
        high_turnover_risk_flag, float_nurse_flag,
        days_of_supply_tier, days_to_expiry, near_expiry_flag,
        age_group, encounter_hour, encounter_day_of_week, encounter_year
    )
    SELECT
        @batch_id, flag_code,
        patient_id, birth_year, age, sex, race_ethnicity, state,
        encounter_id, icd10_code, diagnosis_display, diagnosis_category,
        severity_index, comorbidity_count, has_diabetes, has_hypertension, has_chf,
        sbp_mmhg, dbp_mmhg, heart_rate_bpm, spo2_pct, temperature_f, bmi,
        respiratory_rate, hba1c_pct, glucose_mg_dl, creatinine_mg_dl,
        wbc_10e3_ul, nt_probnp_pg_ml, medication_adherence_pdc, readmission_30d_flag,
        triage_timestamp, admit_timestamp, bed_request_time, bed_assign_time,
        unit_assigned, bed_occupancy_pct, cpt_code, procedure_display,
        or_start, or_end, actual_or_minutes, or_turnover_minutes,
        los_days, discharge_timestamp, safety_incident_flag, incident_type, incident_severity,
        claim_id, payer, npi_billing, drg_weight,
        submitted_charge_usd, allowed_amount_usd, patient_responsibility_usd,
        fraud_upcoding_flag, fraud_duplicate_flag, fraud_unbundling_flag,
        nurse_emp_id, nurse_role, nurse_unit, nurse_tenure_years, nurse_fte,
        surgeon_emp_id, surgeon_specialty, shift_hours, patients_per_nurse_ratio,
        overtime_hours, burnout_exhaustion_mbi, burnout_cynicism_mbi,
        burnout_personal_accomplishment_mbi,  -- ++ ADDED
        turnover_risk_index,
        cahps_nurse_communication, cahps_doctor_communication, cahps_responsiveness,
        cahps_pain_management, cahps_discharge_info, cahps_care_transition,
        cahps_cleanliness, cahps_quietness,
        surgical_kit_id, kit_name, kit_unit_cost_usd, kit_current_stock,
        kit_reorder_point, kit_lead_time_days, kit_expiration_date, stockout_risk_flag,
        weekly_procedure_volume, projected_demand_4wk, days_of_supply,
        hedis_hba1c_tested, hedis_hba1c_poor_control, pdsa_cycle_id, himss_emram_stage,
        severity_tier, glycemic_control_tier, obesity_class, ckd_risk_flag,
        fever_flag, tachycardia_flag, hypoxia_flag, medication_adherent_flag,
        door_to_admit_minutes, bed_wait_minutes, admit_to_or_minutes,
        or_efficiency_ratio, high_occupancy_flag, readmission_window_end,
        payer_tier, charge_inflation_ratio, any_fraud_flag,
        expected_reimbursement_usd, margin_estimate_usd,
        burnout_composite, burnout_tier, experience_tier,
        high_turnover_risk_flag, float_nurse_flag,
        days_of_supply_tier, days_to_expiry, near_expiry_flag,
        age_group, encounter_hour, encounter_day_of_week, encounter_year
    FROM engineered;

    SET @rows_loaded  = @@ROWCOUNT;
    SET @rows_flagged = (
        SELECT COUNT(*)
        FROM silver.encounters_clean
        WHERE _load_batch_id = @batch_id
          AND _validation_flag IS NOT NULL
    );
    SET @rows_clean   = @rows_loaded - @rows_flagged;
    SET @duration_ms  = DATEDIFF(MILLISECOND, @start_time, SYSUTCDATETIME());
END;
GO
