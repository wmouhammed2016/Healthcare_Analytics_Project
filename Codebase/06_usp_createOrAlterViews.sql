/*
================================================================================
06_usp_createOrAlterViews.sql
================================================================================
gold.usp_createOrAlterViews
----------------------------
Creates or refreshes all Gold schema views using CREATE OR ALTER VIEW.
Idempotent -- safe to call on every pipeline run.

Views are grouped into three categories:
  1. Dimension views -- 10 dims created FIRST (fact view depends on three of them)
  2. Fact views      -- gold.vw_fact_encounter (clean rows) and _all (all rows)
  3. DQ views        -- data quality summaries queryable from Power BI or SSMS

IMPORTANT -- CREATION ORDER:
  vw_fact_encounter LEFT JOINs to vw_dim_payer, vw_dim_unit, and
  vw_dim_safety_incident at view-compile time. Those three dims must exist
  before the fact view is created, otherwise SQL Server raises
  "Invalid object name 'gold.vw_dim_payer'" during SP execution.

Migration path to physical tables at 50M+ rows:
  1. SELECT INTO gold.fact_encounter FROM gold.vw_fact_encounter
  2. Add partition scheme on discharge_date by calendar year
  3. Replace views with synonyms pointing to physical tables
  4. Convert Silver SP to incremental MERGE for zero-downtime loads

================================================================================
*/
USE HCWarehouse_N2;
GO

CREATE OR ALTER PROCEDURE gold.usp_createOrAlterViews
    @batch_id       NVARCHAR(20),   -- e.g. '20260622_143201'
    @rows_loaded    INT             = 1 OUTPUT,
    @rows_rejected  INT             = 0 OUTPUT,
    @rows_flagged   INT             = 0 OUTPUT,
    @duration_ms    INT             = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start_time DATETIME2 = SYSUTCDATETIME();

    -- =========================================================================
    -- 1. DIMENSION VIEWS  (must come before the fact view)
    -- =========================================================================

    -- age and age_group removed: they change per encounter, not per patient.
    -- Stable patient attributes only -- guarantees one row per patient_id.
    EXEC('
    CREATE OR ALTER VIEW gold.vw_dim_patient AS
    SELECT DISTINCT
        patient_id, birth_year, sex, race_ethnicity, state
    FROM silver.encounters_clean
    ');

    -- date_sk: integer surrogate key in YYYYMMDD format added for VertiPaq
    -- compression. The fact carries discharge_date_sk (active relationship) and
    -- triage_date_sk (inactive relationship), both pointing to this dim.
    -- DATE 2026-06-25 -> DATEKEY 20260625
    EXEC('
    CREATE OR ALTER VIEW gold.vw_dim_encounter_date AS
    SELECT
        CONVERT(INT, CONVERT(NVARCHAR(8), date_key, 112)) AS date_sk,
        date_key, calendar_year, calendar_quarter,
        calendar_month_num, calendar_month_name,
        calendar_week_num, day_of_week_num, day_of_week_name,
        is_weekend, is_us_federal_holiday
    FROM dbo.ref_date_dimension
    ');

    EXEC('
    CREATE OR ALTER VIEW gold.vw_dim_diagnosis AS
    SELECT DISTINCT icd10_code, diagnosis_display, diagnosis_category
    FROM silver.encounters_clean
    ');

    -- diagnosis_category removed: it is a diagnosis attribute, not a procedure
    -- attribute. Including it here creates an ambiguous filter path in Power BI.
    -- cpt_code INT is already VertiPaq-optimal -- no surrogate key needed.
    EXEC('
    CREATE OR ALTER VIEW gold.vw_dim_procedure AS
    SELECT DISTINCT
        s.cpt_code,
        s.procedure_display,
        pb.expected_or_minutes,
        pb.service_line
    FROM silver.encounters_clean AS s
    LEFT JOIN dbo.ref_procedure_benchmarks AS pb
        ON pb.cpt_code = s.cpt_code
    ');

    -- payer_sk: integer surrogate via DENSE_RANK alphabetical order -- stable
    -- across runs. payer and payer_tier removed from fact; accessible through
    -- the payer_sk FK relationship in Power BI.
    EXEC('
    CREATE OR ALTER VIEW gold.vw_dim_payer AS
    SELECT
        CAST(DENSE_RANK() OVER (ORDER BY payer) AS INT) AS payer_sk,
        payer,
        payer_tier,
        contractual_discount_rate
    FROM (
        SELECT DISTINCT
            s.payer,
            s.payer_tier,
            pd.contractual_discount_rate
        FROM silver.encounters_clean AS s
        LEFT JOIN dbo.ref_payer_discounts AS pd ON pd.payer_name = s.payer
    ) AS src
    ');

    -- unit_sk: integer surrogate added. unit_assigned NVARCHAR(100) removed from
    -- fact; accessible via the unit_sk FK relationship in Power BI.
    EXEC('
    CREATE OR ALTER VIEW gold.vw_dim_unit AS
    SELECT
        CAST(DENSE_RANK() OVER (ORDER BY unit_assigned) AS INT) AS unit_sk,
        unit_assigned,
        unit_tier,
        target_nurse_ratio
    FROM (
        SELECT DISTINCT
            s.unit_assigned,
            ub.unit_tier,
            ub.target_nurse_ratio
        FROM silver.encounters_clean AS s
        LEFT JOIN dbo.ref_unit_benchmarks AS ub ON ub.unit_name = s.unit_assigned
    ) AS src
    ');

    -- nurse_tenure_years added. In a live system with multi-year encounter data,
    -- replace this with hire_date and compute tenure at query time via DATEDIFF.
    EXEC('
    CREATE OR ALTER VIEW gold.vw_dim_nurse AS
    SELECT DISTINCT
        nurse_emp_id, nurse_role, nurse_unit,
        nurse_tenure_years, experience_tier, nurse_fte
    FROM silver.encounters_clean
    ');

    EXEC('
    CREATE OR ALTER VIEW gold.vw_dim_surgeon AS
    SELECT DISTINCT surgeon_emp_id, surgeon_specialty, npi_billing
    FROM silver.encounters_clean
    ');

    -- kit_reorder_point and kit_lead_time_days are stable after the SDG fix
    -- (contract-level attributes, not randomised per encounter).
    -- SELECT DISTINCT now returns exactly 14 rows -- one per kit type.
    EXEC('
    CREATE OR ALTER VIEW gold.vw_dim_surgical_kit AS
    SELECT DISTINCT
        surgical_kit_id, kit_name, kit_unit_cost_usd,
        kit_reorder_point, kit_lead_time_days
    FROM silver.encounters_clean
    ');

    -- Sentinel row (incident_sk = 0, ''None'') absorbs the ~84% of encounters
    -- with no safety incident, preventing null FK gaps from breaking Power BI
    -- slicer cross-filtering.
    -- NOTE: inside EXEC('...'), string literals must use doubled single quotes
    -- (''None'') -- double-quote characters are treated as object identifiers
    -- by SQL Server when QUOTED_IDENTIFIER is ON (the default).
    EXEC('
    CREATE OR ALTER VIEW gold.vw_dim_safety_incident AS

    SELECT
        CAST(0 AS INT)                  AS incident_sk,
        CAST(''None'' AS NVARCHAR(100)) AS incident_type,
        CAST(''None'' AS NVARCHAR(50))  AS incident_severity,
        CAST(0 AS TINYINT)              AS is_hac_eligible

    UNION ALL

    SELECT
        incident_sk,
        incident_type,
        incident_severity,
        is_hac_eligible
    FROM (
        SELECT
            CAST(ROW_NUMBER() OVER (ORDER BY incident_type, incident_severity) AS INT) AS incident_sk,
            incident_type,
            incident_severity,
            CAST(
                CASE WHEN incident_type IN (''CAUTI'', ''CLABSI'', ''SSI'', ''Fall'', ''Pressure Injury'')
                THEN 1 ELSE 0 END
            AS TINYINT) AS is_hac_eligible
        FROM (
            SELECT DISTINCT incident_type, incident_severity
            FROM silver.encounters_clean
            WHERE safety_incident_flag = 1
              AND incident_type IS NOT NULL
        ) AS distinct_incidents
    ) AS ranked
    ');

    -- =========================================================================
    -- 2. FACT VIEWS  (created after all dims exist)
    -- =========================================================================

    -- Primary analytical view -- clean rows only (_validation_flag IS NULL).
    -- Columns removed vs original: sex, race_ethnicity, state (-> dim_patient),
    -- payer, payer_tier (-> dim_payer via payer_sk), unit_assigned (-> dim_unit
    -- via unit_sk), kit_unit_cost_usd (stable kit attr -> dim_surgical_kit),
    -- near_expiry_flag and days_to_expiry (frozen at Silver load time, stale),
    -- days_of_supply_tier (computable DAX calculated column).
    EXEC('
    CREATE OR ALTER VIEW gold.vw_fact_encounter AS
    SELECT
        -- Dimension foreign keys
        s.encounter_id,
        s.patient_id,
        s.icd10_code,
        s.cpt_code,
        s.nurse_emp_id,
        s.surgeon_emp_id,
        s.surgical_kit_id,
        dp.payer_sk,
        du.unit_sk,
        ISNULL(dsi.incident_sk, 0)                                                   AS incident_sk,
        CONVERT(INT, CONVERT(NVARCHAR(8), CAST(s.discharge_timestamp AS DATE), 112)) AS discharge_date_sk,
        CONVERT(INT, CONVERT(NVARCHAR(8), CAST(s.triage_timestamp   AS DATE), 112))  AS triage_date_sk,

        -- Degenerate date columns (kept for DAX date arithmetic without relationship)
        CAST(s.discharge_timestamp AS DATE) AS discharge_date,
        CAST(s.triage_timestamp   AS DATE)  AS triage_date,

        -- Clinical measures & tiers
        s.severity_index, s.severity_tier, s.comorbidity_count,
        s.readmission_30d_flag, s.medication_adherence_pdc, s.medication_adherent_flag,
        s.los_days, s.hba1c_pct, s.glycemic_control_tier,

        -- Operational measures & flags
        s.actual_or_minutes, s.or_efficiency_ratio, s.or_turnover_minutes,
        s.door_to_admit_minutes, s.bed_wait_minutes, s.admit_to_or_minutes,
        s.bed_occupancy_pct, s.high_occupancy_flag, s.safety_incident_flag,
        s.readmission_window_end,

        -- Financial measures & flags
        s.submitted_charge_usd, s.allowed_amount_usd, s.patient_responsibility_usd,
        s.drg_weight, s.charge_inflation_ratio, s.expected_reimbursement_usd,
        s.margin_estimate_usd, s.any_fraud_flag,
        s.fraud_upcoding_flag, s.fraud_duplicate_flag, s.fraud_unbundling_flag,

        -- HR measures, tiers & flags
        s.patients_per_nurse_ratio, s.overtime_hours, s.burnout_composite, s.burnout_tier,
        s.turnover_risk_index, s.high_turnover_risk_flag, s.float_nurse_flag,
        s.cahps_nurse_communication, s.cahps_doctor_communication, s.cahps_responsiveness,
        s.cahps_pain_management, s.cahps_discharge_info, s.cahps_care_transition,
        s.cahps_cleanliness, s.cahps_quietness,

        -- Supply chain measures & flags
        s.days_of_supply,
        s.stockout_risk_flag,

        -- Quality flags
        s.hedis_hba1c_tested, s.hedis_hba1c_poor_control, s.himss_emram_stage,

        -- Demographic & temporal degenerate dimensions
        s.age, s.age_group,
        s.encounter_hour, s.encounter_day_of_week, s.encounter_year

    FROM silver.encounters_clean AS s
    LEFT JOIN gold.vw_dim_payer           AS dp  ON  dp.payer            = s.payer
    LEFT JOIN gold.vw_dim_unit            AS du  ON  du.unit_assigned     = s.unit_assigned
    LEFT JOIN gold.vw_dim_safety_incident AS dsi ON  dsi.incident_type    = s.incident_type
                                                AND  dsi.incident_severity = s.incident_severity
                                                AND  dsi.incident_sk       <> 0
    WHERE s._validation_flag IS NULL
    ');

    -- Companion: all rows including flagged, with the flag reason visible
    EXEC('
    CREATE OR ALTER VIEW gold.vw_fact_encounter_all AS
    SELECT *
    FROM silver.encounters_clean
    ');

    -- =========================================================================
    -- 3. DATA QUALITY VIEWS
    -- =========================================================================

    -- Count of Silver validation flags grouped by rule -- first post-load check
    EXEC('
    CREATE OR ALTER VIEW gold.vw_dq_validation_summary AS
    SELECT
        _load_batch_id      AS batch_id,
        _validation_flag    AS flag_reason,
        COUNT(*)            AS flagged_row_count
    FROM silver.encounters_clean
    WHERE _validation_flag IS NOT NULL
    GROUP BY _load_batch_id, _validation_flag
    ');

    -- Fraud signal audit -- charge inflation ratio by fraud flag combination
    EXEC('
    CREATE OR ALTER VIEW gold.vw_dq_fraud_signal_audit AS
    SELECT
        fraud_upcoding_flag,
        fraud_duplicate_flag,
        fraud_unbundling_flag,
        COUNT(*)                    AS encounter_count,
        AVG(charge_inflation_ratio) AS avg_charge_inflation_ratio,
        MIN(charge_inflation_ratio) AS min_charge_inflation_ratio,
        MAX(charge_inflation_ratio) AS max_charge_inflation_ratio
    FROM silver.encounters_clean
    WHERE _validation_flag IS NULL
    GROUP BY fraud_upcoding_flag, fraud_duplicate_flag, fraud_unbundling_flag
    ');

    -- Clinical coherence -- confirms cross-domain correlations survived the ETL
    EXEC('
    CREATE OR ALTER VIEW gold.vw_dq_clinical_coherence AS
    SELECT
        has_diabetes,
        AVG(hba1c_pct)       AS avg_hba1c,
        has_chf,
        AVG(nt_probnp_pg_ml) AS avg_nt_probnp,
        comorbidity_count,
        AVG(severity_index)  AS avg_severity_index
    FROM silver.encounters_clean
    WHERE _validation_flag IS NULL
    GROUP BY has_diabetes, has_chf, comorbidity_count
    ');

    -- Supply chain health -- stockout risk and expiry summary per kit
    EXEC('
    CREATE OR ALTER VIEW gold.vw_dq_supply_chain_health AS
    SELECT
        kit_name,
        days_of_supply_tier,
        COUNT(*)                AS encounter_count,
        AVG(days_of_supply)     AS avg_days_of_supply,
        SUM(stockout_risk_flag) AS stockout_risk_count,
        SUM(near_expiry_flag)   AS near_expiry_count
    FROM silver.encounters_clean
    WHERE _validation_flag IS NULL
    GROUP BY kit_name, days_of_supply_tier
    ');

    SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, SYSUTCDATETIME());
END;
GO
