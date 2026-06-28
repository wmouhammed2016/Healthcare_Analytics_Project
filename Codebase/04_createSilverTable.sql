/*
================================================================================
04_createSilverTable.sql
================================================================================
Creates silver.encounters_clean — the cleaned, validated, and
feature-engineered analytical table.

Column count : 123 total (92 source + 28 engineered + 3 system)
_load_batch_id is NVARCHAR(20) to match the Python timestamp batch_id.
_validation_flag is NULL for clean rows; set to a rule code for flagged rows.

Target  : SQL Server 2019+ / Azure SQL Database
================================================================================
*/
USE HCWarehouse_N2;
GO

CREATE TABLE silver.encounters_clean (

    -- ── SYSTEM COLUMNS ────────────────────────────────────────────────────────
    _silver_id          BIGINT          NOT NULL
                            CONSTRAINT pk_silver_encounters PRIMARY KEY IDENTITY,
    _load_batch_id      NVARCHAR(20)    NOT NULL,   -- e.g. '20260622_143201'
    _silver_loaded_at   DATETIME2(0)    NOT NULL
                            CONSTRAINT df_silver_loaded_at DEFAULT SYSUTCDATETIME(),
    _validation_flag    NVARCHAR(500)   NULL,        -- NULL = clean row

    -- ── SOURCE COLUMNS (92) — same types as bronze.encounters ─────────────────
    patient_id              NVARCHAR(50)    NOT NULL,
    birth_year              SMALLINT        NOT NULL,
    age                     SMALLINT        NOT NULL,
    sex                     NVARCHAR(10)    NOT NULL,
    race_ethnicity          NVARCHAR(50)    NOT NULL,
    state                   NVARCHAR(2)     NOT NULL,
    encounter_id            NVARCHAR(50)    NOT NULL,
    icd10_code              NVARCHAR(20)    NOT NULL,
    diagnosis_display       NVARCHAR(255)   NOT NULL,
    diagnosis_category      NVARCHAR(20)    NOT NULL,
    severity_index          DECIMAL(10,4)   NOT NULL,
    comorbidity_count       TINYINT         NOT NULL,
    has_diabetes            TINYINT         NOT NULL,
    has_hypertension        TINYINT         NOT NULL,
    has_chf                 TINYINT         NOT NULL,
    sbp_mmhg                DECIMAL(8,2)    NOT NULL,
    dbp_mmhg                DECIMAL(8,2)    NOT NULL,
    heart_rate_bpm          DECIMAL(8,2)    NOT NULL,
    spo2_pct                DECIMAL(8,2)    NOT NULL,
    temperature_f           DECIMAL(8,2)    NOT NULL,
    bmi                     DECIMAL(8,2)    NOT NULL,
    respiratory_rate        SMALLINT        NOT NULL,
    hba1c_pct               DECIMAL(8,2)    NULL,
    glucose_mg_dl           DECIMAL(8,2)    NULL,
    creatinine_mg_dl        DECIMAL(8,2)    NULL,
    wbc_10e3_ul             DECIMAL(8,2)    NULL,
    nt_probnp_pg_ml         DECIMAL(10,2)   NULL,
    medication_adherence_pdc DECIMAL(10,4)  NOT NULL,
    readmission_30d_flag    TINYINT         NOT NULL,
    triage_timestamp        DATETIME2(0)    NOT NULL,
    admit_timestamp         DATETIME2(0)    NOT NULL,
    bed_request_time        DATETIME2(0)    NOT NULL,
    bed_assign_time         DATETIME2(0)    NOT NULL,
    unit_assigned           NVARCHAR(100)   NOT NULL,
    bed_occupancy_pct       DECIMAL(8,2)    NOT NULL,
    cpt_code                INT             NOT NULL,
    procedure_display       NVARCHAR(255)   NOT NULL,
    or_start                DATETIME2(0)    NOT NULL,
    or_end                  DATETIME2(0)    NOT NULL,
    actual_or_minutes       INT             NOT NULL,
    or_turnover_minutes     SMALLINT        NOT NULL,
    los_days                INT             NOT NULL,
    discharge_timestamp     DATETIME2(0)    NOT NULL,
    safety_incident_flag    TINYINT         NOT NULL,
    incident_type           NVARCHAR(100)   NULL,
    incident_severity       NVARCHAR(50)    NULL,
    claim_id                NVARCHAR(50)    NOT NULL,
    payer                   NVARCHAR(50)    NOT NULL,
    npi_billing             BIGINT          NOT NULL,
    drg_weight              DECIMAL(10,4)   NOT NULL,
    submitted_charge_usd    DECIMAL(12,2)   NOT NULL,
    allowed_amount_usd      DECIMAL(12,2)   NOT NULL,
    patient_responsibility_usd DECIMAL(12,2) NOT NULL,
    fraud_upcoding_flag     TINYINT         NOT NULL,
    fraud_duplicate_flag    TINYINT         NOT NULL,
    fraud_unbundling_flag   TINYINT         NOT NULL,
    nurse_emp_id            NVARCHAR(50)    NOT NULL,
    nurse_role              NVARCHAR(50)    NOT NULL,
    nurse_unit              NVARCHAR(100)   NOT NULL,
    nurse_tenure_years      DECIMAL(10,4)   NOT NULL,
    nurse_fte               DECIMAL(10,4)   NOT NULL,
    surgeon_emp_id          NVARCHAR(50)    NOT NULL,
    surgeon_specialty       NVARCHAR(100)   NOT NULL,
    shift_hours             TINYINT         NOT NULL,
    patients_per_nurse_ratio TINYINT        NOT NULL,
    overtime_hours          DECIMAL(8,2)    NOT NULL,
    burnout_exhaustion_mbi  DECIMAL(10,4)   NOT NULL,
    burnout_cynicism_mbi    DECIMAL(10,4)   NOT NULL,
    burnout_personal_accomplishment_mbi  DECIMAL(10,4)  NULL,
    turnover_risk_index     DECIMAL(10,4)   NOT NULL,
    cahps_nurse_communication   DECIMAL(8,2) NOT NULL,
    cahps_doctor_communication  DECIMAL(8,2) NOT NULL,
    cahps_responsiveness        DECIMAL(8,2) NOT NULL,
    cahps_pain_management       DECIMAL(8,2) NOT NULL,
    cahps_discharge_info        DECIMAL(8,2) NOT NULL,
    cahps_care_transition       DECIMAL(8,2) NOT NULL,
    cahps_cleanliness           DECIMAL(8,2) NOT NULL,
    cahps_quietness             DECIMAL(8,2) NOT NULL,
    surgical_kit_id         NVARCHAR(50)    NOT NULL,
    kit_name                NVARCHAR(100)   NOT NULL,
    kit_unit_cost_usd       DECIMAL(12,2)   NOT NULL,
    kit_current_stock       SMALLINT        NOT NULL,
    kit_reorder_point       SMALLINT        NOT NULL,
    kit_lead_time_days      SMALLINT        NOT NULL,
    kit_expiration_date     DATE            NOT NULL,
    stockout_risk_flag      TINYINT         NOT NULL,
    weekly_procedure_volume SMALLINT        NOT NULL,
    projected_demand_4wk    SMALLINT        NOT NULL,
    days_of_supply          DECIMAL(10,4)   NOT NULL,
    hedis_hba1c_tested      TINYINT         NOT NULL,
    hedis_hba1c_poor_control TINYINT        NOT NULL,
    pdsa_cycle_id           NVARCHAR(50)    NULL,
    himss_emram_stage       TINYINT         NOT NULL,

    -- ── ENGINEERED FEATURES (28) ──────────────────────────────────────────────
    -- All computed by silver.usp_loadFromBronze using CASE WHEN / DATEDIFF.

    -- Clinical risk tiers
    severity_tier               NVARCHAR(10)    NULL,  -- Low | Moderate | High | Critical
    glycemic_control_tier       NVARCHAR(15)    NULL,  -- Controlled | Borderline | Poor
    obesity_class               NVARCHAR(20)    NULL,
    ckd_risk_flag               TINYINT         NULL,
    fever_flag                  TINYINT         NULL,
    tachycardia_flag            TINYINT         NULL,
    hypoxia_flag                TINYINT         NULL,
    medication_adherent_flag    TINYINT         NULL,

    -- Operational throughput
    door_to_admit_minutes       INT             NULL,
    bed_wait_minutes            INT             NULL,
    admit_to_or_minutes         INT             NULL,
    or_efficiency_ratio         DECIMAL(10,4)   NULL,
    high_occupancy_flag         TINYINT         NULL,
    readmission_window_end      DATE            NULL,

    -- Financial analytics
    payer_tier                  NVARCHAR(15)    NULL,
    charge_inflation_ratio      DECIMAL(10,4)   NULL,
    any_fraud_flag              TINYINT         NULL,
    expected_reimbursement_usd  DECIMAL(12,2)   NULL,
    margin_estimate_usd         DECIMAL(12,2)   NULL,

    -- HR and workforce
    burnout_composite           DECIMAL(8,4)    NULL,
    burnout_tier                NVARCHAR(10)    NULL,
    experience_tier             NVARCHAR(10)    NULL,
    high_turnover_risk_flag     TINYINT         NULL,
    float_nurse_flag            TINYINT         NULL,

    -- Supply chain
    days_of_supply_tier         NVARCHAR(10)    NULL,
    days_to_expiry              INT             NULL,
    near_expiry_flag            TINYINT         NULL,

    -- Temporal / demographic (pre-computed for query performance)
    age_group                   NVARCHAR(10)    NULL,
    encounter_hour              TINYINT         NULL,
    encounter_day_of_week       NVARCHAR(10)    NULL,
    encounter_year              SMALLINT        NULL
);
GO

-- Indexes for the most common analytical query patterns
CREATE NONCLUSTERED INDEX ix_silver_encounter_id
    ON silver.encounters_clean (encounter_id);

CREATE NONCLUSTERED INDEX ix_silver_patient_id
    ON silver.encounters_clean (patient_id);

CREATE NONCLUSTERED INDEX ix_silver_discharge_date
    ON silver.encounters_clean (discharge_timestamp);

CREATE NONCLUSTERED INDEX ix_silver_diagnosis_category
    ON silver.encounters_clean (diagnosis_category);

CREATE NONCLUSTERED INDEX ix_silver_payer_tier
    ON silver.encounters_clean (payer_tier);

CREATE NONCLUSTERED INDEX ix_silver_severity_tier
    ON silver.encounters_clean (severity_tier);

-- Fast filter for clean-records-only queries (WHERE _validation_flag IS NULL)
CREATE NONCLUSTERED INDEX ix_silver_validation_flag
    ON silver.encounters_clean (_validation_flag)
    INCLUDE (encounter_id, severity_tier, payer_tier);
GO
