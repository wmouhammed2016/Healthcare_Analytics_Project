/*
================================================================================
02_createBronzeTable.sql
================================================================================
Creates bronze.encounters — the typed, constrained source replica.

Design principles:
  - Every source column cast to its correct SQL Server type.
  - DECIMAL(12,2) for financial amounts — MONEY type is never used.
  - TINYINT with CHECK IN (0,1) replaces the absent SQL Server BOOLEAN.
  - DATETIME2(0) preferred over the legacy DATETIME type.
  - No derived columns — those belong in Silver.
  - _load_batch_id is NVARCHAR(20) matching the Python timestamp batch_id.

================================================================================
*/
USE HCWarehouse_N2;
GO

CREATE TABLE bronze.encounters (
    -- System columns
    _bronze_id          BIGINT          NOT NULL
                            CONSTRAINT pk_bronze_encounters PRIMARY KEY IDENTITY,
    -- This is coming from the staging layer's _load_batch_id
    _load_batch_id      NVARCHAR(20)    NOT NULL,   -- e.g. '20260622_143201'
    _bronze_loaded_at   DATETIME2(0)    NOT NULL
                            CONSTRAINT df_bronze_loaded_at DEFAULT SYSUTCDATETIME(),

    -- ── DEMOGRAPHICS ──────────────────────────────────────────────────────────
    patient_id              NVARCHAR(50)    NOT NULL,
    birth_year              SMALLINT        NOT NULL,
    age                     SMALLINT        NOT NULL,
    sex                     NVARCHAR(10)    NOT NULL,
    race_ethnicity          NVARCHAR(50)    NOT NULL,
    state                   NVARCHAR(2)     NOT NULL,

    -- ── CLINICAL ──────────────────────────────────────────────────────────────
    encounter_id            NVARCHAR(50)    NOT NULL,
    icd10_code              NVARCHAR(20)    NOT NULL,
    diagnosis_display       NVARCHAR(255)   NOT NULL,
    diagnosis_category      NVARCHAR(20)    NOT NULL,

    -- This column should be checked to ensure it is in the range between 0 and 1
    severity_index          DECIMAL(10,4)   NOT NULL
                                CONSTRAINT chk_severity
                                CHECK (severity_index BETWEEN 0.0 AND 1.0),
    comorbidity_count       TINYINT         NOT NULL,
    has_diabetes            TINYINT         NOT NULL
                                CONSTRAINT chk_has_diabetes
                                CHECK (has_diabetes IN (0, 1)),
    has_hypertension        TINYINT         NOT NULL
                                CONSTRAINT chk_has_hypertension
                                CHECK (has_hypertension IN (0, 1)),
    has_chf                 TINYINT         NOT NULL
                                CONSTRAINT chk_has_chf
                                CHECK (has_chf IN (0, 1)),
    sbp_mmhg                DECIMAL(8,2)    NOT NULL,
    dbp_mmhg                DECIMAL(8,2)    NOT NULL,
    heart_rate_bpm          DECIMAL(8,2)    NOT NULL,

    -- This to ensure that spo2_pct between 70% - 100% as below or above that is not possible medically.
    spo2_pct                DECIMAL(8,2)    NOT NULL
                                CONSTRAINT chk_spo2
                                CHECK (spo2_pct BETWEEN 70 AND 100),
    temperature_f           DECIMAL(8,2)    NOT NULL,
    bmi                     DECIMAL(8,2)    NOT NULL,
    respiratory_rate        SMALLINT        NOT NULL,
    hba1c_pct               DECIMAL(8,2)    NULL,
    glucose_mg_dl           DECIMAL(8,2)    NULL,
    creatinine_mg_dl        DECIMAL(8,2)    NULL,
    wbc_10e3_ul             DECIMAL(8,2)    NULL,
    nt_probnp_pg_ml         DECIMAL(10,2)   NULL,
    medication_adherence_pdc DECIMAL(10,4)  NOT NULL
                                CONSTRAINT chk_pdc
                                CHECK (medication_adherence_pdc BETWEEN 0.0 AND 1.0),
    readmission_30d_flag    TINYINT         NOT NULL
                                CONSTRAINT chk_readmission
                                CHECK (readmission_30d_flag IN (0, 1)),

    -- ── OPERATIONAL ───────────────────────────────────────────────────────────
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
    safety_incident_flag    TINYINT         NOT NULL
                                CONSTRAINT chk_safety_incident
                                CHECK (safety_incident_flag IN (0, 1)),
    incident_type           NVARCHAR(100)   NULL,
    incident_severity       NVARCHAR(50)    NULL,

    -- ── FINANCIAL ─────────────────────────────────────────────────────────────
    claim_id                NVARCHAR(50)    NOT NULL,
    payer                   NVARCHAR(50)    NOT NULL,
    npi_billing             BIGINT          NOT NULL,   -- 10-digit NPI; exceeds INT range
    drg_weight              DECIMAL(10,4)   NOT NULL,
    submitted_charge_usd    DECIMAL(12,2)   NOT NULL,
    allowed_amount_usd      DECIMAL(12,2)   NOT NULL,
    patient_responsibility_usd DECIMAL(12,2) NOT NULL,
    fraud_upcoding_flag     TINYINT         NOT NULL
                                CONSTRAINT chk_fraud_upcoding
                                CHECK (fraud_upcoding_flag IN (0, 1)),
    fraud_duplicate_flag    TINYINT         NOT NULL
                                CONSTRAINT chk_fraud_duplicate
                                CHECK (fraud_duplicate_flag IN (0, 1)),
    fraud_unbundling_flag   TINYINT         NOT NULL
                                CONSTRAINT chk_fraud_unbundling
                                CHECK (fraud_unbundling_flag IN (0, 1)),

    -- ── HUMAN RESOURCES ───────────────────────────────────────────────────────
    nurse_emp_id            NVARCHAR(50)    NOT NULL,
    nurse_role              NVARCHAR(50)    NOT NULL,
    nurse_unit              NVARCHAR(100)   NOT NULL,
    nurse_tenure_years      DECIMAL(10,4)   NOT NULL,
    nurse_fte               DECIMAL(10,4)   NOT NULL
                                CONSTRAINT chk_nurse_fte
                                CHECK (nurse_fte BETWEEN 0.0 AND 1.0),
    surgeon_emp_id          NVARCHAR(50)    NOT NULL,
    surgeon_specialty       NVARCHAR(100)   NOT NULL,
    shift_hours             TINYINT         NOT NULL
                                CONSTRAINT chk_shift_hours
                                CHECK (shift_hours IN (8, 10, 12)),
    patients_per_nurse_ratio TINYINT        NOT NULL
                                CONSTRAINT chk_nurse_ratio
                                CHECK (patients_per_nurse_ratio BETWEEN 1 AND 12),
    overtime_hours          DECIMAL(8,2)    NOT NULL,
    burnout_exhaustion_mbi  DECIMAL(10,4)   NOT NULL
                                CONSTRAINT chk_exhaustion
                                CHECK (burnout_exhaustion_mbi BETWEEN 0.0 AND 6.0),
    burnout_cynicism_mbi    DECIMAL(10,4)   NOT NULL
                                CONSTRAINT chk_cynicism
                                CHECK (burnout_cynicism_mbi BETWEEN 0.0 AND 6.0),
    burnout_personal_accomplishment_mbi  DECIMAL(10,4)  NOT NULL
                                CONSTRAINT chk_pa_mbi         
                                CHECK (burnout_personal_accomplishment_mbi BETWEEN 0.0 AND 6.0),
    turnover_risk_index     DECIMAL(10,4)   NOT NULL
                                CONSTRAINT chk_turnover_risk
                                CHECK (turnover_risk_index BETWEEN 0.0 AND 1.0),
    cahps_nurse_communication   DECIMAL(8,2) NOT NULL,
    cahps_doctor_communication  DECIMAL(8,2) NOT NULL,
    cahps_responsiveness        DECIMAL(8,2) NOT NULL,
    cahps_pain_management       DECIMAL(8,2) NOT NULL,
    cahps_discharge_info        DECIMAL(8,2) NOT NULL,
    cahps_care_transition       DECIMAL(8,2) NOT NULL,
    cahps_cleanliness           DECIMAL(8,2) NOT NULL,
    cahps_quietness             DECIMAL(8,2) NOT NULL,

    -- ── SUPPLY CHAIN ──────────────────────────────────────────────────────────
    surgical_kit_id         NVARCHAR(50)    NOT NULL,
    kit_name                NVARCHAR(100)   NOT NULL,
    kit_unit_cost_usd       DECIMAL(12,2)   NOT NULL,
    kit_current_stock       SMALLINT        NOT NULL,
    kit_reorder_point       SMALLINT        NOT NULL,
    kit_lead_time_days      SMALLINT        NOT NULL,
    kit_expiration_date     DATE            NOT NULL,
    stockout_risk_flag      TINYINT         NOT NULL
                                CONSTRAINT chk_stockout
                                CHECK (stockout_risk_flag IN (0, 1)),
    weekly_procedure_volume SMALLINT        NOT NULL,
    projected_demand_4wk    SMALLINT        NOT NULL,
    days_of_supply          DECIMAL(10,4)   NOT NULL,

    -- ── QUALITY & STANDARDS ───────────────────────────────────────────────────
    hedis_hba1c_tested      TINYINT         NOT NULL
                                CONSTRAINT chk_hedis_tested
                                CHECK (hedis_hba1c_tested IN (0, 1)),
    hedis_hba1c_poor_control TINYINT        NOT NULL
                                CONSTRAINT chk_hedis_poor
                                CHECK (hedis_hba1c_poor_control IN (0, 1)),
    pdsa_cycle_id           NVARCHAR(50)    NULL,
    himss_emram_stage       TINYINT         NOT NULL
                                CONSTRAINT chk_himss
                                CHECK (himss_emram_stage BETWEEN 0 AND 7)
);
GO

-- CHECK BETWEEN is inclusive (>= low AND <= high) — correct for ratio columns.

CREATE NONCLUSTERED INDEX ix_bronze_encounter_id ON bronze.encounters (encounter_id);
CREATE NONCLUSTERED INDEX ix_bronze_patient_id   ON bronze.encounters (patient_id);
GO
