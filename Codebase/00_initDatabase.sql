/*
================================================================================
00_initDatabase.sql
================================================================================
One-time database initialisation script.
Creates the four data schemas and all reference tables in [dbo].
Run once before the first pipeline execution.

Schemas created : stg, bronze, silver, gold
Reference tables: ref_payer_discounts, ref_cms_base_rates,
                  ref_procedure_benchmarks, ref_unit_benchmarks,
                  ref_hedis_denominators, ref_date_dimension

================================================================================
*/

-- ── DATABASE ──────────────────────────────────────────────────────────────────
IF DB_ID(N'HCWarehouse_N2') IS NULL
BEGIN
    CREATE DATABASE [HCWarehouse_N2];
END
GO

USE HCWarehouse_N2;
GO


-- ── SCHEMAS ───────────────────────────────────────────────────────────────────
IF SCHEMA_ID(N'stg') IS NULL
    EXEC('CREATE SCHEMA stg');

IF SCHEMA_ID(N'bronze') IS NULL
    EXEC('CREATE SCHEMA bronze');

IF SCHEMA_ID(N'silver') IS NULL
    EXEC('CREATE SCHEMA silver');

IF SCHEMA_ID(N'gold') IS NULL
    EXEC('CREATE SCHEMA gold');
GO


-- ── REFERENCE: PAYER DISCOUNTS ────────────────────────────────────────────────
IF OBJECT_ID(N'dbo.ref_payer_discounts', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ref_payer_discounts (
        payer_name                NVARCHAR(100)   NOT NULL PRIMARY KEY,
        payer_tier                NVARCHAR(20)    NOT NULL,  -- Public | Commercial | Self-Pay
        contractual_discount_rate DECIMAL(5,4)    NOT NULL   -- e.g. 0.3800 = 38%
    );

    INSERT INTO dbo.ref_payer_discounts VALUES
        ('Medicare',     'Public',      0.3800),
        ('Medicaid',     'Public',      0.5000),
        ('BlueCross',    'Commercial',  0.3000),
        ('Aetna',        'Commercial',  0.2800),
        ('UnitedHealth', 'Commercial',  0.3200),
        ('Cigna',        'Commercial',  0.2700),
        ('Self-Pay',     'Self-Pay',    0.0000);
END
GO


-- ── REFERENCE: CMS BASE RATES ─────────────────────────────────────────────────
-- These are the rates for the Medicare and Medicaid and it is multiplied by DRG Weight to specify
-- how much money will be paid from the payer
IF OBJECT_ID(N'dbo.ref_cms_base_rates', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ref_cms_base_rates (
        fiscal_year   SMALLINT      NOT NULL CONSTRAINT pk_cms_base_rates PRIMARY KEY,
        base_rate_usd DECIMAL(10,2) NOT NULL
    );

    INSERT INTO dbo.ref_cms_base_rates VALUES
        (2023, 5848.20),
        (2024, 6031.90),
        (2025, 6214.80),
        (2026, 6389.50);
END
GO


-- ── REFERENCE: PROCEDURE BENCHMARKS ──────────────────────────────────────────
IF OBJECT_ID(N'dbo.ref_procedure_benchmarks', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ref_procedure_benchmarks (
        cpt_code            INT           NOT NULL PRIMARY KEY,
        procedure_display   NVARCHAR(255) NOT NULL,
        expected_or_minutes SMALLINT      NOT NULL,
        service_line        NVARCHAR(50)  NOT NULL
    );

    INSERT INTO dbo.ref_procedure_benchmarks VALUES
        (27447, 'Total Knee Arthroplasty',            90,  'MSK'),
        (27130, 'Total Hip Arthroplasty',             95,  'MSK'),
        (33533, 'CABG Arterial',                      210, 'CV'),
        (43239, 'Upper GI Endoscopy with Biopsy',     45,  'GI'),
        (47562, 'Laparoscopic Cholecystectomy',        60,  'GI'),
        (50543, 'Laparoscopic Partial Nephrectomy',   120, 'GU'),
        (58150, 'Total Abdominal Hysterectomy',       110, 'GU'),
        (60500, 'Parathyroidectomy',                   80,  'ENDO'),
        (19307, 'Mastectomy',                          90,  'ONC'),
        (32663, 'Thoracoscopic Lobectomy',            150, 'PULM'),
        (31622, 'Bronchoscopy with BAL',               45,  'PULM'),
        (11043, 'Debridement Muscle/Bone',             60,  'DERM'),
        (22612, 'Lumbar Spinal Fusion',               180, 'MSK'),
        (33361, 'TAVR Transcatheter Aortic Valve',    180, 'CV');
END
GO


-- ── REFERENCE: UNIT BENCHMARKS ────────────────────────────────────────────────
IF OBJECT_ID(N'dbo.ref_unit_benchmarks', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ref_unit_benchmarks (
        unit_name          NVARCHAR(100) NOT NULL PRIMARY KEY,
        unit_tier          NVARCHAR(20)  NOT NULL,  -- ICU | Step-Down | General
        target_nurse_ratio TINYINT       NOT NULL   -- patients per nurse
    );

    INSERT INTO dbo.ref_unit_benchmarks VALUES
        ('ICU',       'ICU',       2),
        ('MICU',      'ICU',       2),
        ('SICU',      'ICU',       2),
        ('Step-Down', 'Step-Down', 3),
        ('Cardiac',   'Step-Down', 3),
        ('Oncology',  'General',   4),
        ('Ortho',     'General',   4),
        ('Med-Surg',  'General',   5),
        ('General',   'General',   5);
END
GO


-- ── REFERENCE: HEDIS DENOMINATORS ────────────────────────────────────────────
IF OBJECT_ID(N'dbo.ref_hedis_denominators', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ref_hedis_denominators (
        measure_code        NVARCHAR(20)   NOT NULL PRIMARY KEY,
        measure_name        NVARCHAR(255)  NOT NULL,
        denominator_logic   NVARCHAR(1000) NOT NULL,
        numerator_threshold NVARCHAR(255)  NOT NULL
    );

    INSERT INTO dbo.ref_hedis_denominators VALUES
        ('CDC-HBA1C-TEST',
         'Comprehensive Diabetes Care — HbA1c Testing',
         'has_diabetes = 1 AND age BETWEEN 18 AND 75',
         'hedis_hba1c_tested = 1'),
        ('CDC-HBA1C-CONTROL',
         'Comprehensive Diabetes Care — HbA1c Poor Control (>9%)',
         'has_diabetes = 1 AND hedis_hba1c_tested = 1',
         'hba1c_pct <= 9.0 (lower = better)');
END
GO


-- ── REFERENCE: DATE DIMENSION (2020–2035) ─────────────────────────────────────
IF OBJECT_ID(N'dbo.ref_date_dimension', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ref_date_dimension (
        date_key              DATE         NOT NULL PRIMARY KEY,
        calendar_year         SMALLINT     NOT NULL,
        calendar_quarter      TINYINT      NOT NULL,
        calendar_month_num    TINYINT      NOT NULL,
        calendar_month_name   NVARCHAR(10) NOT NULL,
        calendar_week_num     TINYINT      NOT NULL,
        day_of_week_num       TINYINT      NOT NULL,  -- 1=Sunday .. 7=Saturday
        day_of_week_name      NVARCHAR(10) NOT NULL,
        is_weekend            TINYINT      NOT NULL,
        is_us_federal_holiday TINYINT      NOT NULL
    );

    -- Populate 2020-01-01 → 2035-12-31 via recursive CTE
    WITH dates AS (
        SELECT CAST('2020-01-01' AS DATE) AS d
        UNION ALL
        SELECT DATEADD(DAY, 1, d) FROM dates WHERE d < '2035-12-31'
    )
    INSERT INTO dbo.ref_date_dimension
    SELECT
        d,
        YEAR(d),
        DATEPART(QUARTER, d),
        MONTH(d),
        DATENAME(MONTH, d),
        DATEPART(WEEK, d),
        DATEPART(WEEKDAY, d),
        DATENAME(WEEKDAY, d),
        CASE WHEN DATEPART(WEEKDAY, d) IN (1, 7) THEN 1 ELSE 0 END,
        0  -- holiday flag — update manually if needed
    FROM dates
    -- This should be set larger than the number of days in order to stop the recursive CTE.
    -- Because the default recursion in SQL Server is 100 times which will lead to stoppage
    -- immediately after eaching 100 days.
    OPTION (MAXRECURSION 6000);
END
GO
