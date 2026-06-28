/*
================================================================================
  00_teardown.sql
  Complete teardown for HCWarehouse_N2
================================================================================
  PURPOSE
  -------
  Drops every object built by the project in reverse dependency order, then
  removes the database itself.  Designed to reset the environment cleanly
  between demo runs or workshop sessions.

  RUN THIS FROM
  -------------
  SSMS — connect to any database (master is fine) and execute.
  The script uses USE statements to self-relocate, so the initial connection
  database does not matter.

  SAFETY FEATURES
  ---------------
  • IF EXISTS / DROP IF EXISTS on every object — safe to run even if some
    steps were never completed (e.g., Gold views not yet created).
  • ALTER DATABASE ... SET SINGLE_USER WITH ROLLBACK IMMEDIATE before the
    final DROP ensures all open connections (SSMS tabs, Python loader,
    Power BI, etc.) are closed instantly so the drop never hangs.

  DROP ORDER  (dependency graph, outermost consumers first)
  ----------
  Step 1 — Gold Views           (query Silver tables + dbo reference tables)
  Step 2 — Stored Procedures    (write to / read from tables)
  Step 3 — Tables               (Silver → Bronze → Staging → dbo reference)
  Step 4 — Schemas              (can only be dropped once empty)
  Step 5 — Database             (must switch to master first)

  Target  : SQL Server 2019+ / Azure SQL Database
================================================================================
*/

-- ─── Switch into the target database ─────────────────────────────────────────
-- All object drops must run inside HCWarehouse_N2.
USE HCWarehouse_N2;
GO


-- ════════════════════════════════════════════════════════════════════════════
-- STEP 1 — GOLD VIEWS  (16 views)
-- Drop before stored procedures and tables because views have SELECT
-- dependencies on silver.encounters_clean and dbo reference tables.
-- ════════════════════════════════════════════════════════════════════════════

-- ── Data Quality views ────────────────────────────────────────────────────────
DROP VIEW IF EXISTS gold.vw_dq_supply_chain_health;
DROP VIEW IF EXISTS gold.vw_dq_clinical_coherence;
DROP VIEW IF EXISTS gold.vw_dq_fraud_signal_audit;
DROP VIEW IF EXISTS gold.vw_dq_validation_summary;

-- ── Dimension views ───────────────────────────────────────────────────────────
DROP VIEW IF EXISTS gold.vw_dim_safety_incident;
DROP VIEW IF EXISTS gold.vw_dim_surgical_kit;
DROP VIEW IF EXISTS gold.vw_dim_surgeon;
DROP VIEW IF EXISTS gold.vw_dim_nurse;
DROP VIEW IF EXISTS gold.vw_dim_unit;
DROP VIEW IF EXISTS gold.vw_dim_payer;
DROP VIEW IF EXISTS gold.vw_dim_procedure;
DROP VIEW IF EXISTS gold.vw_dim_diagnosis;
DROP VIEW IF EXISTS gold.vw_dim_encounter_date;
DROP VIEW IF EXISTS gold.vw_dim_patient;

-- ── Fact views ────────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS gold.vw_fact_encounter_all;
DROP VIEW IF EXISTS gold.vw_fact_encounter;
GO

PRINT '[Step 1 complete]  16 Gold views dropped.';
GO


-- ════════════════════════════════════════════════════════════════════════════
-- STEP 2 — STORED PROCEDURES  (5 procedures)
-- Drop in consumer → producer order:
--   Gold SP  → Silver SP  → Bronze SP  → dbo utility SPs
-- ════════════════════════════════════════════════════════════════════════════

-- Gold — creates/refreshes all views on each pipeline run
DROP PROCEDURE IF EXISTS gold.usp_createOrAlterViews;

-- Silver — validates Bronze rows and engineers the 28 derived features
DROP PROCEDURE IF EXISTS silver.usp_loadFromBronze;

-- Bronze — casts staging text columns to typed values and inserts clean rows
DROP PROCEDURE IF EXISTS bronze.usp_loadFromStaging;

-- dbo utility — batch management (incremental loading support)
DROP PROCEDURE IF EXISTS dbo.usp_deleteBatch;
DROP PROCEDURE IF EXISTS dbo.usp_listBatches;
GO

PRINT '[Step 2 complete]  5 stored procedures dropped.';
GO


-- ════════════════════════════════════════════════════════════════════════════
-- STEP 3 — TABLES  (9 tables)
-- Dropping a table automatically removes all its indexes, constraints,
-- and statistics — no need to drop them separately.
-- Order: Silver (derived) → Bronze (typed source) → Staging (raw text)
--        → dbo reference tables (looked up by Silver SP and Gold views)
-- ════════════════════════════════════════════════════════════════════════════

-- ── Medallion layers ──────────────────────────────────────────────────────────
DROP TABLE IF EXISTS silver.encounters_clean;   -- 123 columns: 92 source + 28 engineered + 3 system
DROP TABLE IF EXISTS bronze.encounters;         -- 93 typed source columns  (inc. PA MBI)
DROP TABLE IF EXISTS stg.encounters_raw;        -- 93 NVARCHAR(500) columns + 3 system

-- ── dbo reference tables ──────────────────────────────────────────────────────
DROP TABLE IF EXISTS dbo.ref_date_dimension;        -- calendar spine 2020–2035
DROP TABLE IF EXISTS dbo.ref_hedis_denominators;    -- HEDIS 2025 measure definitions
DROP TABLE IF EXISTS dbo.ref_unit_benchmarks;       -- target nurse:patient ratios by unit tier
DROP TABLE IF EXISTS dbo.ref_procedure_benchmarks;  -- expected OR minutes by CPT code
DROP TABLE IF EXISTS dbo.ref_cms_base_rates;        -- CMS DRG base rates by fiscal year
DROP TABLE IF EXISTS dbo.ref_payer_discounts;       -- contractual discount rates by payer
GO

PRINT '[Step 3 complete]  9 tables dropped.';
GO


-- ════════════════════════════════════════════════════════════════════════════
-- STEP 4 — SCHEMAS  (4 custom schemas)
-- A schema can only be dropped once it contains no objects.
-- dbo is a SQL Server system schema and cannot be dropped.
-- ════════════════════════════════════════════════════════════════════════════

IF SCHEMA_ID(N'gold')   IS NOT NULL  DROP SCHEMA gold;
IF SCHEMA_ID(N'silver') IS NOT NULL  DROP SCHEMA silver;
IF SCHEMA_ID(N'bronze') IS NOT NULL  DROP SCHEMA bronze;
IF SCHEMA_ID(N'stg')    IS NOT NULL  DROP SCHEMA stg;
GO

PRINT '[Step 4 complete]  4 schemas dropped (stg, bronze, silver, gold).';
GO


-- ════════════════════════════════════════════════════════════════════════════
-- STEP 5 — DATABASE
-- Must switch to master before dropping a database.
-- SET SINGLE_USER WITH ROLLBACK IMMEDIATE closes every open connection
-- (SSMS query windows, Python loader, Power BI Desktop, etc.) instantly,
-- preventing the DROP from hanging on an active connection.
-- ════════════════════════════════════════════════════════════════════════════

USE master;
GO

ALTER DATABASE HCWarehouse_N2
    SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
GO

DROP DATABASE IF EXISTS HCWarehouse_N2;
GO

PRINT '[Step 5 complete]  Database HCWarehouse_N2 dropped.';
PRINT '';
PRINT '════════════════════════════════════════════════════════════';
PRINT '  Teardown complete.  Environment is clean.';
PRINT '  Re-run 00_initDatabase.sql to rebuild from scratch.';
PRINT '════════════════════════════════════════════════════════════';
GO
