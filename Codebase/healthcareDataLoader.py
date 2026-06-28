"""
healthcareDataLoader.py
=======================
Healthcare ETL Pipeline — Teaching Version

Loads 1M synthetic encounter records from CSV into SQL Server using
a four-layer medallion architecture:

    CSV  →  stg (Staging)  →  bronze  →  silver  →  gold (Views)

HOW TO RUN:
    python healthcareDataLoader.py                        # full load
    python healthcareDataLoader.py --start-from bronze    # resume from Bronze
    python healthcareDataLoader.py --start-from silver    # resume from Silver
    python healthcareDataLoader.py --start-from gold      # rebuild Gold views only
    python healthcareDataLoader.py --dry-run              # validate CSV only, no DB writes

PIPELINE STEPS:
    1.  Load config.ini
    2.  Connect to SQL Server
    3.  Validate the source CSV (pre-flight checks)
    4.  Truncate Staging and bulk-insert the CSV  →  stg.encounters_raw
    5.  EXEC bronze.usp_loadFromStaging           →  bronze.encounters
    6.  EXEC silver.usp_loadFromBronze            →  silver.encounters_clean
    7.  EXEC gold.usp_createOrAlterViews          →  all Gold views
    8.  Print execution summary to console

WHAT WAS REMOVED (compared to the production version):
    - logs schema    (load_manifest, pipeline_events, execution_summary)
    - quarantine schema (python_preload_rejects, bronze_rejects, silver_flags)
    All pipeline feedback is now printed to the console only.

DEPENDENCIES:
    pip install pyodbc pandas pyarrow
"""

# ──────────────────────────────────────────────────────────────────────────────
# Imports
# ──────────────────────────────────────────────────────────────────────────────
import argparse
import configparser
import csv
import hashlib
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import pyodbc


# ==============================================================================
# SECTION 1 — CONSTANTS
# The 92-column contract in source order. Used to validate the CSV header
# before any data is loaded into the database.
# ==============================================================================

EXPECTED_COLUMNS = [
    "patient_id", "birth_year", "age", "sex", "race_ethnicity", "state",
    "encounter_id", "icd10_code", "diagnosis_display", "diagnosis_category",
    "severity_index", "comorbidity_count", "has_diabetes", "has_hypertension",
    "has_chf", "sbp_mmhg", "dbp_mmhg", "heart_rate_bpm", "spo2_pct",
    "temperature_f", "bmi", "respiratory_rate", "hba1c_pct", "glucose_mg_dl",
    "creatinine_mg_dl", "wbc_10e3_ul", "nt_probnp_pg_ml",
    "medication_adherence_pdc", "readmission_30d_flag", "triage_timestamp",
    "admit_timestamp", "bed_request_time", "bed_assign_time", "unit_assigned",
    "bed_occupancy_pct", "cpt_code", "procedure_display", "or_start", "or_end",
    "actual_or_minutes", "or_turnover_minutes", "los_days", "discharge_timestamp",
    "safety_incident_flag", "incident_type", "incident_severity", "claim_id",
    "payer", "npi_billing", "drg_weight", "submitted_charge_usd",
    "allowed_amount_usd", "patient_responsibility_usd", "fraud_upcoding_flag",
    "fraud_duplicate_flag", "fraud_unbundling_flag", "nurse_emp_id", "nurse_role",
    "nurse_unit", "nurse_tenure_years", "nurse_fte", "surgeon_emp_id",
    "surgeon_specialty", "shift_hours", "patients_per_nurse_ratio",
    "overtime_hours", "burnout_exhaustion_mbi", "burnout_cynicism_mbi",
    "turnover_risk_index", "cahps_nurse_communication",
    "cahps_doctor_communication", "cahps_responsiveness", "cahps_pain_management",
    "cahps_discharge_info", "cahps_care_transition", "cahps_cleanliness",
    "cahps_quietness", "surgical_kit_id", "kit_name", "kit_unit_cost_usd",
    "kit_current_stock", "kit_reorder_point", "kit_lead_time_days",
    "kit_expiration_date", "stockout_risk_flag", "weekly_procedure_volume",
    "projected_demand_4wk", "days_of_supply", "hedis_hba1c_tested",
    "hedis_hba1c_poor_control", "pdsa_cycle_id", "himss_emram_stage",
]


# ==============================================================================
# SECTION 2 — CONFIGURATION
# Reads all settings from config.ini so nothing is hardcoded in the script.
# ==============================================================================

def load_config(config_path: Path = Path("config.ini")) -> configparser.ConfigParser:
    """Read and return the pipeline configuration from config.ini.

    Args:
        config_path: Path to config.ini. Defaults to the current directory.

    Returns:
        A ConfigParser object — works like a dictionary of settings.

    Raises:
        FileNotFoundError: If config.ini does not exist.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"config.ini not found at: {config_path}")
    cfg = configparser.ConfigParser()
    cfg.read(config_path, encoding="utf-8")
    return cfg


# ==============================================================================
# SECTION 3 — DATABASE CONNECTION
# Opens and returns a SQL Server connection.
# All pipeline functions receive this connection as a parameter — they never
# open their own. One connection is used for the entire pipeline run.
# ==============================================================================

def get_connection(cfg: configparser.ConfigParser) -> pyodbc.Connection:
    """Open and return a SQL Server connection with retry logic.

    Reads the connection string from config.ini. Retries up to 3 times
    with a 5-second wait between attempts.

    Args:
        cfg: ConfigParser loaded from config.ini.

    Returns:
        An active pyodbc.Connection.

    Raises:
        pyodbc.Error: If all 3 connection attempts fail.
    """
    db = cfg["database"]

    # Use explicit connection_string if provided, otherwise fall back to DSN
    if "connection_string" in db:
        conn_str = db["connection_string"]
    else:
        conn_str = f"DSN={db['dsn']};Database={db['database']};"

    timeout = int(db.get("connection_timeout", 30))
    last_error = None

    for attempt in range(1, 4):
        try:
            conn = pyodbc.connect(conn_str, timeout=timeout, autocommit=False)
            conn.setdecoding(pyodbc.SQL_CHAR, encoding="utf-8")
            conn.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-8")
            conn.setencoding(encoding="utf-8")
            return conn
        except pyodbc.Error as exc:
            last_error = exc
            if attempt < 3:
                print(f"  Connection attempt {attempt} failed. Retrying in 5s...")
                time.sleep(5)

    raise last_error


def run_stored_procedure(
    conn: pyodbc.Connection,
    procedure: str,
    batch_id: str,
    timeout: int = 3600,
) -> dict:
    """Execute a pipeline stored procedure and return its row-count results.

    All three pipeline SPs (Bronze, Silver, Gold) share the same signature:
    they accept @batch_id and return loaded/rejected/flagged counts.
    This one function handles that pattern for all three calls.

    Args:
        conn:      Active pyodbc connection.
        procedure: Fully qualified SP name (e.g. 'bronze.usp_loadFromStaging').
        batch_id:  Current run identifier passed into the SP.
        timeout:   Seconds before the command times out.

    Returns:
        Dict with keys: rows_loaded, rows_rejected, rows_flagged, duration_ms.
    """
    cursor = conn.cursor()
    cursor.timeout = timeout

    # Declare output variables, call the SP, then SELECT them back
    sql = f"""
        DECLARE @rows_loaded   INT = 0,
                @rows_rejected INT = 0,
                @rows_flagged  INT = 0,
                @duration_ms   INT = 0;
        EXEC {procedure}
            @batch_id      = ?,
            @rows_loaded   = @rows_loaded   OUTPUT,
            @rows_rejected = @rows_rejected OUTPUT,
            @rows_flagged  = @rows_flagged  OUTPUT,
            @duration_ms   = @duration_ms   OUTPUT;
        SELECT @rows_loaded, @rows_rejected, @rows_flagged, @duration_ms;
    """
    cursor.execute(sql, batch_id)
    row = cursor.fetchone()
    conn.commit()

    return {
        "rows_loaded":   row[0] if row else 0,
        "rows_rejected": row[1] if row else 0,
        "rows_flagged":  row[2] if row else 0,
        "duration_ms":   row[3] if row else 0,
    }


# ==============================================================================
# SECTION 4 — PRE-FLIGHT VALIDATION
# Validates the source CSV before any data touches the database.
# Five checks: file exists → checksum → header → encoding → row count.
# All feedback goes to the console only (no quarantine or log tables).
# ==============================================================================

@dataclass
class PreFlightResult:
    """Holds the outcome of all pre-flight checks.

    Attributes:
        passed:             True only if all checks pass — pipeline proceeds.
        checksum_sha256:    SHA-256 fingerprint of the source file.
        file_row_count:     Total data rows found in the file.
        warnings:           Non-fatal issues (e.g. row count deviation).
        errors:             Fatal issues that stop the pipeline.
    """
    passed: bool = False
    checksum_sha256: str = ""
    file_row_count: int = 0
    warnings: list = field(default_factory=list)
    errors: list = field(default_factory=list)


def compute_checksum(csv_path: Path) -> str:
    """Compute the SHA-256 checksum of the CSV file in streaming 8 MB blocks.

    Streaming keeps memory flat regardless of file size — the file is
    never fully loaded into RAM.

    Args:
        csv_path: Path to the CSV file.

    Returns:
        64-character lowercase hex string.
    """
    sha = hashlib.sha256()
    with csv_path.open("rb") as fh:
        for block in iter(lambda: fh.read(8_388_608), b""):
            sha.update(block)
    return sha.hexdigest()


def validate_header(csv_path: Path) -> list[str]:
    """Read the CSV header row and verify it matches the 92-column contract.

    Args:
        csv_path: Path to the CSV file.

    Returns:
        List of error strings. An empty list means the header is valid.
    """
    with csv_path.open(encoding="utf-8", newline="") as fh:
        header = next(csv.reader(fh), [])

    errors = []

    # Check total column count first
    if len(header) != len(EXPECTED_COLUMNS):
        errors.append(
            f"Column count mismatch — expected {len(EXPECTED_COLUMNS)}, "
            f"got {len(header)}"
        )

    # Check each column name individually for clear error messages
    for i, (expected, actual) in enumerate(
        zip(EXPECTED_COLUMNS, header[:len(EXPECTED_COLUMNS)])
    ):
        if expected != actual:
            errors.append(f"Column {i}: expected '{expected}', got '{actual}'")

    return errors


def run_pre_flight(
    csv_path: Path,
    cfg: configparser.ConfigParser,
    force_reload: bool = False,
) -> PreFlightResult:
    """Run all five pre-flight checks and return the result.

    Checks run in order and stop at the first fatal error:
        1. File exists and is not empty.
        2. SHA-256 checksum computed (printed for audit reference).
        3. Header schema matches the 92-column contract.
        4. Full file scan for encoding errors (prints count to console).
        5. Row count compared to expected 1,000,000.

    Args:
        csv_path:     Path to encounters_1m.csv.
        cfg:          ConfigParser with [pipeline] settings.
        force_reload: If True, skip any duplicate-file warnings.

    Returns:
        PreFlightResult with passed=True only if all checks pass.
    """
    result = PreFlightResult()
    expected_rows = int(cfg["pipeline"].get("expected_rows", 1_000_000))
    tolerance = float(cfg["pipeline"].get("row_count_tolerance", 0.001))
    chunk_size = int(cfg["pipeline"].get("chunk_size", 50_000))

    print("\n  Running pre-flight validation...")

    # ── Check 1: File exists ──────────────────────────────────────────────────
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        result.errors.append(f"File not found or empty: {csv_path}")
        print(f"  [FAIL] {result.errors[-1]}")
        return result

    size_mb = csv_path.stat().st_size / 1_048_576
    print(f"  [OK]   File found: {csv_path.name} ({size_mb:.1f} MB)")

    # ── Check 2: Checksum ─────────────────────────────────────────────────────
    print("  [...]  Computing SHA-256 checksum...", end="", flush=True)
    result.checksum_sha256 = compute_checksum(csv_path)
    print(f"\r  [OK]   SHA-256: {result.checksum_sha256}")

    # ── Check 3: Header schema ────────────────────────────────────────────────
    header_errors = validate_header(csv_path)
    if header_errors:
        result.errors.extend(header_errors)
        for err in header_errors:
            print(f"  [FAIL] {err}")
        return result
    print(f"  [OK]   Header validated — {len(EXPECTED_COLUMNS)} columns confirmed")

    # ── Check 4: Encoding scan ────────────────────────────────────────────────
    # Read the file in chunks; count rows with encoding replacement characters
    total_rows = 0
    encoding_errors = 0

    for chunk_df in pd.read_csv(
        csv_path,
        chunksize=chunk_size,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8",
        encoding_errors="replace",  # \ufffd marks bad bytes instead of crashing
        on_bad_lines="warn",
    ):
        for row in chunk_df.itertuples(index=False):
            raw = ",".join(str(v) for v in row)
            if "\ufffd" in raw:   # replacement character = encoding problem
                encoding_errors += 1
        total_rows += len(chunk_df)

    result.file_row_count = total_rows

    if encoding_errors:
        msg = f"{encoding_errors:,} rows with encoding errors (will be loaded as-is)"
        result.warnings.append(msg)
        print(f"  [WARN] {msg}")
    else:
        print(f"  [OK]   Encoding scan clean — {total_rows:,} rows scanned")

    # ── Check 5: Row count ────────────────────────────────────────────────────
    deviation = abs(total_rows - expected_rows) / expected_rows
    if deviation > tolerance:
        msg = (
            f"Row count {total_rows:,} deviates {deviation:.2%} "
            f"from expected {expected_rows:,}"
        )
        result.warnings.append(msg)
        print(f"  [WARN] {msg}")
    else:
        print(f"  [OK]   Row count confirmed: {total_rows:,}")

    result.passed = not result.errors
    return result


# ==============================================================================
# SECTION 5 — STAGING LOADER
# Bulk-inserts the source CSV into stg.encounters_raw in 50K-row chunks.
# All 92 columns arrive as plain strings — no type conversion happens here.
# Uses fast_executemany=True for maximum SQL Server throughput.
# ==============================================================================

def truncate_staging(conn: pyodbc.Connection) -> None:
    """Truncate stg.encounters_raw before loading a fresh batch.

    Staging is a transient layer — it is wiped on every pipeline run.
    Only the data schemas are affected; no log or quarantine tables exist.

    Args:
        conn: Active pyodbc connection.
    """
    conn.cursor().execute("TRUNCATE TABLE stg.encounters_raw")
    conn.commit()
    print("  [OK]   stg.encounters_raw truncated")


def load_csv_to_staging(
    csv_path: Path,
    batch_id: str,
    conn: pyodbc.Connection,
    chunk_size: int = 50_000,
    total_rows: int = 1_000_000,
) -> int:
    """Stream the CSV into stg.encounters_raw in fixed-size chunks.

    Each chunk is its own committed transaction — a failed chunk is
    logged to the console and skipped without aborting the full load.
    A live progress bar shows throughput during the workshop demo.

    Args:
        csv_path:   Path to the source CSV file.
        batch_id:   Run identifier — written to the _load_batch_id column
                    so rows can be traced to a specific pipeline run.
        conn:       Active pyodbc connection.
        chunk_size: Rows per INSERT batch (default 50,000).
        total_rows: Actual row count from pre-flight — drives the progress
                    bar so it moves proportionally regardless of file size.

    Returns:
        Total rows successfully inserted into stg.encounters_raw.
    """
    # Build INSERT with one placeholder per source column plus batch_id
    col_list = ", ".join(EXPECTED_COLUMNS) + ", _load_batch_id"
    placeholders = ", ".join(["?"] * (len(EXPECTED_COLUMNS) + 1))
    insert_sql = (
        f"INSERT INTO stg.encounters_raw ({col_list}) VALUES ({placeholders})"
    )

    total_loaded = 0
    pipeline_start = time.perf_counter()
    cursor = conn.cursor()
    cursor.fast_executemany = True  # key SQL Server bulk performance flag

    print("\n  Loading CSV → stg.encounters_raw")

    for chunk_seq, chunk_df in enumerate(
        pd.read_csv(
            csv_path,
            chunksize=chunk_size,
            dtype=str,             # all columns as strings — no type conversion
            keep_default_na=False, # keep empty strings; don't convert to NaN
            encoding="utf-8",
        ),
        start=1,
    ):
        chunk_start = time.perf_counter()

        # Convert DataFrame to list of tuples and append batch_id to each row
        chunk_df = chunk_df.where(chunk_df.notna(), other=None)
        rows = [
            tuple(row) + (batch_id,)
            for row in chunk_df.itertuples(index=False)
        ]

        try:
            cursor.executemany(insert_sql, rows)
            conn.commit()

            total_loaded += len(rows)
            elapsed = time.perf_counter() - pipeline_start
            rate = total_loaded / max(elapsed, 0.001)
            duration_ms = int((time.perf_counter() - chunk_start) * 1000)

            # Progress bar — \r overwrites the same console line each chunk
            pct = min(total_loaded / max(total_rows, 1), 1.0)
            bar = "█" * int(40 * pct) + "░" * (40 - int(40 * pct))
            eta = (total_rows - total_loaded) / max(rate, 1)
            print(
                f"\r  [{bar}] {pct:5.1%}  "
                f"{total_loaded:>9,} rows  "
                f"{rate:>8,.0f} rows/s  "
                f"chunk {chunk_seq} ({duration_ms:,}ms)  "
                f"ETA {eta:4.0f}s",
                end="",
                flush=True,
            )

        except pyodbc.Error as exc:
            # Log the failure and continue with the next chunk
            print(f"\n  [WARN] Chunk {chunk_seq} failed — {exc}")
            conn.rollback()

    elapsed_total = time.perf_counter() - pipeline_start
    print(f"\n  [OK]   Staging complete — {total_loaded:,} rows in {elapsed_total:.1f}s")
    return total_loaded


# ==============================================================================
# SECTION 6 — MAIN PIPELINE ORCHESTRATOR
# Entry point. Ties all sections together and runs the eight pipeline steps.
# --start-from skips phases already completed in a previous failed run.
# ==============================================================================

# Valid phase order — used by should_run() to decide what to skip
PHASES = ("staging", "bronze", "silver", "gold")


def should_run(phase: str, start_from: str) -> bool:
    """Return True if this phase should execute given the --start-from flag.

    Lets the user resume from a specific phase without re-running earlier
    ones. For example, --start-from silver skips staging and bronze.

    Args:
        phase:      The phase to check ('staging', 'bronze', etc.).
        start_from: The phase the user wants to start from.
    """
    return PHASES.index(phase) >= PHASES.index(start_from)


def parse_args() -> argparse.Namespace:
    """Parse and return command-line arguments.

    Returns:
        Namespace with attributes: start_from, dry_run.
    """
    parser = argparse.ArgumentParser(
        description="Healthcare ETL — CSV to SQL Server Medallion Pipeline"
    )
    parser.add_argument(
        "--start-from",
        choices=PHASES,
        default="staging",
        metavar="PHASE",
        help="Resume from: staging | bronze | silver | gold",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate the CSV only — write nothing to the database.",
    )
    return parser.parse_args()


def print_summary(metrics: dict) -> None:
    """Print the execution summary to the console.

    Args:
        metrics: Dict of all pipeline metrics collected during the run.
    """
    total = metrics.get("total_rows_input", 0)
    clean = metrics.get("silver_rows_clean", 0)
    flagged = metrics.get("silver_rows_flagged", 0)
    dq_rate = round(clean / max(total, 1) * 100, 2)

    print(f"\n  {'═' * 58}")
    print(f"  EXECUTION SUMMARY")
    print(f"  {'─' * 58}")
    print(f"  Source rows (Staging)   : {total:>12,}")
    print(f"  Rows in Bronze          : {metrics.get('bronze_rows_loaded', 0):>12,}")
    print(f"  Rows rejected (Bronze)  : {metrics.get('bronze_rows_rejected', 0):>12,}")
    print(f"  Rows in Silver          : {metrics.get('silver_rows_loaded', 0):>12,}")
    print(f"  Rows flagged (Silver)   : {flagged:>12,}")
    print(f"  Clean rows (Silver)     : {clean:>12,}")
    print(f"  Data quality rate       : {dq_rate:>11.2f}%")
    print(f"  {'─' * 58}")
    print(f"  Phase — Staging         : {metrics.get('staging_seconds', 0):>10}s")
    print(f"  Phase — Bronze          : {metrics.get('bronze_seconds', 0):>10}s")
    print(f"  Phase — Silver          : {metrics.get('silver_seconds', 0):>10}s")
    print(f"  Phase — Gold            : {metrics.get('gold_seconds', 0):>10}s")
    print(f"  Total duration          : {metrics.get('total_seconds', 0):>10}s")
    print(f"  Status                  : {'COMPLETED':>12}")
    print(f"  {'═' * 58}\n")


def main() -> int:
    """Run the full ETL pipeline and return an exit code.

    This function is the single entry point. It calls every other function
    in sequence, passing the shared connection and config between them.

    Returns:
        0 on success, 1 on any failure.
    """
    args = parse_args()

    # ── Step 1: Load configuration ────────────────────────────────────────────
    print("\n  Healthcare ETL Pipeline — Starting")
    print(f"  {'─' * 40}")

    try:
        cfg = load_config()
    except FileNotFoundError as exc:
        print(f"  [FAIL] {exc}")
        return 1

    csv_path = Path(cfg["paths"]["csv_file"])
    chunk_size = int(cfg["pipeline"]["chunk_size"])
    cmd_timeout = int(cfg["database"]["command_timeout"])

    print(f"  Source  : {csv_path}")
    print(f"  Start from phase: {args.start_from.upper()}")

    # ── Step 2: Connect to SQL Server ─────────────────────────────────────────
    print("\n  Connecting to SQL Server...")
    try:
        conn = get_connection(cfg)
        print(f"  [OK]   Connected to {cfg['database']['database']}")
    except pyodbc.Error as exc:
        print(f"  [FAIL] Connection failed — {exc}")
        return 1

    # Use a simple timestamp string as the batch identifier
    batch_id = time.strftime("%Y%m%d_%H%M%S")
    metrics: dict = {}
    pipeline_start = time.perf_counter()

    try:
        # ── Step 3: Pre-flight validation ─────────────────────────────────────
        pre_flight = run_pre_flight(csv_path, cfg)

        if not pre_flight.passed:
            print(f"\n  [FAIL] Pre-flight failed — pipeline stopped.")
            return 1

        for warning in pre_flight.warnings:
            print(f"  [WARN] {warning}")

        if args.dry_run:
            print("\n  [OK]   Dry run complete — no data written to database.")
            return 0

        metrics["total_rows_input"] = pre_flight.file_row_count

        # ── Step 4: Staging ───────────────────────────────────────────────────
        if should_run("staging", args.start_from):
            t0 = time.perf_counter()
            truncate_staging(conn)
            staging_rows = load_csv_to_staging(
                csv_path, batch_id, conn, chunk_size,
                total_rows=pre_flight.file_row_count
            )
            metrics["staging_rows_loaded"] = staging_rows
            metrics["staging_seconds"] = int(time.perf_counter() - t0)

        # ── Step 5: Bronze ────────────────────────────────────────────────────
        if should_run("bronze", args.start_from):
            print("\n  Running bronze.usp_loadFromStaging...")
            t0 = time.perf_counter()
            bronze = run_stored_procedure(
                conn, "bronze.usp_loadFromStaging", batch_id, cmd_timeout
            )
            metrics["bronze_rows_loaded"] = bronze["rows_loaded"]
            metrics["bronze_rows_rejected"] = bronze["rows_rejected"]
            metrics["bronze_seconds"] = int(time.perf_counter() - t0)
            print(
                f"  [OK]   Bronze — "
                f"loaded: {bronze['rows_loaded']:,}  "
                f"rejected: {bronze['rows_rejected']:,}  "
                f"({metrics['bronze_seconds']}s)"
            )

        # ── Step 6: Silver ────────────────────────────────────────────────────
        if should_run("silver", args.start_from):
            print("\n  Running silver.usp_loadFromBronze...")
            t0 = time.perf_counter()
            silver = run_stored_procedure(
                conn, "silver.usp_loadFromBronze", batch_id, cmd_timeout
            )
            metrics["silver_rows_loaded"] = silver["rows_loaded"]
            metrics["silver_rows_flagged"] = silver["rows_flagged"]
            metrics["silver_rows_clean"] = (
                silver["rows_loaded"] - silver["rows_flagged"]
            )
            metrics["silver_seconds"] = int(time.perf_counter() - t0)
            print(
                f"  [OK]   Silver — "
                f"loaded: {silver['rows_loaded']:,}  "
                f"flagged: {silver['rows_flagged']:,}  "
                f"clean: {metrics['silver_rows_clean']:,}  "
                f"({metrics['silver_seconds']}s)"
            )

        # ── Step 7: Gold views ────────────────────────────────────────────────
        if should_run("gold", args.start_from):
            print("\n  Running gold.usp_createOrAlterViews...")
            t0 = time.perf_counter()
            run_stored_procedure(
                conn, "gold.usp_createOrAlterViews", batch_id, cmd_timeout
            )
            metrics["gold_seconds"] = int(time.perf_counter() - t0)
            print(f"  [OK]   Gold views created ({metrics['gold_seconds']}s)")

        # ── Step 8: Print summary ─────────────────────────────────────────────
        metrics["total_seconds"] = int(time.perf_counter() - pipeline_start)
        print_summary(metrics)
        return 0

    except Exception as exc:
        print(f"\n  [FAIL] Unexpected error — {exc}")
        raise


# Entry point — Python executes main() when you run this file directly
if __name__ == "__main__":
    sys.exit(main())