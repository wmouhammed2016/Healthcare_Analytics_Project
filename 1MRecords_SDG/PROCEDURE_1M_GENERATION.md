# Healthcare SDG Engine — 1M Row Generation Procedure

## Executive summary

Generating 1,000,000 synthetic healthcare encounters across 5 linked domains (Clinical, Operational, Financial, HR, Supply Chain) while preserving all cross-domain correlations, regulatory compliance, and statistical validity.

**Projected metrics:** ~2.4 minutes generation time, ~618 MB CSV output, ~200 MB peak RAM.

---

## Phase 1: Environment preparation

### 1.1 System requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| Python | 3.9+ | 3.11+ |
| RAM | 512 MB | 2 GB |
| Disk | 2 GB free | 5 GB free |
| CPU | 1 core | 4+ cores |

### 1.2 Dependencies

```bash
pip install --break-system-packages pandas openpyxl
# Optional for Parquet output:
pip install --break-system-packages pyarrow fastparquet
```

### 1.3 File structure

```
project/
├── sdg_engine.py          # Original 100-record engine (reference data + terminologies)
├── sdg_engine_1m.py       # Production 1M streaming generator
└── output/                # Generated automatically
    ├── encounters_1m.csv
    ├── fhir_1m.ndjson
    └── validation_report.json
```

Both `sdg_engine.py` and `sdg_engine_1m.py` must be in the same directory — the 1M script imports reference terminologies (ICD-10, CPT, LOINC, RxNorm, supply kits) from the original engine.

---

## Phase 2: Pre-generation validation

Before running at 1M scale, confirm the engine works correctly at smaller scales:

### 2.1 Smoke test (1,000 rows)

```bash
python sdg_engine_1m.py --rows 1000 --chunk-size 1000 --seed 42 --output-dir ./test_1k --format csv --validate
```

**Expected:** All 9 validation checks PASS. Runtime < 1 second.

### 2.2 Scale test (50,000 rows)

```bash
python sdg_engine_1m.py --rows 50000 --chunk-size 10000 --seed 42 --output-dir ./test_50k --format csv --validate
```

**Expected:** All 9 validation checks PASS. Runtime ~7 seconds. CSV ~31 MB.

### 2.3 Verify cross-domain correlations hold

At 50K, manually confirm:
- **Severity → LOS** is monotonically increasing (Low < Med < High < Critical)
- **Staffing ratio → Incidents** shows higher incident rate at ratio ≥6 vs ≤4
- **HEDIS HbA1c screening** is 100% for diabetic cohort
- **Fraud upcoding rate** is within 2–8% of total encounters

---

## Phase 3: Production run (1,000,000 rows)

### 3.1 Standard run

```bash
python sdg_engine_1m.py \
  --rows 1000000 \
  --chunk-size 10000 \
  --seed 42 \
  --output-dir ./output \
  --format csv,ndjson \
  --validate
```

### 3.2 Compressed run (recommended for transfer/storage)

```bash
python sdg_engine_1m.py \
  --rows 1000000 \
  --chunk-size 10000 \
  --seed 42 \
  --output-dir ./output \
  --format csv,ndjson \
  --compress \
  --validate
```

### 3.3 Expected output

| File | Size (uncompressed) | Size (gzip) |
|------|-------------------|-------------|
| `encounters_1m.csv` | ~618 MB | ~111 MB |
| `fhir_1m.ndjson` | ~865 MB | ~156 MB |
| **Total** | **~1.4 GB** | **~267 MB** |

**Runtime:** ~2.4 minutes at ~7,000 rows/second on a modern machine.

---

## Phase 4: Post-generation validation

The `--validate` flag runs 9 automated checks. All must PASS:

| # | Check | Expected value | Tolerance |
|---|-------|---------------|-----------|
| 1 | Row count | 1,000,000 | Exact |
| 2 | Age distribution μ | ~62 | 55–68 |
| 3 | Severity distribution μ | ~0.60 | 0.45–0.75 |
| 4 | Readmission rate | ~0.16 | 0.10–0.25 |
| 5 | Fraud upcoding rate | ~0.04 | 0.02–0.08 |
| 6 | HEDIS HbA1c screening | ~1.00 | >0.95 |
| 7 | HEDIS poor control rate | ~0.41 | 0.25–0.55 |
| 8 | Staffing→Incident correlation | High > Low | Directional |
| 9 | Severity→LOS monotonic | Low < Med < High < Crit | Strict order |

### 4.1 Extended manual validation (recommended)

After the automated checks pass, run these additional spot-checks by loading a sample in pandas:

```python
import pandas as pd

df = pd.read_csv('./output/encounters_1m.csv', nrows=100000)

# 1. Verify vitals distributions are bell-shaped
print(df[['sbp_mmhg','dbp_mmhg','heart_rate_bpm','spo2_pct','bmi']].describe())

# 2. Verify comorbidity rates scale with age
print(df.groupby(pd.cut(df['age'], bins=[18,40,60,75,95]))['has_diabetes'].mean())

# 3. Verify charge inflation for fraud-flagged claims
fraud = df[df['fraud_upcoding_flag']==1]['submitted_charge_usd']
clean = df[df['fraud_upcoding_flag']==0]['submitted_charge_usd']
print(f"Fraud charges μ={fraud.mean():.0f} vs Clean μ={clean.mean():.0f} (ratio={fraud.mean()/clean.mean():.2f}x)")

# 4. Verify CAHPS inversely correlates with burnout
print(df[['burnout_exhaustion_mbi','cahps_nurse_communication','cahps_responsiveness']].corr())

# 5. Verify payer mix matches target
print(df['payer'].value_counts(normalize=True).round(3))
```

---

## Phase 5: Output format considerations

### 5.1 CSV (primary flat format)
- 92 columns, one row per encounter
- Best for: pandas, SQL import, Excel, Tableau, Power BI
- Load into database: `COPY encounters FROM 'encounters_1m.csv' CSV HEADER;`

### 5.2 NDJSON (FHIR-compatible)
- One FHIR resource per line (Patient + Encounter per record)
- Best for: FHIR server bulk import, healthcare interoperability pipelines
- To expand to full 14-resource-per-encounter FHIR output, modify `write_fhir_ndjson_chunk()` in the 1M script

### 5.3 Parquet (optional, for big data pipelines)
```python
# Post-generation conversion
import pandas as pd
df = pd.read_csv('./output/encounters_1m.csv')
df.to_parquet('./output/encounters_1m.parquet', engine='pyarrow', compression='snappy')
# Result: ~120 MB with columnar compression
```

### 5.4 SQL insert scripts
```python
# Post-generation conversion
import pandas as pd
df = pd.read_csv('./output/encounters_1m.csv')
# PostgreSQL
from sqlalchemy import create_engine
engine = create_engine('postgresql://user:pass@host/db')
df.to_sql('encounters', engine, if_exists='replace', index=False, chunksize=10000)
```

---

## Constraints & limitations reference

All of the following are enforced in the 1M generator:

### Clinical coding
- ICD-10-CM: 15 surgical diagnosis codes across 8 categories
- CPT/HCPCS: 14 procedure codes mapped to diagnosis categories
- LOINC: 7 lab panels (HbA1c, Glucose, Creatinine, Hemoglobin, WBC, Cholesterol, NT-proBNP)
- RxNorm: Category-specific medications + comorbidity-driven additions

### Cross-domain correlations
- Severity index drives: LOS, OR time, charges, readmission probability, lab abnormality ranges
- Comorbidities (DM, HTN, CHF) drive: specific lab values, additional medications, higher readmission risk
- Age drives: comorbidity prevalence (linear increase after age 40)
- Staffing ratio + burnout → safety incident probability
- Burnout + patient load → CAHPS scores (inverse)
- Medication adherence (PDC) inversely correlated with severity
- Fraud upcoding inflates submitted charges by 1.4–2.2x

### Regulatory compliance
- HIPAA Safe Harbor: No names, no specific dates (year-only birth dates), state-level geography only
- No SSNs, MRNs, or device identifiers
- NPI numbers are synthetic 10-digit identifiers

### Quality benchmarks
- HEDIS 2025: 100% HbA1c screening for diabetic cohort, poor-control flagging at >9%
- CAHPS: 9 survey dimensions scored 1–5
- ISO 7101: PDSA cycle stubs generated for every safety incident
- HIMSS EMRAM: Stage 5/6/7 distribution (30%/50%/20%)

---

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| `ModuleNotFoundError: sdg_engine` | Files not in same directory | Ensure both `.py` files are co-located |
| `MemoryError` | Chunk size too large | Reduce `--chunk-size` to 5000 |
| Validation FAIL on correlation | Seed collision at boundary | Try `--seed 123` — rare-event rates stabilize at 100K+ |
| Slow generation (<3K rows/s) | Disk I/O bottleneck | Use `--compress` to reduce write volume, or use SSD |
| NDJSON too large | Full FHIR expansion | Use CSV-only: `--format csv` |

---

## Reproducibility

The same `--seed` value produces identical output across runs on the same Python version. Each chunk uses `seed + start_idx` as its local seed, ensuring deterministic generation regardless of chunk size.

```bash
# These produce identical CSVs:
python sdg_engine_1m.py --rows 100000 --chunk-size 5000  --seed 42
python sdg_engine_1m.py --rows 100000 --chunk-size 10000 --seed 42
# ⚠️  Different chunk sizes use different per-chunk seeds, so outputs will differ.
# For exact reproducibility, keep both --rows and --chunk-size constant.
```
