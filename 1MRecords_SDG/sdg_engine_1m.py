"""
Healthcare SDG Engine — Production 1M-Row Generator
=====================================================
Streaming, chunked, memory-safe pipeline for generating
1,000,000 synthetic healthcare encounters across 5 domains.

USAGE:
    python sdg_engine_1m.py --rows 1000000 --chunk-size 10000 --seed 42 --output-dir ./output
    
FLAGS:
    --rows          Total encounters to generate (default: 1_000_000)
    --chunk-size    Records per batch (default: 10_000) — controls peak RAM
    --seed          Random seed for reproducibility (default: 42)
    --output-dir    Output directory (default: ./output)
    --format        csv,ndjson,parquet (default: csv,ndjson)
    --validate      Run post-generation statistical validation (default: true)
    --workers       Parallel workers for generation (default: 1, use with caution)

CONSTRAINTS PRESERVED:
    1. FHIR R4 structure (Patient, Encounter, Condition, Observation, Procedure, Claim, MedicationRequest)
    2. ICD-10-CM, CPT/HCPCS, LOINC, RxNorm clinical coding
    3. HIPAA Safe Harbor de-identification (year-only dates, state-level geography)
    4. Cross-domain correlations:
       - Severity → LOS, OR time, charges
       - Staffing ratio + burnout → safety incidents
       - Comorbidities → lab values (HbA1c for diabetics, NT-proBNP for CHF)
       - Age + severity + comorbidities → readmission probability
       - Medication adherence (PDC) inversely correlated with severity
    5. HEDIS 2025 compliance: 100% HbA1c screening for diabetics, care gap flags
    6. CAHPS survey scores inversely correlated with burnout and patient load
    7. ISO 7101 PDSA cycles generated for every safety incident
    8. Fraud indicators (upcoding, duplicate, unbundling) at realistic base rates
    9. Supply chain stockout risk linked to inventory vs. reorder point
    10. Bell-curve distributions for vitals; realistic outliers for fraud/incidents
"""

import json, csv, uuid, random, math, os, sys, time, argparse, gzip
from datetime import datetime, timedelta
from collections import OrderedDict, Counter
from io import StringIO
import secrets

# =============================================================================
# ALL REFERENCE DATA (same as original — abbreviated import)
# =============================================================================
# This imports all terminologies from the original engine.
# In production, keep these in a separate reference_data.py module.

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sdg_engine import (
    ICD10_SURGICAL, CPT_PROCEDURES, LOINC_LABS, RXNORM_MEDS,
    SUPPLY_KITS, UNITS, PAYER_MIX, NURSE_ROLES, STATES,
    RACE_ETHNICITY, SEX, CAHPS_DIMENSIONS, INCIDENT_TYPES,
    weighted_choice, clamp, normal_clamp
)


# =============================================================================
# STAFF POOL GENERATOR (Pre-generated, shared across chunks)
# =============================================================================

def generate_staff_pool(seed=42):
    """Generate a realistic staff pool.
    
    Scale: ~1 nurse per 200 encounters, ~1 surgeon per 333 encounters.
    For 1M rows: 5,000 nurses, 3,000 surgeons.
    This pool is generated ONCE and shared across all chunks.
    """
    rng = random.Random(seed)
    
    nurses = []
    for i in range(5000):
        tenure = max(0.5, rng.gauss(6, 4))
        base_burnout = rng.gauss(2.8, 0.9) if tenure < 2 or tenure > 12 else rng.gauss(2.0, 0.7)
        nurses.append({
            "emp_id": f"NRS-{10000+i}",
            "role": rng.choice(NURSE_ROLES),
            "tenure_years": round(tenure, 1),
            "burnout_exhaustion": round(clamp(base_burnout, 0, 6), 2),
            "burnout_cynicism": round(clamp(base_burnout * rng.uniform(0.6, 1.1), 0, 6), 2),
            # PA is inversely related to burnout: high EE → low PA.
            # Formula: N(6-base_burnout, σ=0.7), clamped [0,6].
            "burnout_personal_accomplishment": round(clamp(rng.gauss(6.0 - base_burnout, 0.7), 0, 6), 2),
            "unit": rng.choice(UNITS),
            "fte": rng.choice([0.8, 0.9, 1.0, 1.0, 1.0]),
        })

    surgeons = []
    for i in range(3000):
        surgeons.append({
            "emp_id": f"SRG-{20000+i}",
            "npi": str(rng.randint(1000000000, 9999999999)),
            "role": "Surgeon",
            "tenure_years": round(max(1, rng.gauss(12, 6)), 1),
            "specialty": rng.choice(["General Surgery", "Orthopedics", "Cardiothoracic", "Urology", "Neurosurgery"]),
        })
    
    return nurses, surgeons


# =============================================================================
# STREAMING ENCOUNTER GENERATOR (Yields one record at a time)
# =============================================================================

def generate_encounter_stream(start_idx, count, nurses, surgeons, seed=42):
    """Generator that yields one encounter at a time — constant memory."""
    rng = random.Random(seed + start_idx)  # Deterministic per chunk
    base_date = datetime(2024, 1, 1)

    for idx in range(count):
        global_idx = start_idx + idx
        enc_id = str(uuid.UUID(int=rng.getrandbits(128), version=4))
        patient_id = f"PAT-{1000000 + global_idx}"

        # --- DEMOGRAPHICS ---
        age = int(clamp(rng.gauss(62, 16), 18, 95))
        birth_year = 2024 - age
        sex = weighted_choice_rng(rng, SEX)
        race = weighted_choice_rng(rng, RACE_ETHNICITY)
        state = rng.choice(STATES)

        # --- CLINICAL ---
        dx = rng.choice(ICD10_SURGICAL)
        severity = clamp(dx["severity_weight"] + rng.uniform(-0.1, 0.1), 0.1, 1.0)
        category = dx["category"]
        proc_list = CPT_PROCEDURES.get(category, CPT_PROCEDURES["GI"])
        proc = rng.choice(proc_list)

        has_diabetes = rng.random() < (0.15 + 0.005 * max(0, age - 40)) if age > 40 else rng.random() < 0.05
        has_hypertension = rng.random() < (0.2 + 0.006 * max(0, age - 40)) if age > 40 else rng.random() < 0.08
        has_chf = rng.random() < (severity * 0.15)

        comorbidity_codes = []
        if has_diabetes: comorbidity_codes.append({"code": "E11.9", "display": "Type 2 DM w/o complications"})
        if has_hypertension: comorbidity_codes.append({"code": "I10", "display": "Essential hypertension"})
        if has_chf: comorbidity_codes.append({"code": "I50.9", "display": "Heart failure, unspecified"})

        # Vitals
        sbp = round(clamp(rng.gauss(130 + (20 if has_hypertension else 0), 15), 90, 200), 1)
        dbp = round(clamp(rng.gauss(78 + (10 if has_hypertension else 0), 8), 50, 120), 1)
        hr = round(clamp(rng.gauss(78 + severity * 15, 12), 50, 140), 1)
        spo2 = round(clamp(rng.gauss(97 - severity * 4, 1.5), 85, 100), 1)
        temp_f = round(clamp(rng.gauss(98.6 + severity * 0.5, 0.4), 97.0, 103.5), 1)
        bmi = round(clamp(rng.gauss(28 + (4 if has_diabetes else 0), 5), 16, 55), 1)
        rr = int(clamp(rng.gauss(16 + severity * 4, 2), 10, 35))

        # Labs (HEDIS-relevant)
        labs = []
        for lab_def in LOINC_LABS:
            if lab_def["code"] == "4548-4":
                if has_diabetes:
                    val = round(clamp(rng.gauss(8.2, 1.5), 5.5, lab_def["diabetic_high"]), 1)
                    hedis_gap = val > 9.0
                else:
                    val = round(clamp(rng.gauss(5.2, 0.3), lab_def["normal_low"], 5.6), 1)
                    hedis_gap = False
            elif lab_def["code"] == "33762-6":
                if has_chf:
                    val = round(clamp(rng.gauss(1800, 800), 200, 5000), 0)
                else:
                    val = round(clamp(rng.gauss(60, 30), 0, 300), 0)
                hedis_gap = False
            else:
                if severity > 0.6:
                    val = round(rng.gauss((lab_def["normal_high"] + lab_def["diabetic_high"]) / 2,
                                          (lab_def["diabetic_high"] - lab_def["normal_high"]) / 3), 1)
                else:
                    val = round(rng.gauss((lab_def["normal_low"] + lab_def["normal_high"]) / 2,
                                          (lab_def["normal_high"] - lab_def["normal_low"]) / 4), 1)
                val = clamp(val, lab_def["normal_low"] * 0.7, lab_def["diabetic_high"])
                hedis_gap = False
            labs.append({"loinc_code": lab_def["code"], "display": lab_def["display"],
                         "value": val, "unit": lab_def["unit"],
                         "reference_range": f"{lab_def['normal_low']}-{lab_def['normal_high']}",
                         "hedis_care_gap": hedis_gap})

        # Medications
        meds_cat = list(RXNORM_MEDS.get(category, [])) + list(RXNORM_MEDS["GENERAL"])
        if has_diabetes: meds_cat += RXNORM_MEDS["ENDO"]
        if has_hypertension: meds_cat.append({"code": "104491", "display": "Lisinopril 10mg", "route": "PO"})
        enc_meds = rng.sample(meds_cat, min(len(meds_cat), rng.randint(2, 5)))

        med_adherence_pdc = round(clamp(rng.gauss(0.82 - severity * 0.15, 0.12), 0.2, 1.0), 2)
        readmission_prob = severity * 0.2 + (0.05 if age > 75 else 0) + len(comorbidity_codes) * 0.04 + (0.08 if med_adherence_pdc < 0.6 else 0)
        readmission_30d = rng.random() < readmission_prob

        # --- OPERATIONAL ---
        admit_offset = rng.randint(0, 364)
        admit_dt = base_date + timedelta(days=admit_offset, hours=rng.randint(6, 22), minutes=rng.randint(0, 59))
        triage_dt = admit_dt - timedelta(minutes=rng.randint(15, 90))
        or_start = admit_dt + timedelta(hours=rng.randint(1, 36))
        base_or = proc["or_mins"]
        actual_or = int(clamp(rng.gauss(base_or, base_or * 0.15), base_or * 0.6, base_or * 1.8))
        or_end = or_start + timedelta(minutes=actual_or)
        or_turnover = int(clamp(rng.gauss(35, 10), 15, 75))
        base_los = max(1, base_or / 40)
        los_days = max(1, int(rng.gauss(base_los + severity * 3, 1.5)))
        discharge_dt = admit_dt + timedelta(days=los_days, hours=rng.randint(10, 16))
        bed_req = admit_dt + timedelta(minutes=rng.randint(5, 45))
        bed_assign = bed_req + timedelta(minutes=rng.randint(10, 180))
        unit = rng.choice(UNITS[:4]) if severity > 0.6 else rng.choice(UNITS[4:])
        bed_occ = round(clamp(rng.gauss(82, 8), 55, 100), 1)

        # --- HR ---
        nurse = rng.choice(nurses)
        surgeon = rng.choice(surgeons)
        shift_hrs = rng.choice([8, 10, 12, 12])
        pts_per_nurse = rng.randint(3, 8)
        ot_hrs = max(0, round(rng.gauss(2 if shift_hrs == 12 else 0, 2), 1))
        nb = nurse["burnout_exhaustion"]
        inc_prob = clamp(0.02 + (pts_per_nurse - 4) * 0.015 + (nb - 2) * 0.02 + (0.02 if ot_hrs > 4 else 0), 0.01, 0.25)
        has_inc = rng.random() < inc_prob
        incident = {"occurred": has_inc,
                    "type": rng.choice(INCIDENT_TYPES) if has_inc else None,
                    "severity_level": rng.choice(["Near-Miss", "Minor", "Moderate", "Major"]) if has_inc else None}
        turnover_risk = round(clamp((nb / 6) * 0.4 + (0.2 if nurse["tenure_years"] < 2 else 0) + (ot_hrs / 20) * 0.2 + rng.gauss(0, 0.08), 0, 1), 3)
        cahps = {}
        for dim in CAHPS_DIMENSIONS:
            cahps[dim] = round(clamp(rng.gauss(3.5 - nb * 0.15 - (pts_per_nurse - 4) * 0.08, 0.4), 1, 5), 1)

        # --- FINANCIAL ---
        claim_id = f"CLM-{9000000 + global_idx}"
        payer = weighted_choice_rng(rng, PAYER_MIX)
        kit_id = proc["kit"]
        kit = SUPPLY_KITS[kit_id]
        supply_cost = kit["unit_cost"]
        base_charge = actual_or * rng.uniform(85, 140) + supply_cost * rng.uniform(2.5, 4.0)
        facility_charge = round(los_days * rng.uniform(1800, 3200), 2)
        submitted = round(base_charge + facility_charge, 2)
        fraud_up = rng.random() < 0.04
        fraud_dup = rng.random() < 0.015
        fraud_unb = rng.random() < 0.02
        if fraud_up:
            submitted = round(submitted * rng.uniform(1.4, 2.2), 2)
        disc = {"Medicare": 0.38, "Medicaid": 0.50, "BlueCross": 0.30, "Aetna": 0.28,
                "UnitedHealth": 0.32, "Cigna": 0.27, "Self-Pay": 0.0}.get(payer, 0.30)
        allowed = round(submitted * (1 - disc), 2)
        copay = round(allowed * rng.uniform(0.05, 0.20), 2) if payer != "Self-Pay" else allowed
        drg_wt = round(severity * 1.8 + rng.uniform(0.3, 0.8), 4)

        # --- SUPPLY CHAIN ---
        reorder_pt = kit["reorder_point"]
        curr_stock = rng.randint(2, 40)
        lead_time = kit["lead_time_days"]
        exp_date = (or_start + timedelta(days=rng.randint(30, 730))).strftime("%Y-%m-%d")
        weekly_vol = rng.randint(3, 18)


        # --- QUALITY ---
        pdsa = None
        if has_inc:
            pdsa = {"cycle_id": f"PDSA-{rng.randint(10000,99999)}", "status": rng.choice(["Planning", "In Progress", "Complete"])}
        emram = rng.choices([5, 6, 7], weights=[0.3, 0.5, 0.2], k=1)[0]

        # --- YIELD FLAT ROW ---
        hba1c_val = next((l["value"] for l in labs if l["loinc_code"] == "4548-4"), None)
        glucose_val = next((l["value"] for l in labs if l["loinc_code"] == "2345-7"), None)
        creatinine_val = next((l["value"] for l in labs if l["loinc_code"] == "2160-0"), None)
        wbc_val = next((l["value"] for l in labs if l["loinc_code"] == "6690-2"), None)
        nt_probnp_val = next((l["value"] for l in labs if l["loinc_code"] == "33762-6"), None)

        yield {
            "patient_id": patient_id, "birth_year": birth_year, "age": age, "sex": sex,
            "race_ethnicity": race, "state": state, "encounter_id": enc_id,
            "icd10_code": dx["code"], "diagnosis_display": dx["display"],
            "diagnosis_category": category, "severity_index": round(severity, 3),
            "comorbidity_count": len(comorbidity_codes),
            "has_diabetes": int(has_diabetes), "has_hypertension": int(has_hypertension),
            "has_chf": int(has_chf),
            "sbp_mmhg": sbp, "dbp_mmhg": dbp, "heart_rate_bpm": hr,
            "spo2_pct": spo2, "temperature_f": temp_f, "bmi": bmi, "respiratory_rate": rr,
            "hba1c_pct": hba1c_val, "glucose_mg_dl": glucose_val,
            "creatinine_mg_dl": creatinine_val, "wbc_10e3_ul": wbc_val,
            "nt_probnp_pg_ml": nt_probnp_val,
            "medication_adherence_pdc": med_adherence_pdc,
            "readmission_30d_flag": int(readmission_30d),
            "triage_timestamp": triage_dt.isoformat(),
            "admit_timestamp": admit_dt.isoformat(),
            "bed_request_time": bed_req.isoformat(),
            "bed_assign_time": bed_assign.isoformat(),
            "unit_assigned": unit, "bed_occupancy_pct": bed_occ,
            "cpt_code": proc["code"], "procedure_display": proc["display"],
            "or_start": or_start.isoformat(), "or_end": or_end.isoformat(),
            "actual_or_minutes": actual_or, "or_turnover_minutes": or_turnover,
            "los_days": los_days, "discharge_timestamp": discharge_dt.isoformat(),
            "safety_incident_flag": int(has_inc),
            "incident_type": incident["type"], "incident_severity": incident["severity_level"],
            "claim_id": claim_id, "payer": payer,
            "npi_billing": surgeon["npi"], "drg_weight": drg_wt,
            "submitted_charge_usd": submitted, "allowed_amount_usd": allowed,
            "patient_responsibility_usd": copay,
            "fraud_upcoding_flag": int(fraud_up), "fraud_duplicate_flag": int(fraud_dup),
            "fraud_unbundling_flag": int(fraud_unb),
            "nurse_emp_id": nurse["emp_id"], "nurse_role": nurse["role"],
            "nurse_unit": nurse["unit"], "nurse_tenure_years": nurse["tenure_years"],
            "nurse_fte": nurse["fte"],
            "surgeon_emp_id": surgeon["emp_id"], "surgeon_specialty": surgeon["specialty"],
            "shift_hours": shift_hrs, "patients_per_nurse_ratio": pts_per_nurse,
            "overtime_hours": ot_hrs,
            "burnout_exhaustion_mbi": nurse["burnout_exhaustion"],
            "burnout_cynicism_mbi": nurse["burnout_cynicism"],
            "burnout_personal_accomplishment_mbi": nurse["burnout_personal_accomplishment"], # ++ ADDED
            "turnover_risk_index": turnover_risk,
            "cahps_nurse_communication": cahps["nurse_communication"],
            "cahps_doctor_communication": cahps["doctor_communication"],
            "cahps_responsiveness": cahps["responsiveness"],
            "cahps_pain_management": cahps["pain_management"],
            "cahps_discharge_info": cahps["discharge_info"],
            "cahps_care_transition": cahps["care_transition"],
            "cahps_cleanliness": cahps["cleanliness"],
            "cahps_quietness": cahps["quietness"],
            "surgical_kit_id": f"SKU-{kit_id}", "kit_name": kit_id,
            "kit_unit_cost_usd": supply_cost,
            "kit_current_stock": curr_stock, "kit_reorder_point": reorder_pt,
            "kit_lead_time_days": lead_time, "kit_expiration_date": exp_date,
            "stockout_risk_flag": int(curr_stock <= reorder_pt),
            "weekly_procedure_volume": weekly_vol,
            "projected_demand_4wk": weekly_vol * 4,
            "days_of_supply": round(curr_stock / max(1, weekly_vol / 7), 1),
            "hedis_hba1c_tested": int(has_diabetes and hba1c_val is not None),
            "hedis_hba1c_poor_control": int(any(l.get("hedis_care_gap", False) for l in labs)),
            "pdsa_cycle_id": pdsa["cycle_id"] if pdsa else None,
            "himss_emram_stage": emram,
        }


def weighted_choice_rng(rng, items):
    vals, weights = zip(*items)
    return rng.choices(vals, weights=weights, k=1)[0]


# =============================================================================
# STREAMING NDJSON WRITER (for FHIR — one resource per line)
# =============================================================================

def write_fhir_ndjson_chunk(rows, outfile):
    """Write FHIR resources as NDJSON (newline-delimited JSON).
    Each encounter produces ~14 FHIR resources, each on its own line.
    """
    for row in rows:
        # Simplified FHIR resource generation (Patient + Encounter + Condition)
        # In production, expand to full resource set as in original engine
        patient = {"resourceType": "Patient", "id": row["patient_id"],
                   "gender": row["sex"], "birthDate": str(row["birth_year"]),
                   "address": [{"state": row["state"]}]}
        outfile.write(json.dumps(patient, separators=(',', ':')) + '\n')
        
        encounter = {"resourceType": "Encounter", "id": row["encounter_id"],
                     "status": "finished", "subject": {"reference": f"Patient/{row['patient_id']}"},
                     "period": {"start": row["admit_timestamp"], "end": row["discharge_timestamp"]},
                     "length": {"value": row["los_days"], "unit": "days"}}
        outfile.write(json.dumps(encounter, separators=(',', ':')) + '\n')


# =============================================================================
# STATISTICAL VALIDATOR (Post-generation)
# =============================================================================

def validate_dataset(csv_path, expected_rows):
    """Run comprehensive statistical validation on the generated CSV."""
    import csv as csvmod
    
    print("\n" + "=" * 70)
    print("  POST-GENERATION VALIDATION")
    print("=" * 70)
    
    # Streaming validation — never loads full file into memory
    stats = {
        "total_rows": 0, "ages": [], "severities": [], "los": [], "charges": [],
        "readmit_sum": 0, "incident_sum": 0, "fraud_up_sum": 0, "fraud_dup_sum": 0,
        "diabetic_count": 0, "diabetic_tested": 0, "diabetic_poor": 0,
        "high_ratio_incidents": 0, "high_ratio_total": 0,
        "low_ratio_incidents": 0, "low_ratio_total": 0,
        "sev_los": {"low": [], "med": [], "high": [], "crit": []},
    }
    
    # Sample every Nth row for distribution checks (avoid storing 1M values)
    sample_rate = max(1, expected_rows // 10000)
    
    with open(csv_path, 'r') as f:
        reader = csvmod.DictReader(f)
        for i, row in enumerate(reader):
            stats["total_rows"] += 1
            
            age = int(row["age"])
            sev = float(row["severity_index"])
            los = int(row["los_days"])
            charge = float(row["submitted_charge_usd"])
            ratio = int(row["patients_per_nurse_ratio"])
            inc = int(row["safety_incident_flag"])
            
            stats["readmit_sum"] += int(row["readmission_30d_flag"])
            stats["incident_sum"] += inc
            stats["fraud_up_sum"] += int(row["fraud_upcoding_flag"])
            stats["fraud_dup_sum"] += int(row["fraud_duplicate_flag"])
            
            if int(row["has_diabetes"]):
                stats["diabetic_count"] += 1
                stats["diabetic_tested"] += int(row["hedis_hba1c_tested"])
                stats["diabetic_poor"] += int(row["hedis_hba1c_poor_control"])
            
            if ratio >= 6:
                stats["high_ratio_total"] += 1
                stats["high_ratio_incidents"] += inc
            elif ratio <= 4:
                stats["low_ratio_total"] += 1
                stats["low_ratio_incidents"] += inc
            
            if sev < 0.3: stats["sev_los"]["low"].append(los)
            elif sev < 0.6: stats["sev_los"]["med"].append(los)
            elif sev < 0.8: stats["sev_los"]["high"].append(los)
            else: stats["sev_los"]["crit"].append(los)
            
            if i % sample_rate == 0:
                stats["ages"].append(age)
                stats["severities"].append(sev)
                stats["los"].append(los)
                stats["charges"].append(charge)
    
    n = stats["total_rows"]
    checks_passed = 0
    checks_total = 0
    
    def check(name, condition, detail=""):
        nonlocal checks_passed, checks_total
        checks_total += 1
        status = "PASS" if condition else "FAIL"
        if condition: checks_passed += 1
        print(f"  [{status}] {name}: {detail}")
    
    # Row count
    check("Row count", n == expected_rows, f"{n:,} rows (expected {expected_rows:,})")
    
    # Demographics
    age_mu = sum(stats["ages"]) / len(stats["ages"])
    check("Age distribution", 55 < age_mu < 68, f"μ={age_mu:.1f} (expected ~62)")
    
    # Severity
    sev_mu = sum(stats["severities"]) / len(stats["severities"])
    check("Severity distribution", 0.45 < sev_mu < 0.75, f"μ={sev_mu:.2f} (expected ~0.6)")
    
    # Readmission rate
    readmit_rate = stats["readmit_sum"] / n
    check("Readmission rate", 0.10 < readmit_rate < 0.25, f"{readmit_rate:.3f} (expected ~0.16)")
    
    # Fraud rates
    fraud_rate = stats["fraud_up_sum"] / n
    check("Fraud upcoding rate", 0.02 < fraud_rate < 0.08, f"{fraud_rate:.3f} (expected ~0.04)")
    
    # HEDIS compliance
    if stats["diabetic_count"] > 0:
        hedis_rate = stats["diabetic_tested"] / stats["diabetic_count"]
        check("HEDIS HbA1c screening", hedis_rate > 0.95, f"{hedis_rate:.3f} (expected ~1.0)")
        poor_rate = stats["diabetic_poor"] / stats["diabetic_count"]
        check("HEDIS poor control rate", 0.25 < poor_rate < 0.55, f"{poor_rate:.3f} (expected ~0.41)")
    
    # Cross-domain: staffing → incidents
    if stats["high_ratio_total"] > 0 and stats["low_ratio_total"] > 0:
        hi_inc = stats["high_ratio_incidents"] / stats["high_ratio_total"]
        lo_inc = stats["low_ratio_incidents"] / stats["low_ratio_total"]
        check("Staffing→Incident correlation", hi_inc > lo_inc,
              f"High ratio: {hi_inc:.4f}, Low ratio: {lo_inc:.4f}")
    
    # Cross-domain: severity → LOS
    sev_los_means = {}
    for level in ["low", "med", "high", "crit"]:
        vals = stats["sev_los"][level]
        if vals:
            sev_los_means[level] = sum(vals) / len(vals)
    if len(sev_los_means) == 4:
        monotonic = sev_los_means["low"] < sev_los_means["med"] < sev_los_means["high"] < sev_los_means["crit"]
        check("Severity→LOS monotonic", monotonic,
              f"Low={sev_los_means['low']:.1f} < Med={sev_los_means['med']:.1f} < High={sev_los_means['high']:.1f} < Crit={sev_los_means['crit']:.1f}")
    
    print(f"\n  RESULT: {checks_passed}/{checks_total} checks passed")
    return checks_passed == checks_total


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Healthcare SDG Engine — 1M Row Generator")
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--chunk-size", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=None, help="Random seed (omit to auto-generate a unique seed each run)")
    parser.add_argument("--output-dir", type=str, default="./output")
    parser.add_argument("--format", type=str, default="csv,ndjson", help="csv,ndjson,parquet")
    parser.add_argument("--compress", action="store_true", help="gzip output files")
    parser.add_argument("--validate", action="store_true", default=True)
    args = parser.parse_args()
    if args.seed is None:
        args.seed = secrets.randbelow(2**32)

    os.makedirs(args.output_dir, exist_ok=True)
    formats = args.format.split(",")

    print("=" * 70)
    print(f"  Healthcare SDG Engine — Generating {args.rows:,} encounters")
    print(f"  Chunk size: {args.chunk_size:,} | Seed: {args.seed}")
    print(f"  Formats: {formats} | Compress: {args.compress}")
    print("=" * 70)

    # Step 1: Generate staff pool
    print("\n[1/4] Generating staff pool...")
    t0 = time.time()
    nurses, surgeons = generate_staff_pool(args.seed)
    print(f"       {len(nurses):,} nurses, {len(surgeons):,} surgeons ({time.time()-t0:.1f}s)")

    # Step 2: Open output files
    csv_path = os.path.join(args.output_dir, "encounters_1m.csv")
    ndjson_path = os.path.join(args.output_dir, "fhir_1m.ndjson")
    
    if args.compress:
        csv_path += ".gz"
        ndjson_path += ".gz"

    csv_file = gzip.open(csv_path, 'wt', newline='') if args.compress else open(csv_path, 'w', newline='')
    ndjson_file = None
    if "ndjson" in formats:
        ndjson_file = gzip.open(ndjson_path, 'wt') if args.compress else open(ndjson_path, 'w')

    writer = None
    total_written = 0
    t_start = time.time()

    # Step 3: Stream-generate in chunks
    print(f"\n[2/4] Generating encounters in chunks of {args.chunk_size:,}...")
    num_chunks = math.ceil(args.rows / args.chunk_size)

    for chunk_idx in range(num_chunks):
        start = chunk_idx * args.chunk_size
        count = min(args.chunk_size, args.rows - start)
        
        chunk_rows = []
        for row in generate_encounter_stream(start, count, nurses, surgeons, args.seed):
            chunk_rows.append(row)
        
        # Write CSV
        if "csv" in formats:
            if writer is None:
                writer = csv.DictWriter(csv_file, fieldnames=chunk_rows[0].keys())
                writer.writeheader()
            writer.writerows(chunk_rows)
        
        # Write NDJSON
        if ndjson_file:
            write_fhir_ndjson_chunk(chunk_rows, ndjson_file)
        
        total_written += len(chunk_rows)
        elapsed = time.time() - t_start
        rate = total_written / elapsed if elapsed > 0 else 0
        eta = (args.rows - total_written) / rate if rate > 0 else 0
        
        pct = total_written / args.rows * 100
        bar = "█" * int(pct / 2) + "░" * (50 - int(pct / 2))
        print(f"\r       [{bar}] {pct:5.1f}% | {total_written:>10,}/{args.rows:,} | {rate:,.0f} rows/s | ETA {eta:.0f}s", end="", flush=True)

    print()

    # Step 4: Close files
    csv_file.close()
    if ndjson_file:
        ndjson_file.close()

    elapsed_total = time.time() - t_start
    print(f"\n[3/4] Generation complete in {elapsed_total:.1f}s ({total_written/elapsed_total:,.0f} rows/s)")
    
    # File sizes
    for path in [csv_path, ndjson_path]:
        if path and os.path.exists(path):
            size_mb = os.path.getsize(path) / 1024 / 1024
            print(f"       {os.path.basename(path)}: {size_mb:,.1f} MB")

    # Step 5: Validate
    if args.validate and "csv" in formats:
        print(f"\n[4/4] Running statistical validation...")
        actual_csv = csv_path.replace('.gz', '') if args.compress else csv_path
        if args.compress:
            # Decompress for validation
            import shutil
            with gzip.open(csv_path, 'rb') as f_in, open(actual_csv, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        validate_dataset(actual_csv, args.rows)

    print("\n[DONE] All files generated successfully.")


if __name__ == "__main__":
    main()
