# -*- coding: utf-8 -*-
"""
STEP 2.3 – VALUE NORMALIZATION (CITY, COMPANY, EMPLOYMENT, CURRENCY, ETC.)

This step standardizes extracted textual values into
canonical, analysis-ready formats using reference mappings
and lightweight normalization rules.

IMPORTANT:
- This step DOES NOT extract new information from job_description
- It ONLY normalizes values that were already extracted in STEP 2.2
- Existing non-__NA__ values are NEVER overridden with guesses

Handled columns (if present):
- city                → canonical city name (GeoNames standard)
- company_name        → whitespace & formatting normalization
- employment_type     → enum normalization (Full-time, Part-time, Internship, Temporary)
- currency            → ISO currency code (USD, EUR, GBP, ...)
- posting_date        → YYYY-MM-DD
* country and country_iso are not handled here because they depend on the city (normalized city data is required first). See STEP 2.4

Purpose:
- Ensure enum consistency before enrichment & database loading
- Reduce noise from casing, separators, symbols, and variants
- Guarantee downstream steps operate on clean, comparable values

PIPELINE CONTEXT:
STEP 2 – Data Processing
- STEP 2.1 – Column mapping (raw → ERD schema)
- STEP 2.2 – Description signal extraction (weak signals only)
- STEP 2.3 – Value normalization (this step)
- STEP 2.4 – Reference-based geo enrichment (country / ISO / lat-lon)

INPUT:
- data/data_processing/s2.2_data_description_extracted/*.csv

REFERENCE FILES:
- data/data_reference/city_alias_reference.csv
- data/data_reference/cities.csv
- data/data_reference/employment_type_mapping.csv
- data/data_reference/currency_mapping.csv

OUTPUT:
- data/data_processing/s2.3_data_city_company_employment_normalized/normalized_*.csv

DESIGN PRINCIPLES:
1. Normalize, don’t infer
2. Enum-safe outputs only
3. __NA__ stays __NA__
4. No description re-parsing
"""

import pandas as pd
import unicodedata
import re
from pathlib import Path
from collections import Counter

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[2]

INPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.2_data_description_extracted"
OUTPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.3_data_values_normalized"
REF_DIR = BASE_DIR / "data" / "data_reference"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CITY_ALIAS_PATH = REF_DIR / "city_alias_reference.csv"

UNMATCHED_DIR = BASE_DIR / "data" / "data_unmatched_report"
UNMATCHED_DIR.mkdir(parents=True, exist_ok=True)

UNMATCHED_CITY_PATH = UNMATCHED_DIR / "unmatched_city_name.csv"

UNMATCHED_SKILL_PATH = UNMATCHED_DIR / "unmatched_skill_name.csv"
UNMATCHED_SKILL_COUNTER = Counter()
UNMATCHED_SKILL_SOURCE = {}

# =========================
# EXPERIENCE YEAR NORMALIZATION
# =========================

EXP_LEVEL_MAP = {
    "EN": "0",
    "MI": "2",
    "SE": "5",
    "EX": "8",
}

# =========================
# HELPERS
# =========================

def normalize_text(x):
    if pd.isna(x):
        return "__NA__"
    x = str(x)
    x = unicodedata.normalize("NFKD", x)
    x = "".join(c for c in x if not unicodedata.combining(c))
    x = re.sub(r"[^\w\s]", "", x)
    return re.sub(r"\s+", " ", x).strip().upper()

def clean_skill_field_shape(x, NA="__NA__"):
    """
    Shape-only cleaning:
    - Remove list/dict wrappers
    - Flatten to pipe-separated string
    - DO NOT infer, DO NOT normalize semantics
    """
    if pd.isna(x):
        return NA

    s = str(x).strip()
    if s == "" or s == NA:
        return NA

    # dict-like: {'cloud': ['azure'], 'other': ['git']}
    if s.startswith("{") and s.endswith("}"):
        keys = re.findall(r"'([^']+)'\s*:", s)
        return "|".join(keys) if keys else NA

    # list-like: ['sql', 'azure', 'git']
    if s.startswith("[") and s.endswith("]"):
        items = re.findall(r"'([^']+)'", s)
        return "|".join(items) if items else NA

    # fallback: return raw string
    return s

def normalize_company_name(x):
    if pd.isna(x):
        return "__NA__"
    x = str(x).strip()
    x = re.sub(r"\s+", " ", x)
    return x

def normalize_currency(x, NA="__NA__"):
    if x == NA or not isinstance(x, str):
        return x

    s = x.strip().lower()

    # remove noise
    s = s.replace("_", " ")
    s = re.sub(r"\s+", " ", s)

    # direct mapping
    if s in CURRENCY_LOOKUP:
        return CURRENCY_LOOKUP[s]

    return NA

import re

def normalize_posted_date(x, NA="__NA__"):
    """
    Normalize posted_date to YYYY-MM-DD

    Handles automatically:
    - Excel serial datetime (float / int)
    - pandas.Timestamp / datetime
    - MM/DD/YYYY HH:MM  (Excel custom display)
    - YYYY-MM-DD
    - YYYY
    """

    if pd.isna(x):
        return NA

    # ====================================
    # 1. EXCEL SERIAL DATE (CRITICAL FIX)
    # ====================================
    # Excel date starts from 1899-12-30
    if isinstance(x, (int, float)):
        try:
            dt = pd.to_datetime(x, unit="D", origin="1899-12-30")
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return NA

    # ====================================
    # 2. pandas.Timestamp / datetime
    # ====================================
    if hasattr(x, "strftime"):
        try:
            return x.strftime("%Y-%m-%d")
        except Exception:
            return NA

    # ====================================
    # 3. STRING PARSING
    # ====================================
    s = str(x).strip()
    if s == "" or s == NA:
        return NA

    # YYYY only
    if re.fullmatch(r"\d{4}", s):
        return f"{s}-01-01"

    # MM/DD/YYYY HH:MM  (Excel custom display)
    try:
        dt = pd.to_datetime(
            s,
            format="%m/%d/%Y %H:%M",
            errors="coerce"
        )
        if not pd.isna(dt):
            return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    # fallback (ISO / other safe cases)
    try:
        dt = pd.to_datetime(s, errors="coerce")
        if not pd.isna(dt):
            return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    return NA
    
def normalize_remote_option(x, NA="__NA__"):
    if pd.isna(x):
        return NA

    v = str(x).strip()

    if v in {"", NA}:
        return NA

    # numeric percentage (highest priority)
    if v in {"0", "0.0"}:
        return "Onsite"
    if v in {"50", "50.0"}:
        return "Hybrid"
    if v in {"100", "100.0"}:
        return "Remote"

    # boolean
    if v.upper() == "TRUE":
        return "Remote"
    if v.upper() == "FALSE":
        return "Onsite"

    return "__INVALID__"

def normalize_employment_type(x, NA="__NA__"):
    if pd.isna(x):
        return NA

    s = str(x).strip()
    if s == "" or s.upper() == NA:
        return NA

    key = normalize_text(s)

    if key in EMPLOYMENT_TYPE_LOOKUP:
        return EMPLOYMENT_TYPE_LOOKUP[key]

    return "__INVALID__"

def normalize_skill_name_with_mapping(x, NA="__NA__"):
    """
    Normalize skill_name using reference-based canonical mapping only.

    LOGIC (FINAL):
    1. Split skill string by multi-separators (| , / ; - newline)
    2. Normalize token text (uppercase, ascii, no symbols)
    3. Match priority:
        a. Alias exact match (SKILL_ALIAS_LOOKUP)
        b. Strong-term substring match (SKILL_STRONG_LOOKUP, word-boundary safe)
    4. If MATCH:
        - keep canonical skill
    5. If NOT MATCH:
        - REMOVE from main CSV
        - STILL record in unmatched skill report
    6. Deduplicate, keep first occurrence order

    IMPORTANT:
    - NO enrichment
    - NO guessing
    - CSV output contains ONLY mapped canonical skills
    - Unmatched skills are audit-only (report)
    """

    if pd.isna(x):
        return NA

    s = str(x).strip()
    if s == "" or s == NA:
        return NA

    # -------------------------
    # 1. SPLIT BY MULTI-SEPARATOR
    # -------------------------
    tokens = re.split(r"[|,/;\n\-]+", s)
    tokens = [t.strip() for t in tokens if t.strip()]
    if not tokens:
        return NA

    normalized = []

    for token in tokens:
        token_norm = normalize_text(token)

        # ignore trivial noise (1–2 chars)
        if len(token_norm) <= 2:
            continue

        canonical = None

        # -------------------------
        # 2. ALIAS EXACT MATCH
        # -------------------------
        if token_norm in SKILL_ALIAS_LOOKUP:
            canonical = SKILL_ALIAS_LOOKUP[token_norm]

        # -------------------------
        # 3. STRONG TERM MATCH
        # -------------------------
        else:
            for strong_key, strong_canonical in SKILL_STRONG_LOOKUP.items():
                if re.search(rf"\b{re.escape(strong_key)}\b", token_norm):
                    canonical = strong_canonical
                    break

        # -------------------------
        # 4. HANDLE RESULT
        # -------------------------
        if canonical:
            normalized.append(canonical)
        else:
            # skill không match → DROP khỏi CSV
            # nhưng vẫn ghi report để audit mapping
            UNMATCHED_SKILL_COUNTER[token_norm] += 1
            if token_norm not in UNMATCHED_SKILL_SOURCE:
                UNMATCHED_SKILL_SOURCE[token_norm] = token

    # -------------------------
    # 5. DEDUPLICATE, KEEP ORDER
    # -------------------------
    seen = set()
    out = []
    for sk in normalized:
        if sk not in seen:
            seen.add(sk)
            out.append(sk)

    return "|".join(out) if out else NA
    
# =========================
# LOAD CITY ALIAS REFERENCE
# =========================
if not CITY_ALIAS_PATH.exists():
    raise FileNotFoundError(
        f"Missing city alias reference: {CITY_ALIAS_PATH}\n"
        "Please run STEP 0.2 – build_city_alias_reference.py first."
    )

alias_df = pd.read_csv(CITY_ALIAS_PATH, dtype=str)

# alias_norm -> canonical_city
CITY_ALIAS_LOOKUP = {
    normalize_text(alias): canonical
    for canonical, alias in zip(
        alias_df["canonical_city"],
        alias_df["alias"]
    )
}

CITIES_REF_PATH = REF_DIR / "cities.csv"

cities_df = pd.read_csv(CITIES_REF_PATH, dtype=str)

# normalize city name for lookup
cities_df["name_norm"] = cities_df["city_name"].apply(normalize_text)

# norm_name -> official city name (GeoNames)
CITY_NAME_LOOKUP = dict(
    zip(cities_df["name_norm"], cities_df["city_name"])
)

# =========================
# LOAD CURRENCY MAPPING
# =========================

CURRENCY_MAP_PATH = REF_DIR / "currency_mapping.csv"

currency_df = pd.read_csv(CURRENCY_MAP_PATH, dtype=str)

CURRENCY_LOOKUP = {}

for _, row in currency_df.iterrows():
    canonical = row["currency"].strip().upper()
    aliases = [a.strip().lower() for a in row["aliases"].split("|")]

    for a in aliases:
        CURRENCY_LOOKUP[a] = canonical

# =========================
# LOAD EMPLOYMENT TYPE MAPPING
# =========================

EMPLOYMENT_MAP_PATH = REF_DIR / "employment_type_mapping.csv"

emp_df = pd.read_csv(EMPLOYMENT_MAP_PATH, dtype=str)

EMPLOYMENT_TYPE_LOOKUP = {}

for _, row in emp_df.iterrows():
    canonical = row["employment_type"].strip()
    keywords = row["keywords"]

    if pd.isna(keywords):
        continue

    for kw in keywords.split("|"):
        key = normalize_text(kw)
        EMPLOYMENT_TYPE_LOOKUP[key] = canonical

# =========================
# LOAD SKILL MAPPING (CANONICAL SKILL)
# =========================

SKILL_MAPPING_PATH = REF_DIR / "skill_mapping.csv"

if not SKILL_MAPPING_PATH.exists():
    raise FileNotFoundError(
        f"Missing skill mapping file: {SKILL_MAPPING_PATH}"
    )

skill_df = pd.read_csv(SKILL_MAPPING_PATH, dtype=str)

SKILL_ALIAS_LOOKUP = {}

for _, row in skill_df.iterrows():
    canonical = row["canonical_skill"].strip()

    if pd.isna(row["aliases"]):
        continue

    for a in row["aliases"].split("|"):
        key = normalize_text(a)
        SKILL_ALIAS_LOOKUP[key] = canonical

    # =========================
# LOAD STRONG TERM LOOKUP
# =========================

SKILL_STRONG_LOOKUP = {}

for _, row in skill_df.iterrows():
    canonical = row["canonical_skill"].strip()

    strong_terms = row.get("strong_terms")
    if pd.isna(strong_terms):
        continue

    for term in strong_terms.split("|"):
        key = normalize_text(term)
        if key:
            SKILL_STRONG_LOOKUP[key] = canonical

# =========================
# NORMALIZE SINGLE FILE (CSV)
# =========================

def normalize_city_file(file_path: Path):
    print(f"🔄 Normalizing city aliases: {file_path.name}")

    # load (csv / xlsx)
    if file_path.suffix.lower() == ".csv":
        df = pd.read_csv(file_path, dtype=str)
    else:
        df = pd.read_excel(file_path, dtype=str)

    # =========================
    # COUNT UNMATCHED SKILL (PER FILE)
    # =========================
    unmatched_skill_count = 0
    if "skill_name" in df.columns:
        raw_skills = (
            df["skill_name"]
            .dropna()
            .astype(str)
            .str.split("|")
            .explode()
            .map(normalize_text)
        )

        unmatched_skill_count = raw_skills.isin(
            UNMATCHED_SKILL_COUNTER.keys()
        ).sum()

    # =========================
    # SHAPE CLEANING – SKILL FIELDS (REMOVE WRAPPERS / JUNK)
    # =========================
    for col in ["skill_name", "skill_category"]:
        if col in df.columns:
            df[col] = df[col].apply(clean_skill_field_shape)

    # =========================
    # NORMALIZE SKILL NAME (REFERENCE MAPPING ONLY)
    # =========================
    if "skill_name" in df.columns:
        before_skill = df["skill_name"].copy()
        df["skill_name"] = df["skill_name"].apply(
            normalize_skill_name_with_mapping
        )

        skill_norm_count = (
            before_skill.reset_index(drop=True)
            != df["skill_name"].reset_index(drop=True)
        ).sum()
    else:
        skill_norm_count = 0

    # =========================
    # NORMALIZE REQUIRED EXPERIENCE YEARS (ENUM ONLY)
    # =========================
    if "required_exp_years" in df.columns:
        df["required_exp_years"] = (
            df["required_exp_years"]
            .astype(str)
            .str.strip()
            .str.upper()
            .apply(lambda x: EXP_LEVEL_MAP.get(x, x))
        )

    # =========================
    # NORMALIZE REMOTE OPTION (ENUM)
    # =========================
    remote_norm_count = 0
    remote_invalid_count = 0

    if "remote_option" in df.columns:
        before_remote = df["remote_option"].copy()

        for i, val in df["remote_option"].items():
            norm = normalize_remote_option(val, "__NA__")

            if norm == "__INVALID__":
                df.at[i, "remote_option"] = "__INVALID__"
                remote_invalid_count += 1
            else:
                df.at[i, "remote_option"] = norm

        remote_norm_count = (before_remote != df["remote_option"]).sum()

    # =========================
    # NORMALIZE COMPANY NAME (INTERNAL, SIMPLE)
    # =========================
    if "company_name" in df.columns:
        df["company_name"] = df["company_name"].apply(normalize_company_name)

    if "city" not in df.columns:
        print("  ⚠ No city column found, skipping")
        return

    city_before = df["city"].fillna("__NA__")
    df["_city_raw"] = city_before

    city_norm = city_before.apply(normalize_text)

    normalized_city = []

    for raw, norm in zip(city_before, city_norm):
        if norm == "__NA__":
            normalized_city.append("__NA__")
        elif norm in CITY_ALIAS_LOOKUP:
            alias_canonical = CITY_ALIAS_LOOKUP[norm]
            alias_norm = normalize_text(alias_canonical)

            if alias_norm in CITY_NAME_LOOKUP:
                normalized_city.append(CITY_NAME_LOOKUP[alias_norm])
            else:
                normalized_city.append("__UNMATCHED__")

        else:
            normalized_city.append("__UNMATCHED__")

    df["city"] = normalized_city

    # =========================
    # NORMALIZE COMPANY NAME (INTERNAL, SIMPLE)
    # =========================
    company_norm_count = 0
    if "company_name" in df.columns:
        before_company = df["company_name"].copy()
        df["company_name"] = df["company_name"].apply(normalize_company_name)
        company_norm_count = (before_company != df["company_name"]).sum()

    # =========================
    # NORMALIZE EMPLOYMENT TYPE (FORMAT ONLY)
    # =========================
    employment_norm_count = 0
    if "employment_type" in df.columns:
        before_emp = df["employment_type"].copy()
        df["employment_type"] = df["employment_type"].apply(
            lambda x: normalize_employment_type(x, "__NA__")
        )
        employment_norm_count = (before_emp != df["employment_type"]).sum()


    # =========================
    # NORMALIZE CURRENCY (ENUM)
    # =========================
    currency_norm_count = 0
    if "currency" in df.columns:
        before_currency = df["currency"].copy()
        df["currency"] = df["currency"].apply(normalize_currency)
        currency_norm_count = (before_currency != df["currency"]).sum()

    # =========================
    # NORMALIZE POSTED DATE
    # =========================
    posted_date_norm_count = 0
    if "posted_date" in df.columns:
        before_date = df["posted_date"].copy()
        df["posted_date"] = df["posted_date"].apply(normalize_posted_date)

        posted_date_norm_count = (
            (before_date != df["posted_date"])
            & (df["posted_date"] != "__NA__")
        ).sum()


    # =========================
    # EXPORT UNMATCHED CITY
    # =========================

    # normalize raw city & country for comparison
    city_raw_norm = df["_city_raw"].apply(normalize_text)
    country_norm = df["country"].apply(normalize_text) if "country" in df.columns else None

    unmatched_mask = df["city"] == "__UNMATCHED__"

    # exclude cases where city == country
    if country_norm is not None:
        unmatched_mask &= city_raw_norm != country_norm

    unmatched_df = df.loc[
        unmatched_mask,
        ["_city_raw"]
    ].copy()

    unmatched_df.rename(
        columns={"_city_raw": "city_raw"},
        inplace=True
    )

    unmatched_df.rename(
        columns={"_city_raw": "city_raw"},
        inplace=True
    )

    if not unmatched_df.empty:
        unmatched_df.insert(0, "__source_name", file_path.name)
        unmatched_df.insert(1, "__source_id", unmatched_df.index.astype(str))

        if UNMATCHED_CITY_PATH.exists():
            existing = pd.read_csv(UNMATCHED_CITY_PATH, dtype=str)
            pd.concat(
                [existing, unmatched_df],
                ignore_index=True
            ).drop_duplicates().to_csv(
                UNMATCHED_CITY_PATH,
                index=False,
                encoding="utf-8-sig"
            )
        else:
            unmatched_df.to_csv(
                UNMATCHED_CITY_PATH,
                index=False,
                encoding="utf-8-sig"
            )

    # save output
    stem = file_path.stem

    # remove intermediate step prefixes
    for p in [
        "extracted_desc_",
        "mapped_",
        "normalized_",
    ]:
        stem = stem.replace(p, "")

    output_name = f"normalized_{stem}.csv"
    output_path = OUTPUT_DIR / output_name

    output_path = OUTPUT_DIR / output_name.replace(file_path.suffix, ".csv")
    #df.to_excel(output_path.with_suffix(".xlsx"), index=False)
    df.drop(columns=["_city_raw"], inplace=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    # log summary
    total = len(df)
    unmatched = (df["city"] == "__UNMATCHED__").sum()
    na = (df["city"] == "__NA__").sum()
    valid = total - unmatched - na

    # =========================
    # COUNT UNMATCHED SKILL (PER FILE)
    # =========================
    unmatched_skill_count = 0
    if "skill_name" in df.columns:
        raw_skills = (
            df["skill_name"]
            .dropna()
            .astype(str)
            .str.split("|")
            .explode()
            .map(normalize_text)
        )

        unmatched_skill_count = raw_skills.isin(
            UNMATCHED_SKILL_COUNTER.keys()
        ).sum()

    # =========================
    # EXPORT UNMATCHED SKILL REPORT
    # =========================
    if UNMATCHED_SKILL_COUNTER:
        rows = []
        for skill_norm, count in UNMATCHED_SKILL_COUNTER.most_common():
            rows.append({
                "skill_norm": skill_norm,
                "skill_raw_example": UNMATCHED_SKILL_SOURCE.get(skill_norm, ""),
                "count": count
            })

        unmatched_skill_df = pd.DataFrame(rows)
        unmatched_skill_df.to_csv(
            UNMATCHED_SKILL_PATH,
            index=False,
            encoding="utf-8-sig"
        )

    print(
        f"  ✓ Saved: {output_path.name}\n"
        f"    - Total rows                  : {total}\n"
        f"    - City normalized             : {valid}\n"
        f"    - City empty (__NA__)         : {na}\n"
        f"    - City unmatched (!)          : {unmatched}\n"
        f"\n"
        f"    - Company normalized          : {company_norm_count}\n"
        f"    - Employment normalized       : {employment_norm_count}\n"
        f"    - Currency normalized         : {currency_norm_count}\n"
        f"    - Posted date normalized      : {posted_date_norm_count}\n"
        f"\n"
        f"    - Remote normalized           : {remote_norm_count}\n"
        f"    - Remote unmatched enum (!)   : {remote_invalid_count}\n"
        f"\n"
        f"    - Skill name normalized       : {skill_norm_count}\n"
        f"    - Skill unmatched (!)         : {unmatched_skill_count}\n"
        f"  → Folder saved                  : {output_path.parent}\n"
        f"  → Unmatched skill folder        : {UNMATCHED_SKILL_PATH.parent}"
    )
    return unmatched

# =========================
# RUN STEP
# =========================

def run():
    files = [
        f for f in INPUT_DIR.iterdir()
        if f.is_file()
        and not f.name.startswith("normalized_")
    ]

    total_unmatched_city = 0
    if not files:
        print(f"No input files found in {INPUT_DIR}")
        return

    # =========================
    # RESET UNMATCHED REPORT (PER RUN)
    # =========================
    if UNMATCHED_CITY_PATH.exists():
        UNMATCHED_CITY_PATH.unlink()

    for f in files:
        unmatched = normalize_city_file(f)
        if unmatched:
            total_unmatched_city += unmatched

    if UNMATCHED_CITY_PATH.exists():
        print(f"→ Unmatched city report saved: {UNMATCHED_CITY_PATH}")

    print(f"→ Total unmatched city (all files): {total_unmatched_city}")

    # =========================
    # EXPORT UNMATCHED SKILL REPORT
    # =========================
    if UNMATCHED_SKILL_COUNTER:
        rows = []
        for skill_norm, count in UNMATCHED_SKILL_COUNTER.most_common():
            rows.append({
                "skill_norm": skill_norm,
                "skill_raw_example": UNMATCHED_SKILL_SOURCE.get(skill_norm, ""),
                "count": count
            })

        unmatched_skill_df = pd.DataFrame(rows)
        unmatched_skill_df.to_csv(
            UNMATCHED_SKILL_PATH,
            index=False,
            encoding="utf-8-sig"
        )

        print(f"→ Unmatched skill report saved: {UNMATCHED_SKILL_PATH}")

    print("\n=== STEP 2.3 COMPLETED: CITY ALIAS NORMALIZATION ===")

if __name__ == "__main__":
    run()