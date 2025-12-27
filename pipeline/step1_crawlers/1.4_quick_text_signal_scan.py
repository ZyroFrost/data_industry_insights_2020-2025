# -*- coding: utf-8 -*-
"""
STEP 1.4 – QUICK TEXT SIGNAL SCAN

Purpose:
- Scan nhanh raw CSV sau STEP 1.3
- Kiểm tra xem có đủ signal để chạy pipeline hay không
- Dựa trên TEXT + REFERENCE MAPPING (KHÔNG đoán schema)

INPUT:
- data/data_processing/s1_data_extracted/*.csv

REFERENCE (schema-fixed):
- countries.csv              : country_code, country_name
- cities.csv                 : city_name, country_code
- employment_type_mapping.csv: employment_type, keywords
- education_level_mapping.csv: education_level, keywords
- role_names_mapping.csv     : canonical_role, role_family, keywords, aliases, strong_terms, exclude_terms
"""

import pandas as pd
import re
from pathlib import Path

# ==================================================
# PATHS
# ==================================================

BASE_DIR = Path(__file__).resolve().parents[2]

DATA_DIR = BASE_DIR / "data" / "data_processing" / "s1_data_extracted"
REF_DIR = BASE_DIR / "data" / "data_reference"

# ==================================================
# LOAD REFERENCE KEYWORDS (SCHEMA-SAFE)
# ==================================================

def load_simple_column(csv_path: Path, col: str) -> set:
    df = pd.read_csv(csv_path)
    return set(df[col].dropna().astype(str).str.lower())

def load_keywords_column(csv_path: Path, col: str) -> set:
    df = pd.read_csv(csv_path)
    keywords = set()
    for val in df[col].dropna():
        for k in str(val).lower().split("|"):
            k = k.strip()
            if k:
                keywords.add(k)
    return keywords

# ---- Load mappings (EXACT columns) ----

COUNTRY_KEYS = load_simple_column(
    REF_DIR / "countries.csv",
    "country_name"
)

CITY_KEYS = load_simple_column(
    REF_DIR / "cities.csv",
    "city_name"
)

EMPLOYMENT_KEYS = load_keywords_column(
    REF_DIR / "employment_type_mapping.csv",
    "keywords"
)

EDUCATION_KEYS = load_keywords_column(
    REF_DIR / "education_level_mapping.csv",
    "keywords"
)

ROLE_KEYS = set()

_role_df = pd.read_csv(REF_DIR / "role_names_mapping.csv")
for col in ["canonical_role", "keywords", "aliases", "strong_terms"]:
    for val in _role_df[col].dropna():
        for k in str(val).lower().split("|"):
            k = k.strip()
            if k:
                ROLE_KEYS.add(k)

# ==================================================
# REGEX – REMOTE & SALARY
# ==================================================

REMOTE_REGEX = re.compile(
    r"\b(remote|hybrid|work from home|wfh|home[- ]?based)\b",
    re.I
)

CURRENCY_REGEX = re.compile(
    r"(\$|€|£|\busd\b|\beur\b|\bgbp\b|\baud\b|\bcad\b)",
    re.I
)

SALARY_NUMBER_REGEX = re.compile(
    r"(\d{2,3}(?:[.,]\d{3})+|\d+\s?k|\d+\+?)",
    re.I
)

SALARY_CONTEXT_REGEX = re.compile(
    r"(salary|compensation|pay|wage|rate|per\s?(year|annum|month|hour)|/year|/month|/hour)",
    re.I
)

def has_salary(text: str) -> bool:
    return bool(
        CURRENCY_REGEX.search(text)
        and (SALARY_NUMBER_REGEX.search(text) or SALARY_CONTEXT_REGEX.search(text))
    )

# ==================================================
# SIGNAL CHECK
# ==================================================

def has_any(text: str, keywords: set) -> bool:
    return any(k in text for k in keywords)

def scan_file(file_path: Path, chunk_size: int = 10_000):
    print(f"\n📄 FILE: {file_path.name}")

    total_rows = 0
    counters = {
        "salary": 0,
        "remote_option": 0,
        "employment_type": 0,
        "education_level": 0,
        "role_name": 0,
        "country": 0,
        "city": 0,
        "all_signals": 0,
    }

    chunk_idx = 0

    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        chunk_idx += 1
        chunk_rows = len(chunk)
        total_rows += chunk_rows

        # gộp text từng row
        text_series = chunk.astype(str).agg(" ".join, axis=1).str.lower()

        salary_mask = text_series.apply(has_salary)
        remote_mask = text_series.str.contains(REMOTE_REGEX)
        employment_mask = text_series.apply(lambda x: has_any(x, EMPLOYMENT_KEYS))
        education_mask = text_series.apply(lambda x: has_any(x, EDUCATION_KEYS))
        role_mask = text_series.apply(lambda x: has_any(x, ROLE_KEYS))
        country_mask = text_series.apply(lambda x: has_any(x, COUNTRY_KEYS))
        city_mask = text_series.apply(lambda x: has_any(x, CITY_KEYS))

        counters["salary"] += salary_mask.sum()
        counters["remote_option"] += remote_mask.sum()
        counters["employment_type"] += employment_mask.sum()
        counters["education_level"] += education_mask.sum()
        counters["role_name"] += role_mask.sum()
        counters["country"] += country_mask.sum()
        counters["city"] += city_mask.sum()

        all_mask = (
            salary_mask
            & remote_mask
            & employment_mask
            & education_mask
            & role_mask
            & country_mask
            & city_mask
        )
        counters["all_signals"] += all_mask.sum()

        # ===== PROGRESS LOG =====
        print(
            f"  ▶ Chunk {chunk_idx:3d} | rows: {total_rows:7d} "
            f"| all_signals: {counters['all_signals']}"
        )

    if total_rows == 0:
        print("  ⚠️  EMPTY FILE")
        return

    print(f"\n  Rows total: {total_rows}")
    for k in [
        "salary",
        "remote_option",
        "employment_type",
        "education_level",
        "role_name",
        "country",
        "city",
    ]:
        cnt = counters[k]
        print(f"  {k:15}: {cnt:6} ({cnt/total_rows*100:5.1f}%)")

    print(
        f"  {'ALL_SIGNALS':15}: {counters['all_signals']:6} "
        f"({counters['all_signals']/total_rows*100:5.1f}%)"
    )

# ==================================================
# RUN
# ==================================================

def run():
    files = sorted(
        DATA_DIR.glob("*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )

    print("===================================")
    print("STEP 1.4 – QUICK TEXT SIGNAL SCAN")
    print("===================================")
    print(f"Input dir: {DATA_DIR}")
    print(f"Found {len(files)} files")
    print("-----------------------------------")

    for f in files:
        scan_file(f)

    print("-----------------------------------")
    print("✓ STEP 1.4 COMPLETED")

if __name__ == "__main__":
    run()
