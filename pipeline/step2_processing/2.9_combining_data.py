# -*- coding: utf-8 -*-
"""
STEP 2.6 – COMBINE STANDARDIZED DATA FILES

Purpose:
- Merge all standardized CSV files from STEP 2.5
- Produce a single combined CSV file for downstream ERD splitting

Input:
- data/data_processing/s2.5_data_role_name_standardized/*.csv

Output:
- data/data_processing/s2.6_data_combined/combined_all_sources.csv
"""

import pandas as pd
from pathlib import Path

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[2]

INPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.7_data_salary_exp_validated"
OUTPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.9_data_combined"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_PATH = OUTPUT_DIR / "combined_all_sources.csv"

# =========================
# RUN
# =========================

def run():
    files = list(INPUT_DIR.glob("*.csv"))
    if not files:
        raise FileNotFoundError("No CSV files found in s2.5_data_role_name_standardized")

    dfs = []
    total_rows = 0

    for f in files:
        df = pd.read_csv(f, encoding="utf-8-sig")
        dfs.append(df)
        total_rows += len(df)

    combined_df = pd.concat(dfs, ignore_index=True)

    combined_df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

    print(
        f"✓ STEP 2.6 COMPLETED\n"
        f"  - Source files : {len(files)}\n"
        f"  - Total rows  : {total_rows}\n"
        f"  - Output file : {OUTPUT_PATH.name}"
    )

if __name__ == "__main__":
    run()