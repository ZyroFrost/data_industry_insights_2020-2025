# -*- coding: utf-8 -*-
"""
STEP 2.8 – TEMPORAL DISTRIBUTION & SOFT CAP COMPUTATION

PURPOSE:
- Parse posted_date → year
- Count rows per (SOURCE × YEAR)
- Derive:
    - rows per YEAR
    - years covered per SOURCE
    - total rows per SOURCE
- Detect skewed years
- Compute SOFT CAP using:
    - median
    - skew detection
    - P75 of non-skew years
- PRINT full audit logs (NO DATA MODIFICATION)

INPUT:
- data/data_processing/s2.8_data_eligible_rows_filtered/*.csv

OUTPUT:
- rows_per_source_year.csv
- rows_per_year.csv
- source_year_coverage.csv
- year_cap_summary.txt
"""

import pandas as pd
from pathlib import Path
import numpy as np

# ==================================================
# PATHS
# ==================================================

BASE_DIR = Path(__file__).resolve().parents[2]

INPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.7_data_salary_exp_validated"
OUTPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.8_year_cap_computed"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ==================================================
# CONFIG
# ==================================================

SKEW_FACTOR_K = 3
CAP_MULTIPLIER_K2 = 3

# ==================================================
# LOAD & COLLECT (STRICT, NO GUESS)
# ==================================================

dfs = []
skipped_files = []

for file_path in INPUT_DIR.glob("*.csv"):
    try:
        cols = pd.read_csv(file_path, nrows=0).columns.tolist()

        if "posted_date" not in cols:
            skipped_files.append((file_path.name, "missing posted_date"))
            continue

        use_cols = ["posted_date"]
        if "__source_name" in cols:
            use_cols.append("__source_name")

        df = pd.read_csv(file_path, usecols=use_cols, encoding="utf-8-sig")

        df["year"] = pd.to_datetime(
            df["posted_date"], errors="coerce"
        ).dt.year

        df = df.dropna(subset=["year"])
        df["year"] = df["year"].astype(int)

        if "__source_name" not in df.columns:
            df["__source_name"] = file_path.stem.replace("mapped_", "")

        dfs.append(df[["__source_name", "year"]])

    except Exception as e:
        skipped_files.append((file_path.name, str(e)))

if not dfs:
    raise RuntimeError("No valid data with posted_date found for STEP 2.8")

all_df = pd.concat(dfs, ignore_index=True)

# ==================================================
# 1️⃣ COUNT – ROWS PER (SOURCE × YEAR)
# ==================================================

rows_per_source_year = (
    all_df
    .groupby(["__source_name", "year"])
    .size()
    .reset_index(name="rows")
    .sort_values(["__source_name", "year"])
)

rows_per_source_year.to_csv(
    OUTPUT_DIR / "rows_per_source_year.csv",
    index=False,
    encoding="utf-8-sig"
)

# ==================================================
# 2️⃣ DERIVE – ROWS PER YEAR (TOTAL)
# ==================================================

rows_per_year = (
    rows_per_source_year
    .groupby("year", as_index=False)["rows"]
    .sum()
    .sort_values("year")
)

rows_per_year.to_csv(
    OUTPUT_DIR / "rows_per_year.csv",
    index=False,
    encoding="utf-8-sig"
)

# ==================================================
# 3️⃣ DERIVE – SOURCE YEAR COVERAGE (NO MORE '?')
# ==================================================

source_year_coverage = (
    rows_per_source_year
    .groupby("__source_name")
    .agg(
        years_present=("year", lambda x: sorted(x.unique().tolist())),
        num_years=("year", "nunique"),
        total_rows=("rows", "sum"),
    )
    .reset_index()
)

source_year_coverage.to_csv(
    OUTPUT_DIR / "source_year_coverage.csv",
    index=False,
    encoding="utf-8-sig"
)

# ==================================================
# 4️⃣ SOFT CAP COMPUTATION (YEAR LEVEL)
# ==================================================

median_rows = rows_per_year["rows"].median()
skew_threshold = SKEW_FACTOR_K * median_rows

rows_per_year["is_skew"] = rows_per_year["rows"] > skew_threshold

non_skew_years = rows_per_year[~rows_per_year["is_skew"]]
skewed_years = rows_per_year[rows_per_year["is_skew"]]

if non_skew_years.empty:
    raise ValueError("All years detected as skewed – cannot compute CAP")

p75_non_skew = np.percentile(non_skew_years["rows"], 75)
secondary_cap = CAP_MULTIPLIER_K2 * median_rows

final_cap = int(min(p75_non_skew, secondary_cap))

# ==================================================
# PRINT – FULL AUDIT LOG
# ==================================================

print("\n================ STEP 2.8 – TEMPORAL DISTRIBUTION ================\n")

print("ROWS PER (SOURCE × YEAR):")
print(rows_per_source_year.to_string(index=False))

print("\nROWS PER YEAR (TOTAL):")
print(rows_per_year.to_string(index=False))

print("\nSOURCE YEAR COVERAGE:")
print(source_year_coverage.to_string(index=False))

print("\n--- CAP COMPUTATION ---")
print(f"Median rows/year        : {int(median_rows):,}")
print(f"Skew factor (K)         : {SKEW_FACTOR_K}")
print(f"Skew threshold          : {int(skew_threshold):,}")

print("\nSkewed years:")
if skewed_years.empty:
    print("None")
else:
    print(skewed_years.to_string(index=False))

print("\nNon-skew years:")
print(non_skew_years.to_string(index=False))

print("\n--- FINAL CAP ---")
print(f"P75(non-skew years)     : {int(p75_non_skew):,}")
print(f"Secondary cap (K2×med)  : {int(secondary_cap):,}")
print(f"\n>>> FINAL YEAR CAP      : {final_cap:,}")

if skipped_files:
    print("\n--- SKIPPED FILES ---")
    for name, reason in skipped_files:
        print(f"- {name}: {reason}")

print("\n=================================================================\n")

# ==================================================
# SAVE SUMMARY
# ==================================================

summary_path = OUTPUT_DIR / "year_cap_summary.txt"

with open(summary_path, "w", encoding="utf-8") as f:
    f.write("STEP 2.8 – TEMPORAL DISTRIBUTION & CAP SUMMARY\n\n")

    f.write("ROWS PER (SOURCE × YEAR):\n")
    f.write(rows_per_source_year.to_string(index=False))
    f.write("\n\nROWS PER YEAR:\n")
    f.write(rows_per_year.to_string(index=False))
    f.write("\n\nSOURCE YEAR COVERAGE:\n")
    f.write(source_year_coverage.to_string(index=False))

    f.write("\n\n--- CAP COMPUTATION ---\n")
    f.write(f"Median rows/year       : {int(median_rows):,}\n")
    f.write(f"Skew threshold (K={SKEW_FACTOR_K}) : {int(skew_threshold):,}\n\n")
    f.write(f"P75 non-skew years     : {int(p75_non_skew):,}\n")
    f.write(f"Secondary cap (K2×med) : {int(secondary_cap):,}\n")
    f.write(f"\nFINAL YEAR CAP         : {final_cap:,}\n")

print(f"Summary saved to: {summary_path}")
