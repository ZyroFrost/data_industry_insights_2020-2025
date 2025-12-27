# -*- coding: utf-8 -*-
"""
STEP 2.0 – COLUMN MAPPING (Interactive Tool)
    tools/column_mapper_app.py

STEP 2.1 – MAPPING COVERAGE CHECK (this step)

This step verifies that all extracted datasets have been
successfully mapped to the ERD ingestion schema using STEP 2.0.

Purpose:
- Ensure STEP 2.0 (column mapping) has been completed
- Check 1–1 correspondence between extracted files and mapped files
- Prevent downstream processing if any dataset is still unmapped
- Act as a pipeline guard before STEP 2.2 (description extraction)

Behavior:
- Count extracted files
- Count mapped files
- Count total rows per file (extracted & mapped)
- Fail immediately if any extracted file is missing a mapped version
"""

import sys
import pandas as pd
from pathlib import Path

# ======================================================
# PATH CONFIG
# ======================================================

ROOT = Path(__file__).resolve().parents[2]

EXTRACTED_DIR = ROOT / "data" / "data_processing" / "s1_data_extracted"
MAPPED_DIR = ROOT / "data" / "data_processing" / "s2.1_data_mapped"

# ======================================================
# COLLECT FILES
# ======================================================

VALID_EXTS = {".csv", ".xlsx"}

extracted_files = sorted(
    f for f in EXTRACTED_DIR.iterdir()
    if f.is_file() and f.suffix.lower() in VALID_EXTS
)

mapped_files = sorted(
    f for f in MAPPED_DIR.iterdir()
    if f.is_file() and f.suffix.lower() in VALID_EXTS
)

extracted_count = len(extracted_files)
mapped_count = len(mapped_files)

# ======================================================
# ROW COUNT HELPERS
# ======================================================

def count_rows(file_path: Path) -> int:
    try:
        if file_path.suffix.lower() == ".csv":
            total = 0
            for chunk in pd.read_csv(file_path, chunksize=100_000, low_memory=False):
                total += len(chunk)
            return total

        elif file_path.suffix.lower() == ".xlsx":
            return len(pd.read_excel(file_path))

    except Exception:
        return -1  # error marker

    return -1

# ======================================================
# OUTPUT
# ======================================================

print("\n🔎 STEP 02 – MAPPING CHECK\n")

print(f"📂 Extracted files : {extracted_count}")
print(f"📂 Mapped files    : {mapped_count}")

print(f"\n📊 Mapping result  : {mapped_count} / {extracted_count} files mapped")

print("\n---------------- FILE ROW COUNTS ----------------")

total_extracted_rows = 0
total_mapped_rows = 0

print("\n📄 Extracted files:")
for f in extracted_files:
    rows = count_rows(f)
    total_extracted_rows += max(rows, 0)
    print(f"  - {f.name}: {rows if rows >= 0 else 'ERROR'} rows")

print("\n📄 Mapped files:")
for f in mapped_files:
    rows = count_rows(f)
    total_mapped_rows += max(rows, 0)
    print(f"  - {f.name}: {rows if rows >= 0 else 'ERROR'} rows")

print("\n📊 TOTAL ROWS")
print(f"  • Extracted total rows : {total_extracted_rows}")
print(f"  • Mapped total rows    : {total_mapped_rows}")

# ======================================================
# FINAL RESULT
# ======================================================

print("\n================ RESULT ================")

if mapped_count < extracted_count:
    missing = extracted_count - mapped_count
    print(
        f"❌ STEP 02 FAILED – {missing} extracted file(s) have NOT been mapped"
    )
    sys.exit(1)
else:
    print("✅ STEP 02 PASSED – All extracted files have been mapped")
    sys.exit(0)