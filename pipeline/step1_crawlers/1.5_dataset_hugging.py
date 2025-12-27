# -*- coding: utf-8 -*-
"""
STEP 1.5 – INGEST HUGGINGFACE DATASET (data_jobs)

Purpose:
- Load HuggingFace dataset lukebarousse/data_jobs
- Export raw dataset to CSV
- Treat as extracted source, aligned with s2.1 extract layer

IMPORTANT:
- NO cleaning
- NO normalization
- NO column changes
- Just dump to CSV
"""

from datasets import load_dataset
from pathlib import Path

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[2]

OUTPUT_DIR = BASE_DIR / "data" / "data_processing" / "s1_data_extracted"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_CSV = OUTPUT_DIR / "extracted_hf_data_jobs.csv"

# =========================
# LOAD DATASETS
# =========================

dataset_jobs = load_dataset(
    "lukebarousse/data_jobs",
    split="train"
)

dataset_ai_jobs = load_dataset(
    "princekhunt19/2025-ai-data-jobs-dataset",
    split="train"
)
# =========================
# EXPORT TO CSV (OVERWRITE)
# =========================

df_jobs = dataset_jobs.to_pandas()
df_jobs.to_csv(
    OUTPUT_DIR / "extracted_hf_data_jobs.csv",
    index=False,
    encoding="utf-8-sig"
)

df_ai_jobs = dataset_ai_jobs.to_pandas()
df_ai_jobs.to_csv(
    OUTPUT_DIR / "extracted_hf_2025_ai_jobs.csv",
    index=False,
    encoding="utf-8-sig"
)

print(f"✓ Overwritten: extracted_hf_data_jobs.csv ({len(df_jobs):,} rows)")
print(f"✓ Overwritten: extracted_hf_2025_ai_jobs.csv ({len(df_ai_jobs):,} rows)")
