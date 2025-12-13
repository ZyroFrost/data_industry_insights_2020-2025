# -*- coding: utf-8 -*-
"""
Crawler Canada Government DataJobs (2020–2025)
Folder structure (same style as Adzuna):
- Raw JSON → data/data_raw/canada_government_datajobs_2020-2025/
- Metadata → data/metadata
- CSV → data/data_processing/canada_government_datajobs_2020-2025.csv
"""

import requests
import pandas as pd
import time
import json
from pathlib import Path
from datetime import datetime


# ==================================================================
# 1. KEYWORDS
# ==================================================================
KEYWORDS = [
    "data analyst",
    "data analytics",
    "business intelligence",
    "data engineer",
    "data scientist",
    "machine learning",
    "ai",
    "ml"
]


# ==================================================================
# 2. PATH CONFIG (identical to Adzuna style)
# ==================================================================
BASE_URL = "https://open.canada.ca/data/api/3/action/package_search"
ROWS_PER_PAGE = 1000

ROOT = Path(__file__).resolve().parents[4]

RAW_DIR = ROOT / "data" / "data_raw" / "canada_government_datajobs_2020-2025"
META_DIR = ROOT / "data" / "metadata"
PROC_DIR = ROOT / "data" / "data_processing"

RAW_DIR.mkdir(parents=True, exist_ok=True)
META_DIR.mkdir(parents=True, exist_ok=True)
PROC_DIR.mkdir(parents=True, exist_ok=True)

final_records = []


# ==================================================================
# 3. API SEARCH
# ==================================================================
def search_keyword(keyword: str, start_year=2020, end_year=2025):
    print(f"\n[+] Searching: '{keyword}' ({start_year}-{end_year})")

    all_results = []
    start = 0

    while True:
        year_filter = " OR ".join([str(y) for y in range(start_year, end_year + 1)])
        fq = f'"{keyword}" AND ({year_filter})'

        params = {
            "q": "",
            "fq": fq,
            "rows": ROWS_PER_PAGE,
            "start": start,
            "sort": "metadata_modified desc"
        }

        try:
            resp = requests.get(BASE_URL, params=params, timeout=25)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print("   ERROR:", e)
            break

        if not data.get("success"):
            print("   API ERROR:", data)
            break

        results = data["result"]["results"]
        count = data["result"]["count"]

        if not results:
            break

        all_results.extend(results)
        print(f"   + {len(results)} rows (total: {len(all_results)}/{count})")

        if start + ROWS_PER_PAGE >= count:
            break

        start += ROWS_PER_PAGE
        time.sleep(0.5)

    return all_results


# ==================================================================
# 4. EXTRACT FIELDS
# ==================================================================
def extract_fields(dataset: dict):
    resources = dataset.get("resources", [])
    raw_resources = [
        r for r in resources if r.get("format", "").upper()
        in ["CSV", "XLSX", "XLS", "JSON", "GEOJSON"]
    ]

    return {
        "dataset_id": dataset.get("id"),
        "title": dataset.get("title", ""),
        "organization": dataset.get("organization", {}).get("title", ""),
        "notes": dataset.get("notes", "")[:500],
        "keywords": ", ".join(dataset.get("tags", [])),
        "publish_date": dataset.get("metadata_created"),
        "last_update": dataset.get("metadata_modified"),
        "num_resources": len(resources),
        "num_raw_files": len(raw_resources),
        "raw_urls": " | ".join([r.get("url", "") for r in raw_resources]),
        "portal_url": f"https://open.canada.ca/data/en/dataset/{dataset.get('id')}"
    }


# ==================================================================
# 5. RUN CRAWLING
# ==================================================================
def run_crawler():
    final_records = []
    all_raw_json = []

    for kw in KEYWORDS:
        datasets = search_keyword(kw.replace(" ", "+"))

        for ds in datasets:
            rec = extract_fields(ds)
            rec["search_keyword"] = kw
            final_records.append(rec)
            all_raw_json.append(rec)

        time.sleep(1)

    return final_records, all_raw_json


# ==================================================================
# 6. SAVE RAW JSON
# ==================================================================
def save_raw(all_raw_json):
    raw_json_file = RAW_DIR / "all_raw_results.json"
    with open(raw_json_file, "w", encoding="utf-8") as f:
        json.dump(all_raw_json, f, ensure_ascii=False, indent=2)

    print(f"\n✔ Saved RAW JSON → {raw_json_file}")


# ==================================================================
# 7. SAVE METADATA
# ==================================================================
def save_metadata(total_records):
    metadata = {
        "crawler": "canada_government_datajobs_2020-2025",
        "description": "Government of Canada Open Data Portal (2020–2025) - Data/AI/ML keywords.",
        "fields": {
            "dataset_id": "string",
            "title": "string",
            "organization": "string",
            "notes": "string",
            "keywords": "string",
            "publish_date": "datetime",
            "last_update": "datetime",
            "num_resources": "int",
            "num_raw_files": "int",
            "raw_urls": "string",
            "portal_url": "string",
            "search_keyword": "string"
        },
        "total_records": total_records,
        "crawl_timestamp": datetime.now().isoformat()
    }

    meta_file = META_DIR / "canada_government_metadata.json"
    with open(meta_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    print(f"✔ Saved metadata → {meta_file}")


# ==================================================================
# 8. FLATTEN CSV
# ==================================================================
def save_csv(final_records):
    df = pd.DataFrame(final_records)
    df["last_update"] = pd.to_datetime(df["last_update"], errors="coerce")
    df = df.sort_values("last_update", ascending=False)

    csv_file = PROC_DIR / "canada_government_datajobs_2020-2025.csv"
    df.to_csv(csv_file, index=False, encoding="utf-8-sig")

    print(f"\n✔ Saved CSV → {csv_file}")
    print(f"✔ Total records: {len(df)}")
    print(f"✔ Total raw files found: {df['num_raw_files'].sum()}")


# ==================================================================
# 9. MAIN ENTRY POINT (bạn đang thiếu)
# ==================================================================
def run_pipeline():
    print("\n🚀 RUNNING CANADA CRAWLER...\n")

    final_records, all_raw_json = run_crawler()

    save_raw(all_raw_json)
    save_metadata(len(final_records))
    save_csv(final_records)

    print("\n🎉 CANADA PIPELINE DONE — DATA READY.\n")


# ==================================================================
# RUN
# ==================================================================
if __name__ == "__main__":
    run_pipeline()