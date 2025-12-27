# -*- coding: utf-8 -*-
"""
THEIRSTACK API CRAWLER – JOBS YEAR 2024

Location:
pipeline/step1_crawlers/api/authenticated/crawler_theirstack_datajobs.py

PURPOSE
-------
- Crawl job postings from TheirStack official API (authenticated)
- Target: year 2024 (~100k jobs)
- Save RAW JSON per page
- Deduplicate by job_id
- Flatten RAW jobs into s1_data_extracted CSV (NO schema guessing)
- Export metadata schema from REAL API response

IMPORTANT
---------
- STEP 1 ONLY: collect + flatten raw data
- NO ERD mapping here
- NO field guessing (TheirStack schema may change)
"""

import os
import json
import time
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# =========================================================
# 0. LOAD ENV
# =========================================================
load_dotenv()

# ✅ OFFICIAL ENV NAME
THEIRSTACK_API_KEY = os.getenv("THEIRSTACK_API_KEY")

if not THEIRSTACK_API_KEY:
    raise EnvironmentError("Missing THEIRSTACK_API_KEY in .env")

# =========================================================
# 1. PROJECT ROOT (NEW PATH – FIXED)
# =========================================================
# File:
# pipeline/step1_crawlers/api/authenticated/crawler_theirstack_datajobs.py
#
# parents:
# 0 authenticated
# 1 api
# 2 step1_crawlers
# 3 pipeline
# 4 project root ❌ (OLD)
#
# ✅ NEW ROOT:
ROOT = Path(__file__).resolve().parents[3]

# =========================================================
# 2. OUTPUT PATHS (ALIGNED WITH ADZUNA PIPELINE)
# =========================================================
RAW_DIR = ROOT / "data" / "data_raw" / "theirstack_2024"
PROCESSING_DIR = ROOT / "data" / "data_processing" / "s1_data_extracted"
METADATA_DIR = ROOT / "data" / "metadata" / "source"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSING_DIR.mkdir(parents=True, exist_ok=True)
METADATA_DIR.mkdir(parents=True, exist_ok=True)

# =========================================================
# 3. API CONFIG
# =========================================================
API_URL = "https://api.theirstack.com/v1/jobs/search"

HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer {THEIRSTACK_API_KEY}",
}

POSTED_AT_GTE = "2024-01-01"
POSTED_AT_LTE = "2024-12-31"

LIMIT = 100
MAX_PAGES = 1200          # ~120k jobs upper bound
SLEEP_SECONDS = 0.5       # rate-limit safety

# =========================================================
# 4. LOAD EXISTING JOB IDS (DEDUP / RESUME SAFE)
# =========================================================
def load_existing_job_ids():
    job_ids = set()

    for file in RAW_DIR.glob("page_*.json"):
        try:
            data = json.loads(file.read_text(encoding="utf-8"))
            for job in data.get("jobs", []):
                job_id = job.get("id") or job.get("_id")
                if job_id:
                    job_ids.add(job_id)
        except Exception:
            continue

    return job_ids

# =========================================================
# 5. FETCH ONE PAGE
# =========================================================
def fetch_page(page: int) -> dict:
    payload = {
        "include_total_results": False,
        "order_by": [
            {
                "field": "date_posted",
                "desc": True
            }
        ],
        "posted_at_gte": POSTED_AT_GTE,
        "posted_at_lte": POSTED_AT_LTE,
        "page": page,
        "limit": LIMIT,
        "blur_company_data": True
    }

    resp = requests.post(
        API_URL,
        headers=HEADERS,
        json=payload,
        timeout=30
    )
    resp.raise_for_status()
    return resp.json()

# =========================================================
# 6. CRAWL ALL PAGES
# =========================================================
def crawl_all_pages():
    seen_job_ids = load_existing_job_ids()
    print(f"🔁 Existing jobs loaded: {len(seen_job_ids)}")

    for page in range(MAX_PAGES):
        try:
            data = fetch_page(page)
            jobs = data.get("jobs", [])

            if not jobs:
                print(f"⏹ No more data at page {page}")
                break

            new_jobs = []
            skipped = 0

            for job in jobs:
                job_id = job.get("id") or job.get("_id")
                if not job_id or job_id in seen_job_ids:
                    skipped += 1
                    continue

                seen_job_ids.add(job_id)
                new_jobs.append(job)

            if not new_jobs:
                print(f"⏭ Page {page}: all duplicated")
                continue

            data["jobs"] = new_jobs

            save_path = RAW_DIR / f"page_{page:04d}.json"
            save_path.write_text(
                json.dumps(data, indent=2, ensure_ascii=False),
                encoding="utf-8"
            )

            print(
                f"✓ Page {page:04d} saved "
                f"(new={len(new_jobs)}, skipped={skipped})"
            )

            time.sleep(SLEEP_SECONDS)

        except Exception as e:
            print(f"❌ ERROR at page {page}: {e}")
            time.sleep(5)

# =========================================================
# 7. NORMALIZE JOB (NO SCHEMA GUESSING)
# =========================================================
def normalize_job(job: dict) -> dict:
    """
    Flatten job object WITHOUT guessing field meanings.
    - Keep all top-level keys
    - Nested structures are JSON-dumped
    """

    row = {}

    for k, v in job.items():
        if isinstance(v, (str, int, float, bool)) or v is None:
            row[k] = v
        else:
            row[k] = json.dumps(v, ensure_ascii=False)

    row["__source"] = "theirstack"
    return row

# =========================================================
# 8. EXPORT METADATA (RAW SCHEMA)
# =========================================================
def export_metadata(example_job: dict):
    schema = {k: type(v).__name__ for k, v in example_job.items()}
    meta_path = METADATA_DIR / "theirstack_metadata.json"

    meta_path.write_text(
        json.dumps(schema, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    print(f"📄 Metadata exported → {meta_path}")

# =========================================================
# 9. FLATTEN RAW JSON → CSV
# =========================================================
def flatten_all_pages():
    rows = []
    example_saved = False

    for file in sorted(RAW_DIR.glob("page_*.json")):
        data = json.loads(file.read_text(encoding="utf-8"))

        for job in data.get("jobs", []):
            row = normalize_job(job)
            rows.append(row)

            if not example_saved:
                export_metadata(job)
                example_saved = True

    df = pd.DataFrame(rows)

    output_csv = PROCESSING_DIR / "theirstack_jobs_2024.csv"
    df.to_csv(output_csv, index=False, encoding="utf-8")

    print(f"\n🎉 DONE! Saved {len(df)} rows → {output_csv}")

# =========================================================
# 10. ENTRY POINT
# =========================================================
def run():
    print("\n🚀 RUNNING THEIRSTACK 2024 API CRAWLER...\n")
    crawl_all_pages()
    flatten_all_pages()
    print("\n🎯 THEIRSTACK PIPELINE FINISHED.\n")

if __name__ == "__main__":
    run()
