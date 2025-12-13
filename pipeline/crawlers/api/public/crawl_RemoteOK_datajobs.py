# -*- coding: utf-8 -*-
"""
RemoteOK Crawler (Data Jobs 2018–2025)
Chuẩn hóa theo dự án:
- Raw JSON → data/data_raw/remoteok_datajobs_2025/
- Metadata → data/metadata
- Processed CSV → data/data_processing/remoteok_datajobs_2025.csv
"""

import requests
import json
import pandas as pd
from pathlib import Path
from datetime import datetime


# ==========================================================
# 0) CONFIG
# ==========================================================
ROOT = Path(__file__).resolve().parents[4]

RAW_DIR = ROOT / "data" / "data_raw" / "remoteok_datajobs_2025"
RAW_DIR.mkdir(parents=True, exist_ok=True)

META_DIR = ROOT / "data" / "metadata"
META_DIR.mkdir(parents=True, exist_ok=True)

PROC_DIR = ROOT / "data" / "data_processing"
PROC_DIR.mkdir(parents=True, exist_ok=True)


# ==========================================================
# 1) Download ALL RemoteOK job history
# ==========================================================
def download_remoteok():
    url = "https://remoteok.com/api"
    print("📡 Downloading RemoteOK job archive...")

    r = requests.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        },
        timeout=30
    )

    data = r.json()
    data = data[1:]  # first element is metadata

    save_path = RAW_DIR / "all_raw_results.json"
    with open(save_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"✅ RAW saved → {save_path}")
    return data


# ==========================================================
# 2) KEYWORDS & FILTER LOGIC
# ==========================================================
KEYWORDS = [
    "data", "machine learning", "ml", "ai", "artificial intelligence",
    "analytics", "analyst", "analysis",
    "engineer", "engineering",
    "scientist", "science",
    "etl", "sql", "python", "hadoop", "spark", "aws", "gcp", "azure",
    "deep learning", "nlp", "model",
    "database", "big data", "bi", "business intelligence",
]


def is_data_job(job):
    text = (
        (job.get("position") or "") + " " +
        (job.get("description") or "") + " " +
        " ".join(job.get("tags") or [])
    ).lower()

    return any(kw in text for kw in KEYWORDS)


# ==========================================================
# 3) FILTER 2018–2025
# ==========================================================
def filter_data_jobs(raw):
    print("🔍 Filtering Data-related jobs (2018–2025)...")

    rows = []

    for job in raw:
        # Parse year
        try:
            year = datetime.fromtimestamp(job.get("epoch", 0)).year
        except:
            try:
                year = int(str(job.get("date", ""))[:4])
            except:
                continue

        if not (2018 <= year <= 2025):
            continue
        if not is_data_job(job):
            continue

        rows.append({
            "id": job.get("id"),
            "date": job.get("date"),
            "company": job.get("company"),
            "position": job.get("position"),
            "tags": ",".join(job.get("tags", [])),
            "location": job.get("location"),
            "description": job.get("description"),
            "url": job.get("url"),
            "salary": job.get("salary"),
            "year": year
        })

    print(f"📌 Total Data jobs (2018–2025): {len(rows)}")
    return rows


# ==========================================================
# 4) Save CSV — KHÔNG tạo subfolder
# ==========================================================
def save_csv(rows):
    df = pd.DataFrame(rows)
    output_path = PROC_DIR / "remoteok_datajobs_2025.csv"
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"💾 Saved CSV → {output_path}")
    return output_path


# ==========================================================
# 5) Save metadata
# ==========================================================
def save_metadata(total, csv_path):
    meta = {
        "crawler": "remoteok",
        "description": "RemoteOK global job board — filtered for Data/AI/ML jobs (2018–2025).",
        "fields": {
            "id": "string",
            "date": "string",
            "company": "string",
            "position": "string",
            "tags": "string",
            "location": "string",
            "description": "string",
            "url": "string",
            "salary": "string",
            "year": "int",
        },
        "total_records": total,
        "csv_output": str(csv_path),
        "crawl_timestamp": datetime.now().isoformat(),
    }

    meta_file = META_DIR / "remoteok_metadata.json"
    with open(meta_file, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)

    print(f"📝 Metadata saved → {meta_file}")


# ==========================================================
# MAIN RUN
# ==========================================================
if __name__ == "__main__":
    raw = download_remoteok()
    data_jobs = filter_data_jobs(raw)
    csv_path = save_csv(data_jobs)
    save_metadata(len(data_jobs), csv_path)

    print("\n🎉 DONE. RemoteOK Data Job History (2018–2025) ready!")
