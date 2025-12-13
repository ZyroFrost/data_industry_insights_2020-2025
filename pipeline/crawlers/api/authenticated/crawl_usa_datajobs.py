import requests
import json
import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

# =========================================================
# ENV
# =========================================================
load_dotenv()

API_EMAIL = os.getenv("USAJOBS_EMAIL")
API_KEY = os.getenv("USAJOBS_API_KEY")

HEADERS = {
    "Host": "data.usajobs.gov",
    "User-Agent": API_EMAIL,
    "Authorization-Key": API_KEY,
}

# =========================================================
# PROJECT FOLDERS
# =========================================================
ROOT = Path(__file__).resolve().parents[4]

RAW_DIR = ROOT / "data" / "data_raw" / "usa_government_datajobs_2020-2025"
RAW_DIR.mkdir(parents=True, exist_ok=True)

PROC_DIR = ROOT / "data" / "data_processing"
PROC_DIR.mkdir(parents=True, exist_ok=True)

META_DIR = ROOT / "data" / "metadata" / "usa_government_datajobs"
META_DIR.mkdir(parents=True, exist_ok=True)

# =========================================================
# KEYWORDS
# =========================================================
KEYWORDS = [
    "data", "machine learning", "ml", "ai", "artificial intelligence",
    "analytics", "analyst", "analysis", "business intelligence",
    "etl", "sql", "python", "hadoop", "spark", "aws", "gcp", "azure",
    "deep learning", "nlp", "model",
    "engineer", "scientist",
]

def is_data_job(text):
    return any(kw in text.lower() for kw in KEYWORDS)


# =========================================================
# CRAWL 1 YEAR
# =========================================================
def crawl_usajobs_for_year(year):
    print(f"\n📡 Crawling USAJobs for year {year}...")
    jobs = []
    page = 1

    while True:
        url = "https://data.usajobs.gov/api/search"
        params = {
            "Keyword": "data",
            "Page": page,
            "DatePosted": year
        }

        r = requests.get(url, headers=HEADERS, params=params)
        data = r.json()

        items = data.get("SearchResult", {}).get("SearchResultItems", [])
        if not items:
            break

        print(f"→ Page {page}: {len(items)} jobs")

        for item in items:
            job = item.get("MatchedObjectDescriptor", {})
            summary = job.get("UserArea", {}).get("Details", {}).get("JobSummary", "")

            text = f"{job.get('PositionTitle', '')} {summary}"
            if not is_data_job(text):
                continue

            salary_info = job.get("PositionRemuneration", [{}])

            jobs.append({
                "id": job.get("PositionID"),
                "title": job.get("PositionTitle"),
                "company": job.get("OrganizationName"),
                "location": job.get("PositionLocation", [{}])[0].get("LocationName"),
                "salary_min": salary_info[0].get("MinimumRange"),
                "salary_max": salary_info[0].get("MaximumRange"),
                "description": summary,
                "year": year,
                "apply_url": job.get("ApplyURI", [""])[0]
            })

        page += 1
        if page > 40:
            break

    return jobs


# =========================================================
# FULL CRAWL
# =========================================================
def crawl_usa_jobs_all_years(start=2020, end=2025):
    all_jobs = []

    for year in range(start, end + 1):
        yearly_jobs = crawl_usajobs_for_year(year)
        all_jobs.extend(yearly_jobs)

        raw_path = RAW_DIR / f"{year}.json"
        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump(yearly_jobs, f, indent=4, ensure_ascii=False)

        print(f"✔ Saved RAW JSON → {raw_path}")

    df = pd.DataFrame(all_jobs)

    csv_path = PROC_DIR / "usa_government_datajobs.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    print(f"\n🎉 DONE: Saved CSV → {csv_path}")
    print(f"Total jobs collected: {len(all_jobs)}")

    # save metadata
    metadata = {
        "crawler": "usa_government_datajobs",
        "description": "USA Jobs Federal Portal — Data/AI/ML Jobs 2020–2025.",
        "total_records": len(all_jobs),
        "csv_output": str(csv_path),
        "raw_years": [f"{year}.json" for year in range(start, end + 1)],
        "crawl_timestamp": datetime.now().isoformat(),
    }

    meta_file = META_DIR / "usa_government_metadata.json"
    with open(meta_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    print(f"📝 Metadata saved → {meta_file}")


# =========================================================
# ENTRY POINT
# =========================================================
if __name__ == "__main__":
    crawl_usa_jobs_all_years()