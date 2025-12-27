# -*- coding: utf-8 -*-
"""
STEP 2.10 – SPLIT COMBINED DATA INTO ERD TABLES

Input:
- data/data_processing/s2.9_data_combined/*.csv

Output:
- data/data_processed/*.csv (ERD-ready)

Rules:
- Output ALL ERD tables
- Drop __source_id, __source_name
- Drop JOB if:
    + company_name is NULL / __NA__
    + OR row contains no meaningful data (all NA / INVALID / null)
- Skill is N–N (split by '|')
- Skill is ALREADY normalized & enriched in previous steps
- STEP 2.10 does NOT audit / drop / count skill
"""

import pandas as pd
from pathlib import Path

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.9_data_combined"
OUTPUT_DIR = BASE_DIR / "data" / "data_processed"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# ENUMS (ERD)
# =========================

COMPANY_SIZES = {'Startup','Small','Medium','Large','Enterprise'}

INDUSTRIES = {
    'Technology','Finance','Banking','Insurance','Healthcare','Education',
    'E-commerce','Manufacturing','Consulting','Government',
    'Telecommunications','Energy','Retail','Logistics','Real Estate'
}

ROLE_ENUM = {
    'Data Analyst','Business Intelligence Analyst','BI Developer',
    'Analytics Engineer','Data Engineer','Data Scientist',
    'Machine Learning Engineer','AI Engineer','AI Researcher',
    'Applied Scientist','Research Engineer','Data Architect',
    'Data Manager','Data Lead'
}

EDUCATION_LEVELS = {'High School','Bachelor','Master','PhD'}
EMPLOYMENT_TYPES = {'Full-time','Part-time','Internship','Temporary'}
JOB_LEVELS = {'Intern','Junior','Mid','Senior','Lead'}
REMOTE_OPTIONS = {'Onsite','Hybrid','Remote'}

NA_SET = {"__NA__", "__INVALID__", "__UNMATCHED__", "", None}

def to_null(v):
    if pd.isna(v) or str(v).strip() in NA_SET:
        return None
    return v

# =========================
# LOAD
# =========================

def load_combined():
    files = list(INPUT_DIR.glob("*.csv"))
    if not files:
        raise FileNotFoundError("No combined CSV files found")
    return pd.concat(
        [pd.read_csv(f, encoding="utf-8-sig") for f in files],
        ignore_index=True
    )

# =========================
# LOAD SKILL CATEGORY MAPPING (SINGLE SOURCE OF TRUTH)
# =========================

SKILL_MAPPING_PATH = BASE_DIR / "data" / "data_reference" / "skill_mapping.csv"

skill_ref = pd.read_csv(SKILL_MAPPING_PATH, dtype=str, encoding="utf-8-sig")

# canonical_skill -> SINGLE skill_category
SKILL_TO_CATEGORY = {
    row.canonical_skill.strip(): row.skill_category.strip()
    for row in skill_ref.itertuples()
    if (
        pd.notna(row.canonical_skill)
        and pd.notna(row.skill_category)
        and row.skill_category.strip() != "__NA__"
    )
}

# =========================
# RUN
# =========================

def run():
    df = load_combined()
    total_rows = len(df)

    df = df.drop(columns=["__source_id", "__source_name"], errors="ignore")

    # =========================
    # JOB-LEVEL COUNTERS
    # =========================
    dropped_job_no_company = 0
    dropped_job_all_na = 0

    # =========================
    # ID MAPS
    # =========================
    skill_map = {}
    company_map = {}
    location_map = {}
    role_map = {}

    # =========================
    # TABLE BUFFERS
    # =========================
    skills = []
    companies = []
    locations = []
    roles = []
    job_postings = []
    job_skills = []
    job_roles = []
    job_levels = []

    skill_id = company_id = location_id = role_id = job_id = 1

    for _, r in df.iterrows():

        # =========================
        # DROP JOB – NO COMPANY
        # =========================
        if pd.isna(r["company_name"]) or str(r["company_name"]).strip() in NA_SET:
            dropped_job_no_company += 1
            continue

        # =========================
        # DROP JOB – ALL NA / INVALID
        # =========================
        meaningful_values = [
            v for v in r.values
            if pd.notna(v) and str(v).strip() not in NA_SET
        ]
        if not meaningful_values:
            dropped_job_all_na += 1
            continue

        # =========================
        # COMPANY
        # =========================
        if r["company_name"] not in company_map:
            company_map[r["company_name"]] = company_id
            companies.append({
                "company_id": company_id,
                "company_name": r["company_name"],
                "size": r["company_size"] if r["company_size"] in COMPANY_SIZES else None,
                "industry": r["industry"] if r["industry"] in INDUSTRIES else None
            })
            company_id += 1

        # =========================
        # LOCATION
        # =========================
        loc_key = (r["city"], r["country"], r["country_iso"])
        if loc_key not in location_map:
            location_map[loc_key] = location_id
            locations.append({
                "location_id": location_id,
                "city": to_null(r["city"]),
                "country": to_null(r["country"]),
                "country_iso": to_null(r["country_iso"]),
                "latitude": r["latitude"] if pd.notna(r["latitude"]) else None,
                "longitude": r["longitude"] if pd.notna(r["longitude"]) else None,
                "population": r["population"] if pd.notna(r["population"]) else None
            })
            location_id += 1

        # =========================
        # JOB_POSTINGS
        # =========================
        job_postings.append({
            "job_id": job_id,
            "company_id": company_map[r["company_name"]],
            "location_id": location_map[loc_key],
            "posted_date": to_null(r["posted_date"]),
            "min_salary": to_null(r["min_salary"]),
            "max_salary": to_null(r["max_salary"]),
            "currency": to_null(r["currency"]),
            "required_exp_years": to_null(r["required_exp_years"]),
            "education_level": r["education_level"] if r["education_level"] in EDUCATION_LEVELS else None,
            "employment_type": r["employment_type"] if r["employment_type"] in EMPLOYMENT_TYPES else None,
            "job_description": r["job_description"] if pd.notna(r["job_description"]) else None,
            "remote_option": r["remote_option"] if r["remote_option"] in REMOTE_OPTIONS else None
        })

        # =========================
        # SKILLS (N–N) – CANONICAL CATEGORY ONLY
        # =========================
        if (
            pd.notna(r["skill_name"])
            and str(r["skill_name"]).strip() != "__NA__"
        ):
            raw_skills = [
                s.strip()
                for s in str(r["skill_name"]).split("|")
                if s.strip() and s.strip() != "__NA__"
            ]

            for skill in raw_skills:

                # skill MUST exist in mapping, otherwise ignore silently
                if skill not in SKILL_TO_CATEGORY:
                    continue

                if skill not in skill_map:
                    skill_map[skill] = skill_id

                    skills.append({
                        "skill_id": skill_id,
                        "skill_name": skill,
                        # ✅ SINGLE SOURCE OF TRUTH
                        "skill_category": SKILL_TO_CATEGORY[skill]
                    })

                    skill_id += 1

                job_skills.append({
                    "job_id": job_id,
                    "skill_id": skill_map[skill]
                })

        # =========================
        # ROLES (N–N)
        # =========================
        if pd.notna(r["role_name"]):
            for role in [x.strip() for x in str(r["role_name"]).split("|")]:
                if role not in ROLE_ENUM:
                    continue
                if role not in role_map:
                    role_map[role] = role_id
                    roles.append({"role_id": role_id, "role_name": role})
                    role_id += 1
                job_roles.append({"job_id": job_id, "role_id": role_map[role]})

        # =========================
        # LEVEL
        # =========================
        if r["level"] in JOB_LEVELS:
            job_levels.append({"job_id": job_id, "level": r["level"]})

        job_id += 1

    # =========================
    # SAVE
    # =========================

    pd.DataFrame(skills).to_csv(OUTPUT_DIR / "skills.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(companies).to_csv(OUTPUT_DIR / "companies.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(locations).to_csv(OUTPUT_DIR / "locations.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(roles).to_csv(OUTPUT_DIR / "role_names.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(job_postings).to_csv(OUTPUT_DIR / "job_postings.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(job_skills).to_csv(OUTPUT_DIR / "job_skills.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(job_roles).to_csv(OUTPUT_DIR / "job_roles.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(job_levels).to_csv(OUTPUT_DIR / "job_levels.csv", index=False, encoding="utf-8-sig")

    # =========================
    # SUMMARY
    # =========================

    print(
        f"✓ STEP 2.10 DONE\n"
        f"  - Input rows                : {total_rows}\n"
        f"  - Dropped jobs (no company) : {dropped_job_no_company}\n"
        f"  - Dropped jobs (all NA)     : {dropped_job_all_na}\n"
        f"  - Total dropped jobs        : {dropped_job_no_company + dropped_job_all_na}\n"
        f"\n"
        f"  - job_postings              : {len(job_postings)}\n"
        f"  - skills                    : {len(skills)}\n"
        f"  - job_skills                : {len(job_skills)}\n"
        f"  - companies                 : {len(companies)}\n"
        f"  - locations                 : {len(locations)}\n"
        f"  - role_names                : {len(roles)}\n"
        f"  - job_roles                 : {len(job_roles)}\n"
        f"  - job_levels                : {len(job_levels)}"
    )

if __name__ == "__main__":
    run()
