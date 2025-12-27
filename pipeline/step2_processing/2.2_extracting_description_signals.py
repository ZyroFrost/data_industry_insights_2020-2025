# -*- coding: utf-8 -*-
"""
STEP 2.2 – EXTRACTING DESCRIPTION SIGNALS
(This step supports all subsequent steps)

This script extracts weak signals from job_description text
to optionally fill structured fields WHEN they are missing (__NA__).

Extracted signals (simple, rule-based only):
- city
- country
- remote_option
- min_salary / max_salary (very coarse)

IMPORTANT RULES:
1. Description-derived values ONLY fill when target column == "__NA__"
2. NEVER override existing structured data
3. Cannot extract → stay silent (no INVALID, no guess)

PIPELINE:
STEP 2:
- STEP 2.1 – Column mapping
- STEP 2.2 – Description signal extraction (this step)
- STEP 2.3 – City alias normalization
- STEP 2.4 – Geo enrichment

INPUT:
- data/data_processing/s2.1_data_mapped/*.csv or *.xlsx

OUTPUT:
- data/data_processing/s2.2_data_description_extracted/extracted_desc_*.csv
"""

import pandas as pd
import re
from pathlib import Path

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[2]

INPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.1_data_mapped"
OUTPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.2_data_description_extracted"
REF_DIR = BASE_DIR / "data" / "data_reference"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# CONSTANTS
# =========================

NA_VALUE = "__NA__"

REMOTE_KEYWORDS = [
    "remote",
    "work from home",
    "wfh",
    "fully remote",
    "hybrid"
]

# ===== SALARY CONTEXT GUARD =====
SALARY_CONTEXT_KEYWORDS = [
    # currency symbols (expanded with common ones)
    "$", "€", "£", "¥", "¢", "₹", "₽", "₩", "₪", "₱", "฿", "₦", "₴", "₺",

    # currency text/codes (common ISO codes and names)
    "usd", "eur", "euro", "gbp", "pound", "aud", "cad", "jpy", "yen", "cny", "yuan", "inr", "rub", "krw", "chf", "sek", "nok", "dkk", "pln", "czk", "huf",
]

SALARY_INDICATORS = [
    "salary", "salaries", "pay", "compensation",
    "wage", "package", "remuneration",
    "brutto", "netto", "gehalt",
    "per month", "per year", "annually", "monthly",
    "€/month", "$/year"
]

NON_SALARY_CONTEXT = [
    "arr", "revenue", "funding", "series",
    "valuation", "investor", "financing",
    "customers", "market", "growth",
    "raised", "million", "billion",

    # allowance / benefit
    "allowance", "packaging", "bonus",
    "incentive", "benefit", "p/a"
]

# =========================
# LOAD REFERENCE (LIGHT USE)
# =========================

countries_df = pd.read_csv(
    REF_DIR / "countries.csv",
    dtype=str
)

COUNTRY_LOOKUP = {
    c.lower(): c
    for c in countries_df["country_name"].dropna().unique()
}

city_alias_df = pd.read_csv(
    REF_DIR / "city_alias_reference.csv",
    dtype=str
)
def normalize_text(x):
    x = str(x)
    x = re.sub(r"[^\w\s]", "", x)
    return re.sub(r"\s+", " ", x).strip().lower()

CITY_ALIAS_LOOKUP = {
    normalize_text(row.alias): row.canonical_city
    for row in city_alias_df.itertuples(index=False)
}

# =========================
# LOAD SKILL MAPPING
# =========================

skill_map_df = pd.read_csv(
    REF_DIR / "skill_mapping.csv",
    dtype=str
).fillna("")

def split_terms(x):
    return [t.strip() for t in x.split("|") if t.strip()]

SKILL_MAPPING = []

for _, row in skill_map_df.iterrows():
    SKILL_MAPPING.append({
        "canonical": row["canonical_skill"],
        "category": row["skill_category"],
        "aliases": split_terms(row["aliases"].lower()),
        "strong": split_terms(row["strong_terms"].lower()),
        "exclude": split_terms(row["exclude_terms"].lower()),
    })

# canonical skill -> skill category (1-1 dependency)
SKILL_TO_CATEGORY = {
    s["canonical"]: s["category"]
    for s in SKILL_MAPPING
}

# =========================
# LOAD EDUCATION MAPPING
# =========================

edu_map_df = pd.read_csv(
    REF_DIR / "education_level_mapping.csv",
    dtype=str
)

EDU_MAPPING = []
for _, row in edu_map_df.iterrows():
    EDU_MAPPING.append({
        "level": row["education_level"],
        "keywords": [k.strip() for k in row["keywords"].lower().split("|")]
    })

# =========================
# LOAD INDUSTRY MAPPING
# =========================

industry_df = pd.read_csv(
    REF_DIR / "industry_mapping.csv",
    dtype=str
)

INDUSTRY_MAPPING = []
for _, row in industry_df.iterrows():
    INDUSTRY_MAPPING.append({
        "industry": row["industry"],
        "keywords": [k.strip() for k in row["keywords"].lower().split("|")]
    })

# =========================
# LOAD COMPANY SIZE MAPPING
# =========================

size_df = pd.read_csv(
    REF_DIR / "company_size_mapping.csv",
    dtype=str
)

COMPANY_SIZE_MAPPING = []
for _, row in size_df.iterrows():
    COMPANY_SIZE_MAPPING.append({
        "size": row["company_size"],
        "keywords": [k.strip() for k in row["keywords"].lower().split("|")]
    })

# =========================
# LOAD EMPLOYMENT TYPE MAPPING
# =========================

emp_map_df = pd.read_csv(
    REF_DIR / "employment_type_mapping.csv",
    dtype=str
)

EMPLOYMENT_MAPPING = []
for _, row in emp_map_df.iterrows():
    EMPLOYMENT_MAPPING.append({
        "type": row["employment_type"],
        "keywords": [k.strip() for k in row["keywords"].lower().split("|")]
    })

# =========================
# LOAD JOB LEVEL MAPPING
# =========================

level_df = pd.read_csv(
    REF_DIR / "job_level_mapping.csv",
    dtype=str
)

LEVEL_MAPPING = []
for _, row in level_df.iterrows():
    LEVEL_MAPPING.append({
        "level": row["level"],
        "keywords": [k.strip() for k in row["keywords"].lower().split("|")]
    })

# =========================
# HELPERS
# =========================
def normalize_currency(c):
    mapping = {
        "€": "EUR", "eur": "EUR", "euro": "EUR",
        "$": "USD", "usd": "USD",
        "£": "GBP", "gbp": "GBP", "pound": "GBP",
        "¥": "JPY", "jpy": "JPY", "yen": "JPY"
    }
    return mapping.get(c, c.upper())

def extract_remote(text: str):
    return any(k in text for k in REMOTE_KEYWORDS)

def extract_country(text: str):
    for c in COUNTRY_LOOKUP:
        if c in text:
            return COUNTRY_LOOKUP[c]
    return None

def extract_city(text: str):
    for alias, canonical in CITY_ALIAS_LOOKUP.items():
        if alias in text:
            return canonical
    return None

def extract_salary_from_text(desc, cur_min, cur_max, NA="__NA__"):
    text = str(desc).lower()

    def has(val):
        return val != NA and val not in (None, "")

    # ===== 1. TÌM ĐƠN VỊ TIỀN =====
    currency_hits = []
    for k in SALARY_CONTEXT_KEYWORDS:
        start = 0
        while True:
            idx = text.find(k, start)
            if idx == -1:
                break
            currency_hits.append((idx, idx + len(k), k))
            start = idx + len(k)

    if not currency_hits:
        return cur_min, cur_max, NA, False

    extracted_values = []
    detected_currency = NA

    # ===== 2. KHOANH VÙNG THEO ĐƠN VỊ =====
    for start, end, cur in currency_hits:
        left = max(0, start - 20)
        right = min(len(text), end + 20)

        while left > 0 and text[left - 1].isdigit():
            left -= 1
        while right < len(text) and text[right].isdigit():
            right += 1

        window = text[left:right]

        # không có keyword lương → bỏ
        anchor_zone = text[max(0, left - 30):right]
        if not any(k in anchor_zone for k in SALARY_INDICATORS):
            continue

        # dính context công ty / funding → bỏ
        if any(k in window for k in NON_SALARY_CONTEXT):
            continue

        # ===== BẮT SỐ =====
        nums = re.findall(r'\d{1,3}(?:[.,]\d{3})+|\d+', window)
        for n in nums:
            v = int(n.replace(".", "").replace(",", ""))
            if v >= 100:
                extracted_values.append(v)

        if nums and detected_currency == NA:
            detected_currency = normalize_currency(cur)

    if not extracted_values:
        return cur_min, cur_max, NA, False

    lo, hi = min(extracted_values), max(extracted_values)

    if hi - lo > 10_000_000:
        return cur_min, cur_max, NA, False

    if not has(cur_min) and not has(cur_max):
        return lo, hi, detected_currency, True

    return cur_min, cur_max, NA, False

def extract_skills(text: str, role_context: str):
    found = set()
    text = text.lower()

    for s in SKILL_MAPPING:
        def match_r_skill(text: str, role_context: str):
            # R standalone, uppercase
            if re.search(r'\bR\b', role_context):
                return True

            # R strong context
            if any(k in text for k in ["rstudio", "rlang", "r programming"]):
                return True

            return False

        # Special handling for R
        if s["canonical"] == "R":
            if not match_r_skill(text, role_context):
                continue
        else:
            if not any(a in text for a in s["aliases"]):
                continue

        # normal body rule
        if s["strong"] and not any(k in text for k in s["strong"]):
            continue


        if s["exclude"] and any(k in text for k in s["exclude"]):
            continue

        found.add(s["canonical"])

    if not found:
        return NA_VALUE

    return " | ".join(sorted(found))

def extract_experience_years(text: str, NA="__NA__"):
    text = text.lower()

    # =========================
    # 0. GLOBAL EXPERIENCE CONTEXT (BẮT BUỘC)
    # =========================
    EXP_CONTEXT_KEYWORDS = [
        "experience", "exp",
        "year", "years", "yr", "yrs",
        "level", "seniority"
    ]

    if not any(k in text for k in EXP_CONTEXT_KEYWORDS):
        return NA

    # =========================
    # 1. LEVEL KEYWORD (EN / MI / SE / EX) WITH EXCLUSION
    # =========================
    LEVEL_YEAR_MAP = {
        "en": 0,
        "mi": 2,
        "se": 5,
        "ex": 8,
    }

    EXCLUDE_CONTEXT = [
        # brand / proper noun
        "venture", "ecosystem", "company", "group", "lab", "studio",
        # other meanings
        "english", "language",
        "software engineer", "engineer",
        "asia", "europe",
        "executive", "example"
    ]

    COMPANY_CONTEXT = [ "company", "organisation", "organization", "we are",
                        "we have", "our company", "our business", "founded",
                        "since", "established", "leading", "provider",
                        "retailer", "manufacturer", "employees", "stores", 
                        "countries", "customers", "group", "global", "europe", "worldwide"]

    for kw, years in LEVEL_YEAR_MAP.items():
        for m in re.finditer(rf"\b{kw}\b", text):
            left = max(0, m.start() - 30)
            right = min(len(text), m.end() + 30)
            window = text[left:right]

            if any(x in window for x in EXCLUDE_CONTEXT):
                continue

            if any(ctx in window for ctx in ["experience", "level", "seniority"]):
                return str(years)

    # =========================
    # 2. NUMERIC YEARS WITH CONTEXT
    # =========================
    candidates = []

    patterns = [
        ("range", r'(\d+)\s*[-–]\s*(\d+)\s*(year|yr)'),               # 6-13 years
        ("plus", r'(\d+)\s*\+\s*(year|yr)'),                           # 10+ years
        ("min", r'(at least|minimum|min\.?)\s*(\d+)\s*(year|yr)'),    # minimum 6 years
        ("simple", r'(\d+)\s*(year|yr)s?\s*(of)?\s*(experience|exp)'), 
        ("range_to", r'(\d+)\s*(to)\s*(\d+)\s*(year|years|yr|yrs)')
    ]

    for kind, p in patterns:
        for m in re.finditer(p, text):
            left = max(0, m.start() - 40)
            right = min(len(text), m.end() + 40)
            window = text[left:right]

            # ❌ SKIP company experience
            if any(c in window for c in COMPANY_CONTEXT):
                continue

            if kind == "range":
                years = int(m.group(1))
            elif kind == "plus":
                years = int(m.group(1))
            elif kind == "min":
                years = int(m.group(2))
            elif kind == "simple":
                years = int(m.group(1))
            elif kind == "range_to":
                years = int(m.group(1))

            candidates.append(years)

    if not candidates:
        return NA

    years = min(candidates)
    # hard upper bound to avoid company experience
    if years > 20:
        return NA

    if years <= 0:
        return "0"
    if years >= 0:
        return str(years)

    return NA

def extract_education_level(text: str, NA="__NA__"):
    text = text.lower()

    for row in EDU_MAPPING:
        for kw in row["keywords"]:
            if kw in text:
                return row["level"]

    return NA

def extract_industry(text: str, NA="__NA__"):
    text = text.lower()

    for row in INDUSTRY_MAPPING:
        for kw in row["keywords"]:
            if kw in text:
                return row["industry"]

    return NA

def extract_company_size(text: str, NA="__NA__"):
    text = text.lower()

    # ===== 1. EXPLICIT HEADCOUNT =====
    patterns = [
        r'(\d+)\s*\+\s*(employees|people|staff)',
        r'over\s*(\d+)\s*(employees|people|staff)',
        r'more than\s*(\d+)\s*(employees|people|staff)',
        r'(\d+)\s*-\s*(\d+)\s*(employees|people|staff)',
        r'team of\s*(\d+)',
    ]

    for p in patterns:
        m = re.search(p, text)
        if m:
            nums = [int(x) for x in m.groups() if x.isdigit()]
            if not nums:
                continue

            n = max(nums)

            if n < 10:
                return "Startup"
            elif n < 50:
                return "Small"
            elif n < 250:
                return "Medium"
            elif n < 1000:
                return "Large"
            else:
                return "Enterprise"

    # ===== 2. KEYWORD FALLBACK =====
    for row in COMPANY_SIZE_MAPPING:
        for kw in row["keywords"]:
            if kw in text:
                return row["size"]

    return NA

def extract_employment_type(text: str, NA="__NA__"):
    text = text.lower()

    for row in EMPLOYMENT_MAPPING:
        for kw in row["keywords"]:
            if kw in text:
                return row["type"]

    return NA

def extract_job_level(text: str, NA="__NA__"):
    text = text.lower()

    for row in LEVEL_MAPPING:
        for kw in row["keywords"]:
            if kw in text:
                return row["level"]

    return NA

# =========================
# PROCESS SINGLE FILE
# =========================

def extract_from_description(file_path: Path):
    print(f"🔄 Extracting description signals: {file_path.name}")

    # load input
    if file_path.suffix.lower() == ".csv":
        df = pd.read_csv(file_path, dtype=str)
    else:
        df = pd.read_excel(file_path, dtype=str)

    if "job_description" not in df.columns:
        print("  ⚠ No job_description column found, skipping")
        return

    df = df.fillna(NA_VALUE)

    filled_city = 0
    filled_country = 0
    filled_remote = 0
    filled_salary = 0
    filled_currency = 0
    filled_skills = 0
    filled_skill_category = 0
    filled_exp_years = 0
    filled_education = 0
    filled_industry = 0
    filled_company_size = 0
    filled_employment = 0
    filled_level = 0

    for i, row in df.iterrows():
        desc = row["job_description"]
        if desc == NA_VALUE or not isinstance(desc, str):
            continue

        desc_lower = desc.lower()

        # -------- CITY --------
        if "city" in df.columns and df.at[i, "city"] == NA_VALUE:
            city = extract_city(desc_lower)
            if city:
                df.at[i, "city"] = city
                filled_city += 1

        # -------- COUNTRY --------
        if "country" in df.columns and df.at[i, "country"] == NA_VALUE:
            country = extract_country(desc_lower)
            if country:
                df.at[i, "country"] = country
                filled_country += 1

        # -------- REMOTE --------
        if "remote_option" in df.columns and df.at[i, "remote_option"] == NA_VALUE:
            if extract_remote(desc_lower):
                df.at[i, "remote_option"] = "true"
                filled_remote += 1

        # -------- SALARY (+ CURRENCY) --------
        cur_min = df.at[i, "min_salary"]
        cur_max = df.at[i, "max_salary"]

        new_min, new_max, new_currency, filled = extract_salary_from_text(
            desc,
            cur_min,
            cur_max
        )

        if filled:
            df.at[i, "min_salary"] = new_min
            df.at[i, "max_salary"] = new_max

            if "currency" in df.columns and df.at[i, "currency"] == NA_VALUE:
                df.at[i, "currency"] = new_currency

            filled_salary += 1
            filled_currency += 1

        # -------- REQUIRED EXPERIENCE (YEARS) --------
        if (
            "required_exp_years" in df.columns
            and df.at[i, "required_exp_years"] == NA_VALUE
        ):
            exp_years = extract_experience_years(desc)
            if exp_years != NA_VALUE:
                df.at[i, "required_exp_years"] = exp_years
                filled_exp_years += 1

        # -------- EDUCATION LEVEL --------
        if (
            "education_level" in df.columns
            and df.at[i, "education_level"] == NA_VALUE
        ):
            edu = extract_education_level(desc)
            if edu != NA_VALUE:
                df.at[i, "education_level"] = edu
                filled_education += 1

        # -------- INDUSTRY --------
        if (
            "industry" in df.columns
            and df.at[i, "industry"] == NA_VALUE
        ):
            industry = extract_industry(desc_lower)
            if industry != NA_VALUE:
                df.at[i, "industry"] = industry
                filled_industry += 1

        # -------- SKILL_NAME (FROM DESCRIPTION) --------
        if "skill_name" in df.columns and df.at[i, "skill_name"] == NA_VALUE:
            role_context = desc_lower[:300]
            skills = extract_skills(desc, role_context)

            if skills != NA_VALUE:
                df.at[i, "skill_name"] = skills
                filled_skills += 1

        # -------- SKILL_CATEGORY (FROM SKILL_NAME) --------
        if (
            "skill_category" in df.columns
            and df.at[i, "skill_category"] == NA_VALUE
            and df.at[i, "skill_name"] != NA_VALUE
        ):
            first_skill = df.at[i, "skill_name"].split(" | ")[0]
            category = SKILL_TO_CATEGORY.get(first_skill, NA_VALUE)
            df.at[i, "skill_category"] = category
            if category != NA_VALUE:
                filled_skill_category += 1

        # -------- COMPANY SIZE --------
        if (
            "company_size" in df.columns
            and df.at[i, "company_size"] == NA_VALUE
        ):
            size = extract_company_size(desc_lower)
            if size != NA_VALUE:
                df.at[i, "company_size"] = size
                filled_company_size += 1

        # -------- EMPLOYMENT TYPE --------
        if (
            "employment_type" in df.columns
            and df.at[i, "employment_type"] == NA_VALUE
        ):
            emp = extract_employment_type(desc_lower)
            if emp != NA_VALUE:
                df.at[i, "employment_type"] = emp
                filled_employment += 1

        # -------- JOB LEVEL (FROM DESCRIPTION) --------
        if (
            "level" in df.columns
            and df.at[i, "level"] == NA_VALUE
        ):
            lvl = extract_job_level(desc_lower)
            if lvl != NA_VALUE:
                df.at[i, "level"] = lvl
                filled_level += 1

    # =========================
    # SAVE OUTPUT (CSV UTF-8-SIG)
    # =========================

    output_name = (
        file_path.name.replace("mapped_", "extracted_desc_", 1)
        if file_path.name.startswith("mapped_")
        else "extracted_desc_" + file_path.name
    )

    output_path = OUTPUT_DIR / output_name.replace(file_path.suffix, ".csv")

    df.to_csv(
        output_path,
        index=False,
        encoding="utf-8-sig"
    )

    # =========================
    # LOG SUMMARY
    # =========================

    print(
        f"  ✓ Saved: {output_path.name}\n"
        f"    - City filled from desc    : {filled_city}\n"
        f"    - Country filled from desc : {filled_country}\n"
        f"    - Remote filled from desc  : {filled_remote}\n"
        f"    - Salary filled from desc  : {filled_salary}\n"
        f"    - Currency filled from desc: {filled_currency}\n"
        f"    - Skills filled from desc  : {filled_skills}\n"
        f"    - Skill category filled    : {filled_skill_category}\n"
        f"    - Required exp (years)     : {filled_exp_years}\n"
        f"    - Education level filled   : {filled_education}\n"
        f"    - Industry filled from desc: {filled_industry}\n"
        f"    - Company size filled      : {filled_company_size}\n"
        f"    - Employment type filled   : {filled_employment}\n"
        f"    - Job level filled         : {filled_level}\n"
        f"  → Folder saved               : {OUTPUT_DIR}"
    )

# =========================
# RUN STEP
# =========================

def run():
    files = [f for f in INPUT_DIR.iterdir() if f.is_file()]
    if not files:
        print(f"No input files found in {INPUT_DIR}")
        return

    for f in files:
        extract_from_description(f)

    print("\n=== STEP 2.2 COMPLETED: DESCRIPTION SIGNAL EXTRACTION ===")

if __name__ == "__main__":
    run()