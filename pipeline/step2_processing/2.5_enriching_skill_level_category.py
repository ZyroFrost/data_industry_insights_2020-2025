# -*- coding: utf-8 -*-
"""
STEP 2.5 – SKILL ENRICHMENT

This step enriches skill-related derived fields:

A. skill_category
   - Derived from canonical skill_name
   - Reference-based only (skill_mapping.csv)
   - No inference

B. skill_level
   - Inferred from role_name and job_description
   - Rule-based signals only

IMPORTANT:
- skill_category enrichment MUST run BEFORE skill_level
- Two logics are independent but grouped for pipeline simplicity
"""

import pandas as pd
import re
from pathlib import Path

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[2]

INPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.4_data_country_enriched"
OUTPUT_DIR = BASE_DIR / "data" / "data_processing" / "s2.5_data_skill_level_enriched"
REF_DIR = BASE_DIR / "data" / "data_reference"

UNMATCHED_DIR = BASE_DIR / "data" / "data_unmatched_report"
REF_DIR.mkdir(parents=True, exist_ok=True)
UNMATCHED_DIR.mkdir(parents=True, exist_ok=True)
UNMATCHED_BASENAME = "unmatched_skill_level"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
REF_DIR.mkdir(parents=True, exist_ok=True)

LEVEL_MAPPING_PATH = REF_DIR / "skill_level_mapping.csv"
SKILL_MAPPING_PATH = REF_DIR / "skill_mapping.csv"

# =========================
# CONSTANTS
# =========================

LEVEL_ENUM = ["Basic", "Intermediate", "Advanced", "Expert"]
NA_VALUE = "__NA__"

SKILL_SPLIT_PATTERN = r"\s*\|\s*"
CONTEXT_WINDOW = 6

# =========================
# LOAD REFERENCES
# =========================

level_df = pd.read_csv(LEVEL_MAPPING_PATH, encoding="utf-8-sig")
skill_df = pd.read_csv(SKILL_MAPPING_PATH, encoding="utf-8-sig")

# canonical_skill -> skill_category
SKILL_CATEGORY_LOOKUP = (
    skill_df
    .dropna(subset=["canonical_skill", "skill_category"])
    .set_index("canonical_skill")["skill_category"]
    .to_dict()
)

LEVEL_MAPPING = {
    r.level: {
        "role": [k.strip().lower() for k in str(r.role_keywords).split("|") if k.strip()],
        "desc": [k.strip().lower() for k in str(r.description_keywords).split("|") if k.strip()],
        "negative": [k.strip().lower() for k in str(r.negative_keywords).split("|") if k.strip()],
    }
    for r in level_df.itertuples()
}

# =========================
# HELPERS
# =========================

def normalize_text(x):
    if pd.isna(x):
        return ""
    return re.sub(r"\s+", " ", str(x).lower()).strip()


def match_level(text, source):
    matched = []
    for level, rules in LEVEL_MAPPING.items():
        if any(n in text for n in rules["negative"]):
            continue
        if any(k in text for k in rules[source]):
            matched.append(level)
    return matched


def extract_context(text, skill, window):
    tokens = text.split()
    skill_tokens = skill.split()

    for i in range(len(tokens)):
        if tokens[i:i + len(skill_tokens)] == skill_tokens:
            start = max(0, i - window)
            end = min(len(tokens), i + len(skill_tokens) + window)
            return " ".join(tokens[start:end])
    return ""

# =========================
# ENRICH FILE
# =========================

def enrich_file(file_path: Path):
    print(f"🔄 Enriching: {file_path.name}")

    df_raw = pd.read_csv(file_path, dtype=str, encoding="utf-8-sig")

    if "skill_name" not in df_raw.columns:
        df_raw["skill_name"] = NA_VALUE
    if "skill_level_required" not in df_raw.columns:
        df_raw["skill_level_required"] = NA_VALUE

    df_raw["skill_name"] = df_raw["skill_name"].fillna(NA_VALUE)
    df_raw["skill_level_required"] = df_raw["skill_level_required"].fillna(NA_VALUE)

    total_rows = len(df_raw)

    skill_before = df_raw["skill_name"]
    level_before = df_raw["skill_level_required"]

    valid_skill_mask = (
        (skill_before != NA_VALUE) &
        (skill_before != "__INVALID__")
    )

    # =========================
    # INITIAL AUDIT
    # =========================

    skill_empty = (skill_before == NA_VALUE).sum()
    level_empty = (level_before == NA_VALUE).sum()

    level_right = (
        (level_before.isin(LEVEL_ENUM))
    ).sum()

    level_wrong = (
        valid_skill_mask &
        ~level_before.isin(LEVEL_ENUM) &
        (level_before != NA_VALUE)
    ).sum()

    # =========================
    # ENRICH + AUDIT SKILL CATEGORY (MULTI × MULTI)
    # =========================

    if "skill_category" not in df_raw.columns:
        df_raw["skill_category"] = NA_VALUE

    category_right = 0
    category_wrong = 0
    category_enriched = 0
    category_unmatched = 0

    category_out = []

    for _, row in df_raw.iterrows():
        skill_str = row["skill_name"]
        before_cat_str = row["skill_category"]

        # ---------- PARSE SKILLS ----------
        if skill_str != NA_VALUE:
            skills = {
                s.strip()
                for s in re.split(SKILL_SPLIT_PATTERN, skill_str)
                if s.strip()
            }
        else:
            skills = set()

        # ---------- EXPECTED CATEGORIES (FROM MAPPING) ----------
        expected_cats = {
            SKILL_CATEGORY_LOOKUP[sk]
            for sk in skills
            if sk in SKILL_CATEGORY_LOOKUP
        }

        # ---------- PARSE CATEGORY BEFORE ----------
        if before_cat_str != NA_VALUE:
            before_cats = {
                c.strip()
                for c in re.split(SKILL_SPLIT_PATTERN, before_cat_str)
                if c.strip()
            }
        else:
            before_cats = set()

        # ---------- AUDIT RIGHT / WRONG (BEFORE) ----------
        if before_cats:
            if before_cats.issubset(expected_cats):
                category_right += 1
            else:
                category_wrong += 1

        # ---------- ENRICH ----------
        if expected_cats:
            after_cats = expected_cats

            # enriched = wrong but fixed
            if before_cats != after_cats:
                category_enriched += 1

            category_out.append(" | ".join(sorted(after_cats)))
        else:
            # cannot map any category from skill_name
            category_out.append(NA_VALUE)

            if before_cats:
                category_unmatched += 1

    df_raw["skill_category"] = category_out

    # =========================
    # ENRICH
    # =========================

    df = df_raw.copy()

    enriched_count = 0
    unmatched_rows = []

    for idx, row in df.iterrows():
        skill_raw = row["skill_name"]
        role_raw = normalize_text(row.get("role_name", ""))
        desc_raw = normalize_text(row.get("job_description", ""))

        if skill_raw == NA_VALUE:
            continue

        skills = [s.strip() for s in re.split(SKILL_SPLIT_PATTERN, skill_raw)]

        # 1. ROLE-BASED
        role_levels = match_level(role_raw, "role")
        if len(role_levels) == 1:
            df.at[idx, "skill_level_required"] = " | ".join(
                f"{s} ({role_levels[0]})" for s in skills
            )
            enriched_count += 1
            continue

        # 2. DESCRIPTION-BASED
        desc_levels = match_level(desc_raw, "desc")
        if len(desc_levels) == 1:
            df.at[idx, "skill_level_required"] = " | ".join(
                f"{s} ({desc_levels[0]})" for s in skills
            )
            enriched_count += 1
            continue

        # 3. LOCAL CONTEXT
        enriched_skills = []
        for skill in skills:
            context = extract_context(desc_raw, normalize_text(skill), CONTEXT_WINDOW)
            ctx_levels = match_level(context, "desc")

            if ctx_levels:
                enriched_skills.append(f"{skill} ({ctx_levels[0]})")

        # ⛔ KHÔNG skill nào match → giữ nguyên __NA__
        if not enriched_skills:
            df.at[idx, "skill_level_required"] = NA_VALUE
            continue

        # ✅ Có ít nhất 1 skill có level
        df.at[idx, "skill_level_required"] = " | ".join(enriched_skills)
        enriched_count += 1

    # =========================
    # POST AUDIT
    # =========================

    level_after = df["skill_level_required"]

    level_after_hit = level_after.str.contains(
        "|".join(LEVEL_ENUM), regex=True, na=False
    )

    level_enriched = (
        valid_skill_mask &
        (level_before == NA_VALUE) &
        level_after_hit
    ).sum()

    # =========================
    # FINAL UNMATCHED MASK
    # =========================

    level_after = df["skill_level_required"]

    unmatched_mask = (
        valid_skill_mask &
        (
            (level_after == NA_VALUE) |
            ~level_after.str.contains(
                "|".join(LEVEL_ENUM),
                regex=True,
                na=False
            )
        )
    )

    level_unmatched = unmatched_mask.sum()

    # =========================
    # UNMATCHED EXPORT
    # =========================

    unmatched_df = df.loc[
        unmatched_mask,
        ["skill_name", "role_name", "job_description", "skill_level_required"]
    ].copy()

    if not unmatched_df.empty:
        unmatched_df.insert(0, "__source_name", file_path.name)

    # =========================
    # SAVE
    # =========================

    output_path = OUTPUT_DIR / file_path.name
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(
        f"  ✓ Saved: {output_path.name}\n"
        f"    - Total rows                    : {total_rows}\n"
        f"\n"
        f"    - Skill name empty (__NA__)          : {skill_empty}\n"
        f"\n"
        f"    - Skill category right          : {category_right}\n"
        f"    - Skill category wrong          : {category_wrong}\n"
        f"    - Skill category enriched       : {category_enriched}\n"
        f"    - Skill category unmatched !    : {category_unmatched}\n"
        f"\n"
        f"    - Skill level right            : {level_right}\n"
        f"    - Skill level wrong            : {level_wrong}\n"
        f"    - Skill level enriched         : {level_enriched}\n"
        f"    - Skill level unmatched !      : {level_unmatched}"
    )
    return unmatched_df

# =========================
# RUN
# =========================

def run_enrich():
    files = list(INPUT_DIR.iterdir())
    all_unmatched = []
    unmatched_path = UNMATCHED_DIR / f"{UNMATCHED_BASENAME}.csv"

    if unmatched_path.exists():
        unmatched_path.unlink()

    for f in files:
        unmatched = enrich_file(f)
        if unmatched is not None and not unmatched.empty:
            all_unmatched.append(unmatched)

    if all_unmatched:
        pd.concat(all_unmatched, ignore_index=True).drop_duplicates().to_csv(
            unmatched_path, index=False, encoding="utf-8-sig"
        )
        print(f"→ Unmatched saved: {unmatched_path}")

    print("\n=== STEP 2.5 COMPLETED ===")

if __name__ == "__main__":
    run_enrich()