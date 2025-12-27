# -*- coding: utf-8 -*-
"""
STEP 2.0 – COLUMN MAPPING TO ERD SCHEMA (INTERACTIVE TOOL)

This Streamlit application is used to map extracted raw datasets
to the official ERD / ingestion schema of the project.

Purpose:
- Convert heterogeneous source CSV files into a unified ingestion format
- Ensure all datasets conform to the same ERD schema before enrichment
- Reduce manual preprocessing effort via rule-based + similarity-based suggestions

Core functionalities:
1. Load extracted datasets from:
   - data/data_processing/s1_data_extracted/

2. Display ERD schema and helping schema for reference:
   - ERD_schema.json        (official ingestion fields)
   - ERD_helping.json       (helper fields for auto-derivation)

3. Automatically suggest mappings using:
   - Rule-based alias matching
   - Text similarity heuristics
   - Whitelisted semantic rules (salary, location, date, etc.)

4. Allow manual mapping, dropping, undoing, and resetting of columns
5. Validate mapping completeness and detect duplicate ERD assignments
6. Export mapped dataset with:
   - All ERD fields present
   - Missing fields filled with "__NA__"
   - Derived fields auto-generated where applicable

Input:
- data/data_processing/s1_data_extracted/*.csv

Output:
- data/data_processing/s2.1_data_mapped/mapped_*.csv
  (UTF-8-SIG encoded for Excel compatibility)
- data/metadata/mapping/*_mapping_report.json

Pipeline context:
- This step belongs to STEP 2 – DATA PROCESSING
- It must be completed before any enrichment steps (STEP 2.2+)
- Output files from this step are the ONLY valid inputs for geo, role,
  and other reference-based enrichment processes

Notes:
- This tool does NOT perform geo enrichment or normalization
- City, country, role name, and other standardizations are handled
  in later processing steps
"""

import streamlit as st
import pandas as pd
import json
from pathlib import Path
from difflib import SequenceMatcher

@st.cache_data(show_spinner=False)
def load_csv_preview(path):
    return pd.read_csv(path, nrows=50)

@st.cache_data(show_spinner=False)
def load_csv_full(path):
    return pd.read_csv(path)

# ======================================================
# PATH CONFIG
# ======================================================
ROOT = Path(__file__).resolve().parents[2]

SCHEMA_PATH = ROOT / "pipeline" / "tools" / "ERD_schema.json" # REAL SCHEMA
HELPING_SCHEMA_PATH = ROOT / "pipeline" / "tools" / "ERD_helping.json" # HELPING SCHEMA

INPUT_DIR = ROOT / "data" / "data_processing" / "s1_data_extracted"
OUTPUT_DIR = ROOT / "data" / "data_processing" / "s2.1_data_mapped"
MAPPING_REPORT_DIR = ROOT / "data" / "metadata" / "mapping"
NA_VALUE = "__NA__"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ======================================================
# LOAD ERD SCHEMA
# ======================================================

# Open Ingestion (Real) Schema
with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
    INGESTION_SCHEMA = json.load(f)

ERD_COLUMNS = list(INGESTION_SCHEMA.keys())
ERD_OPTIONS = ["— Select —"] + ERD_COLUMNS

# Open Helping Schema
with open(HELPING_SCHEMA_PATH, "r", encoding="utf-8") as f:
    HELPING_SCHEMA = json.load(f)

HELPING_COLUMNS = [
    k for k in HELPING_SCHEMA.keys()
    if not k.startswith("___")
]
ERD_OPTIONS = ["— Select —"] + ERD_COLUMNS + HELPING_COLUMNS


# ======================================================
# SESSION STATE
# ======================================================
st.session_state.setdefault("dropped_cols", set())
st.session_state.setdefault("drop_history", [])
st.session_state.setdefault("is_exporting", False)


# ======================================================
# RULE BASED MAP
# ======================================================
RULE_BASED_MAP = {
    # Sources_id
    "__source_id": ["id", "job_id", "jobid", "posting_id","dataset_id","index"],

    # Skills table
    "skill_name": ["skill","skills","skillset","job_skills"],
    "skill_category": ["skill_category","skill_type","skill group","job_type_skills"],
    #"certification_required": ["certification_required","certificate_required","cert_required","requires_certification"],

    # Companies table
    "company_name": ["company","company name","employer","organization","org_name"],
    "company_size": ["company_size","company size","size","org_size","organization_size"],
    "industry": ["industry","company_industry","sector","business_domain"],

    # Locations table
    "city": ["city","town","municipality"],
    "country": ["country","country_code","company_location","nation"],
    "country_iso": ["country_iso", "country_code", "iso", "iso_code", "country_iso2", "country_source"],
    "latitude": ["latitude","lat"],
    "longitude": ["longitude","lng","lon"],
    "population": ["population","city_population"],

    # Role_Names table
    "role_name": ["role","title","job_title","job title","position","job_title_short"],
    "level": ["level","job_level","role_level","seniority_level"],
    "employment_type": ["employment_type","employment type","contract_time","job_type","work_type","job_schedule_type"],

    # Job_Skills table
    "skill_level_required": ["skill_level","skill_proficiency","required_skill_level"],

    # Job_Postings table
    "posted_date": ["posted_date","created_at","date_posted","date_created","published_date","date","publish_date","work_year","year"],
    "min_salary": ["salary_min","min_salary","salary_from"],
    "max_salary": ["salary_max","max_salary","salary_to"],
    "currency": ["currency","currency_code","salary_currency","pay_currency"],
    "required_exp_years": ["experience_level","exp_level","seniority","years_experience","experience_years"],
    "education_level": ["education_level","education","degree","degree_level"],
    "job_description": ["description","job description","job_desc","details","notes"],
    "remote_option": ["remote_option","remote","work_from_home","wfh", "remote_ratio"],

    # Split Help
    "salary_min_max": ["salary","salary_estimate","salary estimate","salary_range","pay","compensation","salary_in_usd"],
    "location_city_country": ["location","job_location","company_location","city_country","location_text"],

    # Remote / Work from home flag
    "work_from_home_flag": ["job_work_from_home", "work_from_home", "remote_flag", "is_remote", "remote_allowed"],
}

AUTO_ERD_FIELDS = {
    "__source_name",
    "__source_id"
}

AUTO_DROP_FIELDS = {
    "unnamed", "unnamed: 0",
    "url", "urls", "apply_url", "redirect_url",
    "tags", "tag", "category_tag", "keywords",
    "rating", "num_resources", "num_raw_files",
    "employee_residence", "raw_urls", "portal_url",
    "region", "salary_predicted", "search_keyword", 
    "category", "job_category", "search_location",
    "founded", "headquarters", "type of ownership",
    "Revenue", "competitors", "Easy Apply", "job_no_degree_mention",
    "contract_type", "job_via", "job_no_degree_mention"
    "last_update", "job_health_insurance",
    "salary_year_avg", "salary_hour_avg"
}

SIMILARITY_WHITELIST = {
    "title": ["role_name"],
    "salary": ["salary_min", "salary_max", "salary_min_max"],
    "location": ["city", "country"],
    "date": ["posted_date", "expired_date"],
}

REMOTE_KEYWORDS = {
    "remote",
    "globally remote",
    "work from home",
    "wfh",
    "anywhere",
    "distributed",
    "hybrid"
}

# ======================================================
# HELPER FUNCTION
# ======================================================
import re

def parse_salary_min_max(text):
    """
    Parse salary text like:
    "$111K-$181K (Glassdoor est.)"
    "111K – 181K"
    """
    if not isinstance(text, str):
        return None, None

    text = text.replace(",", "").upper()

    # tìm tất cả số + K hoặc M
    matches = re.findall(r'(\d+(?:\.\d+)?)(K|M)?', text)

    if len(matches) < 2:
        return None, None

    def to_number(value, unit):
        value = float(value)
        if unit == "K":
            return int(value * 1_000)
        if unit == "M":
            return int(value * 1_000_000)
        return int(value)

    min_val = to_number(*matches[0])
    max_val = to_number(*matches[1])

    return min_val, max_val

def normalize_col(text: str) -> str:
    """
    Normalize column name for matching:
    - lowercase
    - replace _, -, camelCase → space
    - collapse spaces
    """
    if not isinstance(text, str):
        return ""

    text = text.strip()

    # camelCase → camel case
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)

    # replace separators with space
    text = re.sub(r'[_\-]+', ' ', text)

    # normalize spaces
    text = re.sub(r'\s+', ' ', text)

    return text.lower().strip()

AUTO_DROP_NORMALIZED = {
    normalize_col(x) for x in AUTO_DROP_FIELDS
}

# ======================================================
# SUGGEST FUNCTION
# ======================================================
def suggest_erd_column(raw_col, threshold=0.75):
    raw = normalize_col(raw_col)
    
    # AUTO DROP
    if raw in AUTO_DROP_NORMALIZED or raw.startswith("unnamed"):
        return "__DROP__"

    # 1️⃣ RULE BASED (HELPING + ERD)
    for erd_col, aliases in RULE_BASED_MAP.items():
        for alias in aliases:
            if raw == normalize_col(alias):
                return erd_col

    # 2️⃣ SIMILARITY (CHỈ CHO ERD THẬT)
    for keyword, allowed_erds in SIMILARITY_WHITELIST.items():
        if keyword in raw:
            best, score = None, 0
            for erd in allowed_erds:
                s = SequenceMatcher(None, raw, erd.replace("_", " ")).ratio()
                if s > score:
                    best, score = erd, s
            return best if score >= threshold else None

    return None

# ======================================================
# PAGE CONFIG
# ======================================================
st.set_page_config(layout="wide")
with st.container(horizontal=True, horizontal_alignment="left"):
    col1, col2 = st.columns([0.5, 1.2], vertical_alignment="bottom")
    with col1:
        st.markdown("### 🧩 CSV Column Mapping Tool")
    with col2:
        st.caption("Map extracted CSV columns to ERD / ingestion schema")

# ======================================================
# ERD REFERENCE
# ======================================================
st.markdown("#### ERD / Ingestion Schema Reference")

st.info(
    """
📘 **Lưu ý về ERD & Auto Conversion**

- Chỉ các field trong **ERD / Ingestion Schema** mới là schema chính thức.
- Một số field **không thuộc ERD** nhưng được hỗ trợ để **tự động chuyển đổi**, ví dụ:
  - `salary_min_max` → `min_salary`, `max_salary`
  - `location_city_country` → `city`, `country`, `remote_option`
- Các field auto-convert này **không xuất hiện trong ERD**, nhưng sẽ được xử lý khi export.
"""
)

# ======================================================
# DERIVED TARGET FIELDS
# ======================================================
DERIVED_TARGETS = set()
for meta in INGESTION_SCHEMA.values():
    if meta.get("derive_to"):
        DERIVED_TARGETS.update(meta["derive_to"])

erd_rows = []
for erd_col, meta in INGESTION_SCHEMA.items():

    # 🚫 KHÔNG xử lý derived TARGET ở đây
    if erd_col in DERIVED_TARGETS:
        continue

    info = (
        "[enum] " + " | ".join(meta["enum"])
        if "enum" in meta
        else meta.get("description", "")
    )
    erd_rows.append({
        "column": erd_col,
        "table": meta.get("table", ""),
        "type": meta.get("type", ""),
        "enum / description": info
    })

df_erd = pd.DataFrame(erd_rows)
df_erd.index = range(1, len(df_erd) + 1)

st.dataframe(df_erd, width='stretch', height=260)

# ======================================================
# MAP CSV HEADER – ROW 1 (TITLE + ACTIONS)
# ======================================================
r1_left, r1_right = st.columns([3, 1], vertical_alignment="bottom")

with r1_left:
    st.markdown("#### Map CSV from data_processing/data_extracted")

with r1_right:
    b1, b2 = st.columns(2)
    with b1:
        if st.button("↩️ Undo", width="stretch"):
            if st.session_state.drop_history:
                last = st.session_state.drop_history.pop()
                st.session_state.dropped_cols.discard(last)
                st.rerun()

    with b2:
        if st.button("🔄 Reset", width="stretch"):
            st.session_state.dropped_cols.clear()
            st.session_state.drop_history.clear()
            st.rerun()

# ======================================================
# MAP CSV CONTROLS – ROW 2 (5 COLUMNS)
# ======================================================
c1, c2, c3, c4, c5 = st.columns(
    [2.2, 1.5, 1.3, 1.5, 1.3],
    vertical_alignment="bottom"
)

# ---- Column 1: Select CSV
with c1:
    csv_files = sorted(INPUT_DIR.glob("*.csv"))
    if not csv_files:
        st.error("No CSV files found")
        st.stop()

    selected_file = st.selectbox(
        "Select CSV",
        csv_files,
        format_func=lambda p: p.name
    )

    # 🔒 RESET CHECK STATUS WHEN CHANGE FILE
    if st.session_state.get("_last_selected_file") != selected_file:
        st.session_state.check_ok = False
        st.session_state["_last_selected_file"] = selected_file


df_preview = load_csv_preview(selected_file)
df_full = load_csv_full(selected_file)


# ======================================================
# CALCULATE PROGRESS (DÙNG CHUNG)
# ======================================================
all_cols = list(df_preview.columns)
dropped_cols = st.session_state.dropped_cols

# chỉ các cột còn active (chưa drop)
active_cols = [c for c in all_cols if c not in dropped_cols]

total_source_cols = len(active_cols)
done_cols = 0
mapped_erd = []

for col in active_cols:
    key = f"map_{col}"

    # đã map thủ công
    if key in st.session_state and st.session_state[key] != "— Select —":
        done_cols += 1
        mapped_erd.append(st.session_state[key])
        continue

    # auto suggest coi như đã xử lý
    suggested = suggest_erd_column(col)
    if suggested:
        done_cols += 1
        mapped_erd.append(suggested)

processing_ratio = done_cols / total_source_cols if total_source_cols else 0

mapped_unique = done_cols        # số cột đã xử lý
total_erd = len(ERD_COLUMNS)
erd_ratio = mapped_unique / total_erd if total_erd else 0


# ---- Column 2: Progress bar 1 (Source)
with c2:
    st.markdown("**Source Progress**")
    st.progress(processing_ratio)

# ---- Column 3: Status 1
with c3:
    st.markdown("&nbsp;")  # align
    st.caption(f"⏳ {done_cols}/{total_source_cols} source columns")

# ---- Column 4: Progress bar 2 (ERD)
with c4:
    st.markdown("**ERD Coverage**")
    st.progress(erd_ratio)

# ---- Column 5: Status 2
with c5:
    st.markdown("&nbsp;")
    st.caption(f"📘 {mapped_unique}/{total_erd} ERD columns")

st.markdown("""<hr style="margin: 5px 0; border: none; border-top: 2px solid #333; opacity: 0.3;">""", unsafe_allow_html=True)
# ======================================================
# HEADER ROW
# ======================================================
h1, h2, h3, h4, h5 = st.columns([2.5, 6, 2.5, 4, 1])
h1.markdown("**Column**")
h2.markdown("**Sample Data**")
h3.markdown("**Suggested ERD**")
h4.markdown("**Map to ERD**")
h5.markdown("**Drop**")

# ======================================================
# COLUMN MAPPING UI (SCROLLABLE)
# ======================================================
with st.container(height=520):

    for col in df_preview.columns:
        if col in st.session_state.dropped_cols:
            continue

        samples = (
            df_preview[col]
            .dropna()
            .astype(str)
            .unique()[:5]
        )

        suggested = suggest_erd_column(col)

        c1, c2, c3, c4, c5 = st.columns(
            [2.5, 6, 2.5, 4, 1],
            vertical_alignment="bottom"
        )

        c1.write(f"**{col}**")
        sample_text = " | ".join(samples)

        c2.markdown(
            f"""
            <div style="
                max-height: 200px;
                overflow-y: auto;
                white-space: pre-wrap;
                font-size: 0.85rem;
                border: 1px solid #eee;
                padding: 6px;
                border-radius: 4px;
                background-color: #fafafa;
            ">
                {sample_text}
            </div>
            """,
            unsafe_allow_html=True
        )

        if suggested == "required_exp_years":
            c3.text("required_exp_years", help="(auto convert) · EN = 0 year · MI = 2 years · SE = 5 years · EX = 8 years")
        elif suggested == "salary_min_max":
            c3.text("salary_min_max", help="(auto convert) · split to min & max")
        elif suggested == "location_city_country":
            c3.text(
                "location_city_country",
                help="(auto convert) · split to city & country (& remote) using geo reference"
            )
        else:
            c3.write(suggested if suggested else "—")

        default_index = (
            ERD_OPTIONS.index(suggested)
            if suggested in ERD_OPTIONS else 0
        )

        key = f"map_{col}"

        # ✅ CHỈ SET DEFAULT 1 LẦN DUY NHẤT
        if key not in st.session_state:
            st.session_state[key] = suggested if suggested else "— Select —"


        with c4:
            st.selectbox(
                "Map",
                ERD_OPTIONS,
                key=f"map_{col}",
                label_visibility="collapsed"
            )

        with c5:
            if st.button("🗑️", key=f"drop_{col}"):
                st.session_state.dropped_cols.add(col)
                st.session_state.drop_history.append(col)
                st.rerun()

# ======================================================
# DIALOGS
# ======================================================
@st.dialog("❌ Mapping incomplete")
def dialog_mapping_incomplete(done_cols, total_cols):
    st.error(f"Source columns chưa xử lý xong: {done_cols}/{total_cols}")
    st.caption("Hãy map hoặc drop hết các cột còn lại trước khi export.")

@st.dialog("✅ Mapping OK")
def dialog_mapping_ok(mapped_erd_count, total_erd, missing_erd):
    st.success("Mapping đã hoàn tất.")
    st.write(f"ERD coverage: {mapped_erd_count}/{total_erd}")

    if missing_erd:
        st.markdown("**ERD fields chưa có dữ liệu (sẽ fill `__NA__`):**")
        for col in missing_erd:
            st.markdown(f"- {col}")

    st.caption("Bạn có thể export mapped CSV.")

@st.dialog("💾 Export completed", width="large")
def dialog_export_done(csv_path, report_path):
    st.success("Export thành công 🎉")
    st.markdown("**Mapped CSV:**")
    st.code(str(csv_path))
    st.markdown("**Mapping report:**")
    st.code(str(report_path))

@st.dialog("⚠️ Duplicate ERD mapping detected")
def dialog_duplicate_erd(dup_map):
    st.error("Một ERD field đang được map từ nhiều source columns.")
    st.markdown("**Chi tiết:**")

    for erd, sources in dup_map.items():
        st.markdown(f"- **{erd}** ← {', '.join(sources)}")

    st.caption("Hãy giữ lại 1 cột hoặc drop các cột dư trước khi export.")

@st.dialog("⏳ Processing", width="small")
def dialog_loading():
    st.markdown("### Please wait")
    st.write("System is processing and exporting data…")

@st.dialog("⏳ Processing export", width="small")
def dialog_loading():
    st.markdown("### Please wait")
    st.write("System is processing and exporting data…")

# ======================================================
# FINAL ACTIONS (ADD ONLY – DO NOT TOUCH EXISTING UI)
# ======================================================
if "check_ok" not in st.session_state:
    st.session_state.check_ok = False

left_spacer, spinner_col, btn_export, btn_check = st.columns(
    [5.5, 1.2, 1.6, 1.7],
    vertical_alignment="center"
)

spinner_placeholder = spinner_col.empty()

# CHECK BUTTON
with btn_check:
    if st.button("✅ Check", width="stretch"):

        if done_cols != total_source_cols:
            st.session_state.check_ok = False
            dialog_mapping_incomplete(done_cols, total_source_cols)

        else:
            # ===== BUILD ERD → SOURCE MAP (CHECK DUPLICATE) =====
            erd_to_sources = {}

            for col in active_cols:
                key = f"map_{col}"

                if key in st.session_state:
                    erd = st.session_state[key]
                    if erd and erd not in ("— Select —", "__DROP__"):
                        erd_to_sources.setdefault(erd, []).append(col)

                else:
                    suggested = suggest_erd_column(col)
                    if suggested:
                        erd_to_sources.setdefault(suggested, []).append(col)

            # ===== FIND DUPLICATES =====
            duplicate_erd = {
                erd: cols
                for erd, cols in erd_to_sources.items()
                if len(cols) > 1
            }

            if duplicate_erd:
                st.warning("⚠️ Multiple source columns are mapped to the same ERD field.")
                st.markdown("**The following ERD fields will be merged:**")

                for erd, cols in duplicate_erd.items():
                    st.markdown(f"- **{erd}** ← {', '.join(cols)}")

                confirm_merge = st.checkbox(
                    "✅ I understand and confirm merging these columns"
                )

                if not confirm_merge:
                    st.session_state.check_ok = False
                    st.stop()


            # ===== NO DUPLICATE → PASS CHECK =====
            st.session_state.check_ok = True

            # ===== TÍNH ERD COVERAGE (GIỮ NGUYÊN) =====
            mapped_erd_cols = set(erd_to_sources.keys())
            missing_erd = [c for c in ERD_COLUMNS if c not in mapped_erd_cols and c not in AUTO_ERD_FIELDS]

            dialog_mapping_ok(
                mapped_erd_count=len(mapped_erd_cols),
                total_erd=len(ERD_COLUMNS),
                missing_erd=missing_erd
            )

# ======================================================
# EXPORT BUTTON (CLEAN – FIXED INDENT – NO MAGIC)
# ======================================================
with btn_export:
    export_clicked = st.button(
        "💾 Export mapped CSV",
        width="stretch",
        disabled=not st.session_state.check_ok
    )

if export_clicked:
    # spinner cùng hàng nút
    spinner_placeholder.markdown("⏳ Processing & exporting…")

    df_out = pd.DataFrame()

    # ----------------------------------
    # 1. BUILD SOURCE → ERD MAP
    # ----------------------------------
    erd_to_source = {}

    for col in df_full.columns:
        if col in st.session_state.dropped_cols:
            continue

        erd_col = st.session_state.get(f"map_{col}")

        if not erd_col or erd_col in ("— Select —", "__DROP__"):
            continue

        erd_to_source.setdefault(erd_col, []).append(col)


    # ----------------------------------
    # 2. DERIVED FROM HELPING SCHEMA
    # ----------------------------------
    cities_ref = pd.read_csv(
        ROOT / "data" / "data_reference" / "cities.csv",
        dtype=str
    )

    countries_ref = pd.read_csv(
        ROOT / "data" / "data_reference" / "countries.csv",
        dtype=str
    )

    cities_lookup = {
        c.lower().strip(): c
        for c in cities_ref["city_name"].dropna().unique()
    }

    countries_lookup = {
        c.lower().strip(): c
        for c in countries_ref["country_name"].dropna().unique()
    }

    for helping_col, meta in HELPING_SCHEMA.items():
        if not isinstance(meta, dict) or not meta.get("derive_to"):
            continue

        source_cols = erd_to_source.get(helping_col)
        if not source_cols:
            continue

        # helping field chỉ cho 1 source column
        source_col = source_cols[0]
        raw_series = df_full[source_col]


        raw_series = df_full[source_col]

        if helping_col == "salary_min_max":
            min_vals, max_vals = [], []

            for v in raw_series:
                min_v, max_v = parse_salary_min_max(v)
                min_vals.append(min_v if min_v is not None else NA_VALUE)
                max_vals.append(max_v if max_v is not None else NA_VALUE)

            df_out["min_salary"] = min_vals
            df_out["max_salary"] = max_vals

        elif helping_col == "location_city_country":
            city_vals, country_vals, remote_vals = [], [], []

            for v in raw_series:
                if not isinstance(v, str):
                    city_vals.append(NA_VALUE)
                    country_vals.append(NA_VALUE)
                    remote_vals.append(NA_VALUE)
                    continue

                raw = v.lower().strip()

                is_remote = any(k in raw for k in REMOTE_KEYWORDS)
                remote_vals.append("true" if is_remote else "false")

                parts = [
                    p.strip()
                    for p in re.split(r"[,\-|]", raw)
                    if p.strip()
                ]

                found_city = NA_VALUE
                found_country = NA_VALUE

                for p in parts:
                    if p in cities_lookup and found_city == NA_VALUE:
                        found_city = cities_lookup[p]
                    if p in countries_lookup and found_country == NA_VALUE:
                        found_country = countries_lookup[p]

                city_vals.append(found_city)
                country_vals.append(found_country)

            df_out["city"] = city_vals
            df_out["country"] = country_vals
            df_out["remote_option"] = remote_vals

        elif helping_col == "work_from_home_flag":
            flag_vals = []

            for v in raw_series:
                if pd.isna(v):
                    flag_vals.append(NA_VALUE)
                    continue

                val = str(v).strip().lower()

                if val in ("true", "1", "yes", "y"):
                    flag_vals.append("Remote")
                elif val in ("false", "0", "no", "n"):
                    flag_vals.append("Onsite")
                else:
                    flag_vals.append(NA_VALUE)

            # ⬇️ CHỈ ĐIỀN KHI remote_option CHƯA CÓ
            if "remote_option" in df_out.columns:
                df_out["remote_option"] = [
                    r if r != NA_VALUE else f
                    for r, f in zip(df_out["remote_option"], flag_vals)
                ]
            else:
                df_out["remote_option"] = flag_vals

    # ----------------------------------
    # 3. REAL ERD FIELDS
    # ----------------------------------
    for erd_col in INGESTION_SCHEMA.keys():
        if erd_col in df_out.columns:
            continue

        source_cols = erd_to_source.get(erd_col)

        if not source_cols:
            df_out[erd_col] = NA_VALUE
            continue

        if len(source_cols) == 1:
            df_out[erd_col] = df_full[source_cols[0]]
        else:
            merged = []

            for _, row in df_full[source_cols].iterrows():
                parts = []

                for c in source_cols:
                    v = row[c]

                    if pd.isna(v):
                        continue

                    v = str(v).strip()

                    if v == NA_VALUE or v == "":
                        continue

                    parts.append(v)

                if parts:
                    merged.append(" | ".join(parts))
                else:
                    merged.append(NA_VALUE)

            df_out[erd_col] = merged


     # ----------------------------------
    # 3.1 SOURCE METADATA (AUTO)
    # ----------------------------------
    df_out["__source_name"] = selected_file.name

    # Nếu source KHÔNG map được __source_id → tạo ID giả
    if "__source_id" not in df_out.columns or (df_out["__source_id"] == NA_VALUE).all():
        df_out["__source_id"] = [
            f"{i+1:03d}"
            for i in range(len(df_out))
        ]

    # ----------------------------------
    # 3.2 FORCE ERD COLUMN ORDER (BẮT BUỘC)
    # ----------------------------------
    df_out = df_out.fillna(NA_VALUE)
    df_out = df_out[list(INGESTION_SCHEMA.keys())]

    # ----------------------------------
    # 4. SAVE FILE
    # ----------------------------------
    output_path = OUTPUT_DIR / f"mapped_{selected_file.stem}.csv"
    df_out.to_csv(output_path, index=False, encoding="utf-8-sig")

    # ----------------------------------
    # 5. SAVE MAPPING REPORT
    # ----------------------------------
    MAPPING_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    missing_erd = [
        c for c in INGESTION_SCHEMA.keys()
        if (df_out[c] == NA_VALUE).all()
    ]

    report_path = (
        MAPPING_REPORT_DIR
        / f"{selected_file.stem}_mapping_report.json"
    )

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump({
            "source_file": selected_file.name,
            "active_source_columns": len(active_cols),
            "erd_total": len(INGESTION_SCHEMA),
            "erd_missing": missing_erd
        }, f, ensure_ascii=False, indent=2)

    spinner_placeholder.empty()
    dialog_export_done(output_path, report_path)