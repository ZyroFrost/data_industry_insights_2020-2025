import streamlit as st
import pandas as pd
import json
from pathlib import Path
from difflib import SequenceMatcher

# ======================================================
# PATH CONFIG
# ======================================================
ROOT = Path(__file__).resolve().parents[2]

SCHEMA_PATH = ROOT / "pipeline" / "tools" / "ERD_schema.json"
INPUT_DIR = ROOT / "data" / "data_processing" / "data_extracted"
OUTPUT_DIR = ROOT / "data" / "data_processing" / "data_mapped"
MAPPING_REPORT_DIR = ROOT / "data" / "metadata" / "mapping"
NA_VALUE = "__NA__"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ======================================================
# LOAD ERD SCHEMA
# ======================================================
with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
    INGESTION_SCHEMA = json.load(f)

ERD_COLUMNS = list(INGESTION_SCHEMA.keys())
ERD_OPTIONS = ["— Select —"] + ERD_COLUMNS

# ======================================================
# SESSION STATE
# ======================================================
st.session_state.setdefault("dropped_cols", set())
st.session_state.setdefault("drop_history", [])

# ======================================================
# SUGGEST FUNCTION
# ======================================================
def suggest_erd_column(raw_col, erd_columns, threshold=0.65):
    raw = raw_col.lower().replace("_", " ").replace("-", " ")
    best_match, best_score = None, 0
    for erd in erd_columns:
        score = SequenceMatcher(None, raw, erd.replace("_", " ").lower()).ratio()
        if score > best_score:
            best_match, best_score = erd, score
    return best_match if best_score >= threshold else None

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

erd_rows = []
for col, meta in INGESTION_SCHEMA.items():
    info = (
        "[enum] " + " | ".join(meta["enum"])
        if "enum" in meta
        else meta.get("description", "")
    )
    erd_rows.append({
        "column": col,
        "table": meta.get("table", ""),
        "type": meta.get("type", ""),
        "enum / description": info
    })

df_erd = pd.DataFrame(erd_rows)
df_erd.index = range(1, len(df_erd) + 1)

st.dataframe(df_erd, use_container_width=True, height=260)


# ======================================================
# MAP CSV HEADER – ROW 1 (TITLE + ACTIONS)
# ======================================================
r1_left, r1_right = st.columns([3, 1], vertical_alignment="bottom")

with r1_left:
    st.markdown("#### Map CSV from data_processing/data_extracted")

with r1_right:
    b1, b2 = st.columns(2)
    with b1:
        if st.button("↩️ Undo", use_container_width=True):
            if st.session_state.drop_history:
                last = st.session_state.drop_history.pop()
                st.session_state.dropped_cols.discard(last)
                st.rerun()

    with b2:
        if st.button("🔄 Reset", use_container_width=True):
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

df_preview = pd.read_csv(selected_file, nrows=50)
df_full = pd.read_csv(selected_file)

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
    suggested = suggest_erd_column(col, ERD_COLUMNS)
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
h1, h2, h3, h4, h5 = st.columns([3, 5, 3, 4, 1])
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

        suggested = suggest_erd_column(col, ERD_COLUMNS)

        c1, c2, c3, c4, c5 = st.columns(
            [3, 5, 3, 4, 1],
            vertical_alignment="bottom"
        )

        c1.write(f"**{col}**")
        c2.write(" | ".join(samples))
        c3.write(suggested if suggested else "—")

        default_index = (
            ERD_OPTIONS.index(suggested)
            if suggested in ERD_OPTIONS else 0
        )

        with c4:
            st.selectbox(
                "Map",
                ERD_OPTIONS,
                index=default_index,
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

@st.dialog("💾 Export completed")
def dialog_export_done(csv_path, report_path):
    st.success("Export thành công 🎉")
    st.markdown("**Mapped CSV:**")
    st.code(str(csv_path))
    st.markdown("**Mapping report:**")
    st.code(str(report_path))


# ======================================================
# FINAL ACTIONS (ADD ONLY – DO NOT TOUCH EXISTING UI)
# ======================================================
if "check_ok" not in st.session_state:
    st.session_state.check_ok = False

st.markdown("---")

left_spacer, btn_export, btn_check = st.columns([6, 1.5, 1.5])

# CHECK BUTTON
with btn_check:
    if st.button("✅ Check", use_container_width=True):

        if done_cols != total_source_cols:
            st.session_state.check_ok = False
            dialog_mapping_incomplete(done_cols, total_source_cols)

        else:
            st.session_state.check_ok = True

            # ===== TÍNH ERD COVERAGE =====
            mapped_erd_cols = set()

            for col in active_cols:
                key = f"map_{col}"

                if key in st.session_state and st.session_state[key] != "— Select —":
                    mapped_erd_cols.add(st.session_state[key])
                    continue

                suggested = suggest_erd_column(col, ERD_COLUMNS)
                if suggested:
                    mapped_erd_cols.add(suggested)

            missing_erd = [c for c in ERD_COLUMNS if c not in mapped_erd_cols]

            # ===== GỌI DIALOG (QUAN TRỌNG) =====
            dialog_mapping_ok(
                mapped_erd_count=mapped_unique,
                total_erd=len(ERD_COLUMNS),
                missing_erd=[c for c in ERD_COLUMNS if c not in set(mapped_erd)]
            )

# EXPORT BUTTON
with btn_export:
    if st.button(
        "💾 Export mapped CSV",
        use_container_width=True,
        disabled=not st.session_state.check_ok
    ):
        df_out = pd.DataFrame()

        erd_to_source = {}
        for col in df_full.columns:
            if col in st.session_state.dropped_cols:
                continue

            erd_col = st.session_state.get(f"map_{col}")
            if erd_col and erd_col != "— Select —":
                erd_to_source[erd_col] = col

        for erd_col in ERD_COLUMNS:
            if erd_col in erd_to_source:
                df_out[erd_col] = df_full[erd_to_source[erd_col]]
            else:
                df_out[erd_col] = "__NA__"

        output_path = OUTPUT_DIR / f"mapped_{selected_file.name}"
        df_out.to_csv(output_path, index=False)
        # ----------------------------------
        # SAVE MAPPING REPORT (EXPORT ONLY)
        # ----------------------------------
        erd_to_source = {}
        for col in df_full.columns:
            if col in st.session_state.dropped_cols:
                continue

            erd_col = st.session_state.get(f"map_{col}")
            if erd_col and erd_col != "— Select —":
                erd_to_source[erd_col] = col

        missing_erd = [c for c in ERD_COLUMNS if c not in erd_to_source]

        mapping_report = {
            "source_file": selected_file.name,
            "active_source_columns": total_source_cols,
            "erd_total": len(ERD_COLUMNS),
            "erd_missing": missing_erd,
            "missing_value_convention": f"{NA_VALUE} means source does not provide this field"
        }

        
        MAPPING_REPORT_DIR.mkdir(parents=True, exist_ok=True)
        report_path = MAPPING_REPORT_DIR / f"{selected_file.stem}_mapping_report.json"
        

        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(mapping_report, f, ensure_ascii=False, indent=2)

        # ----------------------------------
        # EXPORT DIALOG (SHOW BOTH PATHS)
        # ----------------------------------
        dialog_export_done(output_path, report_path)

