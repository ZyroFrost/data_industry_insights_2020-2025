import streamlit as st
import pandas as pd
import json
from pathlib import Path

# ===== CONFIG =====
ROOT = Path(__file__).resolve().parents[2]
AUDIT_FILE = ROOT / "data" / "processing" / "schema_audit.xlsx"
OUTPUT_MAPPING = ROOT / "pipeline" / "config" / "column_mapping.json"

CANONICAL_COLUMNS = [
    "role_name",
    "company_name",
    "location",
    "min_salary",
    "max_salary",
    "currency",
    "posted_date",
    "__IGNORE__"
]

st.set_page_config(layout="wide")
st.title("🧩 Column Mapping Tool (Mini)")

# ===== LOAD DATA =====
@st.cache_data
def load_schema():
    return pd.read_excel(AUDIT_FILE, sheet_name="pivot_schema")

df = load_schema()

st.markdown("### Schema Preview")
st.dataframe(df, use_container_width=True)

st.markdown("---")
st.markdown("### Define Column Mapping")

mapping = {}

for col in df["column_name"]:
    with st.expander(f"Column: `{col}`", expanded=False):
        st.write(df[df["column_name"] == col])
        choice = st.selectbox(
            "Map to canonical column:",
            CANONICAL_COLUMNS,
            index=CANONICAL_COLUMNS.index("__IGNORE__"),
            key=col
        )

        if choice != "__IGNORE__":
            mapping[col] = choice

# ===== SAVE =====
if st.button("💾 Save Mapping"):
    OUTPUT_MAPPING.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_MAPPING, "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2)

    st.success(f"Mapping saved to {OUTPUT_MAPPING}")