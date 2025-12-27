import streamlit as st
from pathlib import Path
import time

# =====================================================
# CONFIG
# =====================================================
st.set_page_config(
    page_title="Pipeline Controller",
    layout="wide"
)

# =====================================================
# PIPELINE DEFINITION (EXPLICIT, KHÔNG ĐOÁN)
# =====================================================
PIPELINE_STEPS = [
    {
        "key": "crawl",
        "label": "Crawl",
        "entry_file": "pipeline/crawlers/api/public/adzuna.py",
        "output_dirs": ["data/data_raw"]
    },
    {
        "key": "extract",
        "label": "Extract",
        "entry_file": "pipeline/processing/extract.py",
        "output_dirs": ["data/data_processing/data_extracted"]
    },
    {
        "key": "map",
        "label": "Map",
        "entry_file": "pipeline/tools/column_mapper_app.py",
        "output_dirs": ["data/data_processing/data_mapped"]
    },
    {
        "key": "enrich",
        "label": "Enrich",
        "entry_file": "pipeline/processing/enrich.py",
        "output_dirs": ["data/data_processing/data_enriched"]
    },
    {
        "key": "process",
        "label": "Process",
        "entry_file": "pipeline/main.py",
        "output_dirs": ["data/data_processed"]
    }
]

STEP_KEYS = [s["key"] for s in PIPELINE_STEPS]

# =====================================================
# SESSION STATE INIT
# =====================================================
if "current_step" not in st.session_state:
    st.session_state.current_step = None

if "completed_steps" not in st.session_state:
    st.session_state.completed_steps = set()

if "logs" not in st.session_state:
    st.session_state.logs = {k: [] for k in STEP_KEYS}

if "active_tab" not in st.session_state:
    st.session_state.active_tab = None

if "auto_mode" not in st.session_state:
    st.session_state.auto_mode = False


# =====================================================
# HELPER FUNCTIONS
# =====================================================
def log(step_key, message):
    st.session_state.logs[step_key].append(message)


def run_step(step):
    key = step["key"]

    st.session_state.current_step = key
    st.session_state.active_tab = key

    log(key, f"[{step['label'].upper()}] START")
    log(key, f"Running file: {step['entry_file']}")

    # ---- DEMO EXECUTION (thay bằng pipeline thật sau) ----
    time.sleep(0.5)
    log(key, "Processing...")
    time.sleep(0.5)
    log(key, "Done.")
    # -----------------------------------------------------

    st.session_state.completed_steps.add(key)
    log(key, f"[{step['label'].upper()}] COMPLETE")


def run_full_pipeline():
    st.session_state.auto_mode = True
    for step in PIPELINE_STEPS:
        run_step(step)
    st.session_state.current_step = None
    st.session_state.auto_mode = False


# =====================================================
# SIDEBAR – FOLDER TREE
# =====================================================
with st.sidebar:
    st.title("📁 Project Tree")

    def render_tree(path, highlight_dirs, level=0):
        if not path.exists():
            return

        for item in sorted(path.iterdir()):
            indent = " " * level
            normalized = str(item).replace("\\", "/")

            active = normalized in highlight_dirs
            style = "**" if active else ""

            st.markdown(f"{indent}{style}{item.name}{style}")

            if item.is_dir():
                render_tree(item, highlight_dirs, level + 1)

    active_dirs = []
    if st.session_state.current_step:
        for s in PIPELINE_STEPS:
            if s["key"] == st.session_state.current_step:
                active_dirs.extend(s["output_dirs"])

    render_tree(Path("data"), active_dirs)

# =====================================================
# MAIN LOG VIEWER (TOP – LỚN)
# =====================================================
st.title("🖥 Pipeline Execution Log")

if st.session_state.current_step:
    current_logs = st.session_state.logs[st.session_state.current_step]
    st.code("\n".join(current_logs), language="text")
else:
    st.code("Pipeline idle.", language="text")

st.divider()

# =====================================================
# PIPELINE PROGRESS (RIGHT AREA)
# =====================================================
progress_cols = st.columns(len(PIPELINE_STEPS))

for idx, step in enumerate(PIPELINE_STEPS):
    key = step["key"]

    if key in st.session_state.completed_steps:
        status = "✓"
    elif key == st.session_state.current_step:
        status = "●"
    else:
        status = "⏸"

    with progress_cols[idx]:
        st.markdown(
            f"**{step['label']}**<br>{status}",
            unsafe_allow_html=True
        )

st.divider()

# =====================================================
# STEP TABS – LOG RIÊNG MỖI TIẾN TRÌNH
# =====================================================
tab_labels = [s["label"] for s in PIPELINE_STEPS]
tabs = st.tabs(tab_labels)

for tab, step in zip(tabs, PIPELINE_STEPS):
    with tab:
        logs = st.session_state.logs[step["key"]]
        st.code("\n".join(logs) if logs else "No logs yet.", language="text")

# =====================================================
# CONTROLS
# =====================================================
st.divider()
st.subheader("Controls")

control_cols = st.columns(len(PIPELINE_STEPS) + 1)

with control_cols[0]:
    if st.button("▶ Run FULL Pipeline"):
        run_full_pipeline()

for idx, step in enumerate(PIPELINE_STEPS):
    with control_cols[idx + 1]:
        if st.button(f"Run {step['label']}"):
            run_step(step)
