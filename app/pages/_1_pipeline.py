import streamlit as st
import time

# =====================================================
# PIPELINE DEFINITION
# =====================================================

PIPELINE_STEPS = [
    {"key": "crawl", "label": "Crawl"},
    {"key": "processing", "label": "Processing"},
    {"key": "upload", "label": "Upload Database"},
]

STEP_KEYS = [s["key"] for s in PIPELINE_STEPS]

# =====================================================
# STATE INIT
# =====================================================

def _init_state():
    if "pipeline_logs" not in st.session_state:
        st.session_state.pipeline_logs = {k: [] for k in STEP_KEYS}

    if "pipeline_running" not in st.session_state:
        st.session_state.pipeline_running = False


# =====================================================
# LOG HELPERS
# =====================================================

def _log(step_key, msg):
    st.session_state.pipeline_logs[step_key].append(msg)


def _simulate_step(step_key):
    _log(step_key, f"[{step_key.upper()}] START")
    time.sleep(0.3)
    _log(step_key, "Running...")
    time.sleep(0.3)
    _log(step_key, "Processing...")
    time.sleep(0.3)
    _log(step_key, f"[{step_key.upper()}] DONE")


def _run_full_pipeline():
    st.session_state.pipeline_running = True
    for step in STEP_KEYS:
        _simulate_step(step)
    st.session_state.pipeline_running = False


# =====================================================
# PUBLIC RENDER
# =====================================================

def render_pipeline():
    _init_state()

    # -------------------------------
    # HEADER ROW
    # -------------------------------
    header_left, header_right = st.columns([4, 1])

    with header_left:
        tabs = st.tabs([s["label"] for s in PIPELINE_STEPS])
        for tab, step in zip(tabs, PIPELINE_STEPS):
            with tab:
                st.markdown(f"#### 📄 {step['label']} Logs")

                logs = st.session_state.pipeline_logs[step["key"]]

                st.code(
                    "\n".join(logs) if logs else "No logs yet.",
                    language="bash",
                )

                st.divider()

                if st.button(
                    f"Run {step['label']}",
                    key=f"run_{step['key']}",
                    disabled=st.session_state.pipeline_running,
                ):
                    _simulate_step(step["key"])

    with header_right:
        if st.button(
            "▶ Run Pipeline",
            use_container_width=True,
            disabled=st.session_state.pipeline_running,
        ):
            _run_full_pipeline()
