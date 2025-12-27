import streamlit as st
from streamlit_option_menu import option_menu

from assets.styles import set_global_css, option_menu_css
from pages._1_pipeline import render_pipeline

# =====================================================
# CONFIG
# =====================================================
st.set_page_config(
    page_title="Data Industry Insights",
    layout="wide"
)

set_global_css()

# =====================================================
# PAGE RENDER FUNCTIONS
# =====================================================


def render_database():
    st.subheader("🗄 Database")
    st.markdown("""
    Inspect database integrity and table-level statistics
    after pipeline ingestion.
    """)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Tables Overview")
        st.empty()

    with col2:
        st.markdown("### Integrity Checks")
        st.empty()


def render_analysis():
    st.subheader("📊 Analysis (EDA)")
    st.markdown("""
    Exploratory Data Analysis and statistical inspection.
    This layer is used to **understand the data**, not for final reporting.
    """)

    tabs = st.tabs([
        "Overview",
        "Distribution",
        "Correlation",
        "PCA"
    ])

    with tabs[0]:
        st.markdown("### Dataset Overview")
        st.empty()

    with tabs[1]:
        st.markdown("### Distribution Analysis")
        st.empty()

    with tabs[2]:
        st.markdown("### Correlation Analysis")
        st.empty()

    with tabs[3]:
        st.markdown("### PCA / Dimensionality Reduction")
        st.empty()


def render_dashboard():
    st.subheader("📈 Dashboard (BI)")
    st.markdown("""
    Final reporting and business-facing dashboards.
    This section typically links to **external BI tools (Power BI)**.
    """)

    st.link_button(
        "Open Power BI Dashboard",
        url="https://powerbi.microsoft.com/"
    )

    st.caption("Dashboards are maintained outside Streamlit.")


# =====================================================
# TOP MENU (HORIZONTAL)
# =====================================================

current_page = option_menu(
    menu_title="",
    options=["Pipeline", "Database", "Analysis", "Dashboard"],
    icons=["diagram-3", "database", "bezier2", "bar-chart-line"],
    menu_icon="cast",
    default_index=0,
    orientation="horizontal",
    styles=option_menu_css()
)

st.session_state["current_page"] = current_page

# =====================================================
# MAIN CONTENT CONTAINER
# =====================================================

with st.container():
    if current_page == "Pipeline":
        render_pipeline()

    elif current_page == "Database":
        render_database()

    elif current_page == "Analysis":
        render_analysis()

    elif current_page == "Dashboard":
        render_dashboard()