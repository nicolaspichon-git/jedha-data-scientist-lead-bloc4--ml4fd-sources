"""The dashboard of S6: its entry point. `streamlit run dashboard/app.py`.

Covers: EE03
Lot: R2
Competency: C4

Declares the pages and nothing else. No data is read here: every page opens
its own connection, as fd_dashboard, through common.connection().
"""
import streamlit as st

st.set_page_config(page_title="Automatic Fraud Detection", layout="wide")

PAGES = {
    "Analyse": [
        st.Page("pages/report.py", title="Rapports", url_path="report", default=True),
        st.Page("pages/frauds.py", title="Fraudes", url_path="frauds"),
    ],
    "Exploitation": [
        st.Page("pages/ops.py", title="Etats", url_path="ops"),
        st.Page("pages/operations.py", title="Contrôles", url_path="operations"),
    ],
    "Suivi": [
        st.Page("pages/rejects.py", title="Rejets", url_path="rejects"),
        st.Page("pages/performances.py", title="Performances", url_path="performances"),
    ],
}

st.navigation(PAGES).run()
