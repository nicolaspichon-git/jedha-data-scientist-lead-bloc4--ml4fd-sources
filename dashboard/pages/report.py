"""The morning report, as the analyst opens it.

Block: B3, B4.
Lot: B3-R2, B4-L0.
Competency: B3-C4.
Covers: B3-EE03, B3-EC14.

ORDER IS THE POINT: the date, then every notice -- absences, the digest, the
mounts -- then the figures, then the document. ml4fd/dashboard.morning_view()
decides it; this page only displays.

The document is rendered as Markdown WITHOUT unsafe_allow_html: whatever the
file contains is shown as text, including a file that no longer matches its
digest.
"""
import streamlit as st

import common
from ml4fd import dashboard, report

st.title("Rapports des paiements")

KEY = "report_of_the_day"                  # the selected day survives a rerun
settings = common.settings()

with common.connection() as conn:
    view = dashboard.morning_view(conn, settings, st.session_state.get(KEY))

if view["dates"]:
    st.selectbox("Journée couverte", view["dates"],
                 index=view["dates"].index(view["selected"]),
                 format_func=report._day, key=KEY)

common.show(view["notices"])

if view["figures"]:
    for start in range(0, len(view["figures"]), 4):
        for column, (label, value) in zip(st.columns(4), view["figures"][start:start + 4]):
            with column:
                st.metric(label, value)

if view["document"] is not None:
    st.divider()
    st.markdown(view["document"])
