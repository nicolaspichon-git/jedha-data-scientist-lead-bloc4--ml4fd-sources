"""The state of the chain, as the pipeline owner reads it.

Block: B3, B4.
Lot: B3-R6, B4-L0.
Competency: B3-C4.
Covers: B3-EC14.

The controls are N9's own -- health.check(), what raise_and_notify_ops
runs every minute -- not a second set. ml4fd/dashboard.operations_view()
decides what is shown and in what order; this page only displays. It shows no
transaction: of the quarantine, the volume invariant reads batch_id and
line_no, and fd_dashboard holds neither payload nor reason_detail.
"""
import streamlit as st

import common
from ml4fd import dashboard

st.title("Contrôles d'exploitation")

settings = common.settings()
with common.connection() as conn:
    view = dashboard.operations_view(conn, settings)

common.show(view["notices"])

for column, (label, value) in zip(st.columns(len(view["figures"])),
                                  view["figures"]):
    with column:
        st.metric(label, value)
st.caption(view["note"])

st.subheader("Contrôles")
st.dataframe(view["controls"], hide_index=True)

if view["runs"]:
    st.subheader("Dernières exécutions")
    st.dataframe(view["runs"], hide_index=True)
