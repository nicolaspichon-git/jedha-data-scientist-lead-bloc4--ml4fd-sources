"""The operations channel: what it was told, and when (R9).

Covers: EC14, EC17
Lot: R9
Competency: C4

Reads fd_ops_notifications, the journal notify_ops writes after each message
it sends -- whatever the channel, log or file. ml4fd/dashboard.ops_view()
decides what is shown; this page only displays. The live state of the
controls is on the Exploitation page.
"""
import streamlit as st

import common
from ml4fd import dashboard

st.title("Alertes d'exploitation")

KEY = "ops_period"
settings = common.settings()

# Today IN THE DECLARED ZONE, as on the alerts page; a range on request.
day = dashboard.today(settings)
chosen = st.date_input("Période", value=(day, day), key=KEY, format="YYYY-MM-DD")
first, last = dashboard.period_of(chosen, day)

with common.connection() as conn:
    view = dashboard.ops_view(conn, settings, first, last)

st.subheader(view["title"])
common.show(view["notices"])

for column, (label, value) in zip(st.columns(len(view["figures"])),
                                  view["figures"]):
    with column:
        st.metric(label, value)
st.caption(view["note"])

if view["table"]:
    st.dataframe(view["table"], hide_index=True)
