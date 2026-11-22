"""The alerts of a calendar period, as the analyst reviews them.

Block: B3, B4.
Lot: B3-R3, B3-R8, B4-L0.
Competency: B3-C4.
Covers: B3-EE03, B3-EC17.

The rows are those of the morning reports of the same days -- same query, same
windows end to end, same order -- so the page and the documents never disagree.
A period is one day by default; the picker takes a first and a last day (S6/R8).
ml4fd/dashboard.frauds_view() decides what is shown; this page only displays.

READS AND NOTHING ELSE. No acknowledgement, no comment, no re-send: an alert is
handled outside the system, and fd_dashboard could not write one anyway.
"""
import streamlit as st

import common
from ml4fd import dashboard

st.title("Alertes de fraude")

KEY = "frauds_period"
settings = common.settings()

# The default is today IN THE DECLARED ZONE, not the server's date: at 00:30 in
# Paris the container's UTC clock still says yesterday. The widget keeps the
# chosen period across reruns through its key. While the second day of a range
# is still awaited the widget returns one date: period_of() reads it as a day.
day = dashboard.today(settings)
chosen = st.date_input("Période", value=(day, day), key=KEY, format="YYYY-MM-DD")
first, last = dashboard.period_of(chosen, day)

with common.connection() as conn:
    view = dashboard.frauds_view(conn, settings, first, last)

st.subheader(view["title"])

common.show(view["notices"])

for column, (label, value) in zip(st.columns(len(view["figures"])), view["figures"]):
    with column:
        st.metric(label, value)
st.caption(view["capacity_note"])

if view["table"]:
    st.dataframe(view["table"], hide_index=True)
