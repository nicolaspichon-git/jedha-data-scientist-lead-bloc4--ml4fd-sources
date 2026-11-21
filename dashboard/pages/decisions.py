"""The decisions served, the pipeline's and the third parties', as one journal.

Covers: EC16, EC19
Lot: L5
Competency: C3

Reads fd_scoring_decisions, the view 195_scoring_decisions.sql puts over
fd_scores and fd_scoring_service_calls. For a day the view existed and nothing
read it, which is how a journal becomes decoration: EC19 asks that every call
and its answer be persisted, and a journal nobody can read is a claim nobody
can check.

THE VIEW AND NOT THE TABLES. fd_dashboard has no privilege on either -- 190
revokes them, and a view runs with its owner's privileges -- so this page
cannot drift onto them, and it needs no edit the day the service becomes the
only journal. It shows no cardholder, no merchant and no amount: the view does
not carry them.

ml4fd/dashboard.decisions_view() decides what is shown; this page displays.
"""
import streamlit as st

import common
from ml4fd import dashboard

st.title("Décisions du service de scoring")

KEY = "decisions_period"
settings = common.settings()

day = dashboard.today(settings)
chosen = st.date_input("Période", value=(day, day), key=KEY, format="YYYY-MM-DD")
first, last = dashboard.period_of(chosen, day)

with common.connection() as conn:
    view = dashboard.decisions_view(conn, settings, first, last)

st.subheader(view["title"])
common.show(view["notices"])

for column, (label, value) in zip(st.columns(len(view["figures"])), view["figures"]):
    with column:
        st.metric(label, value)
st.caption(view["note"])

if view["table"]:
    st.subheader("Décisions par source")
    st.dataframe(view["table"], hide_index=True)
    # The word the table counts, defined under it: a reader asked what a
    # "signalée" decision was, and nothing on this page said.
    st.caption(view["flagged_note"])

if view["calls"]:
    st.subheader("Requêtes tierces de la période")
    st.dataframe(view["calls"], hide_index=True)
