"""The rejections of a calendar period, by class and by reason (F07.3).

Covers: EC15, EC22
Lot: R7, R8
Competency: C4

COUNTS ONLY. fd_dashboard reads five columns of the quarantine -- batch_id,
line_no, reason_class, reason_code, rejected_at -- and neither the content of
a rejected line nor the detail of its reason. ml4fd/dashboard.rejects_view()
decides what is shown; this page only displays.
"""
import streamlit as st

import common
from ml4fd import dashboard

st.title("Rejets de validation")

KEY = "rejects_of_the_day"
settings = common.settings()

# Today IN THE DECLARED ZONE, as on the alerts page; a range on request.
day = dashboard.today(settings)
chosen = st.date_input("Période", value=(day, day), key=KEY, format="YYYY-MM-DD")
first, last = dashboard.period_of(chosen, day)

with common.connection() as conn:
    view = dashboard.rejects_view(conn, settings, first, last)

st.subheader(view["title"])

common.show(view["notices"])

for column, (label, value) in zip(st.columns(len(view["figures"])),
                                  view["figures"]):
    with column:
        st.metric(label, value)
st.caption(view["note"])

if view["table"]:
    st.dataframe(view["table"], hide_index=True)
