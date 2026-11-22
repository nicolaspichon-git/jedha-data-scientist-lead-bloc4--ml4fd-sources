"""The surveillance of the model in service, as a jury reads it.

Block: B4.
Lot: B4-L4.
Competency: B4-C6.
Covers: B4-EC45, B4-EC49.

WHAT THIS PAGE ANSWERS and nothing else did: fd_monitoring was written every
cadence and read by nothing. The history EC45 asks to keep was kept and never
shown, and the report EC49 asks for consisted of a terminal printing figures.

ml4fd/dashboard.monitoring_view() decides what is shown; this page only
displays. It shows no transaction and no cardholder: fd_monitoring holds
aggregates, and fd_retrain_requests holds a reason written by the policy.
"""
import datetime as dt

import streamlit as st

import common
from ml4fd import dashboard

st.title("Surveillance du modèle en service")

KEY = "monitoring_period"
settings = common.settings()

day = dashboard.today(settings)
start = day - dt.timedelta(days=dashboard.MONITORING_DEFAULT_DAYS - 1)
chosen = st.date_input("Période des mesures", value=(start, day), key=KEY,
                       format="YYYY-MM-DD")
first, last = dashboard.period_of(chosen, day)

with common.connection() as conn:
    view = dashboard.monitoring_view(conn, settings, first, last)

st.subheader(view["title"])
common.show(view["notices"])

if view["state"]:
    st.subheader("Où en est chaque contrôle")
    st.dataframe(view["state"], hide_index=True)

for serie in view["series"]:
    st.subheader("%s — %s" % (serie["label"], serie["metric"]))
    # Mesure and Limite on the same axes, so the reader sees which is above
    # without the page deciding. A limit that moved shows as a step. The
    # columns come from the view: a metric with no threshold at all -- or none
    # yet, while the window is too thin to judge -- has no Limite to draw.
    st.line_chart(serie["points"], x="Instant", y=serie["columns"])

if view["columns"]:
    st.subheader("Dérive par colonne, dernier passage")
    st.caption("Les figures et la limite sont celles de l'outil. La politique "
               "ne juge que la part des colonnes en dérive ; ces lignes sont "
               "des observations, et ce sont elles qui disent laquelle a bougé.")
    st.dataframe(view["columns"], hide_index=True)

if view["requests"]:
    st.subheader("Réentraînements demandés par la surveillance")
    st.dataframe(view["requests"], hide_index=True)

st.caption(view["note"])
