"""What the pages of the dashboard share. DISPLAY ONLY.

Covers: EC19
Lot: R2
Competency: C4

Everything a page shows is decided in ml4fd/dashboard.py, where pytest can hold
it. This module only does what needs Streamlit: the cached configuration, the
connection that stops the page cleanly when it cannot be opened, and the
mapping of a notice to its colour.
"""
from __future__ import annotations

from contextlib import ExitStack, contextmanager

import streamlit as st

from ml4fd import config, dashboard


@st.cache_resource
def _load():
    """The configuration, read once per container. THE ONLY CACHE OF S6.

    No data is cached: st.cache_data without a lifetime would keep yesterday's
    report, or the alerts of an hour ago, until the container restarts (plan
    S6, 7.4). The reads of this dashboard are small. A load that RAISES is not
    cached, so a corrected .env is picked up on the next page load.
    """
    return config.load()


def settings():
    """The configuration, or a message and a stopped page.

    The likeliest failure is not a bug: FD_DASHBOARD_PASSWORD empty in .env,
    so docker compose hands the container an empty POSTGRES_PASSWORD and
    config.load() refuses to start. The remedy is two commands, and it belongs
    on the page rather than in a hidden traceback.
    """
    try:
        return _load()
    except Exception as problem:
        st.error("La configuration du tableau de bord est incomplète : %s"
                 % problem)
        st.info("Si le mot de passe de fd_dashboard manque :  "
                ".\\fd-manager.ps1 setup   puis   .\\fd-manager.ps1 init   "
                "puis   .\\fd-manager.ps1 dashboard")
        st.stop()


@contextmanager
def connection():
    """A connection as fd_dashboard, or a message and a stopped page.

    Only OPENING the connection is guarded here. An error raised later, by a
    page's own reads, is not swallowed under a generic message: it would hide
    the defect that caused it.
    """
    stack = ExitStack()
    try:
        conn = stack.enter_context(dashboard.connect(settings()))
    except dashboard.WrongIdentity as wrong:
        st.error(str(wrong))
        st.stop()
    except Exception as problem:            # psycopg.OperationalError and kin
        # The driver's message names host, port and user, never the password.
        st.error("La base est inaccessible : %s" % problem)
        st.stop()
    with stack:
        yield conn


_SHOW = {dashboard.ERROR: "error", dashboard.WARNING: "warning",
         dashboard.SUCCESS: "success", dashboard.INFO: "info"}


def show(notices) -> None:
    """Each notice in its colour, in the order ml4fd/dashboard.py decided."""
    for notice in notices:
        getattr(st, _SHOW[notice.level])(notice.text)
