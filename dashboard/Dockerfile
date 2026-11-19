# The dashboard of S6 (lot R4).
#
# Only the dependencies are baked in. The code -- dashboard/, ml4fd/, sql/ --
# and governance.yml are MOUNTED READ-ONLY by docker-compose.yml, like the
# Airflow image does, so a change to a page needs no rebuild.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# NOT ROOT. Nothing in the dashboard needs a privilege, and the one container a
# browser talks to is the last one that should run as root.
RUN useradd --create-home --uid 10001 dashboard
USER dashboard

# The mounted directory, so .streamlit/config.toml is found where Streamlit
# looks for it: the working directory.
WORKDIR /opt/project/dashboard

EXPOSE 8501

# No curl in the slim image: the health check uses Python, which is there.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health', timeout=4)"

CMD ["streamlit", "run", "app.py"]
