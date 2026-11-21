# The scoring service of L5.
#
# python:3.11-slim, matching the producer and the Airflow image: a pandas that
# reads a frame one way there reads it the same way here, and scikit-learn must
# unpickle a model fitted in the Airflow image.
FROM python:3.11-slim

WORKDIR /home/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

# A NON-ROOT USER: this service answers a network and holds credentials of its
# own. It needs no privilege at all -- the same reasoning as the dashboard's
# image, which says it at more length.
RUN useradd --create-home --uid 10001 api && chown -R api /home/app
USER api

ENV PORT=5000
# ml4fd/, sql/ and governance.yml are MOUNTED, never copied: this service
# reuses ml4fd.serve, ml4fd.score and the feature query, and a copy baked in
# would be a second version of the scoring path -- which is the one thing
# reusing them was meant to avoid.
ENV PYTHONPATH=/opt/project
# ONE WORKER, deliberately. score.load() caches the model per process, keyed on
# (name, version), so every worker would hold its own copy of a model of a few
# megabytes and resolve the alias separately. The declared throughput is 7,200
# calls a day; one worker answers that with room to spare, and a second would
# only make "which version answered?" harder to reason about.
CMD ["sh", "-c", "exec gunicorn app:app --bind 0.0.0.0:$PORT \
     --worker-class uvicorn.workers.UvicornWorker --workers 1"]
