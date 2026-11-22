# The scoring service of L5, deployed by L6.
#
# BUILT FROM THE ROOT OF THE REPOSITORY, not from scoring_service/: the image
# carries ml4fd/, sql/ and governance.yml, which live beside this directory.
# What enters the build is fixed by /.dockerignore, an allowlist.
#
# python:3.11-slim, matching the producer and the Airflow image: a pandas that
# reads a frame one way there reads it the same way here, and scikit-learn must
# unpickle a model fitted in the Airflow image.
FROM python:3.11-slim

WORKDIR /home/app

COPY scoring_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scoring_service/app.py .

# THE CODE IS IN THE IMAGE, since L6 (2026-11-22). It used to be MOUNTED from
# the working directory, so that the pipeline and this service ran one scoring
# path: while dag_ingest loaded the model itself, two processes decided, and
# they had to run the same code. L5 removed that reason -- the pipeline now
# sends its rows here over HTTP and only this service decides -- and the mount
# had a cost L6 cannot pay: an image tagged with a commit ran whatever the disk
# held that day, so no deployment could be traced to the code it served.
# Copied, the code is the commit's. Owned by root and read-only for the
# service's user: it reads its code, it never rewrites it.
COPY ml4fd/ /opt/project/ml4fd/
COPY sql/ /opt/project/sql/
COPY governance.yml /opt/project/governance.yml

# A NON-ROOT USER: this service answers a network and holds credentials of its
# own. It needs no privilege at all -- the same reasoning as the dashboard's
# image, which says it at more length.
RUN useradd --create-home --uid 10001 api && chown -R api /home/app
USER api

ENV PORT=5000
ENV PYTHONPATH=/opt/project

# THE COMMIT THIS IMAGE WAS BUILT FROM, announced by /health (B4-EC27) and
# written on the image itself. "local" for a build on this machine; the CI
# passes the commit. Declared last, so that a new commit does not invalidate
# the layers above.
ARG GIT_SHA=local
ENV ML4FD_REVISION=${GIT_SHA}
LABEL org.opencontainers.image.revision=${GIT_SHA} \
      org.opencontainers.image.source=https://github.com/nicolaspichon-git/jedha-data-scientist-lead-bloc4--ml4fd-sources
# ONE WORKER, deliberately. score.load() caches the model per process, keyed on
# (name, version), so every worker would hold its own copy of a model of a few
# megabytes and resolve the alias separately. The declared throughput is 7,200
# calls a day; one worker answers that with room to spare, and a second would
# only make "which version answered?" harder to reason about.
CMD ["sh", "-c", "exec gunicorn app:app --bind 0.0.0.0:$PORT \
     --worker-class uvicorn.workers.UvicornWorker --workers 1"]
