# python:3.11-slim rather than jedha/real-time-payments-api-lead-program.
#
# THE BASE IMAGE WAS OPAQUE: nothing says what it holds, whether it is still
# published, or which Python it carries, and M8 asks that a build be
# reproducible. A slim base with a declared requirements.txt answers all three,
# and `.\fd-manager.ps1 lock` can then record what pip resolved here as it
# does for the Airflow image.
#
# 3.11 to match the Airflow image, so a pandas that reads the corpus one way
# there reads it the same way here.
FROM python:3.11-slim

WORKDIR /home/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY static ./static
COPY templates ./templates

# A NON-ROOT USER: this service is reachable over HTTP and reads a corpus of
# personal data. It needs no privilege at all.
RUN useradd --create-home --uid 10001 producer && chown -R producer /home/app
USER producer

# PORT, so compose decides and the command does not have to be rewritten.
ENV PORT=4000
# ml4fd/ is MOUNTED, not copied: this service reuses ml4fd.fetch, and a copy
# baked into the image would be a second version of the acquisition path --
# which is the one thing reusing it was meant to avoid.
ENV PYTHONPATH=/opt/project
CMD ["sh", "-c", "exec gunicorn app:app --bind 0.0.0.0:$PORT \
     --worker-class uvicorn.workers.UvicornWorker --workers 1"]
