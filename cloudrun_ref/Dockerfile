# Cloud Run Job image (batch enrichment)
# Build context: repo root
# Usage:
#   docker build -f cloudrun_ref/Dockerfile -t habitat-enrich:dev .

FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps for psycopg2
RUN apt-get update \
  && apt-get install -y --no-install-recommends gcc libpq-dev \
  && rm -rf /var/lib/apt/lists/*

COPY cloudrun_ref/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy only what's needed to run the batch scripts
RUN mkdir -p /app/scripts
COPY scripts/enrich_dept_prototype.py /app/scripts/enrich_dept_prototype.py
COPY scripts/enrich_propositions_workflow.py /app/scripts/enrich_propositions_workflow.py
COPY scripts/debug_cloudrun.py /app/scripts/debug_cloudrun.py
# FINESS enrichment pipeline (NB: PAS de ingest_finess.py ni de CSV — ingestion locale uniquement)
COPY scripts/enrich_finess_dept.py /app/scripts/enrich_finess_dept.py
COPY scripts/enrich_finess_config.py /app/scripts/enrich_finess_config.py
COPY database.py /app/database.py
COPY config.py /app/config.py

# Vérifier qu'aucun .env n'a été copié (sécurité)
RUN test ! -f /app/.env || (echo "ERROR: .env found in image!" && exit 1)

# Default command (Cloud Run Job can override args)
ENTRYPOINT ["python", "scripts/enrich_dept_prototype.py"]

# Default args: dry-run small sample
CMD ["--departements", "45", "--limit", "5", "--dry-run", "--out-dir", "/tmp/outputs"]
