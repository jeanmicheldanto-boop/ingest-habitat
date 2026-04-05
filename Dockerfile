# Cloud Run Job image - FINESS National Enrichment
# Build context: repo root
# Usage:
#   docker build -t europe-west1-docker.pkg.dev/gen-lang-client-0230548399/habitat/enrich:latest .

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

# Copy FINESS enrichment pipeline
RUN mkdir -p /app/scripts
COPY scripts/enrich_finess_dept.py /app/scripts/enrich_finess_dept.py
COPY scripts/enrich_finess_config.py /app/scripts/enrich_finess_config.py
COPY scripts/fix_data_quality.py /app/scripts/fix_data_quality.py
COPY scripts/quality_pass_targeted.py /app/scripts/quality_pass_targeted.py
COPY scripts/signaux_v2_passe_b.py /app/scripts/signaux_v2_passe_b.py
COPY scripts/signaux_v2_g0_discovery.py /app/scripts/signaux_v2_g0_discovery.py
COPY scripts/signaux_v2_g2_deep.py /app/scripts/signaux_v2_g2_deep.py
COPY scripts/signaux_v2_exhaustif_common.py /app/scripts/signaux_v2_exhaustif_common.py
COPY database.py /app/database.py
COPY config.py /app/config.py

# Vérifier qu'aucun .env n'a été copié (sécurité)
RUN test ! -f /app/.env || (echo "ERROR: .env found in image!" && exit 1)

# Default command (Cloud Run Job will override args)
ENTRYPOINT ["python"]
CMD ["scripts/enrich_finess_dept.py", "--departements", "01", "--out-dir", "/tmp/outputs"]
