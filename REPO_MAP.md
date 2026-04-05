# Repository Map (Lightweight)

## Core pipelines
- `pipeline_v3_cli.py`: main habitat enrichment CLI.
- `mvp/scrapers/`: extraction/enrichment chain.
- `mvp/deduplication/`: deduplication logic.
- `enrichment/`: normalization and eligibility helpers.

## FINESS + DB enrichment
- `PIPELINE_ENRICHISSEMENT_FINESS_TECHNIQUE.md`: full FINESS technical runbook.
- `database/`, `schema.sql`, `scripts_sql/`: ingestion and schema assets.
- `cloudrun_ref/`, `cloudrun_jobs/`, `cloudrun_job_*.yaml`: Cloud Run batch packaging/deploy.

## DRH and financeurs
- `prospection_DRH/`: DRH/DGS and PME contact workflows.
- `prospection-financeurs/`: financeurs/qualification workflows.

## Local-only zones (not for Git)
- `.env*` (except `.env.example`)
- `scripts/scratch/` for ad-hoc `_*.py` diagnostics
- `outputs/`, `logs/`, `TDS/`, root generated CSVs and `temp_*`

## Safe pre-push command
- `powershell -ExecutionPolicy Bypass -File scripts/prepush_safety_check.ps1 -RepoPath .`
