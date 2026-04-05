# Ingest Habitat Workspace Audit (2026-04-05)

## Scope
Audit and hardening for GitHub publication readiness.

## High-risk findings
- Root `.env` was tracked in Git and contained real API keys.
- Tracked Python bytecode (`*.pyc`) existed in multiple `__pycache__/` folders.
- Root generated CSV artifacts were tracked (`pipeline_*.csv`, `test_*.csv`, `data_*.csv`).
- Workspace contains many ad-hoc root scripts (`_*.py`) and local personal documents (`TDS/`) that should not be pushed.

## Actions applied
- Hardened `.gitignore` for:
  - secret files (`.env*`, with `.env.example` preserved),
  - Python caches and tooling caches,
  - root generated artifacts,
  - personal/local folders (`TDS/`, `data/uploads/`),
  - root scratch scripts (`/_*.py`).
- Generated root `.env.example` with blank values only (safe template).
- Removed tracked `.env` from Git index.
- Removed tracked `__pycache__/*.pyc` from Git index.
- Removed local `__pycache__/` and `.pytest_cache/` folders.

## Functional layout (current)
- Main habitat enrichment core:
  - `pipeline_v3_cli.py`
  - `mvp/scrapers/`
  - `mvp/deduplication/`
  - `enrichment/`
- FINESS enrichment and SQL model:
  - `PIPELINE_ENRICHISSEMENT_FINESS_TECHNIQUE.md`
  - `database/`, `schema.sql`, `scripts_sql/`
- DRH prospecting pipeline:
  - `prospection_DRH/`
- Financeurs/innovation pipeline:
  - `prospection-financeurs/`
- Cloud Run industrialization:
  - `cloudrun_ref/`, `cloudrun_jobs/`, `cloudrun_job_*.yaml`
- Financial analysis artifacts:
  - `bitcoin_charts.py` and related docs/outputs

## Recommended lightweight structure (next step)
- Keep stable production code where it is.
- Move root scratch scripts (`_*.py`) to `scripts/scratch/` when needed.
- Keep root test scripts only if still actively used; otherwise move to `tests/legacy/`.
- Keep generated data under `outputs/` and avoid root-level generated files.

## Pre-push checklist
1. Rotate all keys that were present in tracked `.env`.
2. Run: `powershell -ExecutionPolicy Bypass -File scripts/prepush_safety_check.ps1`.
3. Review staged changes manually: `git status` then `git diff --staged`.
4. Push only after script returns PASS.
