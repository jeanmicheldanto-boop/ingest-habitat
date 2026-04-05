# Prospection DRH - Runbook propre

Ce dossier centralise un pipeline relancable pour:
- DRH + DGS de collectivites (communes IDF)
- DRH de PME IDF (Pappers -> Serper -> Dropcontact)

Les scripts ici appellent les scripts existants du projet et fixent des conventions de sortie.

## 1) Prerequis

- Workspace a la racine `ingest-habitat`
- Environnement Python `.venv` disponible
- Fichier `.env` renseigne avec au minimum:
  - `SERPER_API_KEY`
  - `DROPCONTACT_API_KEY`
  - `PAPPERS_API_KEY` (si extraction PME a refaire)
  - `GEMINI_API_KEY` ou `MISTRAL_API_KEY` selon `LLM_PROVIDER`

Optionnel:
- `HUNTER_API_KEY` (sinon fallback domaine via Serper, deja supporte)

## 2) Sorties standard

- Collectivites (DRH/DGS):
  - `data/mails_collectivites_FINAL.csv`
- PME (DRH + qualification Dropcontact):
  - `data/mails_pme_FINAL.csv`

Fichiers intermediaires PME (par defaut):
- `data/pme_idf_50_500_drh_serper_nodup1630.csv`
- `data/pme_idf_50_500_drh_serper_nodup_with_email.csv`

## 3) Lancer uniquement les collectivites (DRH + DGS)

```powershell
.\prospection_DRH\scripts\run_collectivites_drh_dgs.ps1
```

Options utiles:

```powershell
.\prospection_DRH\scripts\run_collectivites_drh_dgs.ps1 `
  -InputCsv data/communes_idf.csv `
  -MinPopulation 2000 `
  -EnableGenericRhSerper
```

Notes:
- Le script source trouve DRH et DGS en meme temps.
- `-EnableGenericRhSerper` est plus couteux en credits Serper.

## 4) Lancer uniquement le pipeline PME

```powershell
.\prospection_DRH\scripts\run_pme_drh.ps1
```

Ce script fait automatiquement:
1. Enrichissement DRH via Serper (`scripts/enrich_pme_drh_serper.py`)
2. Filtre des lignes ayant un email (`drh_email_public` ou `drh_email_reconstitue`)
3. Qualification/validation Dropcontact (`scripts/enrich_pme_dropcontact.py`)
4. Ecriture finale vers `data/mails_pme_FINAL.csv`

Options utiles:

```powershell
.\prospection_DRH\scripts\run_pme_drh.ps1 `
  -PmeInputCsv data/pme_idf_50_500_pappers_large_nodup_v2.csv `
  -BatchSize 100 `
  -PollSeconds 8
```

## 5) Lancer les deux pipelines d'un coup

```powershell
.\prospection_DRH\scripts\run_all_drh.ps1
```

## 6) Reprise propre apres interruption

- PME Serper: reprise deja geree par `--resume` (actif par defaut) si le meme fichier de sortie est reutilise.
- Dropcontact PME: relancer le script de pipeline PME; il regenera le subset puis la sortie finale.
- Collectivites: rerun complet recommande (ou relance par `-Offset/-Limit` directement sur le script source si besoin fin).

## 7) Commandes brutes (reference)

Collectivites:

```powershell
.\.venv\Scripts\python.exe scripts/enrich_communes_idf_contacts.py `
  --input data/communes_idf.csv `
  --output data/mails_collectivites_FINAL.csv `
  --limit 0 --offset 0 --flush-every 20 --progress-every 1 --min-population 2000
```

PME DRH Serper:

```powershell
.\.venv\Scripts\python.exe scripts/enrich_pme_drh_serper.py `
  --input data/pme_idf_50_500_pappers_large_nodup_v2.csv `
  --output data/pme_idf_50_500_drh_serper_nodup1630.csv `
  --limit 0 --progress-every 20 --flush-every 10
```

PME Dropcontact:

```powershell
.\.venv\Scripts\python.exe scripts/enrich_pme_dropcontact.py `
  --input data/pme_idf_50_500_drh_serper_nodup_with_email.csv `
  --output data/mails_pme_FINAL.csv `
  --limit 0 --batch-size 100 --poll-seconds 8 --max-wait-seconds 900
```
