#!/usr/bin/env python3
"""
Génère 10 fichiers YAML Cloud Run Jobs, chacun avec une partition de départements.
Usage: python generate_parallel_jobs.py
"""
import re
from pathlib import Path
from database import DatabaseManager

# Template YAML
YAML_TEMPLATE = '''apiVersion: run.googleapis.com/v1
kind: Job
metadata:
  name: habitat-enrich-{index}
spec:
  template:
    spec:
      taskCount: 1
      template:
        spec:
          containers:
          - image: europe-west1-docker.pkg.dev/gen-lang-client-0230548399/habitat/enrich:latest
            command:
            - python
            args:
            - scripts/enrich_dept_prototype.py
            - --departements
            - "{departments}"
            - --limit
            - "0"
            - --out-dir
            - /tmp/outputs
            env:
            - name: PYTHONUNBUFFERED
              value: "1"
            - name: GEMINI_MODEL
              value: gemini-2.0-flash
            - name: DB_HOST
              value: db.minwoumfgutampcgrcbr.supabase.co
            - name: DB_NAME
              value: postgres
            - name: DB_USER
              value: postgres
            - name: DB_PORT
              value: "5432"
            - name: DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: DB_PASSWORD
                  key: latest
            - name: SERPER_API_KEY
              valueFrom:
                secretKeyRef:
                  name: SERPER_API_KEY
                  key: latest
            - name: SCRAPINGBEE_API_KEY
              valueFrom:
                secretKeyRef:
                  name: SCRAPINGBEE_API_KEY
                  key: latest
            - name: GEMINI_API_KEY
              valueFrom:
                secretKeyRef:
                  name: GEMINI_API_KEY
                  key: latest
            resources:
              limits:
                cpu: "1"
                memory: 512Mi
          maxRetries: 0
          timeoutSeconds: 3600
          serviceAccountName: habitat-enrich-sa@gen-lang-client-0230548399.iam.gserviceaccount.com
'''

def main():
    db = DatabaseManager()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT DISTINCT departement FROM etablissements WHERE departement IS NOT NULL ORDER BY departement')
            depts = [r[0] for r in cur.fetchall()]
    
    # Extraire les codes de département
    codes = []
    for d in depts:
        m = re.search(r'\((\d+[AB]?)\)', str(d))
        if m:
            codes.append(m.group(1))
    
    print(f"Total départements: {len(codes)}")
    
    # Partitionner en 10
    n = len(codes)
    chunk_size = n // 10
    
    output_dir = Path("cloudrun_jobs")
    output_dir.mkdir(exist_ok=True)
    
    for i in range(10):
        start = i * chunk_size
        end = start + chunk_size if i < 9 else n
        partition = codes[start:end]
        
        departments_str = ",".join(partition)
        yaml_content = YAML_TEMPLATE.format(
            index=str(i + 1).zfill(2),
            departments=departments_str
        )
        
        filename = output_dir / f"job_{str(i + 1).zfill(2)}.yaml"
        filename.write_text(yaml_content, encoding="utf-8")
        
        print(f"Job {i+1}: {len(partition)} depts ({partition[0]}..{partition[-1]}) -> {filename}")
    
    print(f"\n{len(codes)} départements partitionnés en 10 jobs")
    print(f"Fichiers générés dans: {output_dir}")
    
    # Script de déploiement
    deploy_script = '''# Deployer et executer les 10 jobs
$jobs = @()
for ($i = 1; $i -le 10; $i++) {
    $idx = $i.ToString().PadLeft(2, '0')
    Write-Host "Deploiement job $idx..." -NoNewline
    gcloud run jobs replace cloudrun_jobs/job_$idx.yaml --region europe-west1 --project gen-lang-client-0230548399 2>$null
    Write-Host " OK" -ForegroundColor Green
}

Write-Host "`nLancement des 10 jobs en parallele..."
for ($i = 1; $i -le 10; $i++) {
    $idx = $i.ToString().PadLeft(2, '0')
    gcloud run jobs execute habitat-enrich-$idx --region europe-west1 --project gen-lang-client-0230548399 --async 2>$null
    Write-Host "Job $idx lance" -ForegroundColor Cyan
}
'''
    (output_dir / "deploy_all.ps1").write_text(deploy_script, encoding="utf-8")
    print(f"Script de déploiement: {output_dir}/deploy_all.ps1")

if __name__ == "__main__":
    main()
