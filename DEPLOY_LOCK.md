# Déploiement du mécanisme de lock pour exécutions parallèles

## Modifications apportées

1. **`scripts/enrich_dept_prototype.py`** :
   - Ajout du paramètre `use_lock` dans `load_etablissements()`
   - Ajout de `FOR UPDATE SKIP LOCKED` à la requête SQL quand `use_lock=True`
   - Ajout de l'argument `--enable-lock` dans le CLI

2. **`cloudrun_job_update.yaml`** :
   - Ajout de `--enable-lock` dans les arguments

## Étapes de déploiement

### 1. Builder la nouvelle image Docker

```powershell
# Depuis le répertoire racine du projet
docker build -f cloudrun_ref/Dockerfile -t habitat-enrich:latest .
```

### 2. Tagger l'image pour GCP Artifact Registry

```powershell
docker tag habitat-enrich:latest europe-west1-docker.pkg.dev/gen-lang-client-0230548399/habitat/enrich:latest
```

### 3. Pousser l'image vers GCP

```powershell
docker push europe-west1-docker.pkg.dev/gen-lang-client-0230548399/habitat/enrich:latest
```

### 4. Mettre à jour le job Cloud Run

```powershell
gcloud run jobs replace cloudrun_job_update.yaml --region europe-west1
```

### 5. Tester avec une exécution unique

```powershell
gcloud run jobs execute habitat-enrich-test --region europe-west1 --wait
```

### 6. Lancer 10 exécutions parallèles

```powershell
.\run_parallel_jobs.ps1
```

## Comment ça marche ?

Le mécanisme `FOR UPDATE SKIP LOCKED` fonctionne ainsi :

1. Chaque job ouvre une transaction
2. Le `SELECT ... FOR UPDATE SKIP LOCKED` :
   - **Verrouille** les lignes sélectionnées pour cette transaction
   - **Ignore** les lignes déjà verrouillées par d'autres transactions
3. Les autres jobs qui exécutent la même requête en parallèle ne verront **pas** les lignes verrouillées
4. Chaque job traite donc des établissements **différents**
5. Quand un job finit son traitement et commit/rollback, les verrous sont libérés

**Important** : Le lock est maintenu pendant toute la durée du traitement de l'établissement, ce qui peut être long (scraping + LLM). Pour éviter les deadlocks :
- Les transactions doivent être courtes
- On peut envisager de verrouiller par batch plutôt que pour toute la durée

## Test local

Pour tester localement avec le lock activé :

```powershell
python scripts/enrich_dept_prototype.py --departements 60 --limit 5 --enable-lock
```

## Vérification

Après avoir lancé les 10 jobs en parallèle :

```python
# Vérifier que les propositions créées concernent des établissements différents
python -c "
import sys
sys.path.insert(0, 'c:/Users/Lenovo/ingest-habitat')
from database import DatabaseManager

db = DatabaseManager()
with db.get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute('''
            SELECT etablissement_id, COUNT(*) 
            FROM propositions 
            WHERE created_at > NOW() - INTERVAL '30 minutes'
            GROUP BY etablissement_id
            HAVING COUNT(*) > 1
        ''')
        duplicates = cur.fetchall()
        if duplicates:
            print(f'❌ {len(duplicates)} établissements ont été traités plusieurs fois !')
            for etab_id, count in duplicates:
                print(f'  - Etablissement {etab_id}: {count} propositions')
        else:
            print('✅ Aucun doublon détecté !')
"
```

## Prochaines étapes

1. Tester 2-3 exécutions parallèles d'abord
2. Vérifier qu'il n'y a pas de doublons
3. Augmenter progressivement jusqu'à 10
4. Monitorer les logs pour détecter d'éventuels deadlocks
