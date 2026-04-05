#!/usr/bin/env python3
"""
Script de diagnostic pour Cloud Run.
Exécute des tests basiques et écrit les résultats dans stdout/stderr
pour identifier pourquoi les jobs ne fonctionnent pas.
"""
import os
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

# Ajouter le répertoire parent (repo root) au sys.path pour trouver config.py et database.py
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

def log(msg: str):
    """Force immediate output to both stdout and stderr."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    full_msg = f"[{timestamp}] {msg}"
    print(full_msg, flush=True)
    sys.stdout.flush()
    sys.stderr.write(full_msg + "\n")
    sys.stderr.flush()

def main():
    log("="*80)
    log("DEBUG CLOUD RUN - START")
    log("="*80)
    
    # 1. Test environnement Python
    log(f"[PYTHON] Version: {sys.version}")
    log(f"[PYTHON] Executable: {sys.executable}")
    log(f"[PYTHON] CWD: {os.getcwd()}")
    
    # 2. Variables d'environnement critiques
    log("")
    log("[ENV] Variables d'environnement critiques:")
    env_vars = [
        "DB_HOST", "DB_NAME", "DB_USER", "DB_PORT", "DB_PASSWORD",
        "GEMINI_API_KEY", "SERPER_API_KEY", "SCRAPINGBEE_API_KEY",
        "PYTHONUNBUFFERED", "GEMINI_MODEL"
    ]
    for var in env_vars:
        value = os.environ.get(var, "NOT_SET")
        # Masquer les secrets
        if "PASSWORD" in var or "KEY" in var:
            if value and value != "NOT_SET":
                value = f"SET (len={len(value)})"
            else:
                value = "NOT_SET or EMPTY"
        log(f"  {var}: {value}")
    
    # 3. Test fichiers
    log("")
    log("[FILES] Vérification fichiers:")
    files_to_check = [
        "/app/database.py",
        "/app/config.py",
        "/app/scripts/enrich_dept_prototype.py",
        "/app/.env",  # Ne devrait PAS exister
    ]
    for f in files_to_check:
        exists = os.path.exists(f)
        log(f"  {f}: {'EXISTS' if exists else 'NOT FOUND'}")
    
    # 4. Test import config.py (problème potentiel avec load_dotenv)
    log("")
    log("[IMPORT] Test import config.py:")
    try:
        # D'abord, vérifions les vars AVANT import
        db_pwd_before = os.environ.get("DB_PASSWORD", "NOT_SET")
        log(f"  DB_PASSWORD avant import config: {'SET' if db_pwd_before and db_pwd_before != 'NOT_SET' else 'NOT_SET'}")
        
        import config
        
        # Après import - est-ce que load_dotenv a écrasé quelque chose ?
        db_pwd_after = os.environ.get("DB_PASSWORD", "NOT_SET")
        log(f"  DB_PASSWORD apres import config: {'SET' if db_pwd_after and db_pwd_after != 'NOT_SET' else 'NOT_SET'}")
        
        log(f"  config.DATABASE_CONFIG['host']: {config.DATABASE_CONFIG.get('host')}")
        log(f"  config.DATABASE_CONFIG['password'] set: {bool(config.DATABASE_CONFIG.get('password'))}")
        log("  [OK] config.py importé avec succès")
    except Exception as e:
        log(f"  [ERROR] Erreur import config: {e}")
        traceback.print_exc()
    
    # 5. Test import database.py
    log("")
    log("[IMPORT] Test import database.py:")
    try:
        from database import DatabaseManager
        log("  [OK] DatabaseManager importé")
        
        db = DatabaseManager()
        log(f"  DB config host: {db.config.get('host')}")
        log(f"  DB config password set: {bool(db.config.get('password'))}")
        log(f"  DB config password len: {len(db.config.get('password', ''))}")
    except Exception as e:
        log(f"  [ERROR] Erreur import database: {e}")
        traceback.print_exc()
    
    # 6. Test connexion PostgreSQL
    log("")
    log("[DB] Test connexion PostgreSQL:")
    try:
        from database import DatabaseManager
        db = DatabaseManager()
        
        log("  Tentative de connexion...")
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 as test, current_database(), current_user")
                result = cur.fetchone()
                log(f"  [OK] Connexion réussie: test={result[0]}, db={result[1]}, user={result[2]}")
                
                # Test table propositions_enrichissement
                cur.execute("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name = 'propositions_enrichissement'
                """)
                table_exists = cur.fetchone()[0] > 0
                log(f"  Table propositions_enrichissement existe: {table_exists}")
                
                if table_exists:
                    cur.execute("SELECT COUNT(*) FROM propositions_enrichissement")
                    count = cur.fetchone()[0]
                    log(f"  Nombre de propositions existantes: {count}")
                    
                    # Test écriture (INSERT puis DELETE)
                    log("  Test écriture (INSERT temporaire)...")
                    cur.execute("""
                        INSERT INTO propositions_enrichissement 
                        (etablissement_id, type_cible, action, source, review_note)
                        VALUES ('00000000-0000-0000-0000-000000000000', 'test', 'test', 'debug_cloudrun', 'Test depuis Cloud Run')
                        RETURNING id
                    """)
                    test_id = cur.fetchone()[0]
                    log(f"  [OK] INSERT réussi, id={test_id}")
                    
                    # Rollback pour ne pas polluer la base
                    conn.rollback()
                    log("  [OK] ROLLBACK effectué (test non persisté)")
                    
    except Exception as e:
        log(f"  [ERROR] Erreur connexion DB: {e}")
        traceback.print_exc()
    
    # 7. Test chargement établissements dept 21
    log("")
    log("[ETAB] Test chargement établissements département 21:")
    try:
        from database import DatabaseManager
        db = DatabaseManager()
        
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                # Requête similaire à celle du script principal
                cur.execute("""
                    SELECT COUNT(*) FROM etablissements e
                    WHERE (e.departement = '21' OR e.departement ILIKE '%%(21)%%')
                """)
                count = cur.fetchone()[0]
                log(f"  Etablissements dept 21: {count}")
                
                if count > 0:
                    cur.execute("""
                        SELECT e.id, e.nom, e.commune 
                        FROM etablissements e
                        WHERE (e.departement = '21' OR e.departement ILIKE '%%(21)%%')
                        ORDER BY RANDOM()
                        LIMIT 3
                    """)
                    rows = cur.fetchall()
                    for row in rows:
                        log(f"    - {row[0][:8]}... | {row[1][:30]} | {row[2]}")
    except Exception as e:
        log(f"  [ERROR] Erreur chargement etablissements: {e}")
        traceback.print_exc()
    
    log("")
    log("="*80)
    log("DEBUG CLOUD RUN - END (SUCCESS)")
    log("="*80)
    
    return 0

if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(line_buffering=True)
        sys.stderr.reconfigure(line_buffering=True)
    except Exception:
        pass
    
    exit_code = 0
    try:
        exit_code = main()
    except Exception as e:
        log(f"[FATAL] Exception non gérée: {e}")
        traceback.print_exc()
        exit_code = 1
    
    sys.exit(exit_code)
