"""
Configuration du pipeline de prospection.
Les clés API sont chargées depuis les variables d'environnement ou un fichier .env.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent

# Charger le .env du sous-projet s'il existe, sinon fallback sur le .env racine du repo
_env_local = BASE_DIR / ".env"
_env_root  = BASE_DIR.parent / ".env"
if _env_local.exists():
    load_dotenv(_env_local)
elif _env_root.exists():
    load_dotenv(_env_root)

# ── Base de données Supabase (mêmes credentials que le projet principal) ──────
# Ces variables sont partagées avec le repo ingest-habitat (DB_HOST etc.)
DB_HOST: str     = os.getenv("DB_HOST",     "db.minwoumfgutampcgrcbr.supabase.co")
DB_NAME: str     = os.getenv("DB_NAME",     "postgres")
DB_USER: str     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")   # Obligatoire — ne jamais coder en dur
DB_PORT: int     = int(os.getenv("DB_PORT", "5432"))

# ── Clés API ──────────────────────────────────────────────────────────────────
SERPER_API_KEY: str = os.environ["SERPER_API_KEY"]
MISTRAL_API_KEY: str = os.environ["MISTRAL_API_KEY"]

# ── Modèle Mistral ────────────────────────────────────────────────────────────
MISTRAL_MODEL: str = os.getenv("MISTRAL_MODEL", "mistral-small-latest")

# ── Rate limiting ─────────────────────────────────────────────────────────────
SERPER_DELAY_SECONDS: float = float(os.getenv("SERPER_DELAY_SECONDS", "1.0"))
MISTRAL_DELAY_SECONDS: float = float(os.getenv("MISTRAL_DELAY_SECONDS", "1.0"))
MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BASE_DELAY: float = float(os.getenv("RETRY_BASE_DELAY", "2.0"))  # backoff exponentiel
SERPER_TIMEOUT: int = int(os.getenv("SERPER_TIMEOUT", "30"))
MISTRAL_TIMEOUT: int = int(os.getenv("MISTRAL_TIMEOUT", "60"))

# ── Chemins ───────────────────────────────────────────────────────────────────
CONFIG_DIR = BASE_DIR / "config"
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = DATA_DIR / "output"
LOGS_DIR = BASE_DIR / "logs"
PROGRESS_FILE = DATA_DIR / "progress.json"
EMAIL_CACHE_FILE = DATA_DIR / "email_patterns_cache.json"

# ── Options pipeline ──────────────────────────────────────────────────────────
# Types d'entités à traiter : "departement", "dirpjj", "ars"
ENTITY_TYPES: list[str] = os.getenv("ENTITY_TYPES", "departement,dirpjj,ars").split(",")

# Validation croisée email (optionnel, consomme des requêtes Serper supplémentaires)
VALIDATE_EMAILS: bool = os.getenv("VALIDATE_EMAILS", "true").lower() == "true"

# Nombre max de contacts par entité (0 = pas de limite)
MAX_CONTACTS_PER_ENTITY: int = int(os.getenv("MAX_CONTACTS_PER_ENTITY", "5"))
