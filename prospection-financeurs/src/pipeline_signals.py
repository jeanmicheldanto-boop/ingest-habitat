"""
Pipeline signaux — génération des signaux de tension tarification/financement ESSMS
pour les Conseils Départementaux.

Usage :
  # Tous les départements (qui n'ont pas encore de signal)
  python prospection-financeurs/src/pipeline_signals.py

  # Passe complète sur tout le monde
  python prospection-financeurs/src/pipeline_signals.py --force

  # Uniquement certains départements
  python prospection-financeurs/src/pipeline_signals.py --codes 22 75 13

  # Sortie JSON dans un répertoire spécifique
  python prospection-financeurs/src/pipeline_signals.py --out-dir outputs/signals

Les fichiers JSON produits : outputs/signals/dept_{code}_signals.json
"""
import argparse
import json
import logging
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

# Ajouter les deux niveaux au path (ordre important : prospection-financeurs/ doit être avant ingest-habitat/)
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent.parent))   # ingest-habitat/ (en dernier → position 1)
sys.path.insert(0, str(_HERE.parent))          # prospection-financeurs/ (en premier → position 0)

from config.settings import SERPER_DELAY_SECONDS
from src.serper_client import SerperClient
from src.mistral_client import MistralClient
from src.signals_finder import SignalsFinder
from src.supabase_db import ProspectionDB

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("pipeline_signals")

# ── Chemins ───────────────────────────────────────────────────────────────────
_CONFIG_DIR = _HERE.parent / "config"
_DEPT_FILE = _CONFIG_DIR / "departements.json"


def _load_departements() -> list[dict]:
    with open(_DEPT_FILE, encoding="utf-8") as f:
        return json.load(f)


def _load_or_filter(all_depts: list[dict], codes: list[str] | None) -> list[dict]:
    """Retourne la liste filtrée par codes si fournis, sinon tout."""
    if not codes:
        return all_depts
    codes_set = {str(c).upper() for c in codes}
    filtered = [d for d in all_depts if str(d.get("code", "")).upper() in codes_set]
    if not filtered:
        logger.warning("Aucun département trouvé pour les codes : %s", codes)
    return filtered


def _save_signal_json(signal: dict[str, Any], out_dir: Path, code: str) -> Path:
    """Écrit le signal dans outputs/signals/dept_{code}_signals.json."""
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"dept_{code}_signals.json"

    # Si le fichier existe, on ajoute le signal à l'historique
    if path.exists():
        try:
            with open(path, encoding="utf-8") as f:
                existing = json.load(f)
            if isinstance(existing, list):
                history = existing
            else:
                history = [existing]
        except Exception:
            history = []
    else:
        history = []

    history.append(signal)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    return path


def run(
    codes: list[str] | None = None,
    force: bool = False,
    out_dir: Path | None = None,
) -> None:
    """
    Génère les signaux pour les départements spécifiés (ou tous si non spécifiés).

    Args:
        codes   : liste de codes département à traiter (None = tous)
        force   : si True, régénère même si un signal existe déjà
        out_dir : répertoire de sortie JSON (défaut : prospection-financeurs/outputs/signals)
    """
    out_dir = out_dir or (_HERE.parent / "outputs" / "signals")
    all_depts = _load_departements()
    depts = _load_or_filter(all_depts, codes)

    logger.info("=== Pipeline signaux démarré : %d département(s) à traiter ===", len(depts))

    serper = SerperClient()
    mistral = MistralClient()
    finder = SignalsFinder(serper_client=serper, mistral_client=mistral)

    stats = {"traites": 0, "skipped": 0, "erreurs": 0}

    with ProspectionDB() as db:
        db.create_tables()

        already_done = db.get_processed_signal_codes() if not force else set()
        if already_done:
            logger.info("%d département(s) déjà traités (--force pour recalculer)", len(already_done))

        for dept in depts:
            code = str(dept.get("code", ""))
            nom = dept.get("nom", "")
            nom_complet = dept.get("nom_complet", f"Conseil Départemental {nom}")

            if code in already_done:
                logger.info("  [SKIP] %s (%s) — signal déjà présent", nom, code)
                stats["skipped"] += 1
                continue

            logger.info("  [→] %s (%s)", nom_complet, code)

            # Récupérer l'entite_id depuis la DB (doit exister si les contacts ont déjà été traités)
            entite_id = db.get_entite_id("departement", code)
            if entite_id is None:
                # Upsert minimal de l'entité si elle n'existe pas encore
                entite_id = db.upsert_entite(
                    entity={
                        "code": code,
                        "nom": nom,
                        "nom_complet": nom_complet,
                        "site_web": dept.get("site_web", ""),
                        "domaine_email": dept.get("domaine_email", ""),
                        "note": "",
                    },
                    entity_type="departement",
                )
                logger.debug("  Entité créée à la volée : id=%d", entite_id)

            try:
                signal = finder.find_signals(entity_name=nom, code=code)
                signal_id = db.upsert_signal(entite_id=entite_id, signal=signal)

                # Export JSON
                json_path = _save_signal_json(signal, out_dir, code)

                logger.info(
                    "  [OK] %s — alerte=%s | tags=%s | %d snippets | → %s",
                    nom,
                    signal.get("niveau_alerte", "?"),
                    ",".join(signal.get("tags", [])) or "(aucun)",
                    signal.get("nb_resultats_serper", 0),
                    json_path.name,
                )
                stats["traites"] += 1

            except Exception as exc:
                logger.error("  [ERR] %s (%s) : %s", nom, code, exc)
                stats["erreurs"] += 1

            # Délai entre départements (même pool de rate limit que le pipeline contacts)
            time.sleep(SERPER_DELAY_SECONDS)

    logger.info(
        "=== Pipeline signaux terminé : %d traités | %d skipped | %d erreurs ===",
        stats["traites"], stats["skipped"], stats["erreurs"],
    )
    logger.info("Requêtes Serper : %d | Requêtes Mistral : %d",
                serper.request_count, mistral.request_count)


# ── Entrypoint ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Génère les signaux de tension tarification/financement ESSMS par département",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python prospection-financeurs/src/pipeline_signals.py --codes 22
  python prospection-financeurs/src/pipeline_signals.py --codes 22 75 13 --force
  python prospection-financeurs/src/pipeline_signals.py --out-dir /tmp/signals
        """,
    )
    parser.add_argument(
        "--codes",
        nargs="+",
        metavar="CODE",
        help="Codes département à traiter (ex: 22 75 13). Défaut : tous.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Régénère les signaux même si déjà présents en base.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Répertoire de sortie JSON. Défaut : prospection-financeurs/outputs/signals/",
    )

    args = parser.parse_args()
    run(codes=args.codes, force=args.force, out_dir=args.out_dir)


if __name__ == "__main__":
    main()
