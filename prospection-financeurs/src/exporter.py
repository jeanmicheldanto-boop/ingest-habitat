"""
Export des résultats en JSON complet et CSV aplati.
"""
import csv
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def build_record(
    entity: dict[str, Any],
    entity_type: str,
    contact: dict[str, Any],
    pattern_info: dict[str, Any],
) -> dict[str, Any]:
    """Construit le record JSON complet pour un contact."""
    return {
        "entite": {
            "type": entity_type,
            "code": entity.get("code", ""),
            "nom": entity.get("nom", ""),
            "nom_complet": entity.get("nom_complet", ""),
            "site_web": entity.get("site_web", ""),
            "domaine_email": entity.get("domaine_email", ""),
            "pattern_email": pattern_info.get("pattern", ""),
            "pattern_confiance": pattern_info.get("confiance", ""),
        },
        "contact": {
            "nom_complet": contact.get("nom_complet", ""),
            "prenom": contact.get("prenom", ""),
            "nom": contact.get("nom", ""),
            "poste_exact": contact.get("poste_exact", ""),
            "niveau": contact.get("niveau", ""),
            "email_principal": contact.get("email_principal"),
            "email_variantes": contact.get("email_variantes", []),
            "linkedin_url": contact.get("linkedin_url"),
            "source_nom": contact.get("source_nom", ""),
            "confiance_nom": contact.get("confiance_nom", "basse"),
            "confiance_email": contact.get("confiance_email", "basse"),
            "date_extraction": date.today().isoformat(),
        },
    }


def flatten_record(record: dict[str, Any]) -> dict[str, str]:
    """Aplatit un record JSON pour le CSV."""
    e = record["entite"]
    c = record["contact"]
    variantes = c.get("email_variantes", [])
    return {
        "type_entite": e.get("type", ""),
        "code_entite": e.get("code", ""),
        "nom_entite": e.get("nom_complet", ""),
        "domaine_email": e.get("domaine_email", ""),
        "nom_complet": c.get("nom_complet", ""),
        "poste": c.get("poste_exact", ""),
        "niveau": c.get("niveau", ""),
        "email": c.get("email_principal", "") or "",
        "email_variante_1": variantes[0] if len(variantes) > 0 else "",
        "email_variante_2": variantes[1] if len(variantes) > 1 else "",
        "email_variante_3": variantes[2] if len(variantes) > 2 else "",
        "linkedin": c.get("linkedin_url", "") or "",
        "confiance_nom": c.get("confiance_nom", ""),
        "confiance_email": c.get("confiance_email", ""),
        "source": c.get("source_nom", ""),
        "date_extraction": c.get("date_extraction", ""),
    }


class Exporter:
    """
    Gère l'export JSON et CSV des résultats.
    Supporte l'écriture incrémentale (append) pour les longs runs.
    """

    CSV_FIELDS = [
        "type_entite", "code_entite", "nom_entite", "domaine_email",
        "nom_complet", "poste", "niveau", "email",
        "email_variante_1", "email_variante_2", "email_variante_3",
        "linkedin", "confiance_nom", "confiance_email", "source", "date_extraction",
    ]

    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.json_path = self.output_dir / "contacts_enrichis.json"
        self.csv_path = self.output_dir / "contacts_enrichis.csv"
        self._records: list[dict] = []
        self._csv_initialized = False

        # Charger les enregistrements existants si le fichier existe
        if self.json_path.exists():
            try:
                with open(self.json_path, encoding="utf-8") as f:
                    self._records = json.load(f)
                logger.info("Reprise : %d contacts existants chargés", len(self._records))
            except Exception:
                self._records = []

        # Ne pas réécrire l'en-tête si le CSV existe déjà (mode reprise)
        if self.csv_path.exists():
            self._csv_initialized = True

    def add_contact(
        self,
        entity: dict[str, Any],
        entity_type: str,
        contact: dict[str, Any],
        pattern_info: dict[str, Any],
    ) -> None:
        """Ajoute un contact et sauvegarde immédiatement."""
        record = build_record(entity, entity_type, contact, pattern_info)
        self._records.append(record)
        self._save_json()
        self._append_csv(flatten_record(record))

    def _save_json(self) -> None:
        with open(self.json_path, "w", encoding="utf-8") as f:
            json.dump(self._records, f, ensure_ascii=False, indent=2)

    def _append_csv(self, row: dict[str, str]) -> None:
        write_header = not self.csv_path.exists() or not self._csv_initialized
        with open(self.csv_path, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=self.CSV_FIELDS, extrasaction="ignore")
            if write_header:
                writer.writeheader()
                self._csv_initialized = True
            writer.writerow(row)

    def finalize(self) -> None:
        """Sauvegarde finale : réécrit le JSON et le CSV complets à partir de _records."""
        self._save_json()
        self._rewrite_csv()
        logger.info(
            "Export terminé : %d contacts → %s / %s",
            len(self._records),
            self.json_path,
            self.csv_path,
        )

    def _rewrite_csv(self) -> None:
        """Écrit le CSV complet depuis tous les records (sans doublons d'en-tête)."""
        # Déduplication sur (type:code, nom_complet) — garde le dernier en date
        seen: dict[str, dict] = {}
        for r in self._records:
            key = f"{r['entite']['type']}:{r['entite']['code']}:{r['contact']['nom_complet']}"
            seen[key] = flatten_record(r)
        rows = list(seen.values())
        with open(self.csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=self.CSV_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        logger.debug("CSV réécrit : %d lignes (après dédoublonnage)", len(rows))

    @property
    def total_contacts(self) -> int:
        return len(self._records)

    def get_processed_entity_keys(self) -> set[str]:
        """Retourne les clés (type:code) des entités déjà exportées."""
        return {
            f"{r['entite']['type']}:{r['entite']['code']}"
            for r in self._records
        }
