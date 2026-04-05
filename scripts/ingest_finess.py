"""Ingestion FINESS CSV → tables Supabase.

EXÉCUTION LOCALE UNIQUEMENT — Ce script n'est jamais copié dans l'image Docker.

Il lit les 3 fichiers CSV etalab FINESS, filtre optionnellement par département,
et insère les données brutes dans finess_gestionnaire + finess_etablissement.
En option, il croise les gestionnaires avec le fichier des 250 contacts déjà prospectés.

Usage :
    # Ingestion département 65 avec tag 250 contacts
    python scripts/ingest_finess.py \\
        --departement 65 \\
        --prospection-250 outputs/prospection_250_FINAL_FORMATE_V2.xlsx

    # Ingestion nationale (tous départements)
    python scripts/ingest_finess.py

    # Création des tables uniquement (pas d'ingestion)
    python scripts/ingest_finess.py --create-tables-only
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras

# -- Repo root setup ---------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from database import DatabaseManager
from enrich_finess_config import (
    CATEGORIE_NORMALISEE,
    SECTEUR_PAR_CATEGORIE,
    FINANCEUR_PAR_CATEGORIE,
    TARIFICATION_PAR_CATEGORIE,
    SQL_CREATE_TABLES,
)

# Optional for 250-contacts tagging
try:
    import pandas as pd  # type: ignore
except ImportError:
    pd = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# CSV parsing helpers (adapted from export_gestionnaires_essms_gt10.py)
# ---------------------------------------------------------------------------

def _join(parts: list[str], sep: str = " ") -> str:
    return sep.join([p.strip() for p in parts if p and p.strip()])


def _col(row: list[str], idx: int) -> str:
    """Safely read a CSV column."""
    return row[idx].strip() if len(row) > idx else ""


# ---------------------------------------------------------------------------
# Read EJ (gestionnaires) from cs1100501
# ---------------------------------------------------------------------------

def read_gestionnaires(
    path_cs1100501: Path,
    dept_filter: Optional[str] = None,
) -> dict[str, dict]:
    """Parse structureej records from cs1100501.

    Returns dict keyed by finess_ej.
    """
    result: dict[str, dict] = {}
    with path_cs1100501.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        for row in reader:
            if not row or row[0] != "structureej":
                continue
            finess_ej = _col(row, 1)
            if not finess_ej:
                continue

            dept_code = _col(row, 13)
            # We'll decide dept filtering later (some EJ outside dept manage ET inside)

            rs_short = _col(row, 2)
            rs_long = _col(row, 3)
            sigle = _col(row, 4)
            numvoie = _col(row, 5)
            typevoie = _col(row, 6)
            libvoie = _col(row, 7)
            compl1 = _col(row, 8)
            compl2 = _col(row, 9)
            compl3 = _col(row, 10)
            cp_ville = _col(row, 12)
            dept_nom = _col(row, 14)
            telephone = _col(row, 15)
            forme_jur_code = _col(row, 16)
            forme_jur_lib = _col(row, 17)
            siren = _col(row, 20)

            # Extraire code postal et commune
            code_postal = ""
            commune = ""
            if cp_ville:
                parts = cp_ville.split(" ", 1)
                code_postal = parts[0] if parts else ""
                commune = parts[1] if len(parts) > 1 else ""

            voie = _join([numvoie, typevoie, libvoie])
            complement = _join([compl1, compl2, compl3], sep=", ")
            adresse_complete = _join([voie, complement, cp_ville], sep=", ")

            result[finess_ej] = {
                "id_gestionnaire": finess_ej,
                "siren": siren or None,
                "raison_sociale": rs_long or rs_short,
                "sigle": sigle or None,
                "forme_juridique_code": forme_jur_code or None,
                "forme_juridique_libelle": forme_jur_lib or None,
                "adresse_numero": numvoie or None,
                "adresse_type_voie": typevoie or None,
                "adresse_lib_voie": libvoie or None,
                "adresse_complement": complement or None,
                "adresse_complete": adresse_complete or None,
                "code_postal": code_postal or None,
                "commune": commune or None,
                "departement_code": dept_code or None,
                "departement_nom": dept_nom or None,
                "telephone": telephone or None,
            }
    return result


# ---------------------------------------------------------------------------
# Read ET→EJ mapping + ESSMS set from cs1100505
# ---------------------------------------------------------------------------

def read_mapping_et_to_ej(path_cs1100505: Path) -> tuple[dict[str, str], set[str]]:
    """Returns (mapping_et_to_ej, essms_et_ids).

    mapping_et_to_ej: finess_et → finess_ej
    essms_et_ids: set of finess_et from equipementsocial records
    """
    mapping: dict[str, str] = {}
    essms: set[str] = set()
    with path_cs1100505.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        for row in reader:
            if not row:
                continue
            if row[0] == "structureet":
                et = _col(row, 1)
                ej = _col(row, 2)
                if et and ej:
                    mapping[et] = ej
            elif row[0] == "equipementsocial":
                et = _col(row, 1)
                if et:
                    essms.add(et)
    return mapping, essms


# ---------------------------------------------------------------------------
# Read ET details from cs1100502
# ---------------------------------------------------------------------------

def read_etablissements(
    path_cs1100502: Path,
    mapping_et_to_ej: dict[str, str],
    dept_filter: Optional[str] = None,
) -> dict[str, dict]:
    """Parse structureet records from cs1100502.

    Returns dict keyed by finess_et.
    """
    result: dict[str, dict] = {}
    with path_cs1100502.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        for row in reader:
            if not row or row[0] != "structureet":
                continue
            finess_et = _col(row, 1)
            if not finess_et:
                continue

            dept_code = _col(row, 13)
            if dept_filter and dept_code != dept_filter:
                continue

            rs_short = _col(row, 2)
            rs_long = _col(row, 3)
            sigle = _col(row, 4)
            numvoie = _col(row, 5)
            typevoie = _col(row, 6)
            libvoie = _col(row, 7)
            compl1 = _col(row, 8)
            compl2 = _col(row, 9)
            compl3 = _col(row, 10)
            cp_ville = _col(row, 12)
            dept_nom = _col(row, 14)
            telephone = _col(row, 15)
            cat_code = _col(row, 18)
            cat_lib = _col(row, 19)
            grp_code = _col(row, 20)
            grp_lib = _col(row, 21)

            code_postal = ""
            commune = ""
            if cp_ville:
                parts = cp_ville.split(" ", 1)
                code_postal = parts[0] if parts else ""
                commune = parts[1] if len(parts) > 1 else ""

            voie = _join([numvoie, typevoie, libvoie])
            complement = _join([compl1, compl2, compl3], sep=", ")
            adresse_complete = _join([voie, complement, cp_ville], sep=", ")

            # Catégorie normalisée — ne garder que les ESMS (médico-social + social)
            cat_norm = CATEGORIE_NORMALISEE.get(cat_lib)
            if cat_norm is None:
                # Établissement sanitaire ou catégorie non médico-sociale → on l'exclut
                continue

            # Financeur
            financeur = FINANCEUR_PAR_CATEGORIE.get(cat_norm or "", {})
            financeur_principal = financeur.get("principal")
            financeur_secondaire = financeur.get("secondaire")

            # Tarification
            tarification = TARIFICATION_PAR_CATEGORIE.get(cat_norm or "")

            # Secteur d'activité
            secteur = SECTEUR_PAR_CATEGORIE.get(cat_norm or "")

            # EJ mapping
            ej = mapping_et_to_ej.get(finess_et)

            result[finess_et] = {
                "id_finess": finess_et,
                "id_gestionnaire": ej,
                "raison_sociale": rs_long or rs_short,
                "sigle": sigle or None,
                "categorie_code": cat_code or None,
                "categorie_libelle": cat_lib or None,
                "categorie_normalisee": cat_norm,
                "groupe_code": grp_code or None,
                "groupe_libelle": grp_lib or None,
                "secteur_activite": secteur,
                "financeur_principal": financeur_principal,
                "financeur_secondaire": financeur_secondaire,
                "type_tarification": tarification,
                "adresse_numero": numvoie or None,
                "adresse_type_voie": typevoie or None,
                "adresse_lib_voie": libvoie or None,
                "adresse_complement": complement or None,
                "adresse_complete": adresse_complete or None,
                "code_postal": code_postal or None,
                "commune": commune or None,
                "departement_code": dept_code or None,
                "departement_nom": dept_nom or None,
                "telephone": telephone or None,
            }
    return result


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------

def create_tables(cur) -> None:
    """Create all finess_* tables if they don't exist."""
    cur.execute(SQL_CREATE_TABLES)
    print("[SQL] Tables finess_* créées / vérifiées.")


def _size_bucket(nb: int) -> str:
    if nb > 100:
        return ">100"
    if nb > 50:
        return ">50"
    if nb > 20:
        return ">20"
    if nb > 10:
        return ">10"
    return "<=10"


def upsert_gestionnaires(
    cur,
    gestionnaires: dict[str, dict],
    etablissements: dict[str, dict],
    essms_ids: set[str],
) -> int:
    """UPSERT gestionnaires into finess_gestionnaire. Returns count inserted."""
    # Compute nb_etablissements and nb_essms per gestionnaire
    et_by_ej: dict[str, list[str]] = {}
    essms_by_ej: dict[str, list[str]] = {}
    for et_id, et in etablissements.items():
        ej = et.get("id_gestionnaire")
        if ej:
            et_by_ej.setdefault(ej, []).append(et_id)
            if et_id in essms_ids:
                essms_by_ej.setdefault(ej, []).append(et_id)

    # Determine dominant type per gestionnaire
    dominant_by_ej: dict[str, str] = {}
    for ej, ets in et_by_ej.items():
        cat_counter: Counter[str] = Counter()
        for et_id in ets:
            cat = (etablissements.get(et_id) or {}).get("categorie_normalisee") or "AUTRE"
            cat_counter[cat] += 1
        if cat_counter:
            dominant_by_ej[ej] = cat_counter.most_common(1)[0][0]

    # Filter to only gestionnaires that manage at least 1 ET in our dataset
    relevant_ej_ids = set(et_by_ej.keys())
    count = 0

    for ej_id in relevant_ej_ids:
        g = gestionnaires.get(ej_id)
        if not g:
            # EJ outside dept but manages ET inside → create minimal record
            g = {
                "id_gestionnaire": ej_id,
                "raison_sociale": f"EJ {ej_id} (hors référentiel filtré)",
            }

        nb_etab = len(et_by_ej.get(ej_id, []))
        nb_essms = len(essms_by_ej.get(ej_id, []))
        dominant = dominant_by_ej.get(ej_id)
        taille = _size_bucket(nb_essms)

        # Secteur principal du gestionnaire
        secteur_principal = _compute_secteur_gestionnaire(ej_id, etablissements)

        cur.execute("""
            INSERT INTO finess_gestionnaire (
                id_gestionnaire, siren, raison_sociale, sigle,
                forme_juridique_code, forme_juridique_libelle,
                adresse_numero, adresse_type_voie, adresse_lib_voie,
                adresse_complement, adresse_complete,
                code_postal, commune, departement_code, departement_nom,
                telephone,
                nb_etablissements, nb_essms, categorie_taille, dominante_type,
                secteur_activite_principal
            ) VALUES (
                %(id_gestionnaire)s, %(siren)s, %(raison_sociale)s, %(sigle)s,
                %(forme_juridique_code)s, %(forme_juridique_libelle)s,
                %(adresse_numero)s, %(adresse_type_voie)s, %(adresse_lib_voie)s,
                %(adresse_complement)s, %(adresse_complete)s,
                %(code_postal)s, %(commune)s, %(departement_code)s, %(departement_nom)s,
                %(telephone)s,
                %(nb_etablissements)s, %(nb_essms)s, %(categorie_taille)s, %(dominante_type)s,
                %(secteur_activite_principal)s
            )
            ON CONFLICT (id_gestionnaire) DO UPDATE SET
                raison_sociale = EXCLUDED.raison_sociale,
                siren = COALESCE(EXCLUDED.siren, finess_gestionnaire.siren),
                sigle = COALESCE(EXCLUDED.sigle, finess_gestionnaire.sigle),
                forme_juridique_code = COALESCE(EXCLUDED.forme_juridique_code, finess_gestionnaire.forme_juridique_code),
                forme_juridique_libelle = COALESCE(EXCLUDED.forme_juridique_libelle, finess_gestionnaire.forme_juridique_libelle),
                adresse_complete = COALESCE(EXCLUDED.adresse_complete, finess_gestionnaire.adresse_complete),
                code_postal = COALESCE(EXCLUDED.code_postal, finess_gestionnaire.code_postal),
                commune = COALESCE(EXCLUDED.commune, finess_gestionnaire.commune),
                departement_code = COALESCE(EXCLUDED.departement_code, finess_gestionnaire.departement_code),
                departement_nom = COALESCE(EXCLUDED.departement_nom, finess_gestionnaire.departement_nom),
                telephone = COALESCE(EXCLUDED.telephone, finess_gestionnaire.telephone),
                nb_etablissements = EXCLUDED.nb_etablissements,
                nb_essms = EXCLUDED.nb_essms,
                categorie_taille = EXCLUDED.categorie_taille,
                dominante_type = EXCLUDED.dominante_type,
                secteur_activite_principal = EXCLUDED.secteur_activite_principal,
                date_ingestion = NOW()
        """, {
            **g,
            "nb_etablissements": nb_etab,
            "nb_essms": nb_essms,
            "categorie_taille": taille,
            "dominante_type": dominant,
            "secteur_activite_principal": secteur_principal,
        })
        count += 1

    return count


def _compute_secteur_gestionnaire(ej_id: str, etablissements: dict[str, dict]) -> Optional[str]:
    """Compute the dominant secteur for a gestionnaire from its etablissements."""
    secteurs: list[str] = []
    for et in etablissements.values():
        if et.get("id_gestionnaire") == ej_id and et.get("secteur_activite"):
            secteurs.append(et["secteur_activite"])
    if not secteurs:
        return None

    compteur = Counter(secteurs)
    total = len(secteurs)
    top_secteur, top_count = compteur.most_common(1)[0]

    if top_count / total >= 0.7:
        return top_secteur

    significatifs = [s for s, c in compteur.items() if c / total >= 0.3]
    if len(significatifs) >= 2:
        return "Multi-secteurs"

    return top_secteur


def upsert_etablissements(
    cur,
    etablissements: dict[str, dict],
    essms_ids: set[str],
) -> int:
    """UPSERT etablissements into finess_etablissement. Returns count inserted."""
    count = 0
    for et_id, et in etablissements.items():
        cur.execute("""
            INSERT INTO finess_etablissement (
                id_finess, id_gestionnaire, raison_sociale, sigle,
                categorie_code, categorie_libelle, categorie_normalisee,
                groupe_code, groupe_libelle, secteur_activite,
                financeur_principal, financeur_secondaire, type_tarification,
                adresse_numero, adresse_type_voie, adresse_lib_voie,
                adresse_complement, adresse_complete,
                code_postal, commune, departement_code, departement_nom,
                telephone
            ) VALUES (
                %(id_finess)s, %(id_gestionnaire)s, %(raison_sociale)s, %(sigle)s,
                %(categorie_code)s, %(categorie_libelle)s, %(categorie_normalisee)s,
                %(groupe_code)s, %(groupe_libelle)s, %(secteur_activite)s,
                %(financeur_principal)s, %(financeur_secondaire)s, %(type_tarification)s,
                %(adresse_numero)s, %(adresse_type_voie)s, %(adresse_lib_voie)s,
                %(adresse_complement)s, %(adresse_complete)s,
                %(code_postal)s, %(commune)s, %(departement_code)s, %(departement_nom)s,
                %(telephone)s
            )
            ON CONFLICT (id_finess) DO UPDATE SET
                raison_sociale = EXCLUDED.raison_sociale,
                id_gestionnaire = COALESCE(EXCLUDED.id_gestionnaire, finess_etablissement.id_gestionnaire),
                categorie_code = EXCLUDED.categorie_code,
                categorie_libelle = EXCLUDED.categorie_libelle,
                categorie_normalisee = EXCLUDED.categorie_normalisee,
                groupe_code = EXCLUDED.groupe_code,
                groupe_libelle = EXCLUDED.groupe_libelle,
                secteur_activite = EXCLUDED.secteur_activite,
                financeur_principal = EXCLUDED.financeur_principal,
                financeur_secondaire = EXCLUDED.financeur_secondaire,
                type_tarification = EXCLUDED.type_tarification,
                adresse_complete = COALESCE(EXCLUDED.adresse_complete, finess_etablissement.adresse_complete),
                code_postal = COALESCE(EXCLUDED.code_postal, finess_etablissement.code_postal),
                commune = COALESCE(EXCLUDED.commune, finess_etablissement.commune),
                departement_code = COALESCE(EXCLUDED.departement_code, finess_etablissement.departement_code),
                departement_nom = COALESCE(EXCLUDED.departement_nom, finess_etablissement.departement_nom),
                telephone = COALESCE(EXCLUDED.telephone, finess_etablissement.telephone),
                date_ingestion = NOW()
        """, et)
        count += 1
    return count


def tag_gestionnaires_deja_prospectes(cur, path_xlsx: str) -> int:
    """Mark gestionnaires already processed in the 250-contacts pipeline.

    The Excel file contains a 'finess_ej' column identifying each gestionnaire.
    Returns count of rows updated.
    """
    if pd is None:
        print("[WARN] pandas non installé — impossible de lire le fichier des 250 contacts")
        return 0

    path = Path(path_xlsx)
    if not path.exists():
        print(f"[WARN] Fichier des 250 contacts introuvable : {path_xlsx}")
        return 0

    df = pd.read_excel(path)
    if "finess_ej" not in df.columns:
        print("[WARN] Colonne 'finess_ej' absente du fichier des 250 contacts")
        return 0

    finess_list = df["finess_ej"].dropna().astype(str).str.strip().tolist()
    if not finess_list:
        print("[WARN] Aucun finess_ej trouvé dans le fichier des 250 contacts")
        return 0

    cur.execute("""
        UPDATE finess_gestionnaire SET
            deja_prospecte_250 = TRUE,
            deja_prospecte_250_date = NOW()
        WHERE id_gestionnaire = ANY(%s)
    """, (finess_list,))

    updated = cur.rowcount
    print(f"[TAG] {updated} gestionnaires marqués comme déjà prospectés (250 contacts)")
    return updated


# ---------------------------------------------------------------------------
# Main ingestion pipeline
# ---------------------------------------------------------------------------

def ingest_finess(
    path_cs1100501: str,
    path_cs1100502: str,
    path_cs1100505: str,
    departement_filter: Optional[str] = None,
    path_prospection_250: Optional[str] = None,
    create_tables_only: bool = False,
) -> None:
    """Main ingestion workflow."""
    db = DatabaseManager()

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            # 1. Create tables
            create_tables(cur)
            conn.commit()
            if create_tables_only:
                print("[OK] Tables créées. Fin (--create-tables-only).")
                return

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # 2. Parse CSV files
            print(f"[CSV] Lecture cs1100501 (gestionnaires) : {path_cs1100501}")
            gestionnaires = read_gestionnaires(Path(path_cs1100501))

            print(f"[CSV] Lecture cs1100505 (mapping ET→EJ + ESSMS) : {path_cs1100505}")
            mapping_et_to_ej, essms_ids = read_mapping_et_to_ej(Path(path_cs1100505))

            print(f"[CSV] Lecture cs1100502 (établissements) : {path_cs1100502}")
            etablissements = read_etablissements(
                Path(path_cs1100502),
                mapping_et_to_ej,
                dept_filter=departement_filter,
            )

            print(f"[CSV] {len(gestionnaires)} EJ lus, {len(etablissements)} ET lus, "
                  f"{len(essms_ids)} ESSMS identifiés")

            if departement_filter:
                print(f"[FILTRE] Département {departement_filter} : {len(etablissements)} établissements")

            # 3. UPSERT gestionnaires (only those managing ET in our dataset)
            nb_gest = upsert_gestionnaires(cur, gestionnaires, etablissements, essms_ids)
            print(f"[DB] {nb_gest} gestionnaires upsertés")

            # 4. UPSERT établissements
            nb_etab = upsert_etablissements(cur, etablissements, essms_ids)
            print(f"[DB] {nb_etab} établissements upsertés")

            # 5. Tag 250 contacts
            if path_prospection_250:
                tag_gestionnaires_deja_prospectes(cur, path_prospection_250)

            conn.commit()

    # Summary
    print("\n" + "=" * 60)
    print("INGESTION TERMINÉE")
    print(f"  Gestionnaires : {nb_gest}")
    print(f"  Établissements : {nb_etab}")
    if departement_filter:
        print(f"  Département filtré : {departement_filter}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingestion FINESS CSV → Supabase (exécution LOCALE uniquement)"
    )
    parser.add_argument(
        "--cs1100501",
        default="database/etalab-cs1100501-stock-20260107-0343.csv",
        help="Chemin vers cs1100501 (structureej)",
    )
    parser.add_argument(
        "--cs1100502",
        default="database/etalab-cs1100502-stock-20260107-0343.csv",
        help="Chemin vers cs1100502 (structureet details)",
    )
    parser.add_argument(
        "--cs1100505",
        default="database/etalab-cs1100505-stock-20260107-0343.csv",
        help="Chemin vers cs1100505 (equipementsocial + mapping ET→EJ)",
    )
    parser.add_argument(
        "--departement",
        default=None,
        help="Code département pour filtrer (ex: 65). Tous si omis.",
    )
    parser.add_argument(
        "--prospection-250",
        default=None,
        help="Chemin vers le fichier Excel des 250 gestionnaires déjà prospectés",
    )
    parser.add_argument(
        "--create-tables-only",
        action="store_true",
        help="Créer les tables puis quitter (pas d'ingestion)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ingest_finess(
        path_cs1100501=args.cs1100501,
        path_cs1100502=args.cs1100502,
        path_cs1100505=args.cs1100505,
        departement_filter=args.departement,
        path_prospection_250=args.prospection_250,
        create_tables_only=args.create_tables_only,
    )


if __name__ == "__main__":
    main()
