"""Audit read-only des tables/champs d'enrichissement.

Objectif:
- Confirmer la structure réelle de la base (information_schema)
- Mesurer le taux de remplissage des champs clés à enrichir

Usage:
  C:/.../.venv/Scripts/python.exe scripts/audit_enrichment_readonly.py
  C:/.../.venv/Scripts/python.exe scripts/audit_enrichment_readonly.py --departements 45,76

Ce script ne modifie jamais la base.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

import psycopg2

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from database import DatabaseManager


TABLES_TO_AUDIT = [
    "etablissements",
    "avp_infos",
    "tarifications",
    "logements_types",
    "restaurations",
    "services",
    "etablissement_service",
    "sous_categories",
    "etablissement_sous_categorie",
    "propositions",
    "proposition_items",
]


def _parse_departements_arg(value: str | None) -> list[str]:
    if not value:
        return []
    parts = [p.strip() for p in value.split(",")]
    return [p for p in parts if p]


def _print_table_structure(cur, table_name: str) -> None:
    cur.execute(
        """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s
        ORDER BY ordinal_position
        """,
        (table_name,),
    )
    rows = cur.fetchall()

    if not rows:
        print(f"\n🧱 {table_name}: ❌ table absente (ou non visible)")
        return

    print(f"\n🧱 {table_name}: {len(rows)} colonnes")
    for col, dtype, nullable, default in rows:
        default_s = "" if default is None else str(default)
        nullable_s = "NULL" if nullable == "YES" else "NOT NULL"
        if default_s:
            print(f"  - {col} ({dtype}, {nullable_s}, default={default_s})")
        else:
            print(f"  - {col} ({dtype}, {nullable_s})")


def _safe_pct(n: int, d: int) -> float:
    return round(100.0 * n / d, 1) if d else 0.0


def _audit_global_counts(cur) -> None:
    cur.execute("SELECT COUNT(*) FROM etablissements WHERE is_test=false")
    total_etab = cur.fetchone()[0]
    print("\n📊 Comptages globaux")
    print(f"- etablissements (is_test=false): {total_etab:,}")

    cur.execute(
        """
        SELECT
          COUNT(*) FILTER (WHERE COALESCE(NULLIF(TRIM(public_cible),''), NULL) IS NOT NULL) AS with_public_cible,
          COUNT(*) FILTER (WHERE habitat_type IS NOT NULL) AS with_habitat_type,
          COUNT(*) FILTER (WHERE eligibilite_statut IS NOT NULL) AS with_eligibilite_statut,
          COUNT(*) FILTER (WHERE COALESCE(NULLIF(TRIM(site_web),''), NULL) IS NOT NULL) AS with_site_web,
          COUNT(*) FILTER (WHERE COALESCE(NULLIF(TRIM(email),''), NULL) IS NOT NULL) AS with_email,
          COUNT(*) FILTER (WHERE COALESCE(NULLIF(TRIM(telephone),''), NULL) IS NOT NULL) AS with_telephone,
          COUNT(*) FILTER (WHERE COALESCE(NULLIF(TRIM(presentation),''), NULL) IS NOT NULL) AS with_presentation
        FROM etablissements
        WHERE is_test=false
        """
    )
    (
        with_public_cible,
        with_habitat_type,
        with_eligibilite_statut,
        with_site_web,
        with_email,
        with_telephone,
        with_presentation,
    ) = cur.fetchone()

    print("\n🧩 Champs clés etablissements")
    print(f"- public_cible: {with_public_cible:,} ({_safe_pct(with_public_cible, total_etab)}%)")
    print(f"- habitat_type: {with_habitat_type:,} ({_safe_pct(with_habitat_type, total_etab)}%)")
    print(f"- eligibilite_statut: {with_eligibilite_statut:,} ({_safe_pct(with_eligibilite_statut, total_etab)}%)")
    print(f"- site_web: {with_site_web:,} ({_safe_pct(with_site_web, total_etab)}%)")
    print(f"- email: {with_email:,} ({_safe_pct(with_email, total_etab)}%)")
    print(f"- telephone: {with_telephone:,} ({_safe_pct(with_telephone, total_etab)}%)")
    print(f"- presentation: {with_presentation:,} ({_safe_pct(with_presentation, total_etab)}%)")

    cur.execute(
        """
        SELECT statut_editorial, COUNT(*)
        FROM etablissements
        WHERE is_test=false
        GROUP BY statut_editorial
        ORDER BY COUNT(*) DESC
        """
    )
    rows = cur.fetchall()
    print("\n📰 Répartition statut_editorial")
    for statut, c in rows:
        print(f"- {statut}: {c:,}")

    cur.execute(
        """
        SELECT eligibilite_statut, COUNT(*)
        FROM etablissements
        WHERE is_test=false
        GROUP BY eligibilite_statut
        ORDER BY COUNT(*) DESC
        """
    )
    rows = cur.fetchall()
    print("\n🧾 Répartition eligibilite_statut")
    for statut, c in rows:
        s = "(NULL)" if statut is None else statut
        print(f"- {s}: {c:,}")


def _audit_avp_infos(cur) -> None:
    cur.execute(
        """
        SELECT
          COUNT(*) AS rows_total,
          COUNT(*) FILTER (WHERE statut='intention') AS n_intention,
          COUNT(*) FILTER (WHERE statut='en_projet') AS n_en_projet,
          COUNT(*) FILTER (WHERE statut='ouvert') AS n_ouvert,
          COUNT(*) FILTER (WHERE date_intention IS NOT NULL) AS with_date_intention,
          COUNT(*) FILTER (WHERE date_en_projet IS NOT NULL) AS with_date_en_projet,
          COUNT(*) FILTER (WHERE date_ouverture IS NOT NULL) AS with_date_ouverture,
          COUNT(*) FILTER (WHERE COALESCE(NULLIF(TRIM(public_accueilli),''), NULL) IS NOT NULL) AS with_public_accueilli,
          COUNT(*) FILTER (WHERE jsonb_typeof(intervenants)='array' AND jsonb_array_length(intervenants) >= 1) AS with_intervenants,
          COUNT(*) FILTER (
            WHERE COALESCE(NULLIF(TRIM(pvsp_fondamentaux->>'animation_vie_sociale'),''), NULL) IS NOT NULL
          ) AS with_pvsp_animation
        FROM avp_infos
        """
    )
    (
        rows_total,
        n_intention,
        n_en_projet,
        n_ouvert,
        with_date_intention,
        with_date_en_projet,
        with_date_ouverture,
        with_public_accueilli,
        with_intervenants,
        with_pvsp_animation,
    ) = cur.fetchone()

    print("\n🏷️  avp_infos")
    if rows_total == 0:
        print("- 0 lignes")
        return

    print(f"- lignes: {rows_total:,}")
    print(f"- statut intention: {n_intention:,}")
    print(f"- statut en_projet: {n_en_projet:,}")
    print(f"- statut ouvert: {n_ouvert:,}")
    print(f"- date_intention renseignée: {with_date_intention:,} ({_safe_pct(with_date_intention, rows_total)}%)")
    print(f"- date_en_projet renseignée: {with_date_en_projet:,} ({_safe_pct(with_date_en_projet, rows_total)}%)")
    print(f"- date_ouverture renseignée: {with_date_ouverture:,} ({_safe_pct(with_date_ouverture, rows_total)}%)")
    print(f"- public_accueilli renseigné: {with_public_accueilli:,} ({_safe_pct(with_public_accueilli, rows_total)}%)")
    print(f"- intervenants >= 1: {with_intervenants:,} ({_safe_pct(with_intervenants, rows_total)}%)")
    print(f"- pvsp.animation_vie_sociale non vide: {with_pvsp_animation:,} ({_safe_pct(with_pvsp_animation, rows_total)}%)")


def _audit_prices(cur) -> None:
    cur.execute(
        """
        SELECT
          COUNT(*) AS rows_total,
          COUNT(*) FILTER (WHERE prix_min IS NOT NULL OR prix_max IS NOT NULL) AS with_any_price,
          COUNT(*) FILTER (WHERE prix_min IS NOT NULL) AS with_prix_min,
          COUNT(*) FILTER (WHERE prix_max IS NOT NULL) AS with_prix_max,
          COUNT(*) FILTER (WHERE fourchette_prix IS NOT NULL) AS with_fourchette
        FROM tarifications
        """
    )
    rows_total, with_any_price, with_prix_min, with_prix_max, with_fourchette = cur.fetchone()

    print("\n💶 tarifications (qualité prix)")
    if rows_total == 0:
        print("- 0 lignes")
        return

    print(f"- lignes: {rows_total:,}")
    print(f"- prix_min/prix_max au moins un: {with_any_price:,} ({_safe_pct(with_any_price, rows_total)}%)")
    print(f"- prix_min: {with_prix_min:,} ({_safe_pct(with_prix_min, rows_total)}%)")
    print(f"- prix_max: {with_prix_max:,} ({_safe_pct(with_prix_max, rows_total)}%)")
    print(f"- fourchette_prix: {with_fourchette:,} ({_safe_pct(with_fourchette, rows_total)}%)")

    cur.execute(
        """
        SELECT fourchette_prix, COUNT(*)
        FROM tarifications
        WHERE fourchette_prix IS NOT NULL
        GROUP BY fourchette_prix
        ORDER BY COUNT(*) DESC
        """
    )
    rows = cur.fetchall()
    if rows:
        print("- répartition fourchette_prix:")
        for f, c in rows:
            print(f"  - {f}: {c:,}")


def _audit_departement(cur, dep: str) -> None:
    cur.execute("SELECT COUNT(*) FROM etablissements WHERE is_test=false AND departement=%s", (dep,))
    total = cur.fetchone()[0]
    print(f"\n🧭 Département {dep}")
    print(f"- etablissements: {total:,}")

    if total == 0:
        return

    cur.execute(
        """
        SELECT
          COUNT(*) FILTER (WHERE COALESCE(NULLIF(TRIM(public_cible),''), NULL) IS NOT NULL) AS with_public_cible,
          COUNT(*) FILTER (WHERE eligibilite_statut IS NOT NULL) AS with_eligibilite_statut,
          COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM tarifications t WHERE t.etablissement_id=e.id)) AS with_tarifs,
          COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM logements_types lt WHERE lt.etablissement_id=e.id)) AS with_logements,
          COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM restaurations r WHERE r.etablissement_id=e.id)) AS with_restauration,
          COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM etablissement_service es WHERE es.etablissement_id=e.id)) AS with_services,
          COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM avp_infos a WHERE a.etablissement_id=e.id)) AS with_avp_infos
        FROM etablissements e
        WHERE e.is_test=false AND e.departement=%s
        """,
        (dep,),
    )
    (
        with_public_cible,
        with_eligibilite_statut,
        with_tarifs,
        with_logements,
        with_restauration,
        with_services,
        with_avp_infos,
    ) = cur.fetchone()

    print(f"- public_cible: {with_public_cible:,} ({_safe_pct(with_public_cible, total)}%)")
    print(f"- eligibilite_statut: {with_eligibilite_statut:,} ({_safe_pct(with_eligibilite_statut, total)}%)")
    print(f"- tarifications: {with_tarifs:,} ({_safe_pct(with_tarifs, total)}%)")
    print(f"- logements_types: {with_logements:,} ({_safe_pct(with_logements, total)}%)")
    print(f"- restaurations: {with_restauration:,} ({_safe_pct(with_restauration, total)}%)")
    print(f"- services (liaisons): {with_services:,} ({_safe_pct(with_services, total)}%)")
    print(f"- avp_infos: {with_avp_infos:,} ({_safe_pct(with_avp_infos, total)}%)")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--departements",
        help="Liste de départements, ex: 45,76",
        default=None,
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    departements = _parse_departements_arg(args.departements)

    db = DatabaseManager()
    conn = psycopg2.connect(**db.config)
    try:
        with conn.cursor() as cur:
            print("🔎 AUDIT ENRICHISSEMENT (READ-ONLY)")

            print("\n1) Structure (information_schema)")
            for t in TABLES_TO_AUDIT:
                _print_table_structure(cur, t)

            print("\n2) Données (taux de remplissage)")
            _audit_global_counts(cur)
            _audit_avp_infos(cur)
            _audit_prices(cur)

            if departements:
                print("\n3) Zoom départements")
                for dep in departements:
                    _audit_departement(cur, dep)

        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
