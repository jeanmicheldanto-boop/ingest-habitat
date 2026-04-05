"""Workflow URLs via table `propositions`.

But: éviter une modération "une par une".

Ce module propose un flux en 3 étapes:
1) `export` : extraire les URLs litigieuses (ou issues d'un audit) et produire une liste CSV à valider.
2) `import-decisions` : lire un CSV annoté (KEEP / DROP / REPLACE) et créer les `propositions` correspondantes.
3) `apply` : appliquer automatiquement en base toutes les propositions approuvées (ou approuver+appliquer via une liste).

Notes:
- Aucun ajout de colonne n'est requis.
- La table `propositions` + `proposition_items` est faite pour stocker les diffs.
- On reste compatible avec le statut global (`propositions.statut`) ET le statut item (`proposition_items.statut`).

Exemples:
  python scripts/url_propositions_workflow.py export --department "Ain (01)" --limit 300
  python scripts/url_propositions_workflow.py import-decisions --input outputs/url_review_*.csv
  python scripts/url_propositions_workflow.py approve-from-list --input outputs/url_approve_list.txt
  python scripts/url_propositions_workflow.py apply --mode approved

Format CSV attendu pour import-decisions:
- etablissement_id
- current_site_web
- decision: KEEP|DROP|REPLACE
- new_site_web (optionnel, requis si REPLACE)
- note (optionnel)

Ce script utilise la connexion `DatabaseManager` (psycopg2).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# Permet d'exécuter le script depuis le dossier `scripts/`.
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from database import DatabaseManager


@dataclass
class ReviewRow:
    etablissement_id: str
    nom: str
    departement: str
    commune: str
    sous_categories: str
    current_site_web: str
    source: str
    decision: str = ""
    new_site_web: str = ""
    note: str = ""


def _now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def export_review_csv(*, department: str, limit: int, out_path: str) -> int:
    """Export une liste à valider (hors RA/RSS/MARPA)."""

    db = DatabaseManager()

    where_dept = ""
    params: List[Any] = []
    if department:
        where_dept = " AND e.departement = %s "
        params.append(department)

    sql = f"""
    SELECT e.id, e.nom, e.departement, e.commune, COALESCE(e.site_web,'') as site_web, COALESCE(e.source,'') as source,
           array_agg(DISTINCT sc.libelle) FILTER (WHERE sc.libelle IS NOT NULL) as sous_categories
    FROM etablissements e
    LEFT JOIN etablissement_sous_categorie esc ON esc.etablissement_id = e.id
    LEFT JOIN sous_categories sc ON sc.id = esc.sous_categorie_id
    WHERE e.is_test = false
      AND e.site_web IS NOT NULL
      AND trim(e.site_web) != ''
      {where_dept}
      AND NOT EXISTS (
        SELECT 1
        FROM etablissement_sous_categorie esc2
        JOIN sous_categories sc2 ON sc2.id = esc2.sous_categorie_id
        WHERE esc2.etablissement_id = e.id
          AND sc2.libelle IN ('Résidence autonomie','Résidence services seniors','MARPA',
                             'residence_autonomie','residence_services_seniors','marpa')
      )
    GROUP BY e.id, e.nom, e.departement, e.commune, e.site_web, e.source
    ORDER BY e.departement, e.nom
    LIMIT {int(limit)};
    """

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "etablissement_id",
                "nom",
                "departement",
                "commune",
                "sous_categories",
                "current_site_web",
                "source",
                "decision",
                "new_site_web",
                "note",
            ],
        )
        w.writeheader()
        for etab_id, nom, dept, commune, site_web, source, sous_cats in rows:
            w.writerow(
                {
                    "etablissement_id": str(etab_id),
                    "nom": nom or "",
                    "departement": dept or "",
                    "commune": commune or "",
                    "sous_categories": ", ".join([x for x in (sous_cats or []) if x]),
                    "current_site_web": site_web or "",
                    "source": source or "",
                    "decision": "",  # à remplir
                    "new_site_web": "",  # à remplir si REPLACE
                    "note": "",
                }
            )

    print(f"OK export: {len(rows)} lignes -> {out_path}")
    print("Décisions attendues: KEEP | DROP | REPLACE")
    return 0


def _jsonb(val: Any) -> str:
    return json.dumps(val, ensure_ascii=False)


def _parse_jsonb_value(val: Any) -> Any:
    """Convertit une valeur JSONB psycopg2 en valeur Python.

    Selon la config psycopg2, `jsonb` peut revenir déjà parsé (dict/str/None)
    ou sous forme de texte JSON. On évite de transformer une string valide en
    NULL par erreur.
    """

    if val is None:
        return None

    if isinstance(val, (dict, list, bool, int, float)):
        return val

    if isinstance(val, str):
        s = val.strip()
        if s == "":
            return val

        # Essayer de parser uniquement si ça ressemble à un littéral JSON.
        if s[0] in "{[\"" or s in {"null", "true", "false"} or s[0] in "-0123456789":
            try:
                return json.loads(s)
            except Exception:
                return val

        return val

    return val


def _is_publish_check_violation(exc: Exception) -> bool:
    msg = str(exc) or ""
    return "etablissements_publish_check" in msg or "can_publish" in msg


def _force_draft(*, cur, etablissement_id: str) -> None:
    cur.execute(
        """
        UPDATE etablissements
        SET statut_editorial = 'draft'::public.statut_editorial,
            updated_at = now()
        WHERE id = %s;
        """,
        (etablissement_id,),
    )


def _find_existing_prop(cur, etablissement_id: str, new_site_web: Optional[str]) -> Optional[str]:
    """Evite les doublons: cherche une proposition en_attente identique."""

    cur.execute(
        """
        SELECT p.id
        FROM propositions p
        JOIN proposition_items pi ON pi.proposition_id = p.id
        WHERE p.statut = 'en_attente'
          AND p.type_cible = 'etablissement'
          AND p.action = 'update'
          AND p.etablissement_id = %s
          AND pi.table_name = 'etablissements'
          AND pi.column_name = 'site_web'
          AND (
            (pi.new_value IS NULL AND %s IS NULL)
            OR (pi.new_value::text = %s)
          )
        LIMIT 1;
        """,
        (
            etablissement_id,
            new_site_web,
            _jsonb(new_site_web) if new_site_web is not None else None,
        ),
    )
    row = cur.fetchone()
    return str(row[0]) if row else None


def create_proposition_site_web(
    *,
    cur,
    etablissement_id: str,
    current_site_web: str,
    new_site_web: Optional[str],
    payload: Dict[str, Any],
    review_note: str,
) -> str:
    """Crée une proposition + 1 item (site_web). Retourne proposition_id."""

    existing = _find_existing_prop(cur, etablissement_id, new_site_web)
    if existing:
        return existing

    cur.execute(
        """
        INSERT INTO propositions (etablissement_id, type_cible, action, statut, source, payload, review_note)
        VALUES (%s, 'etablissement', 'update', 'en_attente', %s, %s::jsonb, %s)
        RETURNING id;
        """,
        (etablissement_id, "url_audit", _jsonb(payload), review_note),
    )
    proposition_id = cur.fetchone()[0]

    # old_value/new_value en JSONB
    old_json = _jsonb(current_site_web if current_site_web != "" else None)
    new_json = _jsonb(new_site_web)  # None -> "null"

    cur.execute(
        """
        INSERT INTO proposition_items (proposition_id, table_name, column_name, old_value, new_value)
        VALUES (%s, 'etablissements', 'site_web', %s::jsonb, %s::jsonb);
        """,
        (proposition_id, old_json, new_json),
    )

    return str(proposition_id)


def create_proposition_update(*, cur, etablissement_id: str, payload: Dict[str, Any], review_note: str) -> str:
    """Crée une proposition update générique (sans item)."""

    cur.execute(
        """
        INSERT INTO propositions (etablissement_id, type_cible, action, statut, source, payload, review_note)
        VALUES (%s, 'etablissement', 'update', 'en_attente', %s, %s::jsonb, %s)
        RETURNING id;
        """,
        (etablissement_id, "url_audit", _jsonb(payload), review_note),
    )
    proposition_id = cur.fetchone()[0]
    return str(proposition_id)


def add_proposition_item(*, cur, proposition_id: str, column_name: str, old_value: Any, new_value: Any) -> None:
    allowed = {
        # URL cleanup
        "site_web",
        "source",
        # Publish blockers
        "adresse_l1",
        "adresse_l2",
        "code_postal",
        "commune",
        "geom",
        "geocode_precision",
        "gestionnaire",
        "email",
        "habitat_type",
    }
    if column_name not in allowed:
        raise ValueError(f"Unsupported column_name: {column_name}")

    old_json = _jsonb(old_value)
    new_json = _jsonb(new_value)
    cur.execute(
        """
        INSERT INTO proposition_items (proposition_id, table_name, column_name, old_value, new_value)
        VALUES (%s, 'etablissements', %s, %s::jsonb, %s::jsonb);
        """,
        (proposition_id, column_name, old_json, new_json),
    )


def import_decisions(*, input_csv: str, clear_source: bool) -> int:
    """Lit un CSV annoté et crée les propositions correspondantes."""

    if not os.path.exists(input_csv):
        raise FileNotFoundError(input_csv)

    db = DatabaseManager()

    created_props: List[str] = []
    skipped = 0

    with open(input_csv, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        required = {"etablissement_id", "current_site_web", "decision"}
        missing = required - set(r.fieldnames or [])
        if missing:
            raise ValueError(f"Colonnes manquantes: {sorted(missing)}")

        with db.get_connection() as conn:
            with conn.cursor() as cur:
                for row in r:
                    decision = (row.get("decision") or "").strip().upper()
                    if decision in ("", "SKIP"):
                        skipped += 1
                        continue

                    etab_id = (row.get("etablissement_id") or "").strip()
                    current = (row.get("current_site_web") or "").strip()
                    current_source = (row.get("source") or "").strip()
                    note = (row.get("note") or "").strip()

                    site_web_change: Optional[str] = None
                    if decision == "KEEP":
                        site_web_change = "__NO_CHANGE__"
                    elif decision == "DROP":
                        site_web_change = None
                    elif decision == "REPLACE":
                        new_url = (row.get("new_site_web") or "").strip()
                        if not new_url:
                            raise ValueError(f"REPLACE sans new_site_web pour {etab_id}")
                        site_web_change = new_url
                    else:
                        raise ValueError(f"Decision invalide: {decision} (attendu KEEP|DROP|REPLACE)")

                    should_clear_source = bool(clear_source and current_source)
                    should_change_site_web = site_web_change != "__NO_CHANGE__"

                    if (not should_change_site_web) and (not should_clear_source):
                        skipped += 1
                        continue

                    payload = {
                        "workflow": "url_review_csv",
                        "decision": decision,
                        "input_csv": os.path.basename(input_csv),
                        "clear_source": bool(clear_source),
                        "new_site_web": None if site_web_change == "__NO_CHANGE__" else site_web_change,
                    }

                    prop_id = create_proposition_update(
                        cur=cur,
                        etablissement_id=etab_id,
                        payload=payload,
                        review_note=note,
                    )

                    if should_change_site_web:
                        add_proposition_item(
                            cur=cur,
                            proposition_id=prop_id,
                            column_name="site_web",
                            old_value=current if current != "" else None,
                            new_value=site_web_change,
                        )

                    if should_clear_source:
                        add_proposition_item(
                            cur=cur,
                            proposition_id=prop_id,
                            column_name="source",
                            old_value=current_source if current_source != "" else None,
                            new_value=None,
                        )

                    created_props.append(prop_id)

            conn.commit()

    os.makedirs("outputs", exist_ok=True)
    out_ids = os.path.join("outputs", f"url_proposition_ids_{_now_tag()}.txt")
    unique_ids = list(dict.fromkeys(created_props))
    with open(out_ids, "w", encoding="utf-8") as f:
        for pid in unique_ids:
            f.write(pid + "\n")

    print(f"OK import-decisions: propositions_created={len(unique_ids)}, skipped={skipped}")
    print(f"- ids: {out_ids}")
    print("Tu peux maintenant approuver en masse (approve-from-list) puis apply.")
    return 0


def approve_from_list(*, input_path: str, note: str) -> int:
    """Met en `approuvee` une liste de proposition_id (une par ligne)."""

    ids: List[str] = []
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            x = line.strip()
            if not x or x.startswith("#"):
                continue
            ids.append(x)

    if not ids:
        print("Aucun id.")
        return 0

    db = DatabaseManager()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE propositions
                SET statut = 'approuvee', reviewed_at = now(), review_note = COALESCE(review_note,'') || %s
                WHERE id = ANY(%s::uuid[]) AND statut = 'en_attente';
                """,
                ("\n" + note if note else "", ids),
            )
            updated = cur.rowcount

            # On accepte aussi les items associés
            cur.execute(
                """
                UPDATE proposition_items
                SET statut = 'accepted'
                WHERE proposition_id = ANY(%s::uuid[]) AND statut = 'pending';
                """,
                (ids,),
            )
            conn.commit()

    print(f"OK approve-from-list: approuvee={updated} propositions")
    return 0


def apply(*, mode: str, input_path: Optional[str] = None) -> int:
    """Applique en base les items accepted d'un ensemble de propositions."""

    if mode not in {"approved", "accepted-items"}:
        raise ValueError("mode must be approved|accepted-items")

    db = DatabaseManager()

    if mode == "approved":
        where = "p.statut = 'approuvee'"
    else:
        where = "pi.statut = 'accepted'"

    ids: List[str] = []
    if input_path:
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                x = line.strip()
                if not x or x.startswith("#"):
                    continue
                ids.append(x)

        if not ids:
            print("Aucun id.")
            return 0

        where += " AND p.id = ANY(%s::uuid[]) "

        sql = f"""
                SELECT p.id as proposition_id, p.etablissement_id, pi.id as item_id, pi.column_name, pi.old_value, pi.new_value
        FROM propositions p
        JOIN proposition_items pi ON pi.proposition_id = p.id
        WHERE {where}
            AND p.type_cible = 'etablissement'
            AND p.action = 'update'
            AND pi.table_name = 'etablissements'
            AND pi.column_name IN (
                        'site_web','source',
                        'adresse_l1','adresse_l2','code_postal','commune',
                        'geom','geocode_precision',
                        'gestionnaire','email','habitat_type'
            )
        ORDER BY p.created_at;
        """

    applied = 0

    params: List[Any] = []
    if ids:
        params.append(ids)

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            rows = cur.fetchall()

            allowed_cols = {
                "site_web": "site_web",
                "source": "source",
                "adresse_l1": "adresse_l1",
                "adresse_l2": "adresse_l2",
                "code_postal": "code_postal",
                "commune": "commune",
                "geocode_precision": "geocode_precision",
                "gestionnaire": "gestionnaire",
                "email": "email",
                "habitat_type": "habitat_type",
                # geom handled separately (not a plain scalar)
                "geom": "geom",
            }

            for proposition_id, etab_id, item_id, column_name, old_value, new_value in rows:
                col = allowed_cols.get(str(column_name))
                if not col:
                    continue

                new_val = _parse_jsonb_value(new_value)

                if col == "geom":
                    # new_val attendu: {"lat": <float>, "lon": <float>} ou None pour NULL
                    if new_val is None:
                        try:
                            cur.execute(
                                """
                                UPDATE etablissements
                                SET geom = NULL,
                                    statut_editorial = CASE
                                        WHEN statut_editorial = 'publie' AND NOT public.can_publish(id)
                                            THEN 'draft'::public.statut_editorial
                                        ELSE statut_editorial
                                    END,
                                    updated_at = now()
                                WHERE id = %s;
                                """,
                                (etab_id,),
                            )
                        except Exception as e:
                            if _is_publish_check_violation(e):
                                _force_draft(cur=cur, etablissement_id=str(etab_id))
                                cur.execute(
                                    """
                                    UPDATE etablissements
                                    SET geom = NULL,
                                        updated_at = now()
                                    WHERE id = %s;
                                    """,
                                    (etab_id,),
                                )
                            else:
                                raise
                    else:
                        lat = None
                        lon = None
                        if isinstance(new_val, dict):
                            lat = new_val.get("lat")
                            lon = new_val.get("lon")
                        if lat is None or lon is None:
                            raise ValueError(f"Invalid geom payload for {etab_id}: expected {{lat,lon}}")
                        try:
                            cur.execute(
                                """
                                UPDATE etablissements
                                SET geom = ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                                    statut_editorial = CASE
                                        WHEN statut_editorial = 'publie' AND NOT public.can_publish(id)
                                            THEN 'draft'::public.statut_editorial
                                        ELSE statut_editorial
                                    END,
                                    updated_at = now()
                                WHERE id = %s;
                                """,
                                (float(lon), float(lat), etab_id),
                            )
                        except Exception as e:
                            if _is_publish_check_violation(e):
                                _force_draft(cur=cur, etablissement_id=str(etab_id))
                                cur.execute(
                                    """
                                    UPDATE etablissements
                                    SET geom = ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                                        updated_at = now()
                                    WHERE id = %s;
                                    """,
                                    (float(lon), float(lat), etab_id),
                                )
                            else:
                                raise
                else:
                    try:
                        cur.execute(
                            # Important: certains enregistrements legacy peuvent être `publie` mais non publishable.
                            # On downgrade uniquement dans ce cas; si malgré tout ça bloque, on retry en forçant draft.
                            f"""
                            UPDATE etablissements
                            SET {col} = %s,
                                statut_editorial = CASE
                                    WHEN statut_editorial = 'publie' AND NOT public.can_publish(id)
                                        THEN 'draft'::public.statut_editorial
                                    ELSE statut_editorial
                                END,
                                updated_at = now()
                            WHERE id = %s;
                            """,
                            (new_val, etab_id),
                        )
                    except Exception as e:
                        if _is_publish_check_violation(e):
                            _force_draft(cur=cur, etablissement_id=str(etab_id))
                            cur.execute(
                                f"""
                                UPDATE etablissements
                                SET {col} = %s,
                                    updated_at = now()
                                WHERE id = %s;
                                """,
                                (new_val, etab_id),
                            )
                        else:
                            raise

                # Marquer item + proposition
                cur.execute(
                    "UPDATE proposition_items SET statut = 'accepted' WHERE id = %s;",
                    (item_id,),
                )
                cur.execute(
                    "UPDATE propositions SET statut = 'approuvee', reviewed_at = COALESCE(reviewed_at, now()) WHERE id = %s;",
                    (proposition_id,),
                )

                applied += 1

            conn.commit()

    print(f"OK apply: applied_items={applied}")
    return 0


def republish_from_list(*, input_path: str) -> int:
    """Repasse en `publie` les établissements liés à une liste de proposition_id.

    Important: on ne republie que si `public.can_publish(id)` est true, sinon on laisse en draft.
    """

    ids: List[str] = []
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            x = line.strip()
            if not x or x.startswith("#"):
                continue
            ids.append(x)

    if not ids:
        print("Aucun id.")
        return 0

    db = DatabaseManager()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT etablissement_id
                FROM propositions
                WHERE id = ANY(%s::uuid[]);
                """,
                (ids,),
            )
            etab_ids = [str(r[0]) for r in cur.fetchall() if r and r[0]]

            if not etab_ids:
                print("Aucun établissement associé.")
                return 0

            # Republie uniquement si can_publish(id) est vrai
            cur.execute(
                """
                UPDATE etablissements
                SET statut_editorial = 'publie'::public.statut_editorial,
                    updated_at = now()
                WHERE id = ANY(%s::uuid[])
                  AND public.can_publish(id);
                """,
                (etab_ids,),
            )
            republished = cur.rowcount

            conn.commit()

    print(f"OK republish-from-list: republished={republished} etablissements")
    return 0


def diagnose_publish_from_list(*, input_path: str) -> int:
    """Produit un CSV de diagnostic des conditions `can_publish()` pour une liste de proposition_id."""

    ids: List[str] = []
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            x = line.strip()
            if not x or x.startswith("#"):
                continue
            ids.append(x)

    if not ids:
        print("Aucun id.")
        return 0

    db = DatabaseManager()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT etablissement_id
                FROM propositions
                WHERE id = ANY(%s::uuid[]);
                """,
                (ids,),
            )
            etab_ids = [str(r[0]) for r in cur.fetchall() if r and r[0]]

            if not etab_ids:
                print("Aucun établissement associé.")
                return 0

            # Diagnostic aligné avec public.can_publish()
            cur.execute(
                """
                SELECT
                  e.id::text,
                  COALESCE(e.nom,''),
                  COALESCE(e.departement,''),
                  COALESCE(e.commune,''),
                  e.statut_editorial::text,
                  public.can_publish(e.id) as can_publish,
                  (COALESCE(NULLIF(trim(e.nom),''), NULL) IS NULL) as missing_nom,
                  (COALESCE(NULLIF(trim(e.adresse_l1),''), NULLIF(trim(e.adresse_l2),''), NULL) IS NULL) as missing_address,
                  (COALESCE(NULLIF(trim(e.commune),''), NULL) IS NULL) as missing_commune,
                  (COALESCE(NULLIF(trim(e.code_postal),''), NULL) IS NULL) as missing_code_postal,
                  (e.geom IS NULL) as missing_geom,
                  (COALESCE(NULLIF(trim(e.gestionnaire),''), NULL) IS NULL) as missing_gestionnaire,
                  (NOT (e.habitat_type IS NOT NULL OR EXISTS (SELECT 1 FROM public.etablissement_sous_categorie esc WHERE esc.etablissement_id = e.id))) as missing_typage,
                (NOT (
                    e.email IS NULL
                    OR COALESCE(NULLIF(trim(e.email),''), NULL) IS NULL
                    OR e.email ~* '^[A-Za-z0-9._%%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
                )) as invalid_email,
                  COALESCE(e.site_web,'') as site_web,
                  COALESCE(e.source,'') as source
                FROM public.etablissements e
                WHERE e.id = ANY(%s::uuid[])
                ORDER BY e.departement, e.nom;
                """,
                (etab_ids,),
            )
            rows = cur.fetchall()

    os.makedirs("outputs", exist_ok=True)
    out_path = os.path.join("outputs", f"publish_diagnosis_{_now_tag()}.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "etablissement_id",
                "nom",
                "departement",
                "commune",
                "statut_editorial",
                "can_publish",
                "missing_nom",
                "missing_address",
                "missing_commune",
                "missing_code_postal",
                "missing_geom",
                "missing_gestionnaire",
                "missing_typage",
                "invalid_email",
                "site_web",
                "source",
            ],
        )
        w.writeheader()
        for (
            etab_id,
            nom,
            dept,
            commune,
            statut_editorial,
            can_publish,
            missing_nom,
            missing_address,
            missing_commune,
            missing_cp,
            missing_geom,
            missing_gestionnaire,
            missing_typage,
            invalid_email,
            site_web,
            source,
        ) in rows:
            w.writerow(
                {
                    "etablissement_id": etab_id,
                    "nom": nom,
                    "departement": dept,
                    "commune": commune,
                    "statut_editorial": statut_editorial,
                    "can_publish": bool(can_publish),
                    "missing_nom": bool(missing_nom),
                    "missing_address": bool(missing_address),
                    "missing_commune": bool(missing_commune),
                    "missing_code_postal": bool(missing_cp),
                    "missing_geom": bool(missing_geom),
                    "missing_gestionnaire": bool(missing_gestionnaire),
                    "missing_typage": bool(missing_typage),
                    "invalid_email": bool(invalid_email),
                    "site_web": site_web,
                    "source": source,
                }
            )

    print(f"OK diagnose-publish-from-list: {len(rows)} etablissements -> {out_path}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_export = sub.add_parser("export")
    p_export.add_argument("--department", default="", help="ex: 'Seine-Maritime (76)'")
    p_export.add_argument("--limit", type=int, default=500)
    p_export.add_argument("--out", default="", help="Chemin CSV")

    p_import = sub.add_parser("import-decisions")
    p_import.add_argument("--input", required=True)
    p_import.add_argument("--clear-source", action="store_true", help="Efface aussi etablissements.source (met NULL) pour les lignes importées")

    p_import_auto = sub.add_parser("import-auto-review")
    p_import_auto.add_argument("--input", required=True, help="CSV généré par analyze_suspicious_urls.py --export-review-csv")
    p_import_auto.add_argument("--clear-source", action="store_true", help="Efface aussi etablissements.source (met NULL) pour les lignes importées")

    p_approve = sub.add_parser("approve-from-list")
    p_approve.add_argument("--input", required=True, help="Fichier texte: 1 proposition_id par ligne")
    p_approve.add_argument("--note", default="Bulk approval (url cleanup)")

    p_apply = sub.add_parser("apply")
    p_apply.add_argument("--mode", default="approved", choices=["approved", "accepted-items"])
    p_apply.add_argument("--input", default="", help="Optionnel: fichier texte (1 proposition_id par ligne) pour limiter l'application")

    p_republish = sub.add_parser("republish-from-list")
    p_republish.add_argument("--input", required=True, help="Fichier texte: 1 proposition_id par ligne")

    p_diag = sub.add_parser("diagnose-publish-from-list")
    p_diag.add_argument("--input", required=True, help="Fichier texte: 1 proposition_id par ligne")

    args = parser.parse_args()

    if args.cmd == "export":
        out = args.out or os.path.join("outputs", f"url_review_{_now_tag()}.csv")
        return export_review_csv(department=args.department, limit=args.limit, out_path=out)

    if args.cmd == "import-decisions":
        return import_decisions(input_csv=args.input, clear_source=bool(args.clear_source))

    if args.cmd == "import-auto-review":
        return import_decisions(input_csv=args.input, clear_source=bool(args.clear_source))

    if args.cmd == "approve-from-list":
        return approve_from_list(input_path=args.input, note=args.note)

    if args.cmd == "apply":
        return apply(mode=args.mode, input_path=(args.input or None))

    if args.cmd == "republish-from-list":
        return republish_from_list(input_path=args.input)

    if args.cmd == "diagnose-publish-from-list":
        return diagnose_publish_from_list(input_path=args.input)

    raise RuntimeError("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())
