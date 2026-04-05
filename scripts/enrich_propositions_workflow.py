"""Workflow pour appliquer des propositions créées par `scripts/enrich_dept_prototype.py`.

Objectif (test contrôlé)
- approuver une liste de propositions
- appliquer en base une sous-partie sûre (etablissement, tarifications, logements_types)
- repasser en `publie` si `public.can_publish(id)`
- produire un diagnostic `can_publish` (et pourquoi)

Notes
- On applique uniquement des opérations idempotentes (updates) + inserts avec déduplication basique.
- Les propositions `etablissement_service` sont supportées si on peut résoudre un `service_id` existant.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from database import DatabaseManager


def _service_key_to_db_libelle(service_key: str) -> str:
    service_key = (service_key or "").strip()
    mapping = {
        "activites_organisees": "activités organisées",
        "personnel_de_nuit": "personnel de nuit",
        "commerces_a_pied": "commerces à pied",
        "medecin_intervenant": "médecin intervenant",
        "espace_partage": "espace_partage",
        "conciergerie": "conciergerie",
    }
    return mapping.get(service_key, service_key)


def _resolve_service_id(cur, service_key: str) -> Optional[str]:
    service_key = (service_key or "").strip()
    if not service_key:
        return None

    db_name = _service_key_to_db_libelle(service_key)
    cur.execute(
        """
        SELECT id::text
        FROM services
        WHERE libelle = %s
           OR LOWER(REPLACE(libelle, ' ', '_')) = LOWER(%s)
           OR LOWER(libelle) ILIKE LOWER(%s)
        LIMIT 1;
        """,
        (db_name, service_key.replace("_", " "), f"%{service_key.replace('_', ' ')}%"),
    )
    r = cur.fetchone()
    return str(r[0]) if r and r[0] else None


def _now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _read_ids(path: str) -> List[str]:
    ids: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            x = line.strip()
            if not x or x.startswith("#"):
                continue
            ids.append(x)
    return ids


def _parse_jsonb_value(v: Any) -> Any:
    # psycopg2 peut renvoyer déjà décodé (dict/list) ou une string JSON.
    if v is None:
        return None
    if isinstance(v, (dict, list, int, float, bool)):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return ""
        try:
            return json.loads(s)
        except Exception:
            return s
    return v


def approve_from_list(*, input_path: str, note: str) -> int:
    ids = _read_ids(input_path)
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


@dataclass
class PropositionRow:
    proposition_id: str
    etablissement_id: str
    type_cible: str
    action: str
    cible_id: Optional[str]


@dataclass
class ItemRow:
    item_id: str
    table_name: str
    column_name: str
    new_value: Any


def _dedupe_key_for_logement(item: Dict[str, Any]) -> Tuple[Any, ...]:
    return (
        (item.get("libelle") or "").strip().lower(),
        item.get("surface_min"),
        item.get("surface_max"),
        item.get("meuble"),
        item.get("pmr"),
        item.get("domotique"),
        item.get("plain_pied"),
        item.get("nb_unites"),
    )


def apply(*, mode: str, input_path: Optional[str] = None) -> int:
    if mode not in {"approved", "accepted-items"}:
        raise ValueError("mode must be approved|accepted-items")

    ids: List[str] = []
    if input_path:
        ids = _read_ids(input_path)
        if not ids:
            print("Aucun id.")
            return 0

    where = "TRUE"
    if mode == "approved":
        where += " AND p.statut = 'approuvee'"
    else:
        where += " AND pi.statut = 'accepted'"

    params: List[Any] = []
    if ids:
        where += " AND p.id = ANY(%s::uuid[])"
        params.append(ids)

        sql = f"""
    SELECT
      p.id::text as proposition_id,
      p.etablissement_id::text as etablissement_id,
      p.type_cible::text as type_cible,
      p.action::text as action,
      COALESCE(p.cible_id::text, NULL) as cible_id,
      pi.id::text as item_id,
      pi.table_name::text as table_name,
      pi.column_name::text as column_name,
      pi.new_value
    FROM propositions p
    JOIN proposition_items pi ON pi.proposition_id = p.id
    WHERE {where}
        ORDER BY p.created_at, pi.id;
    """

    db = DatabaseManager()

    applied_items = 0
    applied_props: set[str] = set()
    skipped_props: Dict[str, str] = {}

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params if params else None)
            rows = cur.fetchall()

            # Group by proposition
            grouped: Dict[str, Tuple[PropositionRow, List[ItemRow]]] = {}
            for (
                proposition_id,
                etablissement_id,
                type_cible,
                action,
                cible_id,
                item_id,
                table_name,
                column_name,
                new_value,
            ) in rows:
                prop = grouped.get(proposition_id)
                if not prop:
                    grouped[proposition_id] = (
                        PropositionRow(
                            proposition_id=str(proposition_id),
                            etablissement_id=str(etablissement_id),
                            type_cible=str(type_cible),
                            action=str(action),
                            cible_id=(str(cible_id) if cible_id else None),
                        ),
                        [],
                    )
                grouped[proposition_id][1].append(
                    ItemRow(
                        item_id=str(item_id),
                        table_name=str(table_name),
                        column_name=str(column_name),
                        new_value=_parse_jsonb_value(new_value),
                    )
                )

            for prop_id, (prop, items) in grouped.items():
                # 1) etablissements (presentation / public_cible / eligibilite_statut)
                if prop.type_cible == "etablissement" and prop.action == "update":
                    allowed_cols = {
                        "presentation",
                        "public_cible",
                        "eligibilite_statut",
                        # publishability fields
                        "adresse_l1",
                        "adresse_l2",
                        "code_postal",
                        "commune",
                        "gestionnaire",
                        "email",
                        "geocode_precision",
                        # geom handled separately
                        "geom",
                    }
                    updates: Dict[str, Any] = {}
                    geom_update: Optional[Dict[str, Any]] = None
                    item_ids: List[str] = []
                    for it in items:
                        if it.table_name != "etablissements":
                            continue
                        if it.column_name not in allowed_cols:
                            continue
                        if it.column_name == "geom":
                            if isinstance(it.new_value, dict) or it.new_value is None:
                                geom_update = it.new_value
                        else:
                            updates[it.column_name] = it.new_value
                        item_ids.append(it.item_id)

                    if not updates and geom_update is None:
                        skipped_props[prop_id] = "no_supported_items(etablissement)"
                        continue

                    # Update scalar columns
                    if updates:
                        set_parts = ", ".join([f"{k} = %s" for k in updates.keys()])
                        values = list(updates.values())
                        values.append(prop.etablissement_id)
                        cur.execute(
                            f"""
                            UPDATE etablissements
                            SET {set_parts},
                                statut_editorial = CASE
                                    WHEN statut_editorial = 'publie' AND NOT public.can_publish(id)
                                        THEN 'draft'::public.statut_editorial
                                    ELSE statut_editorial
                                END,
                                updated_at = now()
                            WHERE id = %s;
                            """,
                            values,
                        )

                    # Update geom separately
                    if geom_update is not None:
                        if geom_update is None:
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
                                (prop.etablissement_id,),
                            )
                        else:
                            lat = geom_update.get("lat")
                            lon = geom_update.get("lon")
                            if lat is None or lon is None:
                                raise ValueError(f"Invalid geom payload for {prop.etablissement_id}: expected {{lat,lon}}")
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
                                (float(lon), float(lat), prop.etablissement_id),
                            )

                    applied_items += len(item_ids)
                    applied_props.add(prop_id)

                # 2) tarifications
                elif prop.type_cible == "tarifications" and prop.action in {"create", "update"}:
                    allowed_cols = {"prix_min", "prix_max", "loyer_base", "charges", "periode", "fourchette_prix"}
                    values: Dict[str, Any] = {}
                    item_ids: List[str] = []
                    for it in items:
                        if it.table_name != "tarifications":
                            continue
                        if it.column_name not in allowed_cols:
                            continue
                        values[it.column_name] = it.new_value
                        item_ids.append(it.item_id)

                    if not values:
                        skipped_props[prop_id] = "no_supported_items(tarifications)"
                        continue

                    target_id = prop.cible_id
                    if prop.action == "update" and not target_id:
                        # fallback: most recent tarification row for this etablissement
                        cur.execute(
                            """
                            SELECT id::text
                            FROM tarifications
                            WHERE etablissement_id = %s
                            ORDER BY date_observation DESC NULLS LAST, id DESC
                            LIMIT 1;
                            """,
                            (prop.etablissement_id,),
                        )
                        r = cur.fetchone()
                        target_id = str(r[0]) if r and r[0] else None

                    if prop.action == "create" and not target_id:
                        # dédup basique: même période + mêmes montants
                        cur.execute(
                            """
                            SELECT id::text
                            FROM tarifications
                            WHERE etablissement_id = %s
                              AND (periode IS NOT DISTINCT FROM %s)
                              AND (prix_min IS NOT DISTINCT FROM %s)
                              AND (prix_max IS NOT DISTINCT FROM %s)
                              AND (loyer_base IS NOT DISTINCT FROM %s)
                              AND (charges IS NOT DISTINCT FROM %s)
                            LIMIT 1;
                            """,
                            (
                                prop.etablissement_id,
                                values.get("periode"),
                                values.get("prix_min"),
                                values.get("prix_max"),
                                values.get("loyer_base"),
                                values.get("charges"),
                            ),
                        )
                        r = cur.fetchone()
                        target_id = str(r[0]) if r and r[0] else None

                    if target_id:
                        set_parts = ", ".join([f"{k} = %s" for k in values.keys()])
                        params2 = list(values.values())
                        params2.append(target_id)
                        cur.execute(
                            f"""
                            UPDATE tarifications
                            SET {set_parts}
                            WHERE id = %s;
                            """,
                            params2,
                        )
                    else:
                        # create
                        cols = ["etablissement_id"] + list(values.keys()) + ["source", "date_observation"]
                        placeholders = ["%s"] * len(cols)
                        params2 = [prop.etablissement_id] + list(values.values()) + ["enrich_proto", datetime.now().date()]
                        cur.execute(
                            f"""
                            INSERT INTO tarifications ({', '.join(cols)})
                            VALUES ({', '.join(placeholders)})
                            RETURNING id;
                            """,
                            params2,
                        )
                        target_id = str(cur.fetchone()[0])
                        cur.execute("UPDATE propositions SET cible_id = %s WHERE id = %s;", (target_id, prop_id))

                    applied_items += len(item_ids)
                    applied_props.add(prop_id)

                # 3) logements_types (create add-only)
                elif prop.type_cible == "logements_types" and prop.action == "create":
                    # item unique: column logements_types = list
                    logements_list: List[Dict[str, Any]] = []
                    item_ids: List[str] = []
                    for it in items:
                        if it.table_name != "logements_types" or it.column_name != "logements_types":
                            continue
                        if isinstance(it.new_value, list):
                            logements_list = [x for x in it.new_value if isinstance(x, dict)]
                            item_ids.append(it.item_id)

                    if not logements_list:
                        skipped_props[prop_id] = "no_items(logements_types)"
                        continue

                    # récupérer existants pour dédup
                    cur.execute(
                        """
                        SELECT COALESCE(libelle,''), surface_min, surface_max, meuble, pmr, domotique, plain_pied, nb_unites
                        FROM logements_types
                        WHERE etablissement_id = %s;
                        """,
                        (prop.etablissement_id,),
                    )
                    existing = set()
                    for r in cur.fetchall() or []:
                        existing.add(
                            (
                                (str(r[0] or "").strip().lower()),
                                r[1],
                                r[2],
                                r[3],
                                r[4],
                                r[5],
                                r[6],
                                r[7],
                            )
                        )

                    inserted = 0
                    for lg in logements_list:
                        key = _dedupe_key_for_logement(lg)
                        if key in existing:
                            continue
                        cur.execute(
                            """
                            INSERT INTO logements_types (
                              etablissement_id, libelle, surface_min, surface_max, meuble, pmr, domotique, plain_pied, nb_unites
                            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
                            """,
                            (
                                prop.etablissement_id,
                                lg.get("libelle"),
                                lg.get("surface_min"),
                                lg.get("surface_max"),
                                lg.get("meuble"),
                                lg.get("pmr"),
                                lg.get("domotique"),
                                                                bool(lg.get("plain_pied") or False),
                                lg.get("nb_unites"),
                            ),
                        )
                        existing.add(key)
                        inserted += 1

                    if inserted > 0:
                        applied_items += len(item_ids)
                        applied_props.add(prop_id)
                    else:
                        skipped_props[prop_id] = "dedup_all(logements_types)"
                        continue

                # 4) etablissement_service (create add-only)
                elif prop.type_cible == "etablissement_service" and prop.action == "create":
                    service_ids: List[str] = []
                    service_keys: List[str] = []
                    item_ids: List[str] = []

                    for it in items:
                        if it.table_name != "etablissement_service":
                            continue
                        if it.column_name == "service_id" and isinstance(it.new_value, str) and it.new_value.strip():
                            service_ids.append(it.new_value.strip())
                            item_ids.append(it.item_id)
                        elif it.column_name == "service_key" and isinstance(it.new_value, str) and it.new_value.strip():
                            service_keys.append(it.new_value.strip())
                            item_ids.append(it.item_id)

                    # Backward compatibility: resolve service_key -> service_id
                    if not service_ids and service_keys:
                        for sk in service_keys:
                            sid = _resolve_service_id(cur, sk)
                            if sid:
                                service_ids.append(sid)

                    # Dedupe
                    service_ids = sorted({s for s in service_ids if s})
                    if not service_ids:
                        skipped_props[prop_id] = "no_supported_items(etablissement_service)"
                        continue

                    inserted = 0
                    for sid in service_ids:
                        cur.execute(
                            """
                            INSERT INTO etablissement_service (etablissement_id, service_id)
                            SELECT %s, %s
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM etablissement_service
                                WHERE etablissement_id = %s AND service_id = %s
                            );
                            """,
                            (prop.etablissement_id, sid, prop.etablissement_id, sid),
                        )
                        inserted += int(cur.rowcount or 0)

                    if inserted > 0:
                        applied_items += len(item_ids)
                        applied_props.add(prop_id)
                    else:
                        skipped_props[prop_id] = "dedup_all(etablissement_service)"
                        continue

                else:
                    skipped_props[prop_id] = f"unsupported({prop.type_cible}/{prop.action})"
                    continue

            conn.commit()

    print(f"OK apply: propositions_seen={len(grouped)}, applied_props={len(applied_props)}, applied_items={applied_items}, skipped_props={len(skipped_props)}")
    if skipped_props:
        # résumé court
        reason_counts: Dict[str, int] = {}
        for r in skipped_props.values():
            reason_counts[r] = reason_counts.get(r, 0) + 1
        top = sorted(reason_counts.items(), key=lambda x: (-x[1], x[0]))[:10]
        print("Top skipped reasons:")
        for reason, n in top:
            print(f"- {reason}: {n}")

    return 0


def republish_from_list(*, input_path: str) -> int:
    ids = _read_ids(input_path)
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
    ids = _read_ids(input_path)
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
    out_path = os.path.join("outputs", f"publish_diagnosis_enrich_{_now_tag()}.csv")
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


def stats_from_list(*, input_path: str) -> int:
    ids = _read_ids(input_path)
    if not ids:
        print("Aucun id.")
        return 0

    db = DatabaseManager()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  p.type_cible::text,
                  p.action::text,
                  p.statut::text,
                  COUNT(*)::int as n_props,
                  COALESCE(SUM((SELECT COUNT(*) FROM proposition_items pi WHERE pi.proposition_id = p.id)), 0)::int as n_items
                FROM propositions p
                WHERE p.id = ANY(%s::uuid[])
                GROUP BY 1,2,3
                ORDER BY 4 DESC, 1,2,3;
                """,
                (ids,),
            )
            rows = cur.fetchall() or []

            cur.execute(
                """
                SELECT COUNT(DISTINCT etablissement_id)::int
                FROM propositions
                WHERE id = ANY(%s::uuid[]);
                """,
                (ids,),
            )
            n_etabs = int((cur.fetchone() or [0])[0] or 0)

    print(f"OK stats-from-list: propositions={len(ids)}, etablissements={n_etabs}")
    for type_cible, action, statut, n_props, n_items in rows:
        print(f"- {type_cible}/{action}/{statut}: props={int(n_props)}, items={int(n_items)}")
    return 0


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_approve = sub.add_parser("approve-from-list")
    p_approve.add_argument("--input", required=True)
    p_approve.add_argument("--note", default="Bulk approval (enrich proto)")

    p_apply = sub.add_parser("apply")
    p_apply.add_argument("--mode", default="approved", choices=["approved", "accepted-items"])
    p_apply.add_argument("--input", default="")

    p_republish = sub.add_parser("republish-from-list")
    p_republish.add_argument("--input", required=True)

    p_diag = sub.add_parser("diagnose-publish-from-list")
    p_diag.add_argument("--input", required=True)

    p_stats = sub.add_parser("stats-from-list")
    p_stats.add_argument("--input", required=True)

    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.cmd == "approve-from-list":
        return approve_from_list(input_path=args.input, note=args.note)

    if args.cmd == "apply":
        return apply(mode=args.mode, input_path=(args.input or None))

    if args.cmd == "republish-from-list":
        return republish_from_list(input_path=args.input)

    if args.cmd == "diagnose-publish-from-list":
        return diagnose_publish_from_list(input_path=args.input)

    if args.cmd == "stats-from-list":
        return stats_from_list(input_path=args.input)

    raise RuntimeError("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())
