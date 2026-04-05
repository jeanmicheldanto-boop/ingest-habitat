"""
Phase 2 — Recherche des contacts manquants dans les départements.

Pour chaque département sans directeur_enfance / directeur_autonomie
ou sans responsable_tarification : 3 requêtes Serper ciblées + extraction Gemini.

Usage :
    python fix_contacts_phase2.py --dry-run           # simulate
    python fix_contacts_phase2.py                     # live
    python fix_contacts_phase2.py --dept 47           # un seul dept
    python fix_contacts_phase2.py --roles enfance     # un seul role
"""
import sys, os, argparse, json, time, unicodedata, re
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

# ── Config ───────────────────────────────────────────────────────────────────
SERPER_KEY  = os.getenv("SERPER_API_KEY", "").strip()
GEMINI_KEY  = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
SERPER_DELAY = 1.2   # secondes entre requêtes

# Rôles manquants détectés, avec leurs requêtes Serper et niveau DB
ROLE_CONFIGS = {
    "enfance": {
        "niveau": "direction",
        "label": "Directeur Enfance / Protection Enfance",
        "relevance_keywords": ["enfance", "famille", "protection", "jeunesse", "mineur", "ase"],
        "detect_sql": """
            NOT EXISTS (
                SELECT 1 FROM prospection_contacts c2
                WHERE c2.entite_id = e.id
                  AND c2.confiance_nom != 'invalide'
                  AND (LOWER(c2.poste_exact) LIKE '%%enfance%%'
                    OR LOWER(c2.poste_exact) LIKE '%%famille%%'))
        """,
        "queries": [
            '"conseil départemental {nom}" "directeur" "enfance" OR "protection de l\'enfance" OR "enfance famille"',
            '"département {nom}" organigramme "directeur enfance" OR "directrice enfance" OR "directeur enfance famille"',
            'site:linkedin.com/in "conseil départemental {nom}" "directeur" "enfance"',
        ],
    },
    "autonomie": {
        "niveau": "direction",
        "label": "Directeur Autonomie / PA-PH / Médico-social",
        "relevance_keywords": ["autonomie", "pa-ph", "pa/ph", "médico", "medico", "handicap", "personnes âgées", "dépendance", "dependance"],
        "detect_sql": """
            NOT EXISTS (
                SELECT 1 FROM prospection_contacts c2
                WHERE c2.entite_id = e.id
                  AND c2.confiance_nom != 'invalide'
                  AND (LOWER(c2.poste_exact) LIKE '%%autonomie%%'
                    OR LOWER(c2.poste_exact) LIKE '%%pa-ph%%'
                    OR LOWER(c2.poste_exact) LIKE '%%pa/ph%%'
                    OR LOWER(c2.poste_exact) LIKE '%%médico-social%%'
                    OR LOWER(c2.poste_exact) LIKE '%%medico-social%%'))
        """,
        "queries": [
            '"conseil départemental {nom}" "directeur" "autonomie" OR "personnes âgées" OR "PA-PH"',
            '"département {nom}" organigramme "directeur autonomie" OR "directrice autonomie" OR "directeur PA-PH"',
            'site:linkedin.com/in "conseil départemental {nom}" "directeur" "autonomie" OR "personnes âgées"',
        ],
    },
    "tarification": {
        "niveau": "responsable_tarification",
        "label": "Responsable Tarification / Financement ESSMS",
        "relevance_keywords": ["tarification", "financement", "essms", "établissement", "etablissement", "offre", "qualité", "qualite", "habilitation"],
        "detect_sql": """
            NOT EXISTS (
                SELECT 1 FROM prospection_contacts c2
                WHERE c2.entite_id = e.id
                  AND c2.confiance_nom != 'invalide'
                  AND c2.niveau = 'responsable_tarification')
        """,
        "queries": [
            '"conseil départemental {nom}" "tarification" ESSMS OR "médico-social" OR "établissements sociaux"',
            '"département {nom}" "responsable tarification" OR "chef service tarification" médico-social OR ESSMS',
            'site:linkedin.com/in "conseil départemental {nom}" "tarification" "médico-social" OR "ESSMS" OR "établissements"',
        ],
    },
}

PROMPT_EXTRACT = """\
Tu es un expert en organigrammes des conseils départementaux français.

## Entité : Conseil Départemental de {nom} (dept {code})
## Rôle recherché : {role_label}

## Extraits de recherche Google
{snippets}

## Tâche
Identifie le ou les contacts occupant le rôle "{role_label}" dans ce conseil départemental.
Retourne UNIQUEMENT un JSON valide :
{{
  "contacts": [
    {{
      "prenom": "...",
      "nom": "...",
      "poste_exact": "...",
      "confiance": "haute|moyenne|basse",
      "source_url": "..."
    }}
  ]
}}
Règles :
- confiance "haute" : organigramme officiel du site du département (moins de 18 mois)
- confiance "moyenne" : LinkedIn récent, nomination officielle, annuaire tiers fiable
- confiance "basse" : source ancienne (>2 ans), indirecte ou non datée
- N'invente jamais de nom. Si aucun contact clair n'est trouvé, retourne "contacts": []
- prenom et nom doivent être séparés (pas tout en nom_complet)
- poste_exact = intitulé exact lu dans la source (pas reformulé)
- IMPORTANT : si le poste trouvé ne correspond PAS directement au domaine du rôle recherché
  (social, médico-social, enfance/famille, autonomie, ESSMS selon le rôle),
  retourne "contacts": [] — il vaut mieux ne rien retourner que de retourner un faux positif
  (ex: un chargé d'animation économique ou développement économique n'est pas pertinent
  pour un rôle de financement ESSMS)
"""


def _norm(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", s.lower().strip())


def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD"),
        port=int(os.getenv("DB_PORT", 5432)),
    )


def serper_search(query: str) -> list[dict]:
    if not SERPER_KEY:
        return []
    try:
        r = requests.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": SERPER_KEY, "Content-Type": "application/json"},
            json={"q": query, "num": 8},
            timeout=25,
        )
        if r.status_code != 200:
            return []
        return [x for x in (r.json().get("organic") or []) if isinstance(x, dict)]
    except Exception as e:
        print(f"  [SERPER ERR] {e}")
        return []


def gemini_extract(nom: str, code: str, role_label: str, results: list[dict]) -> list[dict]:
    if not GEMINI_KEY or not results:
        return []
    snippets = "\n".join(
        f"- [{r.get('link', '')}] {r.get('title', '')} — {r.get('snippet', '')}"
        for r in results[:10]
    )[:3000]
    prompt = PROMPT_EXTRACT.format(nom=nom, code=code, role_label=role_label, snippets=snippets)
    try:
        r = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_KEY}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.1, "maxOutputTokens": 600},
            },
            timeout=30,
        )
        if r.status_code != 200:
            print(f"  [GEMINI ERR] {r.status_code}")
            return []
        raw = r.json()["candidates"][0]["content"]["parts"][0]["text"]
        # Nettoyer le JSON (enlever les blocs markdown)
        raw = re.sub(r"```json\s*|\s*```", "", raw).strip()
        data = json.loads(raw)
        return data.get("contacts") or []
    except Exception as e:
        print(f"  [GEMINI PARSE ERR] {e}")
        return []


def send_bilan_email(stats: dict, dry_run: bool) -> None:
    api_key = os.getenv("ELASTICMAIL_API_KEY", "").strip()
    recipient = os.getenv("NOTIFICATION_EMAIL", "").strip()
    sender = os.getenv("SENDER_EMAIL", "noreply@bmse.fr").strip()
    if not api_key or not recipient:
        print("  [EMAIL] ELASTICMAIL_API_KEY ou NOTIFICATION_EMAIL non configuré — email ignoré")
        return

    mode = "DRY-RUN" if dry_run else "LIVE"
    lines = [f"Phase 2 Financeurs — Recherche contacts manquants [{mode}]\n"]
    lines.append(f"Départements traités : {stats['nb_depts_traites']}")
    lines.append(f"Durée totale         : {stats['elapsed_min']:.1f} min\n")
    lines.append("-" * 50)
    for role_key, rs in stats["par_role"].items():
        lines.append(f"\n{rs['label']}")
        lines.append(f"  Depts manquants     : {rs['nb_manquants']}")
        lines.append(f"  Contacts insérés    : {rs['inseres']}")
        lines.append(f"  Contacts basse gardés: {rs['basse_gardes']}")
        lines.append(f"  Hors-scope filtrés  : {rs['hors_scope']}")
        if rs["details"]:
            lines.append("  Détail par dept :")
            for d in rs["details"]:
                lines.append(f"    [{d['code']}] {d['nom']} — {d['nom_complet']} ({d['confiance']})")
    lines.append("\n" + "-" * 50)
    lines.append(f"\nTOTAL insérés  : {stats['total_inserted']}")
    lines.append(f"TOTAL basse    : {stats['total_basse']}")

    body = "\n".join(lines)
    subject = f"[Phase2-Financeurs] {stats['total_inserted']} contacts ajoutés sur {stats['nb_depts_traites']} depts"
    try:
        resp = requests.post(
            "https://api.elasticemail.com/v2/email/send",
            data={"apikey": api_key, "from": sender, "to": recipient,
                  "subject": subject, "bodyText": body},
            timeout=15,
        )
        if resp.status_code == 200:
            print(f"  [EMAIL] Bilan envoyé à {recipient}")
        else:
            print(f"  [EMAIL ERR] {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        print(f"  [EMAIL ERR] {e}")


def run_phase2(dry_run: bool, dept_filter: str | None, roles_filter: list[str], accept_basse: bool = False):
    import time as _time
    t_start = _time.time()
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    total_found = 0
    total_inserted = 0
    total_basse_kept = 0
    depts_vus: set = set()

    stats: dict = {"par_role": {}}

    roles_to_run = {k: v for k, v in ROLE_CONFIGS.items() if k in roles_filter}

    for role_key, role_cfg in roles_to_run.items():
        print(f"\n{'=' * 65}")
        print(f"  ROLE : {role_cfg['label']}")
        print(f"{'=' * 65}")

        # Départements manquants ce rôle
        where_dept = ""
        params: list = []
        if dept_filter:
            where_dept = "AND e.code = %s"
            params = [dept_filter]

        # detect_sql ne contient pas de %s — on peut combiner sans conflit
        cur.execute(f"""
            SELECT e.id, e.code, e.nom, e.domaine_email
            FROM prospection_entites e
            WHERE e.type_entite = 'departement'
              AND {role_cfg['detect_sql']}
              {where_dept}
            ORDER BY e.code
        """, params)
        depts = cur.fetchall()
        print(f"  Departements manquants : {len(depts)}")

        role_stats = {
            "label": role_cfg["label"],
            "nb_manquants": len(depts),
            "inseres": 0,
            "basse_gardes": 0,
            "hors_scope": 0,
            "details": [],
        }

        for dept in depts:
            eid = dept["id"]
            code = dept["code"]
            nom = dept["nom"]
            depts_vus.add(code)
            print(f"\n  [{code}] {nom}")

            # Lancer les 3 requêtes Serper
            all_results = []
            for q_template in role_cfg["queries"]:
                query = q_template.format(nom=nom, code=code)
                results = serper_search(query)
                all_results.extend(results)
                print(f"    Serper: {len(results)} résultats — {query[:70]}")
                time.sleep(SERPER_DELAY)

            if not all_results:
                print(f"    => Aucun résultat Serper")
                continue

            # Extraction Gemini
            contacts = gemini_extract(nom, code, role_cfg["label"], all_results)
            print(f"    => Gemini: {len(contacts)} contacts extraits")

            for c in contacts:
                prenom = (c.get("prenom") or "").strip()
                nom_c = (c.get("nom") or "").strip()
                poste = (c.get("poste_exact") or "").strip()
                confiance = (c.get("confiance") or "basse").strip().lower()
                source = (c.get("source_url") or "").strip()

                if not nom_c or not poste:
                    continue

                # Vérifier pertinence du poste via relevance_keywords
                poste_norm = _norm(poste)
                keywords = role_cfg.get("relevance_keywords", [])
                if keywords and not any(_norm(kw) in poste_norm for kw in keywords):
                    print(f"    [SKIP hors-scope] {prenom} {nom_c} — {poste}")
                    role_stats["hors_scope"] += 1
                    continue

                if confiance == "basse" and not accept_basse:
                    print(f"    [REVIEW basse]  {prenom} {nom_c} — {poste}")
                    print(f"                    (relancer avec --accept-basse pour insérer)")
                    continue

                nom_complet = f"{prenom} {nom_c}".strip()
                if confiance == "basse":
                    total_basse_kept += 1
                    role_stats["basse_gardes"] += 1
                else:
                    total_found += 1

                # Vérifier doublon
                cur.execute("""
                    SELECT id FROM prospection_contacts
                    WHERE entite_id = %s AND LOWER(TRIM(nom_complet)) = LOWER(TRIM(%s))
                """, (eid, nom_complet))
                if cur.fetchone():
                    print(f"    [EXISTS] {nom_complet}")
                    continue

                marker = "[DRY]" if dry_run else "[ADD]"
                print(f"    {marker} {nom_complet:30s} | {poste[:45]:45s} | conf={confiance}")

                if not dry_run:
                    cur.execute("""
                        INSERT INTO prospection_contacts
                            (entite_id, nom_complet, prenom, nom, poste_exact,
                             niveau, source_nom, confiance_nom, date_extraction)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_DATE)
                        ON CONFLICT (entite_id, nom_complet) DO UPDATE SET
                            poste_exact    = EXCLUDED.poste_exact,
                            niveau         = EXCLUDED.niveau,
                            source_nom     = EXCLUDED.source_nom,
                            confiance_nom  = EXCLUDED.confiance_nom,
                            updated_at     = NOW()
                    """, (
                        eid, nom_complet, prenom, nom_c, poste,
                        role_cfg["niveau"], source, confiance,
                    ))
                    conn.commit()
                    total_inserted += 1
                    role_stats["inseres"] += 1
                    role_stats["details"].append({
                        "code": code, "nom": nom,
                        "nom_complet": nom_complet, "confiance": confiance,
                    })

        stats["par_role"][role_key] = role_stats

    elapsed_min = (_time.time() - t_start) / 60
    print(f"\n{'=' * 65}")
    mode = "DRY-RUN" if dry_run else "APPLIQUE"
    print(f"  BILAN [{mode}]")
    print(f"{'=' * 65}")
    print(f"  Contacts trouves (conf. >= moyenne) : {total_found:>4}")
    if accept_basse:
        print(f"  Contacts basse confiance inclus      : {total_basse_kept:>4}")
    print(f"  Contacts inseres en base             : {total_inserted:>4}")
    print(f"  Duree totale                         : {elapsed_min:.1f} min")

    cur.close()
    conn.close()

    stats["total_inserted"] = total_inserted
    stats["total_basse"] = total_basse_kept
    stats["nb_depts_traites"] = len(depts_vus)
    stats["elapsed_min"] = elapsed_min

    if not dry_run:
        send_bilan_email(stats, dry_run=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--dept", type=str, default=None, help="Code dept unique ex: 47")
    parser.add_argument(
        "--roles",
        type=str,
        default="enfance,autonomie,tarification",
        help="Roles à chercher (enfance,autonomie,tarification)",
    )
    parser.add_argument(
        "--accept-basse",
        action="store_true",
        default=False,
        help="Insérer aussi les contacts confiance=basse si le poste est pertinent",
    )
    args = parser.parse_args()
    roles = [r.strip() for r in args.roles.split(",") if r.strip()]
    run_phase2(dry_run=args.dry_run, dept_filter=args.dept, roles_filter=roles, accept_basse=args.accept_basse)
