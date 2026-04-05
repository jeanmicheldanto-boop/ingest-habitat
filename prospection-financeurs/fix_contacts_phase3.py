"""
Phase 3 — Reconstruction des emails pour prospection_contacts.

Stratégie par domaine :
  1. Collecter les emails déjà présents en base pour ce domaine
     (depuis prospection_contacts ET prospection_email_patterns)
  2. Si pas assez d'exemples → Serper "@domaine" pour trouver des emails réels
  3. Détecter le pattern dominant (prenom.nom / p.nom / nom.prenom / ...)
  4. Reconstruire email_principal pour tous les contacts sans email du domaine
  5. Mettre à jour confiance_email (haute ≥2 exemples, moyenne sinon)
  6. Aussi mettre à jour confiance_email pour les contacts AVEC email mais confiance=basse
     si le pattern confirme que l'email existant est cohérent

Usage :
    python fix_contacts_phase3.py --dry-run
    python fix_contacts_phase3.py
"""
import sys, os, re, unicodedata, time, json
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import psycopg2, psycopg2.extras, requests
from collections import Counter
from dotenv import load_dotenv
import argparse

load_dotenv()

SERPER_KEY   = os.getenv("SERPER_API_KEY", "").strip()
SERPER_DELAY = 1.0
MIN_EXAMPLES_FOR_HAUTE = 2   # nb exemples pour confiance "haute"


# ── Helpers normalization ─────────────────────────────────────────────────────

def _norm(text: str) -> str:
    text = text.lower().strip()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = re.sub(r"[^a-z0-9]", "", text)
    return text


def _is_person_email(email: str) -> bool:
    generic = {
        "contact", "info", "accueil", "direction", "secretariat", "admin",
        "rh", "communication", "compta", "comptabilite", "standard",
        "reception", "candidature", "recrutement", "noreply", "no-reply",
        "webmaster", "courrier", "mairie", "prefecture", "sg",
    }
    local = email.split("@")[0].lower()
    local_stripped = re.sub(r"[^a-z]", "", local)
    return local_stripped not in generic and len(local_stripped) > 2


def _detect_pattern(emails: list[str]) -> tuple[str | None, str]:
    """Retourne (pattern, confiance) depuis une liste d'emails personnels."""
    patterns: list[str] = []
    for email in emails:
        local = email.split("@")[0].lower()
        if "." in local:
            parts = local.split(".")
            if len(parts) == 2:
                patterns.append("p.nom" if len(parts[0]) == 1 else "prenom.nom")
            else:
                patterns.append("prenom.nom")
        elif "-" in local:
            patterns.append("prenom-nom")
        elif "_" in local:
            patterns.append("prenom_nom")
    if not patterns:
        return None, "basse"
    top, count = Counter(patterns).most_common(1)[0]
    confiance = "haute" if count >= MIN_EXAMPLES_FOR_HAUTE else "moyenne"
    return top, confiance


def _build_email(prenom: str, nom: str, pattern: str, domaine: str) -> str | None:
    p = _norm(prenom)
    n = _norm(nom)
    if not p or not n:
        return None
    templates = {
        "prenom.nom":  f"{p}.{n}@{domaine}",
        "p.nom":       f"{p[0]}.{n}@{domaine}",
        "nom.prenom":  f"{n}.{p}@{domaine}",
        "prenom-nom":  f"{p}-{n}@{domaine}",
        "prenom_nom":  f"{p}_{n}@{domaine}",
    }
    return templates.get(pattern)


def _email_matches_pattern(email: str, prenom: str, nom: str, pattern: str, domaine: str) -> bool:
    """Vérifie si un email existant est cohérent avec le pattern détecté."""
    expected = _build_email(prenom, nom, pattern, domaine)
    if not expected:
        return False
    return email.lower().strip() == expected.lower().strip()


# ── Serper ────────────────────────────────────────────────────────────────────

def serper_find_emails(domaine: str, nom_entite: str) -> list[str]:
    """Cherche des emails @domaine via Serper. Retourne les emails trouvés."""
    if not SERPER_KEY:
        return []
    found: set[str] = set()
    queries = [
        f'"@{domaine}"',
        f'"@{domaine}" "{nom_entite}"',
    ]
    for q in queries:
        try:
            r = requests.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": SERPER_KEY, "Content-Type": "application/json"},
                json={"q": q, "num": 10},
                timeout=25,
            )
            if r.status_code == 200:
                for item in (r.json().get("organic") or []):
                    text = f"{item.get('snippet','')} {item.get('title','')} {item.get('link','')}"
                    matches = re.findall(
                        r"[a-zA-Z0-9._%+\-]+@" + re.escape(domaine), text, re.IGNORECASE
                    )
                    found.update(m.lower() for m in matches)
        except Exception as e:
            print(f"    [SERPER ERR] {e}")
        time.sleep(SERPER_DELAY)
    return list(found)


# ── DB ────────────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD"),
        port=int(os.getenv("DB_PORT", 5432)),
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def run_phase3(dry_run: bool):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Récupérer tous les domaines avec au moins un contact sans email
    cur.execute("""
        SELECT e.domaine_email, e.nom AS nom_entite, e.id AS entite_id,
               COUNT(c.id) nb_sans_email
        FROM prospection_entites e
        JOIN prospection_contacts c ON c.entite_id = e.id
        WHERE c.email_principal IS NULL
          AND c.confiance_nom != 'invalide'
          AND e.domaine_email IS NOT NULL
        GROUP BY e.domaine_email, e.nom, e.id
        ORDER BY e.domaine_email
    """)
    entites = cur.fetchall()
    print(f"  {len(entites)} entités avec contacts sans email\n")

    total_rebuilt    = 0
    total_upgraded   = 0
    total_no_pattern = 0

    for entite in entites:
        domaine     = entite["domaine_email"]
        nom_entite  = entite["nom_entite"]
        entite_id   = entite["entite_id"]
        nb_manquants = entite["nb_sans_email"]

        print(f"[{domaine}]  {nom_entite}  ({nb_manquants} sans email)")

        # 1. Emails déjà connus en base pour ce domaine
        cur.execute("""
            SELECT email_principal FROM prospection_contacts
            WHERE entite_id = %s
              AND email_principal IS NOT NULL
              AND confiance_nom != 'invalide'
        """, (entite_id,))
        known_emails = [r["email_principal"] for r in cur.fetchall()]

        # Aussi vérifier prospection_email_patterns si la table existe
        try:
            cur.execute("""
                SELECT email_exemple FROM prospection_email_patterns
                WHERE domaine = %s LIMIT 20
            """, (domaine,))
            known_emails += [r["email_exemple"] for r in cur.fetchall() if r["email_exemple"]]
        except psycopg2.errors.UndefinedTable:
            conn.rollback()
        except Exception:
            conn.rollback()

        person_emails = [e for e in known_emails if _is_person_email(e)]
        print(f"  Emails connus en base : {len(person_emails)}")

        # 2. Serper si moins de 2 exemples
        serper_done = False
        if len(person_emails) < MIN_EXAMPLES_FOR_HAUTE:
            serper_found = serper_find_emails(domaine, nom_entite)
            serper_persons = [e for e in serper_found if _is_person_email(e)]
            print(f"  Serper : {len(serper_found)} emails trouvés, {len(serper_persons)} personnels")
            person_emails = list(set(person_emails + serper_persons))
            serper_done = True

        # 3. Détecter pattern
        pattern, confiance_pattern = _detect_pattern(person_emails)
        if pattern:
            print(f"  Pattern : {pattern}  (confiance={confiance_pattern}, {len(person_emails)} exemples)")
        else:
            print(f"  Pattern : AUCUN détecté — skip")
            total_no_pattern += 1
            print()
            continue

        # 4. Reconstruire emails manquants
        cur.execute("""
            SELECT id, prenom, nom, poste_exact
            FROM prospection_contacts
            WHERE entite_id = %s
              AND email_principal IS NULL
              AND confiance_nom != 'invalide'
              AND prenom IS NOT NULL AND TRIM(prenom) != ''
              AND nom IS NOT NULL AND TRIM(nom) != ''
        """, (entite_id,))
        sans_email = cur.fetchall()

        for c in sans_email:
            email = _build_email(c["prenom"], c["nom"], pattern, domaine)
            if not email:
                continue
            marker = "[DRY]" if dry_run else "[ADD]"
            print(f"  {marker} {c['prenom']} {c['nom']:20s} → {email}  (conf={confiance_pattern})")
            if not dry_run:
                cur.execute("""
                    UPDATE prospection_contacts
                    SET email_principal = %s,
                        confiance_email = %s,
                        updated_at = NOW()
                    WHERE id = %s
                """, (email, confiance_pattern, c["id"]))
                total_rebuilt += 1
            else:
                total_rebuilt += 1

        # 5. Upgrader confiance_email=basse pour contacts qui ont déjà un email cohérent
        cur.execute("""
            SELECT id, prenom, nom, email_principal, confiance_email
            FROM prospection_contacts
            WHERE entite_id = %s
              AND email_principal IS NOT NULL
              AND confiance_email = 'basse'
              AND confiance_nom != 'invalide'
              AND prenom IS NOT NULL AND nom IS NOT NULL
        """, (entite_id,))
        avec_email_basse = cur.fetchall()

        for c in avec_email_basse:
            if _email_matches_pattern(c["email_principal"], c["prenom"], c["nom"], pattern, domaine):
                marker = "[DRY]" if dry_run else "[UPG]"
                print(f"  {marker} upgrade {c['prenom']} {c['nom']:20s}  {c['email_principal']} → conf={confiance_pattern}")
                if not dry_run:
                    cur.execute("""
                        UPDATE prospection_contacts
                        SET confiance_email = %s, updated_at = NOW()
                        WHERE id = %s
                    """, (confiance_pattern, c["id"]))
                    total_upgraded += 1
                else:
                    total_upgraded += 1

        if not dry_run:
            conn.commit()
        print()

    print("=" * 65)
    mode = "DRY-RUN" if dry_run else "APPLIQUE"
    print(f"  BILAN [{mode}]")
    print("=" * 65)
    print(f"  Emails reconstruits     : {total_rebuilt}")
    print(f"  Confiances upgradées    : {total_upgraded}")
    print(f"  Domaines sans pattern   : {total_no_pattern}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", default=False)
    args = parser.parse_args()
    run_phase3(dry_run=args.dry_run)
