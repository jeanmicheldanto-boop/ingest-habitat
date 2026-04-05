"""
Phase 1 — Nettoyage SQL des contacts financeurs.

Actions :
  1. Hors-scope → confiance_nom='invalide'
     - Assistants de direction
     - Coordinateurs
     - Directeurs territoriaux (élargit check-territorial existant)
     - Chefs de service sans lien tarification/ESSMS
     - Ingénieurs sociaux

  2. Reclassification → niveau='responsable_tarification'
     - Chefs de service tarification/financement/ESSMS/PA-PH/ASE

  3. Déduplication
     - Même entité + même nom normalisé → supprime le doublon (garde le + ancien)
"""
import sys, os, argparse, unicodedata, re
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, str(os.path.join(os.path.dirname(__file__), "..")))

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def _norm(s: str) -> str:
    """Normalise un nom : minuscules, sans accents, sans espaces multiples."""
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


# ── Règles hors-scope ────────────────────────────────────────────────────────
# Un poste est hors-scope si l'un des patterns ci-dessous matche ET
# qu'aucun mot-clé ESSMS ne le sauve.

HORS_SCOPE_PATTERNS = [
    "%assistant%",
    "%assistante%",
    "%coordinat%",
    "%ingenieur%",
    "%ingénieur%",
    "%adjoint au chef de service%",
    "%adjoint de la directrice%",
    "%directeur territorial%",
    "%directrice territoriale%",
    "%directeur de territoire%",
    "%directrice de territoire%",
    "%responsable territorial%",
    "%responsable territoriale%",
]

# Ces mots dans le poste SAUVENT un contact malgré un pattern hors-scope
ESSMS_RESCUE_KEYWORDS = [
    "tarif", "essms", "autonomie", "enfance", "pa-ph", "pa/ph",
    "handicap", "insertion", "medico-social", "médico-social",
    "établissement", "etablissement", "financement", "ase",
]

# Chefs de service génériques (sans lien ESSMS) → hors-scope
CHEF_SERVICE_GENERIQUE = [
    "chef de service%",
    "chef de pôle%",
    "chef de pole%",
]
CHEF_SERVICE_RESCUE = [
    "tarif", "essms", "financement", "etablissement", "établissement",
    "pa-ph", "pa/ph", "ase", "médico", "medico", "autonomie", "enfance",
]


# ── Règles reclassification responsable_tarification ────────────────────────
TARIF_UPGRADE_PATTERNS = [
    "%chef de service%tarif%",
    "%chef de service%financement%",
    "%chef de service%essms%",
    "%chef de service%etablissement%",
    "%chef de service%établissement%",
    "%chef de service%pa%-ph%",
    "%chef de service%protection de l%enfance%",
    "%chef de service%ase%",
    "%chef de service%médico%",
    "%chef de service%medico%",
    "%chef de service%autonomie%",
]


def _is_hors_scope(poste: str) -> tuple[bool, str]:
    """Retourne (True, raison) si le poste est hors-scope."""
    p = _norm(poste or "")

    # Patterns directs hors-scope
    for pat in HORS_SCOPE_PATTERNS:
        needle = pat.replace("%", "")
        if needle in p:
            rescued = any(kw in p for kw in ESSMS_RESCUE_KEYWORDS)
            if not rescued:
                return True, f"pattern hors-scope: '{needle}'"

    # Chefs de service génériques
    for pat in CHEF_SERVICE_GENERIQUE:
        needle = pat.rstrip("%").replace("%", "")
        if p.startswith(needle) or needle in p:
            rescued = any(kw in p for kw in CHEF_SERVICE_RESCUE)
            if not rescued:
                return True, f"chef de service sans lien ESSMS"

    return False, ""


def _is_tarif_upgrade(poste: str) -> bool:
    """Retourne True si le poste est un chef de service tarification."""
    p = _norm(poste or "")
    return any(
        all(
            seg.replace("%", "") in p
            for seg in pat.split("%") if seg
        )
        for pat in TARIF_UPGRADE_PATTERNS
    )


def run_phase1(dry_run: bool):
    conn = get_conn()
    cur = conn.cursor()

    total_invalide = 0
    total_tarif = 0
    total_dedup = 0

    # ── 1. Hors-scope → invalide ─────────────────────────────────────────────
    print("=" * 65)
    print("  PHASE 1.1 — Contacts hors-scope -> invalide")
    print("=" * 65)

    cur.execute("""
        SELECT c.id, c.nom_complet, c.poste_exact, c.niveau, e.code, e.nom
        FROM prospection_contacts c
        JOIN prospection_entites e ON e.id = c.entite_id
        WHERE c.confiance_nom != 'invalide'
        ORDER BY e.code, c.nom_complet
    """)
    rows = cur.fetchall()

    invalide_ids = []
    for cid, nom, poste, niveau, code, entite in rows:
        hors, raison = _is_hors_scope(poste)
        if hors:
            invalide_ids.append(cid)
            marker = "[DRY]" if dry_run else "[DEL]"
            print(f"  {marker} [{code}] {(nom or '')[:30]:30s} | {(poste or '')[:55]:55s}")
            print(f"         raison: {raison}")

    print(f"\n  => {len(invalide_ids)} contacts a invalider")
    if invalide_ids and not dry_run:
        cur.execute(
            "UPDATE prospection_contacts SET confiance_nom='invalide' WHERE id = ANY(%s)",
            (invalide_ids,)
        )
        conn.commit()
    total_invalide = len(invalide_ids)

    # ── 2. Reclassification → responsable_tarification ──────────────────────
    print(f"\n{'=' * 65}")
    print("  PHASE 1.2 — Chefs de service tarif -> responsable_tarification")
    print("=" * 65)

    cur.execute("""
        SELECT c.id, c.nom_complet, c.poste_exact, c.niveau, e.code
        FROM prospection_contacts c
        JOIN prospection_entites e ON e.id = c.entite_id
        WHERE c.niveau != 'responsable_tarification'
          AND c.confiance_nom != 'invalide'
        ORDER BY e.code
    """)
    rows = cur.fetchall()

    tarif_ids = []
    for cid, nom, poste, niveau, code in rows:
        if _is_tarif_upgrade(poste):
            tarif_ids.append(cid)
            marker = "[DRY]" if dry_run else "[UPD]"
            print(f"  {marker} [{code}] {(nom or '')[:30]:30s} | {(poste or '')[:55]:55s}")

    print(f"\n  => {len(tarif_ids)} contacts a reclasser en responsable_tarification")
    if tarif_ids and not dry_run:
        cur.execute(
            "UPDATE prospection_contacts SET niveau='responsable_tarification' WHERE id = ANY(%s)",
            (tarif_ids,)
        )
        conn.commit()
    total_tarif = len(tarif_ids)

    # ── 3. Déduplication ─────────────────────────────────────────────────────
    print(f"\n{'=' * 65}")
    print("  PHASE 1.3 — Deduplication (meme entite + meme nom)")
    print("=" * 65)

    cur.execute("""
        SELECT c.id, c.entite_id, c.nom_complet, c.confiance_nom, e.code
        FROM prospection_contacts c
        JOIN prospection_entites e ON e.id = c.entite_id
        ORDER BY c.entite_id, c.id
    """)
    rows = cur.fetchall()

    # Grouper par (entite_id, nom_normalise)
    seen: dict[tuple, int] = {}   # (entite_id, nom_norm) -> id_a_garder (premier = plus ancien)
    delete_ids = []
    for cid, entite_id, nom, confiance, code in rows:
        key = (entite_id, _norm(nom))
        if not key[1]:
            continue
        if key in seen:
            # doublon — supprimer ce contact (le plus récent)
            delete_ids.append(cid)
            marker = "[DRY]" if dry_run else "[DEL]"
            print(f"  {marker} [{code}] doublon id={cid}: '{nom}' (garde id={seen[key]})")
        else:
            seen[key] = cid

    print(f"\n  => {len(delete_ids)} doublons a supprimer")
    if delete_ids and not dry_run:
        cur.execute("DELETE FROM prospection_contacts WHERE id = ANY(%s)", (delete_ids,))
        conn.commit()
    total_dedup = len(delete_ids)

    # ── Bilan ────────────────────────────────────────────────────────────────
    print(f"\n{'=' * 65}")
    mode = "DRY-RUN — aucune modification" if dry_run else "APPLIQUE"
    print(f"  BILAN [{mode}]")
    print(f"{'=' * 65}")
    print(f"  Invalides (hors-scope)         : {total_invalide:>4}")
    print(f"  Reclasses responsable_tarif    : {total_tarif:>4}")
    print(f"  Doublons supprimes             : {total_dedup:>4}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", default=False)
    args = parser.parse_args()
    run_phase1(dry_run=args.dry_run)
