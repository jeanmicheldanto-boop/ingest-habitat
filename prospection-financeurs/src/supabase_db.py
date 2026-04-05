"""
Client PostgreSQL/Supabase pour le pipeline de prospection.
Gère : création des tables, upsert des entités, patterns et contacts.
Réutilise les credentials DB du projet principal (DB_HOST, DB_PASSWORD, etc.).
Aucune interaction avec les autres tables du projet.
"""
import json
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

logger = logging.getLogger(__name__)

# ── DDL ──────────────────────────────────────────────────────────────────────
_DDL_SIGNAUX = """
CREATE TABLE IF NOT EXISTS prospection_signaux (
    id                  BIGSERIAL       PRIMARY KEY,
    entite_id           BIGINT          NOT NULL REFERENCES prospection_entites(id) ON DELETE CASCADE,
    resume              TEXT,
    tags                TEXT[],
    niveau_alerte       TEXT            CHECK (niveau_alerte IN ('faible', 'moyen', 'fort')),
    sources             TEXT[],
    confiance           TEXT            CHECK (confiance IN ('faible', 'moyen', 'fort')),
    periode_couverte    TEXT,
    nb_resultats_serper INTEGER         DEFAULT 0,
    created_at          TIMESTAMPTZ     DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_signaux_entite       ON prospection_signaux (entite_id);
CREATE INDEX IF NOT EXISTS idx_signaux_alerte       ON prospection_signaux (niveau_alerte);
"""

_DDL = """
CREATE TABLE IF NOT EXISTS prospection_entites (
    id              BIGSERIAL       PRIMARY KEY,
    type_entite     TEXT            NOT NULL CHECK (type_entite IN ('departement', 'dirpjj', 'ars')),
    code            TEXT            NOT NULL,
    nom             TEXT            NOT NULL,
    nom_complet     TEXT,
    site_web        TEXT,
    domaine_email   TEXT,
    note            TEXT,
    created_at      TIMESTAMPTZ     DEFAULT now(),
    updated_at      TIMESTAMPTZ     DEFAULT now(),
    UNIQUE (type_entite, code)
);

CREATE TABLE IF NOT EXISTS prospection_email_patterns (
    id              BIGSERIAL       PRIMARY KEY,
    domaine         TEXT            NOT NULL UNIQUE,
    pattern         TEXT            NOT NULL DEFAULT 'prenom.nom',
    accents         TEXT            NOT NULL DEFAULT 'supprimés',
    tirets_noms     TEXT            NOT NULL DEFAULT 'point',
    exemples        JSONB,
    confiance       TEXT            NOT NULL DEFAULT 'basse',
    created_at      TIMESTAMPTZ     DEFAULT now(),
    updated_at      TIMESTAMPTZ     DEFAULT now()
);

CREATE TABLE IF NOT EXISTS prospection_contacts (
    id                  BIGSERIAL       PRIMARY KEY,
    entite_id           BIGINT          NOT NULL REFERENCES prospection_entites(id) ON DELETE CASCADE,
    nom_complet         TEXT            NOT NULL,
    prenom              TEXT,
    nom                 TEXT,
    poste_exact         TEXT,
    niveau              TEXT,
    email_principal     TEXT,
    email_variantes     JSONB,
    linkedin_url        TEXT,
    source_nom          TEXT,
    confiance_nom       TEXT            DEFAULT 'basse',
    confiance_email     TEXT            DEFAULT 'basse',
    email_valide_web    BOOLEAN         DEFAULT FALSE,
    date_extraction     DATE,
    created_at          TIMESTAMPTZ     DEFAULT now(),
    updated_at          TIMESTAMPTZ     DEFAULT now(),
    UNIQUE (entite_id, nom_complet)
);

CREATE INDEX IF NOT EXISTS idx_contacts_entite        ON prospection_contacts (entite_id);
CREATE INDEX IF NOT EXISTS idx_contacts_niveau        ON prospection_contacts (niveau);
CREATE INDEX IF NOT EXISTS idx_contacts_confiance_nom ON prospection_contacts (confiance_nom);
CREATE INDEX IF NOT EXISTS idx_entites_type_code      ON prospection_entites   (type_entite, code);

CREATE OR REPLACE FUNCTION prospection_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END;
$$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_entites_updated_at') THEN
    CREATE TRIGGER trg_entites_updated_at
      BEFORE UPDATE ON prospection_entites
      FOR EACH ROW EXECUTE FUNCTION prospection_set_updated_at();
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_patterns_updated_at') THEN
    CREATE TRIGGER trg_patterns_updated_at
      BEFORE UPDATE ON prospection_email_patterns
      FOR EACH ROW EXECUTE FUNCTION prospection_set_updated_at();
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_contacts_updated_at') THEN
    CREATE TRIGGER trg_contacts_updated_at
      BEFORE UPDATE ON prospection_contacts
      FOR EACH ROW EXECUTE FUNCTION prospection_set_updated_at();
  END IF;
END $$;
"""


class ProspectionDB:
    """
    Encapsule toutes les opérations DB pour le pipeline de prospection.
    Utilise psycopg2 + connexion directe PostgreSQL Supabase.
    """

    def __init__(self):
        self._conn: psycopg2.extensions.connection | None = None

    def connect(self) -> None:
        self._conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            sslmode="require",
            connect_timeout=15,
        )
        self._conn.autocommit = False
        logger.info("Connexion Supabase OK (%s)", DB_HOST)

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.debug("Connexion Supabase fermée")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn and not self._conn.closed:
            if exc_type:
                self._conn.rollback()
            else:
                self._conn.commit()
        self.close()

    # ── Schema ────────────────────────────────────────────────────────────────

    def create_tables(self) -> None:
        """Crée les tables prospection si elles n'existent pas encore."""
        with self._conn.cursor() as cur:
            cur.execute(_DDL)
            cur.execute(_DDL_SIGNAUX)
        self._conn.commit()
        logger.info("Tables prospection créées / vérifiées")

    # ── Entités ───────────────────────────────────────────────────────────────

    def upsert_entite(self, entity: dict[str, Any], entity_type: str) -> int:
        """
        Insère ou met à jour une entité.
        Retourne l'id de la ligne.
        """
        sql = """
            INSERT INTO prospection_entites
                (type_entite, code, nom, nom_complet, site_web, domaine_email, note)
            VALUES (%(type_entite)s, %(code)s, %(nom)s, %(nom_complet)s,
                    %(site_web)s, %(domaine_email)s, %(note)s)
            ON CONFLICT (type_entite, code) DO UPDATE SET
                nom            = EXCLUDED.nom,
                nom_complet    = EXCLUDED.nom_complet,
                site_web       = EXCLUDED.site_web,
                domaine_email  = EXCLUDED.domaine_email,
                note           = EXCLUDED.note,
                updated_at     = now()
            RETURNING id
        """
        params = {
            "type_entite":  entity_type,
            "code":         entity.get("code", ""),
            "nom":          entity.get("nom", ""),
            "nom_complet":  entity.get("nom_complet", ""),
            "site_web":     entity.get("site_web", ""),
            "domaine_email": entity.get("domaine_email", ""),
            "note":         entity.get("note", ""),
        }
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            self._conn.commit()
        entite_id = row[0]
        logger.debug("Entité upsert : %s %s → id=%d", entity_type, entity.get("code"), entite_id)
        return entite_id

    def get_entite_id(self, entity_type: str, code: str) -> int | None:
        """Récupère l'id d'une entité par type+code."""
        sql = "SELECT id FROM prospection_entites WHERE type_entite = %s AND code = %s"
        with self._conn.cursor() as cur:
            cur.execute(sql, (entity_type, code))
            row = cur.fetchone()
        return row[0] if row else None

    # ── Email patterns ────────────────────────────────────────────────────────

    def upsert_email_pattern(self, domain: str, pattern_info: dict[str, Any]) -> None:
        """Insère ou met à jour le pattern email d'un domaine."""
        sql = """
            INSERT INTO prospection_email_patterns
                (domaine, pattern, accents, tirets_noms, exemples, confiance)
            VALUES (%(domaine)s, %(pattern)s, %(accents)s, %(tirets_noms)s,
                    %(exemples)s, %(confiance)s)
            ON CONFLICT (domaine) DO UPDATE SET
                pattern     = EXCLUDED.pattern,
                accents     = EXCLUDED.accents,
                tirets_noms = EXCLUDED.tirets_noms,
                exemples    = EXCLUDED.exemples,
                confiance   = EXCLUDED.confiance,
                updated_at  = now()
        """
        exemples = pattern_info.get("exemples_trouves", pattern_info.get("exemples", []))
        params = {
            "domaine":      domain,
            "pattern":      pattern_info.get("pattern", "prenom.nom"),
            "accents":      pattern_info.get("accents", "supprimés"),
            "tirets_noms":  pattern_info.get("tirets_noms", "point"),
            "exemples":     json.dumps(exemples, ensure_ascii=False),
            "confiance":    pattern_info.get("confiance", "basse"),
        }
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
        self._conn.commit()
        logger.debug("Email pattern upsert : @%s → %s", domain, params["pattern"])

    def get_email_pattern(self, domain: str) -> dict[str, Any] | None:
        """Retourne le pattern email stocké pour un domaine, ou None."""
        sql = "SELECT pattern, accents, tirets_noms, exemples, confiance FROM prospection_email_patterns WHERE domaine = %s"
        with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (domain,))
            row = cur.fetchone()
        if not row:
            return None
        result = dict(row)
        if isinstance(result.get("exemples"), str):
            result["exemples"] = json.loads(result["exemples"])
        return result

    # ── Contacts ──────────────────────────────────────────────────────────────

    def upsert_contact(self, entite_id: int, contact: dict[str, Any]) -> int:
        """
        Insère ou met à jour un contact.
        Retourne l'id de la ligne.
        """
        sql = """
            INSERT INTO prospection_contacts (
                entite_id, nom_complet, prenom, nom, poste_exact, niveau,
                email_principal, email_variantes, linkedin_url,
                source_nom, confiance_nom, confiance_email,
                email_valide_web, date_extraction
            ) VALUES (
                %(entite_id)s, %(nom_complet)s, %(prenom)s, %(nom)s,
                %(poste_exact)s, %(niveau)s,
                %(email_principal)s, %(email_variantes)s, %(linkedin_url)s,
                %(source_nom)s, %(confiance_nom)s, %(confiance_email)s,
                %(email_valide_web)s, %(date_extraction)s
            )
            ON CONFLICT (entite_id, nom_complet) DO UPDATE SET
                prenom          = EXCLUDED.prenom,
                nom             = EXCLUDED.nom,
                poste_exact     = EXCLUDED.poste_exact,
                niveau          = EXCLUDED.niveau,
                email_principal = EXCLUDED.email_principal,
                email_variantes = EXCLUDED.email_variantes,
                linkedin_url    = COALESCE(EXCLUDED.linkedin_url, prospection_contacts.linkedin_url),
                source_nom      = EXCLUDED.source_nom,
                confiance_nom   = EXCLUDED.confiance_nom,
                confiance_email = EXCLUDED.confiance_email,
                email_valide_web = EXCLUDED.email_valide_web,
                date_extraction  = EXCLUDED.date_extraction,
                updated_at       = now()
            RETURNING id
        """
        variantes = contact.get("email_variantes", [])
        params = {
            "entite_id":        entite_id,
            "nom_complet":      contact.get("nom_complet", ""),
            "prenom":           contact.get("prenom", ""),
            "nom":              contact.get("nom", ""),
            "poste_exact":      contact.get("poste_exact", ""),
            "niveau":           contact.get("niveau", ""),
            "email_principal":  contact.get("email_principal"),
            "email_variantes":  json.dumps(variantes, ensure_ascii=False),
            "linkedin_url":     contact.get("linkedin_url"),
            "source_nom":       contact.get("source_nom", ""),
            "confiance_nom":    contact.get("confiance_nom", "basse"),
            "confiance_email":  contact.get("confiance_email", "basse"),
            "email_valide_web": contact.get("email_valide_web", False),
            "date_extraction":  contact.get("date_extraction") or date.today().isoformat(),
        }
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            self._conn.commit()
        contact_id = row[0]
        logger.debug(
            "Contact upsert : %s → id=%d, email=%s",
            params["nom_complet"], contact_id, params["email_principal"]
        )
        return contact_id

    def mark_email_validated(self, contact_id: int) -> None:
        """Marque un contact comme ayant son email validé par recherche web."""
        with self._conn.cursor() as cur:
            cur.execute(
                "UPDATE prospection_contacts SET email_valide_web = TRUE, updated_at = now() WHERE id = %s",
                (contact_id,)
            )
        self._conn.commit()

    # ── Statistiques ─────────────────────────────────────────────────────────

    def stats(self) -> dict[str, Any]:
        """Retourne des statistiques sur l'état de la base."""
        queries = {
            "total_entites":          "SELECT COUNT(*) FROM prospection_entites",
            "total_contacts":         "SELECT COUNT(*) FROM prospection_contacts",
            "contacts_avec_email":    "SELECT COUNT(*) FROM prospection_contacts WHERE email_principal IS NOT NULL",
            "contacts_avec_linkedin": "SELECT COUNT(*) FROM prospection_contacts WHERE linkedin_url IS NOT NULL",
            "contacts_email_valide":  "SELECT COUNT(*) FROM prospection_contacts WHERE email_valide_web = TRUE",
            "patterns_domaines":      "SELECT COUNT(*) FROM prospection_email_patterns",
        }
        result: dict[str, Any] = {}
        with self._conn.cursor() as cur:
            for key, sql in queries.items():
                cur.execute(sql)
                result[key] = cur.fetchone()[0]

        # Par type
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT type_entite, COUNT(*) FROM prospection_entites GROUP BY type_entite"
            )
            result["entites_par_type"] = dict(cur.fetchall())
        return result

    def get_processed_entity_codes(self, entity_type: str) -> set[str]:
        """Retourne les codes des entités qui ont déjà au moins un contact en base."""
        sql = """
            SELECT DISTINCT e.code
            FROM prospection_entites e
            JOIN prospection_contacts c ON c.entite_id = e.id
            WHERE e.type_entite = %s
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (entity_type,))
            return {row[0] for row in cur.fetchall()}

    # ── Signaux ───────────────────────────────────────────────────────────────

    def upsert_signal(self, entite_id: int, signal: dict[str, Any]) -> int:
        """
        Insère un nouveau signal pour une entité.
        Chaque appel crée une nouvelle ligne (historique conservé).
        Retourne l'id de la ligne.
        """
        sql = """
            INSERT INTO prospection_signaux
                (entite_id, resume, tags, niveau_alerte, sources,
                 confiance, periode_couverte, nb_resultats_serper)
            VALUES
                (%(entite_id)s, %(resume)s, %(tags)s, %(niveau_alerte)s, %(sources)s,
                 %(confiance)s, %(periode_couverte)s, %(nb_resultats_serper)s)
            RETURNING id
        """
        tags = signal.get("tags", []) or []
        sources = signal.get("sources_utilisees", []) or []
        params = {
            "entite_id":            entite_id,
            "resume":               signal.get("resume", ""),
            "tags":                 tags,
            "niveau_alerte":        signal.get("niveau_alerte", "faible"),
            "sources":              sources,
            "confiance":            signal.get("confiance", "faible"),
            "periode_couverte":     signal.get("periode_couverte", "2023-2025"),
            "nb_resultats_serper":  signal.get("nb_resultats_serper", 0),
        }
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            self._conn.commit()
        signal_id = row[0]
        logger.debug("Signal upsert : entite_id=%d → id=%d, alerte=%s",
                     entite_id, signal_id, params["niveau_alerte"])
        return signal_id

    def get_latest_signal(self, entite_id: int) -> dict[str, Any] | None:
        """
        Retourne le signal le plus récent pour une entité, ou None.
        """
        sql = """
            SELECT id, resume, tags, niveau_alerte, sources,
                   confiance, periode_couverte, nb_resultats_serper, created_at
            FROM prospection_signaux
            WHERE entite_id = %s
            ORDER BY created_at DESC
            LIMIT 1
        """
        with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (entite_id,))
            row = cur.fetchone()
        return dict(row) if row else None

    def get_processed_signal_codes(self) -> set[str]:
        """
        Retourne les codes des départements pour lesquels un signal a déjà été généré.
        """
        sql = """
            SELECT DISTINCT e.code
            FROM prospection_entites e
            JOIN prospection_signaux s ON s.entite_id = e.id
            WHERE e.type_entite = 'departement'
        """
        with self._conn.cursor() as cur:
            cur.execute(sql)
            return {row[0] for row in cur.fetchall()}
