-- ============================================================
-- Schéma Supabase — Pipeline prospection financeurs ESSMS
-- Tables isolées, aucune dépendance aux autres tables du projet
-- À exécuter dans l'éditeur SQL Supabase une seule fois
-- ============================================================

-- ── 1. Entités (départements, DIRPJJ, ARS) ───────────────────────────────────
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

COMMENT ON TABLE prospection_entites IS 'Référentiel des entités financeurs/tarificateurs ESSMS (101 CD + 9 DIRPJJ + 18 ARS)';

-- ── 2. Patterns email par domaine ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS prospection_email_patterns (
    id              BIGSERIAL       PRIMARY KEY,
    domaine         TEXT            NOT NULL UNIQUE,
    pattern         TEXT            NOT NULL DEFAULT 'prenom.nom',
    accents         TEXT            NOT NULL DEFAULT 'supprimés' CHECK (accents IN ('conservés', 'supprimés')),
    tirets_noms     TEXT            NOT NULL DEFAULT 'point'     CHECK (tirets_noms IN ('conservés', 'point', 'supprimés')),
    exemples        JSONB,          -- liste des emails exemples trouvés
    confiance       TEXT            NOT NULL DEFAULT 'basse'     CHECK (confiance IN ('haute', 'moyenne', 'basse')),
    created_at      TIMESTAMPTZ     DEFAULT now(),
    updated_at      TIMESTAMPTZ     DEFAULT now()
);

COMMENT ON TABLE prospection_email_patterns IS 'Patterns de construction des emails par domaine (mis en cache, partagés entre contacts d''une même entité)';

-- ── 3. Contacts enrichis ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS prospection_contacts (
    id                  BIGSERIAL       PRIMARY KEY,
    entite_id           BIGINT          NOT NULL REFERENCES prospection_entites(id) ON DELETE CASCADE,
    nom_complet         TEXT            NOT NULL,
    prenom              TEXT,
    nom                 TEXT,
    poste_exact         TEXT,
    niveau              TEXT            CHECK (niveau IN ('dga', 'direction', 'direction_adjointe', 'responsable_tarification', 'operationnel')),
    email_principal     TEXT,
    email_variantes     JSONB,          -- tableau des variantes ex : ["j.dupont@...","jean.dupont@..."]
    linkedin_url        TEXT,
    source_nom          TEXT,           -- URL source du nom
    confiance_nom       TEXT            DEFAULT 'basse' CHECK (confiance_nom IN ('haute', 'moyenne', 'basse', 'invalide')),
    confiance_email     TEXT            DEFAULT 'basse' CHECK (confiance_email IN ('haute', 'moyenne', 'basse', 'inconnue')),
    email_valide_web    BOOLEAN         DEFAULT FALSE,  -- résultat de la validation croisée
    date_extraction     DATE,
    created_at          TIMESTAMPTZ     DEFAULT now(),
    updated_at          TIMESTAMPTZ     DEFAULT now(),
    -- On évite les doublons par (entité, nom normalisé)
    UNIQUE (entite_id, nom_complet)
);

COMMENT ON TABLE prospection_contacts IS 'Contacts identifiés (décideurs et responsables tarification) par entité, avec email reconstitué et profil LinkedIn';

-- ── Index utiles ──────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_contacts_entite        ON prospection_contacts (entite_id);
CREATE INDEX IF NOT EXISTS idx_contacts_niveau        ON prospection_contacts (niveau);
CREATE INDEX IF NOT EXISTS idx_contacts_confiance_nom ON prospection_contacts (confiance_nom);
CREATE INDEX IF NOT EXISTS idx_entites_type_code      ON prospection_entites   (type_entite, code);

-- ── Trigger updated_at ────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION prospection_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_entites_updated_at  ON prospection_entites;
DROP TRIGGER IF EXISTS trg_patterns_updated_at ON prospection_email_patterns;
DROP TRIGGER IF EXISTS trg_contacts_updated_at ON prospection_contacts;

CREATE TRIGGER trg_entites_updated_at
    BEFORE UPDATE ON prospection_entites
    FOR EACH ROW EXECUTE FUNCTION prospection_set_updated_at();

CREATE TRIGGER trg_patterns_updated_at
    BEFORE UPDATE ON prospection_email_patterns
    FOR EACH ROW EXECUTE FUNCTION prospection_set_updated_at();

CREATE TRIGGER trg_contacts_updated_at
    BEFORE UPDATE ON prospection_contacts
    FOR EACH ROW EXECUTE FUNCTION prospection_set_updated_at();

-- ── 4. Signaux de tension tarification/financement (départements uniquement) ──
CREATE TABLE IF NOT EXISTS prospection_signaux (
    id                  BIGSERIAL       PRIMARY KEY,
    entite_id           BIGINT          NOT NULL REFERENCES prospection_entites(id) ON DELETE CASCADE,
    resume              TEXT,                           -- synthèse LLM en 2-4 phrases
    tags                TEXT[],                         -- ex : {"retards_paiement","restriction_budgetaire"}
    niveau_alerte       TEXT            CHECK (niveau_alerte IN ('faible', 'moyen', 'fort')),
    sources             TEXT[],                         -- URLs utilisées par le LLM
    confiance           TEXT            CHECK (confiance IN ('faible', 'moyen', 'fort')),
    periode_couverte    TEXT,                           -- ex : "2023-2025"
    nb_resultats_serper INTEGER         DEFAULT 0,      -- nombre de snippets Serper analysés
    created_at          TIMESTAMPTZ     DEFAULT now()   -- pas de updated_at : chaque signal est immutable
);

COMMENT ON TABLE prospection_signaux IS 'Signaux de tension sur la tarification/financement ESSMS par département, générés par LLM à partir de Serper. Un nouveau signal est créé à chaque run (historique conservé).';

CREATE INDEX IF NOT EXISTS idx_signaux_entite  ON prospection_signaux (entite_id);
CREATE INDEX IF NOT EXISTS idx_signaux_alerte  ON prospection_signaux (niveau_alerte);
