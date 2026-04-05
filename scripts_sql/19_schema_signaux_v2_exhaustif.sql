-- Schema V2 exhaustif : couverture universelle + stockage fin des snippets
-- Objectif : couvrir tous les gestionnaires restants a cout maitrise,
-- tout en stockant les preuves web pour recalibrer les regles sans reconsommer Serper.

BEGIN;

ALTER TABLE public.finess_gestionnaire
    ADD COLUMN IF NOT EXISTS signal_v2_statut_couverture TEXT,
    ADD COLUMN IF NOT EXISTS signal_v2_niveau_suspicion TEXT,
    ADD COLUMN IF NOT EXISTS signal_v2_imputabilite TEXT,
    ADD COLUMN IF NOT EXISTS signal_v2_review_required BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS signal_v2_scope_issue TEXT,
    ADD COLUMN IF NOT EXISTS signal_v2_phase TEXT,
    ADD COLUMN IF NOT EXISTS signal_v2_run_id TEXT,
    ADD COLUMN IF NOT EXISTS signal_v2_queries_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS signal_v2_snippets_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS signal_v2_last_query_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS signal_v2_decision_detail TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'finess_gestionnaire_signal_v2_statut_couverture_check'
    ) THEN
        ALTER TABLE public.finess_gestionnaire
            ADD CONSTRAINT finess_gestionnaire_signal_v2_statut_couverture_check
            CHECK (
                signal_v2_statut_couverture IS NULL
                OR signal_v2_statut_couverture IN (
                    'a_traiter',
                    'no_signal_public',
                    'signal_public_non_tension',
                    'signal_tension_probable',
                    'signal_ambigu_review'
                )
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'finess_gestionnaire_signal_v2_niveau_suspicion_check'
    ) THEN
        ALTER TABLE public.finess_gestionnaire
            ADD CONSTRAINT finess_gestionnaire_signal_v2_niveau_suspicion_check
            CHECK (
                signal_v2_niveau_suspicion IS NULL
                OR signal_v2_niveau_suspicion IN (
                    'aucun',
                    'possible',
                    'probable',
                    'certain'
                )
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'finess_gestionnaire_signal_v2_imputabilite_check'
    ) THEN
        ALTER TABLE public.finess_gestionnaire
            ADD CONSTRAINT finess_gestionnaire_signal_v2_imputabilite_check
            CHECK (
                signal_v2_imputabilite IS NULL
                OR signal_v2_imputabilite IN (
                    'inconnue',
                    'gestionnaire_probable',
                    'gestionnaire_certain'
                )
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'finess_gestionnaire_signal_v2_phase_check'
    ) THEN
        ALTER TABLE public.finess_gestionnaire
            ADD CONSTRAINT finess_gestionnaire_signal_v2_phase_check
            CHECK (
                signal_v2_phase IS NULL
                OR signal_v2_phase IN (
                    'G0',
                    'G1',
                    'G2',
                    'passe_a',
                    'passe_b'
                )
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'finess_gestionnaire_signal_v2_scope_issue_check'
    ) THEN
        ALTER TABLE public.finess_gestionnaire
            ADD CONSTRAINT finess_gestionnaire_signal_v2_scope_issue_check
            CHECK (
                signal_v2_scope_issue IS NULL
                OR signal_v2_scope_issue IN (
                    'groupe_non_imputable',
                    'ancrage_faible',
                    'signal_local_limite',
                    'conflit_entite',
                    'bruit_sectoriel',
                    'preuves_insuffisantes',
                    'polarite_ambigue'
                )
            );
    END IF;
END $$;

COMMENT ON COLUMN public.finess_gestionnaire.signal_v2_statut_couverture IS 'Issue du criblage exhaustif V2: a_traiter, no_signal_public, signal_public_non_tension, signal_tension_probable, signal_ambigu_review';
COMMENT ON COLUMN public.finess_gestionnaire.signal_v2_niveau_suspicion IS 'Niveau de suspicion issu du criblage large: aucun, possible, probable, certain';
COMMENT ON COLUMN public.finess_gestionnaire.signal_v2_imputabilite IS 'Niveau d imputabilite du signal au gestionnaire: inconnue, gestionnaire_probable, gestionnaire_certain';
COMMENT ON COLUMN public.finess_gestionnaire.signal_v2_review_required IS 'Flag QA pour les cas ambigus ou multi-axes insuffisamment etayes';
COMMENT ON COLUMN public.finess_gestionnaire.signal_v2_scope_issue IS 'Cause principale de doute d imputabilite ou de portee';
COMMENT ON COLUMN public.finess_gestionnaire.signal_v2_phase IS 'Derniere phase de traitement exhaustive: G0, G1, G2, passe_a, passe_b';
COMMENT ON COLUMN public.finess_gestionnaire.signal_v2_run_id IS 'Identifiant logique du run ayant produit la derniere decision V2';
COMMENT ON COLUMN public.finess_gestionnaire.signal_v2_queries_count IS 'Nombre de requetes web executees pour ce gestionnaire dans le cadre V2 exhaustif';
COMMENT ON COLUMN public.finess_gestionnaire.signal_v2_snippets_count IS 'Nombre de snippets stockes pour ce gestionnaire dans la campagne V2 exhaustive';
COMMENT ON COLUMN public.finess_gestionnaire.signal_v2_last_query_at IS 'Date de la derniere requete de discovery/qualification V2';
COMMENT ON COLUMN public.finess_gestionnaire.signal_v2_decision_detail IS 'Trace textuelle courte de la decision de criblage ou de qualification';

UPDATE public.finess_gestionnaire
SET signal_v2_statut_couverture = COALESCE(signal_v2_statut_couverture, 'a_traiter'),
    signal_v2_niveau_suspicion = COALESCE(
        signal_v2_niveau_suspicion,
        CASE
            WHEN signal_financier OR signal_rh OR signal_qualite OR signal_juridique THEN 'certain'
            ELSE 'aucun'
        END
    ),
    signal_v2_imputabilite = COALESCE(
        signal_v2_imputabilite,
        CASE
            WHEN signal_v2_methode = 'serper_passe_b' THEN 'gestionnaire_probable'
            WHEN signal_financier OR signal_rh OR signal_qualite OR signal_juridique THEN 'gestionnaire_probable'
            ELSE 'inconnue'
        END
    ),
    signal_v2_phase = COALESCE(
        signal_v2_phase,
        CASE
            WHEN signal_v2_methode = 'serper_passe_b' THEN 'passe_b'
            WHEN signal_v2_methode = 'serper_passe_a' THEN 'passe_a'
            ELSE NULL
        END
    ),
    signal_v2_review_required = COALESCE(signal_v2_review_required, FALSE)
WHERE signal_v2_statut_couverture IS NULL
   OR signal_v2_niveau_suspicion IS NULL
   OR signal_v2_imputabilite IS NULL
   OR signal_v2_phase IS NULL;

CREATE TABLE IF NOT EXISTS public.finess_signal_v2_run (
    run_id TEXT PRIMARY KEY,
    phase TEXT NOT NULL,
    scope_type TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running',
    initiated_by TEXT,
    model_provider TEXT,
    model_name TEXT,
    query_budget INTEGER,
    notes TEXT,
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'finess_signal_v2_run_phase_check'
    ) THEN
        ALTER TABLE public.finess_signal_v2_run
            ADD CONSTRAINT finess_signal_v2_run_phase_check
            CHECK (phase IN ('G0', 'G1', 'G2', 'passe_a', 'passe_b'));
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'finess_signal_v2_run_status_check'
    ) THEN
        ALTER TABLE public.finess_signal_v2_run
            ADD CONSTRAINT finess_signal_v2_run_status_check
            CHECK (status IN ('running', 'completed', 'failed', 'cancelled'));
    END IF;
END $$;

COMMENT ON TABLE public.finess_signal_v2_run IS 'Journal des campagnes de criblage/qualification V2 exhaustive';

CREATE TABLE IF NOT EXISTS public.finess_signal_v2_snippet (
    id BIGSERIAL PRIMARY KEY,
    id_gestionnaire TEXT NOT NULL,
    run_id TEXT,
    phase TEXT NOT NULL,
    query_hash TEXT NOT NULL,
    query_text TEXT NOT NULL,
    serper_rank INTEGER,
    title TEXT,
    snippet TEXT,
    url TEXT,
    domain TEXT,
    published_at TIMESTAMPTZ,
    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    alias_hit_type TEXT,
    alias_hit_value TEXT,
    scope_label TEXT,
    imputabilite TEXT,
    suspicion_level TEXT,
    risk_score NUMERIC(6,2),
    scope_score NUMERIC(6,2),
    freshness_score NUMERIC(6,2),
    source_confidence TEXT,
    used_for_decision BOOLEAN NOT NULL DEFAULT FALSE,
    discarded_reason TEXT,
    llm_payload JSONB,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'finess_signal_v2_snippet_gestionnaire_fk'
    ) THEN
        ALTER TABLE public.finess_signal_v2_snippet
            ADD CONSTRAINT finess_signal_v2_snippet_gestionnaire_fk
            FOREIGN KEY (id_gestionnaire)
            REFERENCES public.finess_gestionnaire(id_gestionnaire)
            ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'finess_signal_v2_snippet_run_fk'
    ) THEN
        ALTER TABLE public.finess_signal_v2_snippet
            ADD CONSTRAINT finess_signal_v2_snippet_run_fk
            FOREIGN KEY (run_id)
            REFERENCES public.finess_signal_v2_run(run_id)
            ON DELETE SET NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'finess_signal_v2_snippet_phase_check'
    ) THEN
        ALTER TABLE public.finess_signal_v2_snippet
            ADD CONSTRAINT finess_signal_v2_snippet_phase_check
            CHECK (phase IN ('G0', 'G1', 'G2', 'passe_a', 'passe_b'));
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'finess_signal_v2_snippet_alias_hit_type_check'
    ) THEN
        ALTER TABLE public.finess_signal_v2_snippet
            ADD CONSTRAINT finess_signal_v2_snippet_alias_hit_type_check
            CHECK (
                alias_hit_type IS NULL
                OR alias_hit_type IN ('strong', 'weak', 'none')
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'finess_signal_v2_snippet_scope_label_check'
    ) THEN
        ALTER TABLE public.finess_signal_v2_snippet
            ADD CONSTRAINT finess_signal_v2_snippet_scope_label_check
            CHECK (
                scope_label IS NULL
                OR scope_label IN (
                    'gestionnaire_exact',
                    'etablissement_local',
                    'entite_du_groupe',
                    'secteur_general',
                    'hors_perimetre'
                )
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'finess_signal_v2_snippet_imputabilite_check'
    ) THEN
        ALTER TABLE public.finess_signal_v2_snippet
            ADD CONSTRAINT finess_signal_v2_snippet_imputabilite_check
            CHECK (
                imputabilite IS NULL
                OR imputabilite IN ('inconnue', 'gestionnaire_probable', 'gestionnaire_certain')
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'finess_signal_v2_snippet_suspicion_level_check'
    ) THEN
        ALTER TABLE public.finess_signal_v2_snippet
            ADD CONSTRAINT finess_signal_v2_snippet_suspicion_level_check
            CHECK (
                suspicion_level IS NULL
                OR suspicion_level IN ('aucun', 'possible', 'probable', 'certain')
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'finess_signal_v2_snippet_source_confidence_check'
    ) THEN
        ALTER TABLE public.finess_signal_v2_snippet
            ADD CONSTRAINT finess_signal_v2_snippet_source_confidence_check
            CHECK (
                source_confidence IS NULL
                OR source_confidence IN ('haute', 'moyenne', 'basse')
            );
    END IF;
END $$;

COMMENT ON TABLE public.finess_signal_v2_snippet IS 'Snippets stockes pour recalibration offline et audit des decisions V2';
COMMENT ON COLUMN public.finess_signal_v2_snippet.query_hash IS 'Hash stable de la requete pour rapprochement avec finess_cache_serper';
COMMENT ON COLUMN public.finess_signal_v2_snippet.used_for_decision IS 'Indique si le snippet a contribue a la decision finale';
COMMENT ON COLUMN public.finess_signal_v2_snippet.discarded_reason IS 'Motif d exclusion du snippet (bruit, groupe, non imputable, etc.)';
COMMENT ON COLUMN public.finess_signal_v2_snippet.llm_payload IS 'Trace JSON optionnelle des classifications LLM sur ce snippet';

CREATE UNIQUE INDEX IF NOT EXISTS uq_finess_signal_v2_snippet_run_query_url
    ON public.finess_signal_v2_snippet (
        (COALESCE(run_id, '')),
        phase,
        query_hash,
        (COALESCE(url, ''))
    );

CREATE INDEX IF NOT EXISTS idx_finess_signal_v2_snippet_gestionnaire
    ON public.finess_signal_v2_snippet (id_gestionnaire, retrieved_at DESC);

CREATE INDEX IF NOT EXISTS idx_finess_signal_v2_snippet_run
    ON public.finess_signal_v2_snippet (run_id, phase, retrieved_at DESC);

CREATE INDEX IF NOT EXISTS idx_finess_signal_v2_snippet_scope
    ON public.finess_signal_v2_snippet (scope_label, imputabilite, suspicion_level);

CREATE INDEX IF NOT EXISTS idx_finess_signal_v2_snippet_used
    ON public.finess_signal_v2_snippet (used_for_decision, source_confidence);

CREATE INDEX IF NOT EXISTS idx_finess_signal_v2_snippet_domain
    ON public.finess_signal_v2_snippet (domain, retrieved_at DESC);

CREATE INDEX IF NOT EXISTS idx_finess_gestionnaire_signal_v2_couverture
    ON public.finess_gestionnaire (signal_v2_statut_couverture, signal_v2_niveau_suspicion, signal_v2_phase);

CREATE INDEX IF NOT EXISTS idx_finess_gestionnaire_signal_v2_review
    ON public.finess_gestionnaire (signal_v2_review_required, signal_v2_imputabilite)
    WHERE signal_v2_review_required = TRUE;

CREATE OR REPLACE VIEW public.v_gestionnaires_signaux_v2_couverture AS
SELECT
    g.id_gestionnaire,
    g.raison_sociale,
    g.sigle,
    g.departement_code,
    g.departement_nom,
    g.secteur_activite_principal,
    g.nb_etablissements,
    g.signal_v2_phase,
    g.signal_v2_statut_couverture,
    g.signal_v2_niveau_suspicion,
    g.signal_v2_imputabilite,
    g.signal_v2_review_required,
    g.signal_v2_scope_issue,
    g.signal_v2_queries_count,
    g.signal_v2_snippets_count,
    g.signal_v2_last_query_at,
    g.signal_v2_run_id,
    g.signal_v2_decision_detail,
    g.signal_v2_methode,
    g.signal_v2_confiance,
    (g.signal_financier OR g.signal_rh OR g.signal_qualite OR g.signal_juridique) AS has_v2_axis,
    (
        SELECT COUNT(*)
        FROM public.finess_signal_v2_snippet s
        WHERE s.id_gestionnaire = g.id_gestionnaire
    ) AS nb_snippets_stockes,
    (
        SELECT COUNT(*)
        FROM public.finess_signal_v2_snippet s
        WHERE s.id_gestionnaire = g.id_gestionnaire
          AND s.used_for_decision = TRUE
    ) AS nb_snippets_utiles
FROM public.finess_gestionnaire g;

CREATE OR REPLACE VIEW public.v_signal_v2_snippet_reuse_90j AS
SELECT
    s.id_gestionnaire,
    s.phase,
    s.domain,
    s.scope_label,
    s.imputabilite,
    s.suspicion_level,
    COUNT(*) AS nb_snippets,
    COUNT(*) FILTER (WHERE s.used_for_decision) AS nb_used,
    MIN(s.retrieved_at) AS first_seen_at,
    MAX(s.retrieved_at) AS last_seen_at
FROM public.finess_signal_v2_snippet s
WHERE s.retrieved_at >= NOW() - INTERVAL '90 days'
GROUP BY
    s.id_gestionnaire,
    s.phase,
    s.domain,
    s.scope_label,
    s.imputabilite,
    s.suspicion_level;

COMMIT;