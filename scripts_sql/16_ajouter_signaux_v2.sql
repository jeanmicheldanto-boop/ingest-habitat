-- Migration V2 des signaux gestionnaire
-- Objectif : ajouter des axes de difficultes individuelles sans detruire la V1
-- Script additif, compatible avec le schema actuel observe en base

BEGIN;

ALTER TABLE public.finess_gestionnaire
    ADD COLUMN IF NOT EXISTS signal_financier BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS signal_financier_detail TEXT,
    ADD COLUMN IF NOT EXISTS signal_financier_sources TEXT[],
    ADD COLUMN IF NOT EXISTS signal_rh BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS signal_rh_detail TEXT,
    ADD COLUMN IF NOT EXISTS signal_qualite BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS signal_qualite_detail TEXT,
    ADD COLUMN IF NOT EXISTS signal_qualite_sources TEXT[],
    ADD COLUMN IF NOT EXISTS signal_juridique BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS signal_juridique_detail TEXT,
    ADD COLUMN IF NOT EXISTS signal_v2_confiance TEXT,
    ADD COLUMN IF NOT EXISTS signal_v2_date TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS signal_v2_methode TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'finess_gestionnaire_signal_v2_confiance_check'
    ) THEN
        ALTER TABLE public.finess_gestionnaire
            ADD CONSTRAINT finess_gestionnaire_signal_v2_confiance_check
            CHECK (
                signal_v2_confiance IS NULL
                OR signal_v2_confiance IN ('haute', 'moyenne', 'basse')
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'finess_gestionnaire_signal_v2_methode_check'
    ) THEN
        ALTER TABLE public.finess_gestionnaire
            ADD CONSTRAINT finess_gestionnaire_signal_v2_methode_check
            CHECK (
                signal_v2_methode IS NULL
                OR signal_v2_methode IN (
                    'keywords_v1',
                    'keywords_v1_excluded',
                    'serper_passe_a',
                    'serper_passe_b'
                )
            );
    END IF;
END $$;

COMMENT ON COLUMN public.finess_gestionnaire.signal_financier IS 'Signal V2 : difficulte financiere individuelle du gestionnaire';
COMMENT ON COLUMN public.finess_gestionnaire.signal_rh IS 'Signal V2 : difficulte RH grave individuelle';
COMMENT ON COLUMN public.finess_gestionnaire.signal_qualite IS 'Signal V2 : difficulte qualite / inspection';
COMMENT ON COLUMN public.finess_gestionnaire.signal_juridique IS 'Signal V2 : difficulte juridique / procedure';
COMMENT ON COLUMN public.finess_gestionnaire.signal_v2_confiance IS 'Confiance V2 : haute, moyenne ou basse';
COMMENT ON COLUMN public.finess_gestionnaire.signal_v2_date IS 'Date de qualification V2';
COMMENT ON COLUMN public.finess_gestionnaire.signal_v2_methode IS 'Methode V2 : keywords_v1, keywords_v1_excluded, serper_passe_a, serper_passe_b';

CREATE INDEX IF NOT EXISTS idx_finess_gest_signal_v2_methode
    ON public.finess_gestionnaire (signal_v2_methode)
    WHERE signal_v2_methode IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_finess_gest_signal_v2_any
    ON public.finess_gestionnaire (departement_code, nb_etablissements)
    WHERE signal_financier OR signal_rh OR signal_qualite OR signal_juridique;

CREATE OR REPLACE VIEW public.v_gestionnaires_signaux_v2 AS
SELECT
    g.id_gestionnaire,
    g.siren,
    g.raison_sociale,
    g.sigle,
    g.departement_code,
    g.departement_nom,
    g.region,
    g.secteur_activite_principal,
    g.categorie_taille,
    g.nb_etablissements,
    g.nb_essms,
    g.signal_tension AS signal_v1,
    g.signal_tension_detail AS signal_v1_detail,
    g.signaux_recents,
    (g.signal_financier OR g.signal_qualite OR g.signal_juridique OR g.signal_rh) AS signal_difficulte_v2,
    (
        g.signal_financier::int +
        g.signal_rh::int +
        g.signal_qualite::int +
        g.signal_juridique::int
    ) AS signal_v2_nb_axes,
    g.signal_financier,
    g.signal_financier_detail,
    g.signal_financier_sources,
    g.signal_rh,
    g.signal_rh_detail,
    g.signal_qualite,
    g.signal_qualite_detail,
    g.signal_qualite_sources,
    g.signal_juridique,
    g.signal_juridique_detail,
    g.signal_v2_confiance,
    g.signal_v2_date,
    g.signal_v2_methode,
    CASE
        WHEN (g.signal_financier::int + g.signal_rh::int + g.signal_qualite::int + g.signal_juridique::int) >= 2 THEN 'multi_axes'
        WHEN g.signal_juridique THEN 'juridique'
        WHEN g.signal_financier THEN 'financier'
        WHEN g.signal_qualite THEN 'qualite'
        WHEN g.signal_rh THEN 'rh'
        ELSE NULL
    END AS signal_type_dominant_v2
FROM public.finess_gestionnaire g;

COMMIT;