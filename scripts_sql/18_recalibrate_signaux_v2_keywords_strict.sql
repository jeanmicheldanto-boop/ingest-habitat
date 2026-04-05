-- Recalibration stricte du backfill keywords V2
-- Objectif : reduire les faux positifs lies aux phrases neutres type
-- "Aucune information sur ..." qui contiennent des mots cles sans signal reel.

BEGIN;

-- 0) Reinitialiser uniquement la classification issue du backfill keywords
UPDATE public.finess_gestionnaire
SET
    signal_financier = FALSE,
    signal_rh = FALSE,
    signal_qualite = FALSE,
    signal_juridique = FALSE,
    signal_v2_confiance = NULL,
    signal_v2_date = NULL,
    signal_v2_methode = NULL
WHERE signal_v2_methode IN ('keywords_v1', 'keywords_v1_excluded');

WITH source_data AS (
    SELECT
        g.id_gestionnaire,
        COALESCE(g.signal_tension_detail, '') AS detail_txt,
        COALESCE(
            CASE
                WHEN jsonb_typeof(g.signaux_recents) = 'array' THEN (
                    SELECT string_agg(
                        lower(
                            COALESCE(elem->>'type', '') || ' ' ||
                            COALESCE(elem->>'impact', '') || ' ' ||
                            COALESCE(elem->>'resume', '') || ' ' ||
                            COALESCE(elem->>'source_url', '')
                        ),
                        ' '
                    )
                    FROM jsonb_array_elements(g.signaux_recents) elem
                )
                ELSE NULL
            END,
            ''
        ) AS recents_txt,
        COALESCE(
            CASE
                WHEN jsonb_typeof(g.signaux_recents) = 'array' THEN (
                    SELECT bool_or(
                        NULLIF(trim(COALESCE(elem->>'source_url', '')), '') IS NOT NULL
                    )
                    FROM jsonb_array_elements(g.signaux_recents) elem
                )
                ELSE FALSE
            END,
            FALSE
        ) AS has_source
    FROM public.finess_gestionnaire g
    WHERE g.signal_v2_methode IS NULL
      AND (g.signal_tension_detail IS NOT NULL OR g.signaux_recents IS NOT NULL)
),
normalized AS (
    SELECT
        s.id_gestionnaire,
        s.has_source,
        lower(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        (s.detail_txt || ' ' || s.recents_txt),
                        'aucune information[^\.;\n]*',
                        ' ',
                        'gi'
                    ),
                    'aucun signal[^\.;\n]*',
                    ' ',
                    'gi'
                ),
                'n''est disponible[^\.;\n]*',
                ' ',
                'gi'
            )
        ) AS corpus
    FROM source_data s
),
flags AS (
    SELECT
        n.id_gestionnaire,
        n.corpus,
        n.has_source,
        (
            n.corpus ~* '(tensions? financi[èe]res?|deficit|d[ée]ficit|tr[ée]sorerie|plan social|\bPSE\b|licenciement|licenciements|redressement judiciaire|liquidation judiciaire|proc[ée]dure collective|cessation de paiement|insolvabil|administrateur judiciaire|menace de fermeture|fermeture(?! administrative))'
        ) AS is_financier,
        (
            n.has_source
            AND n.corpus ~* '(redressement judiciaire|liquidation judiciaire|proc[ée]dure collective|tribunal de commerce|tribunal judiciaire|administrateur judiciaire|mise sous administration provisoire|condamnation|contentieux)'
        ) AS is_juridique,
        (
            n.has_source
            AND n.corpus ~* '(injonction ARS|mise en demeure|fermeture administrative|maltraitance|incident grave|rapport ARS|inspection ARS|non[- ]conformit[ée]|signalement)'
        ) AS is_qualite,
        (
            n.has_source
            AND n.corpus ~* '(gr[èe]ve|conflit social|plan de d[ée]parts|fermeture faute de personnel|sous[- ]effectif critique|absent[ée]isme massif)'
        ) AS is_rh,
        (
            n.corpus ~* '(menace de fermeture|fermeture(?! administrative))'
        ) AS has_fermeture,
        (
            n.corpus ~* '(mobilisation|p[ée]tition|manifestation|inter[- ]associatif|branche professionnelle|convention collective|accord de branche|non[- ]revalorisation|sous[- ]dotation|nexem|fehap|uniopss|synerpa)'
        ) AS has_bruit,
        (
            n.corpus ~* '(cpom|appel [àa] projets|construction|extension|r[ée]novation|transformation|changement de direction|recrutement)'
        ) AS has_positive,
        (
            length(trim(n.corpus)) = 0
            OR n.corpus ~* '(aucune information|aucun signal|non disponible|pas d information|pas d informations)'
        ) AS has_no_info
    FROM normalized n
),
prepared AS (
    SELECT
        f.id_gestionnaire,
        f.is_financier,
        f.is_rh,
        f.is_qualite,
        f.is_juridique,
        (
            NOT (f.is_financier OR f.is_rh OR f.is_qualite OR f.is_juridique)
            AND (f.has_bruit OR f.has_positive OR f.has_no_info OR NOT f.has_source)
            AND NOT f.has_fermeture
        ) AS is_excluded,
        CASE
            WHEN (f.is_financier OR f.is_rh OR f.is_qualite OR f.is_juridique) THEN 'keywords_v1'
            WHEN (
                NOT (f.is_financier OR f.is_rh OR f.is_qualite OR f.is_juridique)
                AND (f.has_bruit OR f.has_positive OR f.has_no_info OR NOT f.has_source)
                AND NOT f.has_fermeture
            ) THEN 'keywords_v1_excluded'
            ELSE NULL
        END AS methode,
        CASE
            WHEN (f.is_financier OR f.is_rh OR f.is_qualite OR f.is_juridique) THEN 'basse'
            WHEN (
                NOT (f.is_financier OR f.is_rh OR f.is_qualite OR f.is_juridique)
                AND (f.has_bruit OR f.has_positive OR f.has_no_info OR NOT f.has_source)
                AND NOT f.has_fermeture
            ) THEN 'basse'
            ELSE NULL
        END AS confiance
    FROM flags f
)
UPDATE public.finess_gestionnaire g
SET
    signal_financier = p.is_financier,
    signal_rh = p.is_rh,
    signal_qualite = p.is_qualite,
    signal_juridique = p.is_juridique,
    signal_v2_methode = p.methode,
    signal_v2_confiance = p.confiance,
    signal_v2_date = CASE WHEN p.methode IS NOT NULL THEN NOW() ELSE g.signal_v2_date END
FROM prepared p
WHERE g.id_gestionnaire = p.id_gestionnaire;

COMMIT;
