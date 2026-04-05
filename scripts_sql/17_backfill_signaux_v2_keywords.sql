-- Backfill initial V2 a partir des donnees V1
-- Source: signal_tension_detail + signaux_recents (JSONB)
-- Objectif: peupler une premiere qualification automatisable

BEGIN;

WITH source_data AS (
    SELECT
        g.id_gestionnaire,
        (
            COALESCE(g.signal_tension_detail, '') || ' ' ||
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
            )
        ) AS corpus
    FROM public.finess_gestionnaire g
    WHERE g.signal_v2_methode IS NULL
      AND (
          g.signal_tension_detail IS NOT NULL
          OR g.signaux_recents IS NOT NULL
      )
),
flags AS (
    SELECT
        s.id_gestionnaire,
        s.corpus,
        (
            s.corpus ~* '(deficit|d[ée]ficit|tr[ée]sorerie|plan social|\bPSE\b|licenciement|licenciements|redressement judiciaire|liquidation judiciaire|proc[ée]dure collective|cessation de paiement|insolvabil|administrateur judiciaire)'
        ) AS is_financier,
        (
            s.corpus ~* '(redressement judiciaire|liquidation judiciaire|proc[ée]dure collective|tribunal de commerce|tribunal judiciaire|administrateur judiciaire|mise sous administration provisoire|condamnation|contentieux)'
        ) AS is_juridique,
        (
            s.corpus ~* '(injonction ARS|mise en demeure|fermeture administrative|maltraitance|incident grave|rapport ARS|inspection ARS|non[- ]conformit[ée]|signalement)'
        ) AS is_qualite,
        (
            s.corpus ~* '(gr[èe]ve|conflit social|plan de d[ée]parts|plan social personnel|fermeture faute de personnel|sous[- ]effectif critique|absent[ée]isme massif)'
        ) AS is_rh,
        (
            s.corpus ~* '(mobilisation|p[ée]tition|manifestation|inter[- ]associatif|branche professionnelle|convention collective|accord de branche|non[- ]revalorisation|sous[- ]dotation|nexem|fehap|uniopss|synerpa)'
        ) AS has_bruit,
        (
            s.corpus ~* '(cpom|appel [àa] projets|construction|extension|r[ée]novation|transformation|changement de direction|recrutement)'
        ) AS has_positive,
        (
            length(trim(s.corpus)) = 0
            OR s.corpus ~* '(aucune information|aucun signal|non disponible|pas d information|pas d informations)'
        ) AS has_no_info
    FROM source_data s
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
            AND (f.has_bruit OR f.has_positive OR f.has_no_info)
        ) AS is_excluded,
        (
            CASE
                WHEN (f.is_financier OR f.is_rh OR f.is_qualite OR f.is_juridique) THEN 'keywords_v1'
                WHEN (
                    NOT (f.is_financier OR f.is_rh OR f.is_qualite OR f.is_juridique)
                    AND (f.has_bruit OR f.has_positive OR f.has_no_info)
                ) THEN 'keywords_v1_excluded'
                ELSE NULL
            END
        ) AS methode,
        (
            CASE
                WHEN (f.is_financier OR f.is_rh OR f.is_qualite OR f.is_juridique) THEN 'basse'
                WHEN (
                    NOT (f.is_financier OR f.is_rh OR f.is_qualite OR f.is_juridique)
                    AND (f.has_bruit OR f.has_positive OR f.has_no_info)
                ) THEN 'basse'
                ELSE NULL
            END
        ) AS confiance
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
