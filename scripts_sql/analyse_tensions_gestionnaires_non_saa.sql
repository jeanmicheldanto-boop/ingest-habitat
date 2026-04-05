-- Analyse des signaux de tension financiere des gestionnaires ESSMS
-- Regle metier appliquee: exclusion des gestionnaires n'ayant que des ET de categorie SAA.
--
-- Utilisation:
-- 1) Executer ce script dans SQL Editor Supabase.
-- 2) Lire les resultats bloc par bloc (chaque SELECT renvoie un tableau).

-- Version resultat unique (recommandee si l'editeur n'affiche qu'un seul resultat)
-- Cette requete renvoie une seule ligne JSON avec les 8 blocs de synthese.
WITH eligible_gestionnaires AS (
    SELECT
        g.id_gestionnaire,
        g.raison_sociale,
        COALESCE(NULLIF(g.secteur_activite_principal, ''), 'NON_RENSEIGNE') AS categorie_gestionnaire,
        COALESCE(NULLIF(g.categorie_taille, ''), 'NON_RENSEIGNE') AS categorie_taille,
        COALESCE(g.signal_tension, FALSE) AS signal_tension,
        g.signal_tension_detail
    FROM public.finess_gestionnaire g
    WHERE EXISTS (
        SELECT 1
        FROM public.finess_etablissement e
        WHERE e.id_gestionnaire = g.id_gestionnaire
          AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
    )
),
eligible_et AS (
    SELECT e.*
    FROM public.finess_etablissement e
    WHERE e.categorie_normalisee IS DISTINCT FROM 'SAA'
),
et_tension AS (
    SELECT e.*, g.categorie_taille
    FROM eligible_et e
    JOIN eligible_gestionnaires g ON g.id_gestionnaire = e.id_gestionnaire
    WHERE g.signal_tension = TRUE
)
SELECT jsonb_build_object(
    'resultat_1_kpi_gestionnaires', (
        SELECT to_jsonb(x)
        FROM (
            SELECT
                COUNT(*) AS nb_gestionnaires_eligibles,
                COUNT(*) FILTER (WHERE signal_tension) AS nb_gestionnaires_avec_tension,
                ROUND(
                    100.0 * COUNT(*) FILTER (WHERE signal_tension)
                    / NULLIF(COUNT(*), 0),
                    2
                ) AS pct_gestionnaires_avec_tension
            FROM eligible_gestionnaires
        ) x
    ),
    'resultat_2_par_categorie_gestionnaire', (
        SELECT COALESCE(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
        FROM (
            SELECT
                categorie_gestionnaire,
                COUNT(*) AS nb_gestionnaires,
                COUNT(*) FILTER (WHERE signal_tension) AS nb_gestionnaires_tension,
                ROUND(
                    100.0 * COUNT(*) FILTER (WHERE signal_tension)
                    / NULLIF(COUNT(*), 0),
                    2
                ) AS taux_tension_pct
            FROM eligible_gestionnaires
            GROUP BY categorie_gestionnaire
            ORDER BY taux_tension_pct DESC NULLS LAST, nb_gestionnaires_tension DESC, nb_gestionnaires DESC
        ) x
    ),
    'resultat_3_par_taille_gestionnaire', (
        SELECT COALESCE(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
        FROM (
            SELECT
                categorie_taille,
                COUNT(*) AS nb_gestionnaires,
                COUNT(*) FILTER (WHERE signal_tension) AS nb_gestionnaires_tension,
                ROUND(
                    100.0 * COUNT(*) FILTER (WHERE signal_tension)
                    / NULLIF(COUNT(*), 0),
                    2
                ) AS taux_tension_pct
            FROM eligible_gestionnaires
            GROUP BY categorie_taille
            ORDER BY taux_tension_pct DESC NULLS LAST, nb_gestionnaires_tension DESC, nb_gestionnaires DESC
        ) x
    ),
    'resultat_4_kpi_et_concernes', (
        SELECT to_jsonb(x)
        FROM (
            SELECT
                (SELECT COUNT(*) FROM eligible_et) AS nb_et_hors_saa,
                COUNT(*) AS nb_et_chez_gestionnaire_tension,
                ROUND(
                    100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM eligible_et), 0),
                    2
                ) AS pct_et_chez_gestionnaire_tension,
                COUNT(DISTINCT id_gestionnaire) AS nb_gestionnaires_tension_concernes,
                ROUND(
                    1.0 * COUNT(*) / NULLIF(COUNT(DISTINCT id_gestionnaire), 0),
                    2
                ) AS nb_moyen_et_par_gestionnaire_tension
            FROM et_tension
        ) x
    ),
    'resultat_5_et_par_categorie', (
        SELECT COALESCE(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
        FROM (
            SELECT
                COALESCE(NULLIF(categorie_normalisee, ''), 'NON_RENSEIGNE') AS categorie_et,
                COUNT(*) AS nb_et,
                ROUND(
                    100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM et_tension), 0),
                    2
                ) AS part_pct
            FROM et_tension
            GROUP BY categorie_et
            ORDER BY nb_et DESC
        ) x
    ),
    'resultat_6_et_par_taille_gestionnaire', (
        SELECT COALESCE(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
        FROM (
            SELECT
                categorie_taille,
                COUNT(*) AS nb_et,
                COUNT(DISTINCT id_gestionnaire) AS nb_gestionnaires,
                ROUND(1.0 * COUNT(*) / NULLIF(COUNT(DISTINCT id_gestionnaire), 0), 2) AS nb_moyen_et_par_gestionnaire
            FROM et_tension
            GROUP BY categorie_taille
            ORDER BY nb_et DESC
        ) x
    ),
    'resultat_7_top_categories_impactees', (
        SELECT COALESCE(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
        FROM (
            SELECT
                COALESCE(NULLIF(categorie_normalisee, ''), 'NON_RENSEIGNE') AS categorie_et,
                COUNT(*) AS nb_et
            FROM et_tension
            GROUP BY categorie_et
            ORDER BY nb_et DESC
            LIMIT 15
        ) x
    ),
    'resultat_8_top_100_gestionnaires_tension', (
        SELECT COALESCE(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
        FROM (
            SELECT
                g.id_gestionnaire,
                g.raison_sociale,
                g.categorie_gestionnaire,
                g.categorie_taille,
                COUNT(e.id_finess) AS nb_et_hors_saa,
                g.signal_tension_detail
            FROM eligible_gestionnaires g
            LEFT JOIN eligible_et e ON e.id_gestionnaire = g.id_gestionnaire
            WHERE g.signal_tension = TRUE
            GROUP BY
                g.id_gestionnaire,
                g.raison_sociale,
                g.categorie_gestionnaire,
                g.categorie_taille,
                g.signal_tension_detail
            ORDER BY nb_et_hors_saa DESC
            LIMIT 100
        ) x
    )
) AS synthese_tensions;

WITH eligible_gestionnaires AS (
    SELECT
        g.id_gestionnaire,
        g.raison_sociale,
        COALESCE(NULLIF(g.secteur_activite_principal, ''), 'NON_RENSEIGNE') AS categorie_gestionnaire,
        COALESCE(NULLIF(g.categorie_taille, ''), 'NON_RENSEIGNE') AS categorie_taille,
        COALESCE(g.signal_tension, FALSE) AS signal_tension,
        g.signal_tension_detail
    FROM public.finess_gestionnaire g
    WHERE EXISTS (
        SELECT 1
        FROM public.finess_etablissement e
        WHERE e.id_gestionnaire = g.id_gestionnaire
          AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
    )
),
eligible_et AS (
    SELECT e.*
    FROM public.finess_etablissement e
    WHERE e.categorie_normalisee IS DISTINCT FROM 'SAA'
)
SELECT
    COUNT(*) AS nb_gestionnaires_eligibles,
    COUNT(*) FILTER (WHERE signal_tension) AS nb_gestionnaires_avec_tension,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE signal_tension)
        / NULLIF(COUNT(*), 0),
        2
    ) AS pct_gestionnaires_avec_tension
FROM eligible_gestionnaires;

WITH eligible_gestionnaires AS (
    SELECT
        g.id_gestionnaire,
        COALESCE(NULLIF(g.secteur_activite_principal, ''), 'NON_RENSEIGNE') AS categorie_gestionnaire,
        COALESCE(g.signal_tension, FALSE) AS signal_tension
    FROM public.finess_gestionnaire g
    WHERE EXISTS (
        SELECT 1
        FROM public.finess_etablissement e
        WHERE e.id_gestionnaire = g.id_gestionnaire
          AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
    )
)
SELECT
    categorie_gestionnaire,
    COUNT(*) AS nb_gestionnaires,
    COUNT(*) FILTER (WHERE signal_tension) AS nb_gestionnaires_tension,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE signal_tension)
        / NULLIF(COUNT(*), 0),
        2
    ) AS taux_tension_pct
FROM eligible_gestionnaires
GROUP BY categorie_gestionnaire
ORDER BY taux_tension_pct DESC NULLS LAST, nb_gestionnaires_tension DESC, nb_gestionnaires DESC;

WITH eligible_gestionnaires AS (
    SELECT
        g.id_gestionnaire,
        COALESCE(NULLIF(g.categorie_taille, ''), 'NON_RENSEIGNE') AS categorie_taille,
        COALESCE(g.signal_tension, FALSE) AS signal_tension
    FROM public.finess_gestionnaire g
    WHERE EXISTS (
        SELECT 1
        FROM public.finess_etablissement e
        WHERE e.id_gestionnaire = g.id_gestionnaire
          AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
    )
)
SELECT
    categorie_taille,
    COUNT(*) AS nb_gestionnaires,
    COUNT(*) FILTER (WHERE signal_tension) AS nb_gestionnaires_tension,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE signal_tension)
        / NULLIF(COUNT(*), 0),
        2
    ) AS taux_tension_pct
FROM eligible_gestionnaires
GROUP BY categorie_taille
ORDER BY taux_tension_pct DESC NULLS LAST, nb_gestionnaires_tension DESC, nb_gestionnaires DESC;

WITH eligible_gestionnaires AS (
    SELECT
        g.id_gestionnaire,
        COALESCE(g.signal_tension, FALSE) AS signal_tension
    FROM public.finess_gestionnaire g
    WHERE EXISTS (
        SELECT 1
        FROM public.finess_etablissement e
        WHERE e.id_gestionnaire = g.id_gestionnaire
          AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
    )
),
eligible_et AS (
    SELECT e.*
    FROM public.finess_etablissement e
    WHERE e.categorie_normalisee IS DISTINCT FROM 'SAA'
),
et_tension AS (
    SELECT e.*
    FROM eligible_et e
    JOIN eligible_gestionnaires g
      ON g.id_gestionnaire = e.id_gestionnaire
    WHERE g.signal_tension = TRUE
)
SELECT
    (SELECT COUNT(*) FROM eligible_et) AS nb_et_hors_saa,
    COUNT(*) AS nb_et_chez_gestionnaire_tension,
    ROUND(
        100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM eligible_et), 0),
        2
    ) AS pct_et_chez_gestionnaire_tension,
    COUNT(DISTINCT id_gestionnaire) AS nb_gestionnaires_tension_concernes,
    ROUND(
        1.0 * COUNT(*) / NULLIF(COUNT(DISTINCT id_gestionnaire), 0),
        2
    ) AS nb_moyen_et_par_gestionnaire_tension
FROM et_tension;

WITH eligible_gestionnaires AS (
    SELECT
        g.id_gestionnaire,
        COALESCE(g.signal_tension, FALSE) AS signal_tension
    FROM public.finess_gestionnaire g
    WHERE EXISTS (
        SELECT 1
        FROM public.finess_etablissement e
        WHERE e.id_gestionnaire = g.id_gestionnaire
          AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
    )
),
eligible_et AS (
    SELECT e.*
    FROM public.finess_etablissement e
    WHERE e.categorie_normalisee IS DISTINCT FROM 'SAA'
),
et_tension AS (
    SELECT e.*
    FROM eligible_et e
    JOIN eligible_gestionnaires g
      ON g.id_gestionnaire = e.id_gestionnaire
    WHERE g.signal_tension = TRUE
)
SELECT
    COALESCE(NULLIF(categorie_normalisee, ''), 'NON_RENSEIGNE') AS categorie_et,
    COUNT(*) AS nb_et,
    ROUND(
        100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM et_tension), 0),
        2
    ) AS part_pct
FROM et_tension
GROUP BY categorie_et
ORDER BY nb_et DESC;

WITH eligible_gestionnaires AS (
    SELECT
        g.id_gestionnaire,
        COALESCE(g.signal_tension, FALSE) AS signal_tension,
        COALESCE(NULLIF(g.categorie_taille, ''), 'NON_RENSEIGNE') AS categorie_taille
    FROM public.finess_gestionnaire g
    WHERE EXISTS (
        SELECT 1
        FROM public.finess_etablissement e
        WHERE e.id_gestionnaire = g.id_gestionnaire
          AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
    )
),
eligible_et AS (
    SELECT e.*
    FROM public.finess_etablissement e
    WHERE e.categorie_normalisee IS DISTINCT FROM 'SAA'
),
et_tension AS (
    SELECT e.*, g.categorie_taille
    FROM eligible_et e
    JOIN eligible_gestionnaires g
      ON g.id_gestionnaire = e.id_gestionnaire
    WHERE g.signal_tension = TRUE
)
SELECT
    categorie_taille,
    COUNT(*) AS nb_et,
    COUNT(DISTINCT id_gestionnaire) AS nb_gestionnaires,
    ROUND(1.0 * COUNT(*) / NULLIF(COUNT(DISTINCT id_gestionnaire), 0), 2) AS nb_moyen_et_par_gestionnaire
FROM et_tension
GROUP BY categorie_taille
ORDER BY nb_et DESC;

WITH eligible_gestionnaires AS (
    SELECT
        g.id_gestionnaire,
        g.raison_sociale,
        COALESCE(NULLIF(g.categorie_taille, ''), 'NON_RENSEIGNE') AS categorie_taille,
        COALESCE(NULLIF(g.secteur_activite_principal, ''), 'NON_RENSEIGNE') AS categorie_gestionnaire,
        COALESCE(g.signal_tension, FALSE) AS signal_tension,
        g.signal_tension_detail
    FROM public.finess_gestionnaire g
    WHERE EXISTS (
        SELECT 1
        FROM public.finess_etablissement e
        WHERE e.id_gestionnaire = g.id_gestionnaire
          AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
    )
),
eligible_et AS (
    SELECT e.*
    FROM public.finess_etablissement e
    WHERE e.categorie_normalisee IS DISTINCT FROM 'SAA'
)
SELECT
    g.id_gestionnaire,
    g.raison_sociale,
    g.categorie_gestionnaire,
    g.categorie_taille,
    COUNT(e.id_finess) AS nb_et_hors_saa,
    g.signal_tension_detail
FROM eligible_gestionnaires g
LEFT JOIN eligible_et e
  ON e.id_gestionnaire = g.id_gestionnaire
WHERE g.signal_tension = TRUE
GROUP BY
    g.id_gestionnaire,
    g.raison_sociale,
    g.categorie_gestionnaire,
    g.categorie_taille,
    g.signal_tension_detail
ORDER BY nb_et_hors_saa DESC
LIMIT 100;
