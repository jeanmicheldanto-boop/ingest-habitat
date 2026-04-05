-- Analyse exploitable immediatement sur le schema V1 actuel
-- Objectif : mesurer la population des petits gestionnaires silencieux
-- avant de deployer la migration V2

-- 1. Distribution V1 hors SAA
SELECT
    COUNT(*) FILTER (WHERE COALESCE(g.signal_tension, FALSE) = TRUE) AS nb_signal_tension_true,
    COUNT(*) FILTER (WHERE COALESCE(g.signal_tension, FALSE) = FALSE) AS nb_signal_tension_false,
    COUNT(*) AS nb_total_eligibles
FROM finess_gestionnaire g
WHERE EXISTS (
    SELECT 1
    FROM finess_etablissement e
    WHERE e.id_gestionnaire = g.id_gestionnaire
      AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
);

-- 2. Taille de la population V1 des petits gestionnaires silencieux
SELECT COUNT(*) AS nb_v1_petits_silencieux
FROM finess_gestionnaire g
WHERE COALESCE(g.signal_tension, FALSE) = FALSE
  AND COALESCE(g.nb_etablissements, 0) <= 10
  AND EXISTS (
      SELECT 1
      FROM finess_etablissement e
      WHERE e.id_gestionnaire = g.id_gestionnaire
        AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
  );

-- 2bis. Profil de cette population : combien ont 0, 1-2, 3-5, 6+ items dans signaux_recents
WITH base AS (
    SELECT
        g.id_gestionnaire,
        COALESCE(g.nb_etablissements, 0) AS nb_etablissements,
        COALESCE(jsonb_array_length(g.signaux_recents), 0) AS nb_signaux_recents
    FROM finess_gestionnaire g
    WHERE COALESCE(g.signal_tension, FALSE) = FALSE
      AND COALESCE(g.nb_etablissements, 0) <= 10
      AND EXISTS (
          SELECT 1
          FROM finess_etablissement e
          WHERE e.id_gestionnaire = g.id_gestionnaire
            AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
      )
)
SELECT
    COUNT(*) AS nb_total,
    COUNT(*) FILTER (WHERE nb_signaux_recents = 0) AS nb_zero_signal,
    COUNT(*) FILTER (WHERE nb_signaux_recents BETWEEN 1 AND 2) AS nb_1_2,
    COUNT(*) FILTER (WHERE nb_signaux_recents BETWEEN 3 AND 5) AS nb_3_5,
    COUNT(*) FILTER (WHERE nb_signaux_recents >= 6) AS nb_6_plus,
    COUNT(*) FILTER (WHERE nb_etablissements = 1) AS nb_isoles
FROM base;

-- 3. Echantillon aleatoire de cette population
SELECT
    g.id_gestionnaire,
    g.raison_sociale,
    g.sigle,
    g.secteur_activite_principal,
    g.categorie_taille,
    g.nb_etablissements,
    g.signal_tension,
    g.signal_tension_detail,
    COALESCE(jsonb_array_length(g.signaux_recents), 0) AS nb_signaux_recents
FROM finess_gestionnaire g
WHERE COALESCE(g.signal_tension, FALSE) = FALSE
  AND COALESCE(g.nb_etablissements, 0) <= 10
  AND EXISTS (
      SELECT 1
      FROM finess_etablissement e
      WHERE e.id_gestionnaire = g.id_gestionnaire
        AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
  )
ORDER BY random()
LIMIT 10;

-- 4. Variante prioritaire : focus sur les cas silencieux mais avec au moins 3 items V1
-- Attention : beaucoup d'items peuvent etre neutres et auto-remplis ; cet echantillon sert a relire la qualite reelle du contenu.
SELECT
    g.id_gestionnaire,
    g.raison_sociale,
    g.sigle,
    g.secteur_activite_principal,
    g.nb_etablissements,
    COALESCE(jsonb_array_length(g.signaux_recents), 0) AS nb_signaux_recents,
    g.signal_tension,
    g.signal_tension_detail,
    g.signaux_recents
FROM finess_gestionnaire g
WHERE COALESCE(g.signal_tension, FALSE) = FALSE
  AND COALESCE(g.nb_etablissements, 0) <= 10
  AND COALESCE(jsonb_array_length(g.signaux_recents), 0) >= 3
  AND EXISTS (
      SELECT 1
      FROM finess_etablissement e
      WHERE e.id_gestionnaire = g.id_gestionnaire
        AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
  )
ORDER BY COALESCE(jsonb_array_length(g.signaux_recents), 0) DESC, random()
LIMIT 12;

-- 5. Répartition sectorielle de cette zone aveugle
WITH base AS (
    SELECT
        g.secteur_activite_principal,
        COALESCE(jsonb_array_length(g.signaux_recents), 0) AS nb_signaux_recents
    FROM finess_gestionnaire g
    WHERE COALESCE(g.signal_tension, FALSE) = FALSE
      AND COALESCE(g.nb_etablissements, 0) <= 10
      AND EXISTS (
          SELECT 1
          FROM finess_etablissement e
          WHERE e.id_gestionnaire = g.id_gestionnaire
            AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
      )
)
SELECT
    secteur_activite_principal,
    COUNT(*) AS nb_gestionnaires,
    COUNT(*) FILTER (WHERE nb_signaux_recents >= 3) AS nb_gestionnaires_avec_3_signaux_plus
FROM base
GROUP BY secteur_activite_principal
ORDER BY nb_gestionnaires_avec_3_signaux_plus DESC, nb_gestionnaires DESC
LIMIT 10;
