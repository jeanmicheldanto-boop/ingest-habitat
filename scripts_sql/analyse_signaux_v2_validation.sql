-- Validation V2 : echantillons de revue manuelle
-- A executer dans l'editeur SQL Supabase
-- Prerequis : les colonnes V2 (signal_financier, signal_v2_methode, etc.) doivent deja exister en base


-- 1. KPI synthese V2 hors SAA
WITH elig AS (
    SELECT g.*
    FROM finess_gestionnaire g
    WHERE EXISTS (
        SELECT 1
        FROM finess_etablissement e
        WHERE e.id_gestionnaire = g.id_gestionnaire
          AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
    )
)
SELECT
    COUNT(*) FILTER (WHERE signal_financier) AS nb_financier,
    COUNT(*) FILTER (WHERE signal_rh) AS nb_rh_grave,
    COUNT(*) FILTER (WHERE signal_qualite) AS nb_qualite_inspection,
    COUNT(*) FILTER (WHERE signal_juridique) AS nb_juridique,
    COUNT(*) FILTER (
        WHERE (signal_financier::int + signal_rh::int + signal_qualite::int + signal_juridique::int) >= 2
    ) AS nb_multi_axes,
    COUNT(*) FILTER (
        WHERE signal_financier OR signal_qualite OR signal_juridique OR signal_rh
    ) AS nb_difficulte_individuelle_v2,
    COUNT(*) FILTER (WHERE signal_tension) AS nb_signal_v1_original
FROM elig;

-- 2. Echantillon : exclusions de perimetre keywords_v1_excluded
SELECT
    id_gestionnaire,
    raison_sociale,
    sigle,
    secteur_activite_principal,
    categorie_taille,
    nb_etablissements,
    signal_tension,
    signal_tension_detail,
    signaux_recents,
    signal_v2_methode
FROM finess_gestionnaire
WHERE signal_v2_methode = 'keywords_v1_excluded'
ORDER BY random()
LIMIT 20;

-- 3. Taille de la population d'angle mort structurel
SELECT COUNT(*) AS nb_population_angle_mort_structurel
FROM finess_gestionnaire
WHERE COALESCE(signal_tension, FALSE) = FALSE
  AND COALESCE(nb_etablissements, 0) <= 10
  AND signal_v2_methode IS NULL
  AND EXISTS (
      SELECT 1
      FROM finess_etablissement e
      WHERE e.id_gestionnaire = finess_gestionnaire.id_gestionnaire
        AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
  );

-- 4. Echantillon : angle mort structurel V1+V2
SELECT
    id_gestionnaire,
    raison_sociale,
    sigle,
    secteur_activite_principal,
    categorie_taille,
    nb_etablissements,
    signal_tension,
    signal_tension_detail,
    signaux_recents,
    signal_v2_methode
FROM finess_gestionnaire
WHERE COALESCE(signal_tension, FALSE) = FALSE
  AND COALESCE(nb_etablissements, 0) <= 10
  AND signal_v2_methode IS NULL
  AND EXISTS (
      SELECT 1
      FROM finess_etablissement e
      WHERE e.id_gestionnaire = finess_gestionnaire.id_gestionnaire
        AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
  )
ORDER BY random()
LIMIT 10;

-- 5. Top 50 multi-axes pour revue prioritaire
SELECT
    id_gestionnaire,
    raison_sociale,
    sigle,
    secteur_activite_principal,
    categorie_taille,
    nb_etablissements,
    (signal_financier::int + signal_rh::int + signal_qualite::int + signal_juridique::int) AS signal_v2_nb_axes,
    signal_financier,
    signal_financier_detail,
    signal_juridique,
    signal_juridique_detail,
    signal_qualite,
    signal_qualite_detail,
    signal_rh,
    signal_rh_detail,
    signal_v2_confiance,
    signal_v2_date
FROM finess_gestionnaire
WHERE (signal_financier OR signal_juridique OR signal_qualite OR signal_rh)
  AND EXISTS (
      SELECT 1
      FROM finess_etablissement e
      WHERE e.id_gestionnaire = finess_gestionnaire.id_gestionnaire
        AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
  )
ORDER BY
    (signal_juridique::int + signal_financier::int + signal_qualite::int + signal_rh::int) DESC,
    nb_etablissements DESC
LIMIT 50;