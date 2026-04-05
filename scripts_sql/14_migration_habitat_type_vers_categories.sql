-- SCRIPT 14 : MIGRATION COHÉRENTE HABITAT_TYPE VERS CATÉGORIES
-- Fichier : 14_migration_habitat_type_vers_categories.sql

-- ⚠️ Ce script réorganise la base pour que habitat_type soit cohérent avec les catégories

-- 1. ÉTAT ACTUEL DÉTAILLÉ - Identifier les incohérences
SELECT 
    '=== ANALYSE DES INCOHÉRENCES ===' as titre;

SELECT 
    e.id,
    e.nom,
    e.habitat_type::text as habitat_type_actuel,
    c.libelle as categorie_actuelle,
    sc.libelle as sous_categorie_actuelle,
    e.commune,
    e.statut_editorial
FROM etablissements e
LEFT JOIN etablissement_sous_categorie esc ON e.id = esc.etablissement_id
LEFT JOIN sous_categories sc ON esc.sous_categorie_id = sc.id
LEFT JOIN categories c ON sc.categorie_id = c.id
WHERE 
    -- Incohérences détectées
    (e.habitat_type = 'residence' AND c.libelle = 'habitat partagé') OR
    (e.habitat_type = 'habitat_partage' AND c.libelle = 'logement individuel en résidence') OR
    (e.habitat_type IS NOT NULL AND c.libelle IS NULL)  -- Sans catégorie
ORDER BY e.habitat_type, e.nom;

-- 2. PROPOSITION DE MAPPING COHÉRENT
SELECT 
    '=== MAPPING PROPOSÉ ===' as titre;

-- Créer une table temporaire avec le mapping proposé
CREATE TEMP TABLE mapping_habitat_categories AS
SELECT * FROM (VALUES
    ('residence', 'logement individuel en résidence'),
    ('habitat_partage', 'habitat partagé'),
    ('logement_independant', 'habitat individuel')
) AS mapping(habitat_type_source, categorie_cible);

SELECT * FROM mapping_habitat_categories;

-- 3. PLAN DE CORRECTION AUTOMATIQUE

-- 3a. Identifier les établissements à corriger
WITH etablissements_a_corriger AS (
    SELECT 
        e.id,
        e.nom,
        e.habitat_type::text as habitat_type_actuel,
        mhc.categorie_cible,
        cat_cible.id as categorie_cible_id,
        -- Sous-catégorie par défaut selon la catégorie cible
        CASE 
            WHEN mhc.categorie_cible = 'logement individuel en résidence' THEN 'résidence autonomie'
            WHEN mhc.categorie_cible = 'habitat partagé' THEN 'habitat inclusif'  
            WHEN mhc.categorie_cible = 'habitat individuel' THEN 'habitat regroupé'
        END as sous_categorie_defaut
    FROM etablissements e
    JOIN mapping_habitat_categories mhc ON e.habitat_type::text = mhc.habitat_type_source
    LEFT JOIN categories cat_cible ON cat_cible.libelle = mhc.categorie_cible
    LEFT JOIN etablissement_sous_categorie esc ON e.id = esc.etablissement_id
    WHERE esc.etablissement_id IS NULL  -- Pas encore catégorisé
       OR EXISTS (  -- Ou catégorisé de manière incohérente
           SELECT 1 
           FROM etablissement_sous_categorie esc2
           JOIN sous_categories sc2 ON esc2.sous_categorie_id = sc2.id
           JOIN categories c2 ON sc2.categorie_id = c2.id
           WHERE esc2.etablissement_id = e.id 
             AND c2.libelle != mhc.categorie_cible
       )
)
SELECT 
    '=== ÉTABLISSEMENTS À CORRIGER ===' as titre,
    COUNT(*) as nb_a_corriger
FROM etablissements_a_corriger;

-- 4. SCRIPT DE CORRECTION (À DÉCOMMENTER POUR EXÉCUTER)
/*
-- 4a. Supprimer les associations incohérentes
DELETE FROM etablissement_sous_categorie 
WHERE etablissement_id IN (
    SELECT e.id
    FROM etablissements e
    JOIN etablissement_sous_categorie esc ON e.id = esc.etablissement_id
    JOIN sous_categories sc ON esc.sous_categorie_id = sc.id
    JOIN categories c ON sc.categorie_id = c.id
    JOIN mapping_habitat_categories mhc ON e.habitat_type::text = mhc.habitat_type_source
    WHERE c.libelle != mhc.categorie_cible
);

-- 4b. Ajouter les associations cohérentes
WITH associations_coherentes AS (
    SELECT 
        e.id as etablissement_id,
        sc.id as sous_categorie_id
    FROM etablissements e
    JOIN mapping_habitat_categories mhc ON e.habitat_type::text = mhc.habitat_type_source
    JOIN categories c ON c.libelle = mhc.categorie_cible
    JOIN sous_categories sc ON sc.categorie_id = c.id
    LEFT JOIN etablissement_sous_categorie esc ON e.id = esc.etablissement_id
    WHERE esc.etablissement_id IS NULL  -- Pas encore associé
      AND sc.libelle = CASE 
          WHEN mhc.categorie_cible = 'logement individuel en résidence' THEN 'résidence autonomie'
          WHEN mhc.categorie_cible = 'habitat partagé' THEN 'habitat inclusif'  
          WHEN mhc.categorie_cible = 'habitat individuel' THEN 'habitat regroupé'
      END
)
INSERT INTO etablissement_sous_categorie (etablissement_id, sous_categorie_id)
SELECT etablissement_id, sous_categorie_id FROM associations_coherentes;
*/

-- 5. VÉRIFICATION APRÈS CORRECTION
SELECT 
    '=== VÉRIFICATION POST-MIGRATION ===' as titre;

SELECT 
    e.habitat_type::text as habitat_type,
    c.libelle as categorie,
    COUNT(*) as nb_etablissements,
    COUNT(CASE WHEN public.can_publish(e.id) THEN 1 END) as publiables
FROM etablissements e
LEFT JOIN etablissement_sous_categorie esc ON e.id = esc.etablissement_id
LEFT JOIN sous_categories sc ON esc.sous_categorie_id = sc.id
LEFT JOIN categories c ON sc.categorie_id = c.id
WHERE e.habitat_type IS NOT NULL
GROUP BY e.habitat_type, c.libelle
ORDER BY e.habitat_type, nb_etablissements DESC;

-- 6. PROPOSITION D'ÉVOLUTION : NOUVEAU CHAMP BASÉ SUR LES CATÉGORIES
SELECT 
    '=== ÉVOLUTION FUTURE PROPOSÉE ===' as titre;

-- Option : Créer un champ calculé basé sur les catégories
/*
-- Ajouter une colonne calculée (optionnel)
ALTER TABLE etablissements ADD COLUMN categorie_principale text GENERATED ALWAYS AS (
    COALESCE(
        (SELECT c.libelle 
         FROM etablissement_sous_categorie esc
         JOIN sous_categories sc ON esc.sous_categorie_id = sc.id 
         JOIN categories c ON sc.categorie_id = c.id
         WHERE esc.etablissement_id = etablissements.id
         LIMIT 1),
        'non_categorise'
    )
) STORED;
*/