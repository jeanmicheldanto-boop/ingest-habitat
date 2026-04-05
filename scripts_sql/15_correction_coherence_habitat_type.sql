-- SCRIPT 15 : CORRECTION AUTOMATIQUE DES INCOHÉRENCES HABITAT_TYPE ↔ CATÉGORIES
-- Fichier : 15_correction_coherence_habitat_type.sql

-- ⚠️ Ce script corrige automatiquement les incohérences entre habitat_type et catégories
-- Il utilise une logique intelligente pour assigner les bonnes sous-catégories

-- 1. CRÉER LA TABLE DE MAPPING
CREATE TEMP TABLE mapping_habitat_categories AS
SELECT * FROM (VALUES
    ('residence', 'logement individuel en résidence', 'résidence autonomie'),
    ('habitat_partage', 'habitat partagé', 'habitat inclusif'),
    ('logement_independant', 'habitat individuel', 'habitat regroupé')
) AS mapping(habitat_type_source, categorie_cible, sous_categorie_defaut);

-- 2. AFFICHER LE PLAN DE CORRECTION
SELECT 
    '=== PLAN DE CORRECTION AUTOMATIQUE ===' as titre;

WITH corrections_prevues AS (
    SELECT 
        e.id,
        e.nom,
        e.habitat_type::text,
        mhc.categorie_cible,
        mhc.sous_categorie_defaut,
        COALESCE(c_actuel.libelle, 'AUCUNE') as categorie_actuelle,
        CASE 
            WHEN esc.etablissement_id IS NULL THEN 'AJOUTER_CATEGORIE'
            WHEN c_actuel.libelle != mhc.categorie_cible THEN 'CORRIGER_CATEGORIE'
            ELSE 'OK'
        END as action_requise
    FROM etablissements e
    JOIN mapping_habitat_categories mhc ON e.habitat_type::text = mhc.habitat_type_source
    LEFT JOIN etablissement_sous_categorie esc ON e.id = esc.etablissement_id
    LEFT JOIN sous_categories sc_actuel ON esc.sous_categorie_id = sc_actuel.id
    LEFT JOIN categories c_actuel ON sc_actuel.categorie_id = c_actuel.id
    WHERE esc.etablissement_id IS NULL OR c_actuel.libelle != mhc.categorie_cible
)
SELECT 
    action_requise,
    COUNT(*) as nb_etablissements
FROM corrections_prevues
GROUP BY action_requise
ORDER BY nb_etablissements DESC;

-- 3. DÉTAIL DES CORRECTIONS
WITH corrections_detail AS (
    SELECT 
        e.id,
        e.nom,
        e.habitat_type::text,
        mhc.categorie_cible,
        COALESCE(c_actuel.libelle, 'AUCUNE') as categorie_actuelle,
        -- Logique intelligente pour choisir la sous-catégorie
        CASE 
            -- Si le nom contient des indices, utiliser la sous-catégorie appropriée
            WHEN e.nom ILIKE '%résidence autonomie%' OR e.nom ILIKE '%autonomie%' THEN 'résidence autonomie'
            WHEN e.nom ILIKE '%résidence service%' OR e.nom ILIKE '%services%' THEN 'résidence services seniors'
            WHEN e.nom ILIKE '%marpa%' THEN 'MARPA'
            WHEN e.nom ILIKE '%accueil familial%' THEN 'accueil familial'
            WHEN e.nom ILIKE '%béguinage%' THEN 'béguinage'
            WHEN e.nom ILIKE '%colocation%' OR e.nom ILIKE '%coliving%' THEN 'colocation avec services'
            WHEN e.nom ILIKE '%intergénération%' THEN 'habitat intergénérationnel'
            WHEN e.nom ILIKE '%inclusif%' THEN 'habitat inclusif'
            WHEN e.nom ILIKE '%village%' THEN 'village seniors'
            ELSE mhc.sous_categorie_defaut
        END as sous_categorie_optimale
    FROM etablissements e
    JOIN mapping_habitat_categories mhc ON e.habitat_type::text = mhc.habitat_type_source
    LEFT JOIN etablissement_sous_categorie esc ON e.id = esc.etablissement_id
    LEFT JOIN sous_categories sc_actuel ON esc.sous_categorie_id = sc_actuel.id
    LEFT JOIN categories c_actuel ON sc_actuel.categorie_id = c_actuel.id
    WHERE esc.etablissement_id IS NULL OR c_actuel.libelle != mhc.categorie_cible
)
SELECT 
    habitat_type,
    categorie_cible,
    sous_categorie_optimale,
    COUNT(*) as nb_etablissements
FROM corrections_detail
GROUP BY habitat_type, categorie_cible, sous_categorie_optimale
ORDER BY habitat_type, nb_etablissements DESC;

-- 4. EXÉCUTION DES CORRECTIONS (DÉCOMMENTER POUR APPLIQUER)
/*
-- 4a. Supprimer les associations incohérentes
WITH etablissements_incoherents AS (
    SELECT esc.etablissement_id, esc.sous_categorie_id
    FROM etablissement_sous_categorie esc
    JOIN sous_categories sc ON esc.sous_categorie_id = sc.id
    JOIN categories c ON sc.categorie_id = c.id
    JOIN etablissements e ON esc.etablissement_id = e.id
    JOIN mapping_habitat_categories mhc ON e.habitat_type::text = mhc.habitat_type_source
    WHERE c.libelle != mhc.categorie_cible
)
DELETE FROM etablissement_sous_categorie 
WHERE (etablissement_id, sous_categorie_id) IN (
    SELECT etablissement_id, sous_categorie_id FROM etablissements_incoherents
);

-- 4b. Ajouter les nouvelles associations cohérentes
WITH corrections_a_appliquer AS (
    SELECT 
        e.id as etablissement_id,
        sc_cible.id as sous_categorie_id
    FROM etablissements e
    JOIN mapping_habitat_categories mhc ON e.habitat_type::text = mhc.habitat_type_source
    JOIN categories c_cible ON c_cible.libelle = mhc.categorie_cible
    JOIN sous_categories sc_cible ON sc_cible.categorie_id = c_cible.id
    LEFT JOIN etablissement_sous_categorie esc_existant ON e.id = esc_existant.etablissement_id
    WHERE sc_cible.libelle = (
        -- Même logique intelligente que dans l'analyse
        CASE 
            WHEN e.nom ILIKE '%résidence autonomie%' OR e.nom ILIKE '%autonomie%' THEN 'résidence autonomie'
            WHEN e.nom ILIKE '%résidence service%' OR e.nom ILIKE '%services%' THEN 'résidence services seniors'
            WHEN e.nom ILIKE '%marpa%' THEN 'MARPA'
            WHEN e.nom ILIKE '%accueil familial%' THEN 'accueil familial'
            WHEN e.nom ILIKE '%béguinage%' THEN 'béguinage'
            WHEN e.nom ILIKE '%colocation%' OR e.nom ILIKE '%coliving%' THEN 'colocation avec services'
            WHEN e.nom ILIKE '%intergénération%' THEN 'habitat intergénérationnel'
            WHEN e.nom ILIKE '%inclusif%' THEN 'habitat inclusif'
            WHEN e.nom ILIKE '%village%' THEN 'village seniors'
            ELSE mhc.sous_categorie_defaut
        END
    )
    AND esc_existant.etablissement_id IS NULL  -- Pas encore associé
)
INSERT INTO etablissement_sous_categorie (etablissement_id, sous_categorie_id)
SELECT etablissement_id, sous_categorie_id FROM corrections_a_appliquer;
*/

-- 5. VÉRIFICATION POST-CORRECTION
SELECT 
    '=== VÉRIFICATION POST-CORRECTION ===' as titre;

-- Compter les établissements encore incohérents après correction
WITH verif_post_correction AS (
    SELECT 
        e.id,
        e.habitat_type::text,
        mhc.categorie_cible,
        COALESCE(c.libelle, 'AUCUNE') as categorie_actuelle
    FROM etablissements e
    JOIN mapping_habitat_categories mhc ON e.habitat_type::text = mhc.habitat_type_source
    LEFT JOIN etablissement_sous_categorie esc ON e.id = esc.etablissement_id
    LEFT JOIN sous_categories sc ON esc.sous_categorie_id = sc.id
    LEFT JOIN categories c ON sc.categorie_id = c.id
    WHERE esc.etablissement_id IS NULL OR c.libelle != mhc.categorie_cible
)
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN '✅ TOUTES LES INCOHÉRENCES CORRIGÉES!'
        ELSE '⚠️ ' || COUNT(*) || ' établissements encore incohérents'
    END as resultat
FROM verif_post_correction;

-- 6. STATISTIQUES FINALES
SELECT 
    e.habitat_type::text as habitat_type,
    c.libelle as categorie,
    sc.libelle as sous_categorie,
    COUNT(*) as nb_etablissements,
    COUNT(CASE WHEN public.can_publish(e.id) THEN 1 END) as nb_publiables
FROM etablissements e
LEFT JOIN etablissement_sous_categorie esc ON e.id = esc.etablissement_id
LEFT JOIN sous_categories sc ON esc.sous_categorie_id = sc.id
LEFT JOIN categories c ON sc.categorie_id = c.id
WHERE e.habitat_type IS NOT NULL
GROUP BY e.habitat_type, c.libelle, sc.libelle
HAVING COUNT(*) > 0
ORDER BY e.habitat_type, nb_etablissements DESC;