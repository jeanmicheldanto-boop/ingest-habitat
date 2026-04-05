-- SCRIPT DE RÉPARATION : ASSOCIER DES SOUS-CATÉGORIES BASÉES SUR HABITAT_TYPE
-- Fichier : reparer_associations_manquantes.sql

-- Ce script répare le problème des établissements importés sans sous-catégories/services

-- 1. DIAGNOSTIC : Voir les établissements sans sous-catégories
SELECT 
    'ÉTABLISSEMENTS SANS SOUS-CATÉGORIES' as section,
    COUNT(*) as total_sans_sous_categories
FROM etablissements e 
WHERE statut_editorial = 'publie'
  AND NOT EXISTS (
      SELECT 1 FROM etablissement_sous_categorie esc 
      WHERE esc.etablissement_id = e.id
  );

-- 2. RÉPARTITION PAR HABITAT_TYPE
SELECT 
    'RÉPARTITION PAR HABITAT_TYPE' as section,
    habitat_type,
    COUNT(*) as nombre_etablissements,
    COUNT(CASE WHEN EXISTS (
        SELECT 1 FROM etablissement_sous_categorie esc 
        WHERE esc.etablissement_id = e.id
    ) THEN 1 END) as avec_sous_categories,
    COUNT(CASE WHEN NOT EXISTS (
        SELECT 1 FROM etablissement_sous_categorie esc 
        WHERE esc.etablissement_id = e.id
    ) THEN 1 END) as sans_sous_categories
FROM etablissements e
WHERE statut_editorial = 'publie'
GROUP BY habitat_type
ORDER BY habitat_type;

-- 3. VOIR LES SOUS-CATÉGORIES DISPONIBLES PAR CATÉGORIE
SELECT 
    'SOUS-CATÉGORIES DISPONIBLES' as section,
    c.libelle as categorie,
    sc.libelle as sous_categorie,
    sc.id
FROM categories c
JOIN sous_categories sc ON c.id = sc.categorie_id
ORDER BY c.libelle, sc.libelle;

-- 4. RÉPARATION AUTOMATIQUE - Associer des sous-catégories par défaut

-- Pour habitat_partage -> "habitat partagé" (sous-catégorie générique)
INSERT INTO etablissement_sous_categorie (etablissement_id, sous_categorie_id)
SELECT 
    e.id,
    sc.id
FROM etablissements e
CROSS JOIN sous_categories sc
JOIN categories c ON c.id = sc.categorie_id
WHERE e.statut_editorial = 'publie'
  AND e.habitat_type = 'habitat_partage'
  AND c.libelle = 'habitat partagé'
  AND sc.libelle ILIKE '%habitat partagé%'  -- Sous-catégorie générique
  AND NOT EXISTS (
      SELECT 1 FROM etablissement_sous_categorie esc 
      WHERE esc.etablissement_id = e.id
  );

-- Pour residence -> "résidence" (sous-catégorie générique)
INSERT INTO etablissement_sous_categorie (etablissement_id, sous_categorie_id)
SELECT 
    e.id,
    sc.id
FROM etablissements e
CROSS JOIN sous_categories sc
JOIN categories c ON c.id = sc.categorie_id
WHERE e.statut_editorial = 'publie'
  AND e.habitat_type = 'residence'
  AND c.libelle = 'résidence'
  AND sc.libelle ILIKE '%résidence%'  -- Sous-catégorie la plus générique
  AND NOT EXISTS (
      SELECT 1 FROM etablissement_sous_categorie esc 
      WHERE esc.etablissement_id = e.id
  )
LIMIT 1000;  -- Sécurité

-- Pour logement_independant -> "logement indépendant" (sous-catégorie générique)
INSERT INTO etablissement_sous_categorie (etablissement_id, sous_categorie_id)
SELECT 
    e.id,
    sc.id
FROM etablissements e
CROSS JOIN sous_categories sc
JOIN categories c ON c.id = sc.categorie_id
WHERE e.statut_editorial = 'publie'
  AND e.habitat_type = 'logement_independant'
  AND c.libelle = 'logement indépendant'
  AND sc.libelle ILIKE '%logement indépendant%'  -- Sous-catégorie générique
  AND NOT EXISTS (
      SELECT 1 FROM etablissement_sous_categorie esc 
      WHERE esc.etablissement_id = e.id
  );

-- 5. VÉRIFICATION APRÈS RÉPARATION
SELECT 
    'APRÈS RÉPARATION' as section,
    habitat_type,
    COUNT(*) as total,
    COUNT(CASE WHEN EXISTS (
        SELECT 1 FROM etablissement_sous_categorie esc 
        WHERE esc.etablissement_id = e.id
    ) THEN 1 END) as avec_sous_categories,
    COUNT(CASE WHEN NOT EXISTS (
        SELECT 1 FROM etablissement_sous_categorie esc 
        WHERE esc.etablissement_id = e.id
    ) THEN 1 END) as sans_sous_categories
FROM etablissements e
WHERE statut_editorial = 'publie'
GROUP BY habitat_type
ORDER BY habitat_type;

-- 6. TESTER NOTRE ÉTABLISSEMENT SPÉCIFIQUE
SELECT 
    'TEST ÉTABLISSEMENT SPÉCIFIQUE' as section,
    e.nom,
    e.habitat_type,
    COALESCE(
        string_agg(sc.libelle, ', ' ORDER BY sc.libelle),
        'Aucune'
    ) as sous_categories_assignees
FROM etablissements e
LEFT JOIN etablissement_sous_categorie esc ON esc.etablissement_id = e.id
LEFT JOIN sous_categories sc ON sc.id = esc.sous_categorie_id
WHERE e.id = '2f2cf469-f657-481b-bc3c-f33f066dd186'
GROUP BY e.id, e.nom, e.habitat_type;

-- 7. OPTIONNEL: Si les sous-catégories génériques n'existent pas, les créer
-- (Décommentez si nécessaire)

/*
-- Créer "habitat partagé" générique si n'existe pas
INSERT INTO sous_categories (categorie_id, libelle)
SELECT c.id, 'habitat partagé'
FROM categories c 
WHERE c.libelle = 'habitat partagé'
  AND NOT EXISTS (
    SELECT 1 FROM sous_categories sc 
    WHERE sc.categorie_id = c.id 
    AND sc.libelle = 'habitat partagé'
  );

-- Créer "résidence" générique si n'existe pas  
INSERT INTO sous_categories (categorie_id, libelle)
SELECT c.id, 'résidence'
FROM categories c 
WHERE c.libelle = 'résidence'
  AND NOT EXISTS (
    SELECT 1 FROM sous_categories sc 
    WHERE sc.categorie_id = c.id 
    AND sc.libelle = 'résidence'
  );

-- Créer "logement indépendant" générique si n'existe pas
INSERT INTO sous_categories (categorie_id, libelle)
SELECT c.id, 'logement indépendant'
FROM categories c 
WHERE c.libelle = 'logement indépendant'
  AND NOT EXISTS (
    SELECT 1 FROM sous_categories sc 
    WHERE sc.categorie_id = c.id 
    AND sc.libelle = 'logement indépendant'
  );
*/