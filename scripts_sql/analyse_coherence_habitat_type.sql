-- ANALYSE DE LA COHÉRENCE ACTUELLE ENTRE HABITAT_TYPE ET CATÉGORIES
-- Fichier : analyse_coherence_habitat_type.sql

-- 1. VÉRIFIER les valeurs actuelles de habitat_type
SELECT 
    habitat_type,
    COUNT(*) as nb_etablissements,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as pourcentage
FROM etablissements 
WHERE habitat_type IS NOT NULL
GROUP BY habitat_type
ORDER BY nb_etablissements DESC;

-- 2. ANALYSER la correspondance actuelle habitat_type <-> sous-catégories
SELECT 
    e.habitat_type,
    c.libelle as categorie,
    sc.libelle as sous_categorie,
    COUNT(*) as nb_etablissements
FROM etablissements e
LEFT JOIN etablissement_sous_categorie esc ON e.id = esc.etablissement_id
LEFT JOIN sous_categories sc ON esc.sous_categorie_id = sc.id
LEFT JOIN categories c ON sc.categorie_id = c.id
WHERE e.habitat_type IS NOT NULL
GROUP BY e.habitat_type, c.libelle, sc.libelle
ORDER BY e.habitat_type, nb_etablissements DESC;

-- 3. IDENTIFIER les incohérences potentielles
WITH habitat_mapping AS (
    SELECT 
        e.id,
        e.nom,
        e.habitat_type,
        array_agg(DISTINCT c.libelle) as categories_liees,
        array_agg(DISTINCT sc.libelle) as sous_categories_liees
    FROM etablissements e
    LEFT JOIN etablissement_sous_categorie esc ON e.id = esc.etablissement_id
    LEFT JOIN sous_categories sc ON esc.sous_categorie_id = sc.id
    LEFT JOIN categories c ON sc.categorie_id = c.id
    WHERE e.habitat_type IS NOT NULL
    GROUP BY e.id, e.nom, e.habitat_type
)
SELECT 
    habitat_type,
    COUNT(*) as nb_etablissements,
    COUNT(CASE WHEN categories_liees = ARRAY[NULL::text] THEN 1 END) as sans_categorie,
    COUNT(CASE WHEN sous_categories_liees = ARRAY[NULL::text] THEN 1 END) as sans_sous_categorie
FROM habitat_mapping
GROUP BY habitat_type
ORDER BY nb_etablissements DESC;

-- 4. DÉTAIL des établissements sans catégorisation
SELECT 
    id,
    nom,
    habitat_type,
    commune,
    statut_editorial
FROM etablissements e
WHERE habitat_type IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM etablissement_sous_categorie esc 
      WHERE esc.etablissement_id = e.id
  )
ORDER BY habitat_type, nom
LIMIT 20;