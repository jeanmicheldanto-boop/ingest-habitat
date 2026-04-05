-- SCRIPT 7 : VÉRIFICATION DES SOUS-CATÉGORIES
-- Fichier : 07_verifier_sous_categories.sql

-- 1. SOUS-CATÉGORIES pour vos établissements des Landes
SELECT 
    e.nom as etablissement,
    COUNT(esc.sous_categorie_id) as nb_sous_categories,
    string_agg(
        CONCAT(c.libelle, ' > ', sc.libelle), 
        ', ' ORDER BY c.libelle, sc.libelle
    ) as categories_completes
FROM etablissements e
LEFT JOIN etablissement_sous_categorie esc ON esc.etablissement_id = e.id
LEFT JOIN sous_categories sc ON sc.id = esc.sous_categorie_id
LEFT JOIN categories c ON c.id = sc.categorie_id
WHERE e.departement = 'Landes (40)' 
  AND e.source != 'import_csv'
GROUP BY e.id, e.nom
ORDER BY nb_sous_categories DESC, e.nom;

-- 2. RÉSUMÉ des sous-catégories disponibles dans la base
SELECT 
    c.libelle as categorie,
    COUNT(sc.id) as nb_sous_categories,
    string_agg(sc.libelle, ', ' ORDER BY sc.libelle) as sous_categories_list
FROM categories c
JOIN sous_categories sc ON c.id = sc.categorie_id
GROUP BY c.id, c.libelle
ORDER BY c.libelle;

-- 3. ÉTABLISSEMENTS SANS SOUS-CATÉGORIES
SELECT 
    nom,
    commune,
    habitat_type
FROM etablissements e
WHERE e.departement = 'Landes (40)' 
  AND e.source != 'import_csv'
  AND NOT EXISTS (
      SELECT 1 FROM etablissement_sous_categorie esc 
      WHERE esc.etablissement_id = e.id
  )
ORDER BY nom;