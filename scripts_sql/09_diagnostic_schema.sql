-- SCRIPT 9 : DIAGNOSTIC COMPLET DU SCHÉMA BASE DE DONNÉES
-- Fichier : 09_diagnostic_schema.sql

-- 1. TOUTES les catégories et sous-catégories
SELECT 
    c.id as cat_id,
    c.libelle as categorie,
    COUNT(sc.id) as nb_sous_categories,
    string_agg(sc.libelle, ' | ' ORDER BY sc.libelle) as sous_categories_list
FROM categories c
LEFT JOIN sous_categories sc ON c.id = sc.categorie_id
GROUP BY c.id, c.libelle
ORDER BY c.libelle;

-- 2. DÉTAIL de toutes les sous-catégories
SELECT 
    c.libelle as categorie,
    sc.libelle as sous_categorie,
    sc.id as sous_cat_id
FROM categories c
JOIN sous_categories sc ON c.id = sc.categorie_id
ORDER BY c.libelle, sc.libelle;

-- 3. SERVICES disponibles
SELECT COUNT(*) as nb_services, 'Services totaux' as type
FROM services
UNION ALL
SELECT COUNT(DISTINCT libelle) as nb_services_uniques, 'Services uniques' as type  
FROM services
ORDER BY type;

-- 4. ÉCHANTILLON des services
SELECT libelle
FROM services
ORDER BY libelle
LIMIT 20;