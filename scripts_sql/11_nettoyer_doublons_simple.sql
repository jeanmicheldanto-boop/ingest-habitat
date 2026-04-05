-- SCRIPT 11 ALTERNATIF : NETTOYER LES DOUBLONS (VERSION SIMPLE)
-- Fichier : 11_nettoyer_doublons_simple.sql

-- 1. IDENTIFIER les doublons
SELECT 
    sc.libelle,
    sc.categorie_id,
    COUNT(*) as nombre_doublons,
    string_agg(sc.id::text, ', ') as ids_doublons
FROM sous_categories sc
GROUP BY sc.libelle, sc.categorie_id
HAVING COUNT(*) > 1
ORDER BY sc.libelle;

-- 2. CRÉER une table temporaire avec les IDs à garder
CREATE TEMP TABLE ids_a_garder AS
SELECT DISTINCT ON (libelle, categorie_id) 
    id,
    libelle,
    categorie_id
FROM sous_categories
ORDER BY libelle, categorie_id, id::text;

-- 3. MIGRER les associations vers les IDs à garder
UPDATE etablissement_sous_categorie esc
SET sous_categorie_id = iag.id
FROM ids_a_garder iag
WHERE esc.sous_categorie_id IN (
    SELECT sc.id 
    FROM sous_categories sc
    WHERE sc.libelle = iag.libelle 
      AND sc.categorie_id = iag.categorie_id
      AND sc.id != iag.id
);

-- 4. SUPPRIMER les doublons
DELETE FROM sous_categories 
WHERE id NOT IN (SELECT id FROM ids_a_garder);

-- 5. NETTOYAGE
DROP TABLE ids_a_garder;

-- 6. VÉRIFICATION
SELECT 
    c.libelle as categorie,
    COUNT(sc.id) as nb_sous_categories,
    string_agg(sc.libelle, ' | ' ORDER BY sc.libelle) as sous_categories_list
FROM categories c
LEFT JOIN sous_categories sc ON c.id = sc.categorie_id
GROUP BY c.id, c.libelle
ORDER BY c.libelle;