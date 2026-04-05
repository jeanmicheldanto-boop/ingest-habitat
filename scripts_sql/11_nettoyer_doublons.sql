-- SCRIPT 11 : NETTOYER LES DOUBLONS DANS SOUS_CATEGORIES
-- Fichier : 11_nettoyer_doublons.sql

-- 1. IDENTIFIER les doublons avec leurs usages
SELECT 
    sc.libelle,
    sc.categorie_id,
    COUNT(*) as nombre_doublons,
    string_agg(sc.id::text, ', ') as ids_doublons,
    COUNT(esc.etablissement_id) as nb_etablissements_lies
FROM sous_categories sc
LEFT JOIN etablissement_sous_categorie esc ON esc.sous_categorie_id = sc.id
GROUP BY sc.libelle, sc.categorie_id
HAVING COUNT(*) > 1
ORDER BY sc.libelle;

-- 2. MIGRER les associations vers le premier ID de chaque doublon (en évitant les doublons)
-- Première étape : supprimer les associations qui causeraient des conflits
DELETE FROM etablissement_sous_categorie
WHERE (etablissement_id, sous_categorie_id) IN (
    SELECT DISTINCT esc.etablissement_id, subquery.premier_id
    FROM etablissement_sous_categorie esc
    JOIN (
        SELECT 
            sc.libelle,
            sc.categorie_id,
            (ARRAY_AGG(sc.id ORDER BY sc.id::text))[1] as premier_id,
            ARRAY_AGG(sc.id ORDER BY sc.id::text) as tous_ids
        FROM sous_categories sc
        GROUP BY sc.libelle, sc.categorie_id
        HAVING COUNT(*) > 1
    ) subquery ON esc.sous_categorie_id = ANY(subquery.tous_ids[2:])
    WHERE EXISTS (
        SELECT 1 FROM etablissement_sous_categorie esc2 
        WHERE esc2.etablissement_id = esc.etablissement_id 
          AND esc2.sous_categorie_id = subquery.premier_id
    )
);

-- Deuxième étape : migrer les associations restantes
UPDATE etablissement_sous_categorie 
SET sous_categorie_id = subquery.premier_id
FROM (
    SELECT 
        sc.libelle,
        sc.categorie_id,
        (ARRAY_AGG(sc.id ORDER BY sc.id::text))[1] as premier_id,
        ARRAY_AGG(sc.id ORDER BY sc.id::text) as tous_ids
    FROM sous_categories sc
    GROUP BY sc.libelle, sc.categorie_id
    HAVING COUNT(*) > 1
) subquery
WHERE sous_categorie_id = ANY(subquery.tous_ids[2:]);

-- 3. SUPPRIMER les doublons (garder le plus ancien ID par ordre alphabétique)
DELETE FROM sous_categories 
WHERE id NOT IN (
    SELECT (ARRAY_AGG(id ORDER BY id::text))[1]
    FROM sous_categories
    GROUP BY libelle, categorie_id
);

-- 4. VÉRIFICATION après nettoyage
SELECT 
    c.libelle as categorie,
    COUNT(sc.id) as nb_sous_categories,
    string_agg(sc.libelle, ' | ' ORDER BY sc.libelle) as sous_categories_list
FROM categories c
LEFT JOIN sous_categories sc ON c.id = sc.categorie_id
GROUP BY c.id, c.libelle
ORDER BY c.libelle;

-- 5. VÉRIFIER qu'il n'y a plus de doublons
SELECT 
    sc.libelle,
    sc.categorie_id,
    COUNT(*) as nombre_occurrences
FROM sous_categories sc
GROUP BY sc.libelle, sc.categorie_id
HAVING COUNT(*) > 1
ORDER BY sc.libelle;