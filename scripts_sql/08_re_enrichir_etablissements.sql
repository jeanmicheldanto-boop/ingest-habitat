-- SCRIPT 8 : RÉ-ENRICHIR VOS ÉTABLISSEMENTS EXISTANTS AVEC SOUS-CATÉGORIES
-- Fichier : 08_re_enrichir_etablissements.sql

-- 1. ASSOCIER MANUELLEMENT DES SOUS-CATÉGORIES BASÉES SUR LES NOMS D'ÉTABLISSEMENTS

-- Résidences autonomie
INSERT INTO etablissement_sous_categorie (etablissement_id, sous_categorie_id)
SELECT e.id, sc.id
FROM etablissements e, sous_categories sc
WHERE e.departement = 'Landes (40)' 
  AND e.source != 'import_csv'
  AND (e.nom ILIKE '%résidence autonomie%' OR e.nom ILIKE '%residence autonomie%')
  AND sc.libelle ILIKE '%résidence autonomie%'
  AND NOT EXISTS (
    SELECT 1 FROM etablissement_sous_categorie esc 
    WHERE esc.etablissement_id = e.id AND esc.sous_categorie_id = sc.id
  );

-- Résidences services seniors
INSERT INTO etablissement_sous_categorie (etablissement_id, sous_categorie_id)
SELECT e.id, sc.id
FROM etablissements e, sous_categories sc
WHERE e.departement = 'Landes (40)' 
  AND e.source != 'import_csv'
  AND (e.nom ILIKE '%domitys%' OR e.nom ILIKE '%jardins d''arcadie%' OR e.nom ILIKE '%résidence services%')
  AND sc.libelle ILIKE '%résidence services seniors%'
  AND NOT EXISTS (
    SELECT 1 FROM etablissement_sous_categorie esc 
    WHERE esc.etablissement_id = e.id AND esc.sous_categorie_id = sc.id
  );

-- Habitat inclusif
INSERT INTO etablissement_sous_categorie (etablissement_id, sous_categorie_id)
SELECT e.id, sc.id
FROM etablissements e, sous_categories sc
WHERE e.departement = 'Landes (40)' 
  AND e.source != 'import_csv'
  AND e.nom ILIKE '%habitat inclusif%'
  AND sc.libelle ILIKE '%habitat inclusif%'
  AND NOT EXISTS (
    SELECT 1 FROM etablissement_sous_categorie esc 
    WHERE esc.etablissement_id = e.id AND esc.sous_categorie_id = sc.id
  );

-- Habitat intergénérationnel
INSERT INTO etablissement_sous_categorie (etablissement_id, sous_categorie_id)
SELECT e.id, sc.id
FROM etablissements e, sous_categories sc
WHERE e.departement = 'Landes (40)' 
  AND e.source != 'import_csv'
  AND (e.nom ILIKE '%intergénérationnel%' OR e.nom ILIKE '%intergenerationnel%')
  AND sc.libelle ILIKE '%habitat intergénérationnel%'
  AND NOT EXISTS (
    SELECT 1 FROM etablissement_sous_categorie esc 
    WHERE esc.etablissement_id = e.id AND esc.sous_categorie_id = sc.id
  );

-- 2. VÉRIFICATION DES ASSOCIATIONS CRÉÉES
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