-- SCRIPT 10 : AJOUTER TYPES D'HABITAT INTERMÉDIAIRE MANQUANTS
-- Fichier : 10_ajouter_marpa.sql

-- 1. Identifier les catégories disponibles
SELECT id, libelle FROM categories ORDER BY libelle;

-- 2. AJOUTER MARPA dans "logement individuel en résidence"
INSERT INTO sous_categories (id, categorie_id, libelle)
SELECT 
    gen_random_uuid(),
    c.id,
    'MARPA'
FROM categories c 
WHERE c.libelle = 'logement individuel en résidence'
  AND NOT EXISTS (
    SELECT 1 FROM sous_categories sc 
    WHERE sc.categorie_id = c.id AND sc.libelle ILIKE '%marpa%'
  );

-- 3. AJOUTER Villages seniors dans "habitat individuel"
INSERT INTO sous_categories (id, categorie_id, libelle)
SELECT 
    gen_random_uuid(),
    c.id,
    'village seniors'
FROM categories c 
WHERE c.libelle = 'habitat individuel'
  AND NOT EXISTS (
    SELECT 1 FROM sous_categories sc 
    WHERE sc.categorie_id = c.id AND sc.libelle ILIKE '%village seniors%'
  );

-- 4. AJOUTER Habitat regroupé dans "habitat individuel"
INSERT INTO sous_categories (id, categorie_id, libelle)
SELECT 
    gen_random_uuid(),
    c.id,
    'habitat regroupé'
FROM categories c 
WHERE c.libelle = 'habitat individuel'
  AND NOT EXISTS (
    SELECT 1 FROM sous_categories sc 
    WHERE sc.categorie_id = c.id AND sc.libelle ILIKE '%habitat regroupé%'
  );

-- 5. AJOUTER Habitat alternatif dans "habitat partagé"
INSERT INTO sous_categories (id, categorie_id, libelle)
SELECT 
    gen_random_uuid(),
    c.id,
    'habitat alternatif'
FROM categories c 
WHERE c.libelle = 'habitat partagé'
  AND NOT EXISTS (
    SELECT 1 FROM sous_categories sc 
    WHERE sc.categorie_id = c.id AND sc.libelle ILIKE '%habitat alternatif%'
  );

-- 6. AJOUTER Maisons d'accueil familial dans "habitat partagé" (si pas déjà couvert par "accueil familial")
INSERT INTO sous_categories (id, categorie_id, libelle)
SELECT 
    gen_random_uuid(),
    c.id,
    'maison d''accueil familial'
FROM categories c 
WHERE c.libelle = 'habitat partagé'
  AND NOT EXISTS (
    SELECT 1 FROM sous_categories sc 
    WHERE sc.categorie_id = c.id AND sc.libelle ILIKE '%maison d''accueil familial%'
  )
  AND NOT EXISTS (
    SELECT 1 FROM sous_categories sc 
    WHERE sc.categorie_id = c.id AND sc.libelle ILIKE 'accueil familial'
  );

-- 7. VÉRIFIER tous les ajouts
SELECT 
    c.libelle as categorie,
    sc.libelle as sous_categorie
FROM categories c
JOIN sous_categories sc ON c.id = sc.categorie_id
WHERE sc.libelle IN ('MARPA', 'village seniors', 'habitat regroupé', 'habitat alternatif', 'maison d''accueil familial')
ORDER BY c.libelle, sc.libelle;