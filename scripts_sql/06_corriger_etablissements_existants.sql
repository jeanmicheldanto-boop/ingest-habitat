-- SCRIPT 6 : CORRIGER VOS ÉTABLISSEMENTS EXISTANTS DES LANDES
-- Fichier : 06_corriger_etablissements_existants.sql

-- 1. CORRIGER LA GÉOLOCALISATION (centres des communes)
-- Coordonnées approximatives des centres des principales villes des Landes

-- Dax (40100)
UPDATE etablissements 
SET geom = ST_SetSRID(ST_MakePoint(-1.0568, 43.7102), 4326)
WHERE departement = 'Landes (40)'
  AND source != 'import_csv'
  AND geom IS NULL
  AND (commune ILIKE '%dax%' OR code_postal = '40100');

-- Donzacq (40360) 
UPDATE etablissements 
SET geom = ST_SetSRID(ST_MakePoint(-0.7547, 43.7231), 4326)
WHERE departement = 'Landes (40)'
  AND source != 'import_csv'
  AND geom IS NULL
  AND (commune ILIKE '%donzacq%' OR code_postal = '40360');

-- Mimizan (40200)
UPDATE etablissements 
SET geom = ST_SetSRID(ST_MakePoint(-1.2292, 44.2128), 4326)
WHERE departement = 'Landes (40)'
  AND source != 'import_csv'
  AND geom IS NULL
  AND (commune ILIKE '%mimizan%' OR code_postal = '40200');

-- Saint-Paul-lès-Dax (40990)
UPDATE etablissements 
SET geom = ST_SetSRID(ST_MakePoint(-1.0547, 43.7256), 4326)
WHERE departement = 'Landes (40)'
  AND source != 'import_csv'
  AND geom IS NULL
  AND (commune ILIKE '%saint-paul%' OR code_postal = '40990');

-- Mont-de-Marsan (40000)
UPDATE etablissements 
SET geom = ST_SetSRID(ST_MakePoint(-0.5017, 43.8936), 4326)
WHERE departement = 'Landes (40)'
  AND source != 'import_csv'
  AND geom IS NULL
  AND (commune ILIKE '%mont-de-marsan%' OR code_postal = '40000');

-- Biscarrosse (40600)
UPDATE etablissements 
SET geom = ST_SetSRID(ST_MakePoint(-1.1667, 44.3944), 4326)
WHERE departement = 'Landes (40)'
  AND source != 'import_csv'
  AND geom IS NULL
  AND (commune ILIKE '%biscarrosse%' OR code_postal = '40600');

-- Centre générique Landes pour les cas non couverts (Mont-de-Marsan)
UPDATE etablissements 
SET geom = ST_SetSRID(ST_MakePoint(-0.5017, 43.8936), 4326)
WHERE departement = 'Landes (40)'
  AND source != 'import_csv'
  AND geom IS NULL;

-- 2. PUBLIER AUTOMATIQUEMENT LES ÉTABLISSEMENTS MAINTENANT GÉOLOCALISÉS
UPDATE etablissements 
SET statut_editorial = 'publie'
WHERE departement = 'Landes (40)'
  AND source != 'import_csv'
  AND can_publish(id) = true
  AND statut_editorial != 'publie';

-- 3. VÉRIFICATION FINALE
SELECT 
    nom,
    commune,
    CASE WHEN geom IS NULL THEN '❌ Pas géolocalisé' ELSE '✅ Géolocalisé' END as geoloc_status,
    CASE WHEN can_publish(id) THEN '✅ Publiable' ELSE '❌ Problème' END as publish_status,
    statut_editorial
FROM etablissements 
WHERE departement = 'Landes (40)'
  AND source != 'import_csv'
ORDER BY statut_editorial DESC, nom;