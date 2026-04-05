-- SCRIPT 5 : CORRIGER LA GÉOLOCALISATION DES ÉTABLISSEMENTS
-- Fichier : 05_corriger_geolocalisation.sql

-- 1. VOIR les établissements sans géolocalisation et leurs adresses
SELECT 
    id,
    nom,
    adresse,
    code_postal,
    commune,
    CONCAT(adresse, ', ', code_postal, ' ', commune) as adresse_complete
FROM etablissements 
WHERE departement = 'Landes (40)'
  AND source != 'import_csv'
  AND geom IS NULL
ORDER BY nom;

-- 2. EXEMPLE de correction manuelle pour UN établissement
-- (Remplacez les coordonnées par les vraies coordonnées géographiques)
-- 
-- UPDATE etablissements 
-- SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
-- WHERE id = 'ID_DE_L_ETABLISSEMENT';

-- 3. VÉRIFICATION après correction
-- SELECT 
--     nom,
--     ST_X(geom) as longitude,
--     ST_Y(geom) as latitude,
--     can_publish(id) as peut_maintenant_publier
-- FROM etablissements 
-- WHERE departement = 'Landes (40)'
--   AND source != 'import_csv'
--   AND geom IS NOT NULL;

-- 4. PUBLICATION AUTOMATIQUE des établissements maintenant géolocalisés
-- UPDATE etablissements 
-- SET statut_editorial = 'publie'
-- WHERE departement = 'Landes (40)'
--   AND source != 'import_csv'
--   AND can_publish(id) = true
--   AND statut_editorial != 'publie';