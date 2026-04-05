-- SCRIPT 1 : DIAGNOSTIC VOS 13 ETABLISSEMENTS DES LANDES
-- Fichier : 01_diagnostic_landes.sql

-- 1. VOS établissements des Landes (tous ceux qui ne viennent pas d'import CSV)
SELECT 
    nom, commune, source, statut_editorial, created_at,
    CASE WHEN can_publish(id) THEN '✅ Peut être publié' ELSE '❌ Problèmes' END as peut_publier
FROM etablissements 
WHERE departement = 'Landes (40)'
  AND source != 'import_csv'
ORDER BY created_at DESC;

-- 2. RÉSUMÉ de vos établissements des Landes
SELECT 
    COUNT(*) as total_etablissements,
    COUNT(CASE WHEN statut_editorial = 'publie' THEN 1 END) as publies,
    COUNT(CASE WHEN statut_editorial = 'draft' THEN 1 END) as en_draft,
    COUNT(CASE WHEN can_publish(id) = true THEN 1 END) as peuvent_etre_publies
FROM etablissements 
WHERE departement = 'Landes (40)'
  AND source != 'import_csv';

-- 3. DIAGNOSTIC détaillé - Quels problèmes empêchent la publication ?
SELECT 
    LEFT(nom, 40) as nom_court, 
    CASE WHEN can_publish(id) THEN '✅' ELSE '❌' END as publiable,
    CASE WHEN geom IS NULL THEN '❌' ELSE '✅' END as geoloc_ok,
    CASE WHEN email IS NULL OR email !~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' THEN '❌' ELSE '✅' END as email_ok,
    CASE WHEN habitat_type IS NULL THEN '❌' ELSE '✅' END as habitat_type_ok,
    CASE WHEN gestionnaire IS NULL OR trim(gestionnaire) = '' THEN '❌' ELSE '✅' END as gestionnaire_ok
FROM etablissements 
WHERE departement = 'Landes (40)'
  AND source != 'import_csv'
ORDER BY can_publish(id) DESC, nom;