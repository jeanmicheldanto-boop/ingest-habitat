-- SCRIPT 3 : PUBLIER LES ÉTABLISSEMENTS VALIDES
-- Fichier : 03_publier_etablissements.sql

-- 1. VOIR quels établissements PEUVENT être publiés (avant publication)
SELECT 
    nom, commune,
    CASE WHEN can_publish(id) THEN '✅ Prêt à publier' ELSE '❌ Problèmes restants' END as statut
FROM etablissements 
WHERE departement = 'Landes (40)'
  AND source != 'import_csv'
  AND statut_editorial = 'draft'
ORDER BY can_publish(id) DESC, nom;

-- 2. PUBLIER tous les établissements des Landes qui respectent les critères
UPDATE etablissements 
SET statut_editorial = 'publie'
WHERE departement = 'Landes (40)'
  AND source != 'import_csv'
  AND statut_editorial = 'draft' 
  AND can_publish(id) = true;

-- 3. VÉRIFIER les résultats après publication
SELECT 
    statut_editorial,
    COUNT(*) as nombre
FROM etablissements 
WHERE departement = 'Landes (40)'
  AND source != 'import_csv'
GROUP BY statut_editorial;