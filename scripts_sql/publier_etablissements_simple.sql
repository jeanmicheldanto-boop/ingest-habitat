-- SCRIPT SIMPLE : PUBLICATION DIRECTE DES ÉTABLISSEMENTS ÉLIGIBLES
-- Fichier : publier_etablissements_simple.sql

-- ✅ PUBLICATION DIRECTE - Passe tous les établissements éligibles à "publié"
UPDATE public.etablissements 
SET 
    statut_editorial = 'publie',
    date_verification = CURRENT_DATE
WHERE statut_editorial = 'draft' 
  AND public.can_publish(id) = true;

-- 📊 RÉSULTAT - Affiche le résumé après publication
WITH stats AS (
    SELECT 
        statut_editorial,
        COUNT(*) as nombre
    FROM public.etablissements 
    GROUP BY statut_editorial
)
SELECT 
    statut_editorial,
    nombre,
    (nombre::float / (SELECT COUNT(*) FROM public.etablissements) * 100)::numeric(5,2) as pourcentage
FROM stats
ORDER BY statut_editorial;

-- 📋 DÉTAIL PAR DÉPARTEMENT
SELECT 
    departement,
    COUNT(*) as total,
    COUNT(CASE WHEN statut_editorial = 'publie' THEN 1 END) as publies,
    COUNT(CASE WHEN statut_editorial = 'draft' THEN 1 END) as drafts,
    (COUNT(CASE WHEN statut_editorial = 'publie' THEN 1 END)::float / COUNT(*) * 100)::numeric(5,2) as taux_publication
FROM public.etablissements 
WHERE departement IS NOT NULL
GROUP BY departement
ORDER BY taux_publication DESC;