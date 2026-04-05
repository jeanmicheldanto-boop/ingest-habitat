-- SCRIPT 14 : PUBLICATION EN MASSE DES ÉTABLISSEMENTS ÉLIGIBLES
-- Fichier : 14_publier_etablissements_eligibles.sql

-- ⚠️ Ce script passe tous les établissements éligibles de 'draft' à 'publie'
-- Il utilise la fonction can_publish() améliorée (plus permissive pour les emails)

-- 1. VÉRIFIER combien d'établissements seront affectés AVANT la mise à jour
WITH stats_avant AS (
    SELECT 
        statut_editorial,
        COUNT(*) as nombre,
        COUNT(CASE WHEN public.can_publish(id) THEN 1 END) as eligibles_publication
    FROM public.etablissements 
    GROUP BY statut_editorial
)
SELECT 
    'AVANT PUBLICATION' as moment,
    statut_editorial,
    nombre,
    eligibles_publication,
    (eligibles_publication::float / nombre * 100)::numeric(5,2) as pourcentage_eligible
FROM stats_avant
ORDER BY statut_editorial;

-- 2. LISTER les établissements qui vont être publiés (pour vérification)
SELECT 
    id,
    nom,
    commune,
    departement,
    email,
    gestionnaire,
    habitat_type,
    statut_editorial,
    'Sera publié' as action
FROM public.etablissements 
WHERE statut_editorial = 'draft' 
  AND public.can_publish(id) = true
ORDER BY departement, commune, nom
LIMIT 20;  -- Limite pour éviter trop de résultats, retirez pour voir tous

-- 3. PUBLICATION EFFECTIVE - Décommentez les lignes ci-dessous pour exécuter
/*
UPDATE public.etablissements 
SET 
    statut_editorial = 'publie',
    date_verification = CURRENT_DATE
WHERE statut_editorial = 'draft' 
  AND public.can_publish(id) = true;
*/

-- 4. VÉRIFICATION APRÈS PUBLICATION - Décommentez après l'UPDATE
/*
WITH stats_apres AS (
    SELECT 
        statut_editorial,
        COUNT(*) as nombre
    FROM public.etablissements 
    GROUP BY statut_editorial
)
SELECT 
    'APRÈS PUBLICATION' as moment,
    statut_editorial,
    nombre,
    (nombre::float / (SELECT COUNT(*) FROM public.etablissements) * 100)::numeric(5,2) as pourcentage
FROM stats_apres
ORDER BY statut_editorial;
*/

-- 5. STATISTIQUES DÉTAILLÉES PAR DÉPARTEMENT - Décommentez pour voir
/*
SELECT 
    departement,
    COUNT(*) as total_etablissements,
    COUNT(CASE WHEN statut_editorial = 'publie' THEN 1 END) as publies,
    COUNT(CASE WHEN statut_editorial = 'draft' THEN 1 END) as drafts,
    COUNT(CASE WHEN statut_editorial = 'draft' AND public.can_publish(id) THEN 1 END) as drafts_eligibles,
    (COUNT(CASE WHEN statut_editorial = 'publie' THEN 1 END)::float / COUNT(*) * 100)::numeric(5,2) as taux_publication
FROM public.etablissements 
WHERE departement IS NOT NULL
GROUP BY departement
ORDER BY taux_publication DESC;
*/

-- 6. IDENTIFIER LES ÉTABLISSEMENTS NON ÉLIGIBLES À LA PUBLICATION
SELECT 
    'ÉTABLISSEMENTS NON ÉLIGIBLES' as section,
    id,
    nom,
    commune,
    departement,
    CASE 
        WHEN COALESCE(NULLIF(trim(nom),''), NULL) IS NULL THEN 'Nom manquant'
        WHEN COALESCE(NULLIF(trim(adresse_l1),''), NULLIF(trim(adresse_l2),''), NULL) IS NULL THEN 'Adresse manquante'
        WHEN COALESCE(NULLIF(trim(commune),''), NULL) IS NULL THEN 'Commune manquante'
        WHEN COALESCE(NULLIF(trim(code_postal),''), NULL) IS NULL THEN 'Code postal manquant'
        WHEN geom IS NULL THEN 'Géolocalisation manquante'
        WHEN COALESCE(NULLIF(trim(gestionnaire),''), NULL) IS NULL THEN 'Gestionnaire manquant'
        WHEN habitat_type IS NULL AND NOT EXISTS (
            SELECT 1 FROM public.etablissement_sous_categorie esc WHERE esc.etablissement_id = etablissements.id
        ) THEN 'Type d''habitat manquant'
        WHEN email IS NOT NULL 
             AND COALESCE(NULLIF(trim(email),''), NULL) IS NOT NULL
             AND NOT (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN 'Email invalide'
        ELSE 'Autre raison'
    END as raison_non_eligible
FROM public.etablissements 
WHERE statut_editorial = 'draft' 
  AND public.can_publish(id) = false
ORDER BY departement, commune, nom
LIMIT 50;  -- Limite pour éviter trop de résultats

-- 7. POUR ROLLBACK (en cas de problème) - NE PAS DÉCOMMENTER SAUF URGENCE
/*
UPDATE public.etablissements 
SET 
    statut_editorial = 'draft',
    date_verification = NULL
WHERE statut_editorial = 'publie' 
  AND date_verification = CURRENT_DATE;
*/

-- =======================================
-- INSTRUCTIONS D'UTILISATION :
-- =======================================
-- 1. Exécutez d'abord les sections 1 et 2 pour voir ce qui va être modifié
-- 2. Si tout semble correct, décommentez la section 3 (UPDATE) et exécutez
-- 3. Décommentez et exécutez les sections 4 et 5 pour vérifier le résultat
-- 4. Consultez la section 6 pour comprendre pourquoi certains ne sont pas publiés
-- 5. En cas de problème, utilisez la section 7 pour annuler (ROLLBACK)