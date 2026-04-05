-- SCRIPT 13 : ASSOUPLIR LES CONTRAINTES DE PUBLICATION POUR L'IMPORT CSV
-- Fichier : 13_assouplir_contraintes_publication.sql

-- ⚠️ Ce script modifie la fonction can_publish() pour être plus permissive 
-- avec les établissements importés via CSV qui peuvent ne pas avoir d'email/site web

-- 1. VÉRIFIER la fonction actuelle
SELECT pg_get_functiondef(oid) 
FROM pg_proc 
WHERE proname = 'can_publish' AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public');

-- 2. SAUVEGARDER l'ancienne version (pour rollback si besoin)
CREATE OR REPLACE FUNCTION public.can_publish_original(p_etab uuid) RETURNS boolean
    LANGUAGE sql STABLE
    AS $_$
WITH e AS (
  SELECT *
  FROM public.etablissements
  WHERE id = p_etab
)
SELECT
  -- 1) nom
  COALESCE(NULLIF(trim(nom),''), NULL) IS NOT NULL
  -- 2) adresse (accepte adresse_l1 OU adresse_l2), commune, code postal (non vide), géoloc non nulle
  AND COALESCE(NULLIF(trim(adresse_l1),''), NULLIF(trim(adresse_l2),''), NULL) IS NOT NULL
  AND COALESCE(NULLIF(trim(commune),''), NULL) IS NOT NULL
  AND COALESCE(NULLIF(trim(code_postal),''), NULL) IS NOT NULL
  AND geom IS NOT NULL
  -- 3) gestionnaire
  AND COALESCE(NULLIF(trim(gestionnaire),''), NULL) IS NOT NULL
  -- 4) typage d'habitat : **nouveau champ** OU (legacy) au moins une sous-catégorie liée
  AND (
        habitat_type IS NOT NULL
     OR EXISTS (
          SELECT 1
          FROM public.etablissement_sous_categorie esc
          WHERE esc.etablissement_id = p_etab
        )
  )
  -- 5) email au format simple (ANCIENNE VERSION - stricte)
  AND email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
FROM e;
$_$;

-- 3. NOUVELLE VERSION plus permissive de la fonction can_publish()
CREATE OR REPLACE FUNCTION public.can_publish(p_etab uuid) RETURNS boolean
    LANGUAGE sql STABLE
    AS $_$
WITH e AS (
  SELECT *
  FROM public.etablissements
  WHERE id = p_etab
)
SELECT
  -- 1) nom
  COALESCE(NULLIF(trim(nom),''), NULL) IS NOT NULL
  -- 2) adresse (accepte adresse_l1 OU adresse_l2), commune, code postal (non vide), géoloc non nulle
  AND COALESCE(NULLIF(trim(adresse_l1),''), NULLIF(trim(adresse_l2),''), NULL) IS NOT NULL
  AND COALESCE(NULLIF(trim(commune),''), NULL) IS NOT NULL
  AND COALESCE(NULLIF(trim(code_postal),''), NULL) IS NOT NULL
  AND geom IS NOT NULL
  -- 3) gestionnaire
  AND COALESCE(NULLIF(trim(gestionnaire),''), NULL) IS NOT NULL
  -- 4) typage d'habitat : **nouveau champ** OU (legacy) au moins une sous-catégorie liée
  AND (
        habitat_type IS NOT NULL
     OR EXISTS (
          SELECT 1
          FROM public.etablissement_sous_categorie esc
          WHERE esc.etablissement_id = p_etab
        )
  )
  -- 5) email au format simple OU NULL/vide (plus permissif pour import CSV)
  AND (
        email IS NULL 
        OR COALESCE(NULLIF(trim(email),''), NULL) IS NULL
        OR email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
  )
FROM e;
$_$;

-- 4. TESTER la nouvelle fonction sur quelques établissements des Landes
SELECT 
    e.id,
    e.nom,
    e.email,
    e.site_web,
    e.statut_editorial,
    public.can_publish(e.id) as peut_publier_maintenant
FROM etablissements e 
WHERE departement = 'Landes (40)' 
  AND statut_editorial != 'publie'
ORDER BY nom
LIMIT 10;

-- 5. VÉRIFIER combien d'établissements peuvent maintenant être publiés
WITH stats AS (
    SELECT 
        departement,
        COUNT(*) as total_etablissements,
        COUNT(CASE WHEN statut_editorial = 'publie' THEN 1 END) as deja_publies,
        COUNT(CASE WHEN public.can_publish(id) THEN 1 END) as peuvent_etre_publies,
        COUNT(CASE WHEN email IS NULL OR trim(email) = '' THEN 1 END) as sans_email,
        COUNT(CASE WHEN site_web IS NULL OR trim(site_web) = '' THEN 1 END) as sans_site_web
    FROM etablissements 
    WHERE departement = 'Landes (40)'
    GROUP BY departement
)
SELECT 
    departement,
    total_etablissements,
    deja_publies,
    peuvent_etre_publies,
    (peuvent_etre_publies - deja_publies) as nouveaux_publiables,
    sans_email,
    sans_site_web
FROM stats;

-- 6. SI VOUS VOULEZ ROLLBACK (annuler les changements)
-- Décommentez et exécutez ces lignes :
/*
CREATE OR REPLACE FUNCTION public.can_publish(p_etab uuid) RETURNS boolean
    LANGUAGE sql STABLE
    AS $_$
WITH e AS (
  SELECT *
  FROM public.etablissements
  WHERE id = p_etab
)
SELECT
  -- 1) nom
  COALESCE(NULLIF(trim(nom),''), NULL) IS NOT NULL
  -- 2) adresse (accepte adresse_l1 OU adresse_l2), commune, code postal (non vide), géoloc non nulle
  AND COALESCE(NULLIF(trim(adresse_l1),''), NULLIF(trim(adresse_l2),''), NULL) IS NOT NULL
  AND COALESCE(NULLIF(trim(commune),''), NULL) IS NOT NULL
  AND COALESCE(NULLIF(trim(code_postal),''), NULL) IS NOT NULL
  AND geom IS NOT NULL
  -- 3) gestionnaire
  AND COALESCE(NULLIF(trim(gestionnaire),''), NULL) IS NOT NULL
  -- 4) typage d'habitat : **nouveau champ** OU (legacy) au moins une sous-catégorie liée
  AND (
        habitat_type IS NOT NULL
     OR EXISTS (
          SELECT 1
          FROM public.etablissement_sous_categorie esc
          WHERE esc.etablissement_id = p_etab
        )
  )
  -- 5) email au format simple (RETOUR VERSION STRICTE)
  AND email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
FROM e;
$_$;
*/