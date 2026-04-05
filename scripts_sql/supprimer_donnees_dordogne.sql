-- ============================================
-- Script de suppression des données Dordogne (24)
-- ============================================
-- Ce script supprime TOUS les établissements du département 24 (Dordogne)
-- et toutes leurs données associées dans les tables liées
-- ⚠️ ATTENTION : Cette opération est IRRÉVERSIBLE
-- ============================================

BEGIN;

-- Étape 1: Identifier les établissements de la Dordogne
-- (département commence par "Dordogne" ou code postal commence par "24")
CREATE TEMP TABLE etabs_dordogne AS
SELECT id 
FROM public.etablissements 
WHERE departement LIKE 'Dordogne%' 
   OR code_postal LIKE '24%'
   OR departement LIKE 'Département (24)%';

-- Afficher le nombre d'établissements à supprimer
DO $$
DECLARE
    nb_etabs INTEGER;
BEGIN
    SELECT COUNT(*) INTO nb_etabs FROM etabs_dordogne;
    RAISE NOTICE '📊 Nombre d''établissements de la Dordogne à supprimer: %', nb_etabs;
END $$;

-- Étape 2: Supprimer les données liées dans l'ordre (respect des FK)

-- 2.1 AVP Infos
DELETE FROM public.avp_infos 
WHERE etablissement_id IN (SELECT id FROM etabs_dordogne);

-- 2.2 Établissement - Sous-catégorie (table de liaison)
DELETE FROM public.etablissement_sous_categorie 
WHERE etablissement_id IN (SELECT id FROM etabs_dordogne);

-- 2.3 Établissement - Service (table de liaison)
DELETE FROM public.etablissement_service 
WHERE etablissement_id IN (SELECT id FROM etabs_dordogne);

-- 2.4 Restaurations
DELETE FROM public.restaurations 
WHERE etablissement_id IN (SELECT id FROM etabs_dordogne);

-- 2.5 Tarifications
DELETE FROM public.tarifications 
WHERE etablissement_id IN (SELECT id FROM etabs_dordogne);

-- 2.6 Logements types
DELETE FROM public.logements_types 
WHERE etablissement_id IN (SELECT id FROM etabs_dordogne);

-- 2.7 Public cible (si la table existe)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'public_cible') THEN
        DELETE FROM public.public_cible WHERE etablissement_id IN (SELECT id FROM etabs_dordogne);
    END IF;
END $$;

-- 2.8 Photos (si la table existe)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'photos') THEN
        DELETE FROM public.photos WHERE etablissement_id IN (SELECT id FROM etabs_dordogne);
    END IF;
END $$;

-- Étape 3: Supprimer les établissements eux-mêmes
DELETE FROM public.etablissements 
WHERE id IN (SELECT id FROM etabs_dordogne);

-- Nettoyage de la table temporaire
DROP TABLE etabs_dordogne;

-- Validation finale
DO $$
DECLARE
    nb_restants INTEGER;
BEGIN
    SELECT COUNT(*) INTO nb_restants 
    FROM public.etablissements 
    WHERE departement LIKE 'Dordogne%' 
       OR code_postal LIKE '24%'
       OR departement LIKE 'Département (24)%';
    
    IF nb_restants > 0 THEN
        RAISE EXCEPTION '❌ ERREUR: % établissements Dordogne restants!', nb_restants;
    ELSE
        RAISE NOTICE '✅ SUCCÈS: Toutes les données Dordogne ont été supprimées';
    END IF;
END $$;

COMMIT;

-- ============================================
-- Fin du script
-- ============================================
