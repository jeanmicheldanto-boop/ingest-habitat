-- SCRIPT 12 : SUPPRIMER TOUTES LES DONNÉES DES LANDES POUR NOUVEL IMPORT
-- Fichier : 12_supprimer_donnees_landes.sql

-- ⚠️ ATTENTION: Ce script va supprimer TOUTES les données des établissements des Landes
-- Exécutez SEULEMENT si vous voulez repartir de zéro !

-- 1. VÉRIFIER les données qui vont être supprimées
SELECT 
    'AVANT SUPPRESSION' as statut,
    COUNT(*) as nb_etablissements,
    COUNT(CASE WHEN statut_editorial = 'publie' THEN 1 END) as nb_publies,
    string_agg(DISTINCT source, ', ') as sources
FROM etablissements 
WHERE departement = 'Landes (40)' 
  AND source != 'import_csv';

-- 2. SAUVEGARDER les IDs des établissements à supprimer (pour traçabilité)
SELECT 
    id,
    nom,
    commune,
    source,
    statut_editorial,
    created_at
FROM etablissements 
WHERE departement = 'Landes (40)' 
  AND source != 'import_csv'
ORDER BY nom;

-- 3. SUPPRIMER les données liées (dans l'ordre des contraintes FK)

-- Services associés
DELETE FROM etablissement_service 
WHERE etablissement_id IN (
    SELECT id FROM etablissements 
    WHERE departement = 'Landes (40)' 
      AND source != 'import_csv'
);

-- Sous-catégories associées  
DELETE FROM etablissement_sous_categorie
WHERE etablissement_id IN (
    SELECT id FROM etablissements 
    WHERE departement = 'Landes (40)' 
      AND source != 'import_csv'
);

-- Restaurations
DELETE FROM restaurations
WHERE etablissement_id IN (
    SELECT id FROM etablissements 
    WHERE departement = 'Landes (40)' 
      AND source != 'import_csv'
);

-- Tarifications
DELETE FROM tarifications
WHERE etablissement_id IN (
    SELECT id FROM etablissements 
    WHERE departement = 'Landes (40)' 
      AND source != 'import_csv'
);

-- 4. SUPPRIMER les établissements principaux
DELETE FROM etablissements 
WHERE departement = 'Landes (40)' 
  AND source != 'import_csv';

-- 5. VÉRIFICATION après suppression
SELECT 
    'APRÈS SUPPRESSION' as statut,
    COUNT(*) as nb_etablissements_restants
FROM etablissements 
WHERE departement = 'Landes (40)' 
  AND source != 'import_csv';

-- 6. VÉRIFICATION générale de la base (tous départements)
SELECT 
    departement,
    COUNT(*) as nb_etablissements,
    COUNT(CASE WHEN statut_editorial = 'publie' THEN 1 END) as nb_publies
FROM etablissements
GROUP BY departement
ORDER BY departement;