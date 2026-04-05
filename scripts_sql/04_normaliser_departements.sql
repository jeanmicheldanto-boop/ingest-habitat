-- SCRIPT 4 : NORMALISER LES NOMS DE DÉPARTEMENTS AU FORMAT "Nom (XX)"
-- Fichier : 04_normaliser_departements.sql

-- 1. VOIR tous les formats de départements actuels
SELECT DISTINCT departement, COUNT(*) as nb_etablissements
FROM etablissements 
WHERE departement IS NOT NULL
GROUP BY departement
ORDER BY departement;

-- 2. NORMALISER AU FORMAT "Nom (XX)" - Exemples principaux

-- Garder Landes (40) comme modèle (déjà correct)

-- Autres normalisations courantes :
UPDATE etablissements SET departement = 'Pyrénées-Atlantiques (64)' WHERE departement ILIKE '%pyrénées%atlantiques%' AND departement NOT LIKE '% (64)';
UPDATE etablissements SET departement = 'Gers (32)' WHERE departement ILIKE '%gers%' AND departement NOT LIKE '% (32)';
UPDATE etablissements SET departement = 'Lot-et-Garonne (47)' WHERE departement ILIKE '%lot%garonne%' AND departement NOT LIKE '% (47)';
UPDATE etablissements SET departement = 'Gironde (33)' WHERE departement ILIKE '%gironde%' AND departement NOT LIKE '% (33)';
UPDATE etablissements SET departement = 'Haute-Garonne (31)' WHERE departement ILIKE '%haute%garonne%' AND departement NOT LIKE '% (31)';
UPDATE etablissements SET departement = 'Hautes-Pyrénées (65)' WHERE departement ILIKE '%hautes%pyrénées%' AND departement NOT LIKE '% (65)';
UPDATE etablissements SET departement = 'Paris (75)' WHERE departement ILIKE '%paris%' AND departement NOT LIKE '% (75)';
UPDATE etablissements SET departement = 'Bouches-du-Rhône (13)' WHERE departement ILIKE '%bouches%rhône%' AND departement NOT LIKE '% (13)';
UPDATE etablissements SET departement = 'Nord (59)' WHERE departement ILIKE '%nord%' AND departement NOT LIKE '% (59)' AND departement NOT ILIKE '%pas%calais%';
UPDATE etablissements SET departement = 'Pas-de-Calais (62)' WHERE departement ILIKE '%pas%calais%' AND departement NOT LIKE '% (62)';

-- Normalisation générique pour les départements simples (si pattern détectable)
-- Exemple: "Ain" -> "Ain (01)", "Aisne" -> "Aisne (02)", etc.

-- 3. IDENTIFIER les départements non normalisés restants
SELECT DISTINCT departement
FROM etablissements 
WHERE departement IS NOT NULL
  AND departement NOT SIMILAR TO '%\s\([0-9AB]{2,3}\)'  -- Ne correspond pas au pattern "Nom (XX)"
ORDER BY departement;

-- 4. VÉRIFIER après normalisation
SELECT DISTINCT departement, COUNT(*) as nb_etablissements
FROM etablissements 
WHERE departement IS NOT NULL
GROUP BY departement
ORDER BY departement;