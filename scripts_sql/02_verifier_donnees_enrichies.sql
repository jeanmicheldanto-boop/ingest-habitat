-- SCRIPT 2 : VÉRIFICATION DES DONNÉES ENRICHIES
-- Fichier : 02_verifier_donnees_enrichies.sql

-- 1. SERVICES pour vos établissements des Landes
SELECT 
    e.nom as etablissement,
    COUNT(es.service_id) as nb_services,
    string_agg(s.libelle, ', ' ORDER BY s.libelle) as services_list
FROM etablissements e
LEFT JOIN etablissement_service es ON es.etablissement_id = e.id
LEFT JOIN services s ON s.id = es.service_id
WHERE e.departement = 'Landes (40)' 
  AND e.source != 'import_csv'
GROUP BY e.id, e.nom
ORDER BY nb_services DESC, e.nom;

-- 2. RESTAURATION pour vos établissements des Landes  
SELECT 
    e.nom as etablissement,
    CASE WHEN r.id IS NULL THEN '❌ Aucune donnée resto' ELSE '✅ Données resto présentes' END as a_restauration,
    CASE WHEN r.kitchenette THEN '✅ Kitchenette' ELSE '❌' END as kitchenette,
    CASE WHEN r.resto_collectif THEN '✅ Restaurant collectif' ELSE '❌' END as resto_collectif,
    CASE WHEN r.portage_repas THEN '✅ Portage repas' ELSE '❌' END as portage_repas
FROM etablissements e
LEFT JOIN restaurations r ON r.etablissement_id = e.id
WHERE e.departement = 'Landes (40)' 
  AND e.source != 'import_csv'
ORDER BY e.nom;

-- 3. TARIFICATIONS pour vos établissements des Landes
SELECT 
    e.nom as etablissement,
    COUNT(t.id) as nb_tarifs,
    string_agg(DISTINCT t.fourchette_prix::text, ', ') as fourchettes_prix
FROM etablissements e
LEFT JOIN tarifications t ON t.etablissement_id = e.id
WHERE e.departement = 'Landes (40)' 
  AND e.source != 'import_csv'
GROUP BY e.id, e.nom
ORDER BY nb_tarifs DESC, e.nom;