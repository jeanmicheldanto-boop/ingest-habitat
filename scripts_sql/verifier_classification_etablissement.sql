-- VÉRIFICATION DES DONNÉES DE CLASSIFICATION POUR UN ÉTABLISSEMENT SPÉCIFIQUE
-- ID: 2f2cf469-f657-481b-bc3c-f33f066dd186

-- 1. INFORMATIONS GÉNÉRALES DE L'ÉTABLISSEMENT
SELECT 
    'ÉTABLISSEMENT' as section,
    id,
    nom,
    commune,
    departement,
    statut_editorial,
    habitat_type,
    created_at,
    updated_at
FROM public.etablissements 
WHERE id = '2f2cf469-f657-481b-bc3c-f33f066dd186';

-- 2. SOUS-CATÉGORIES ASSOCIÉES
SELECT 
    'SOUS-CATÉGORIES' as section,
    sc.id as sous_categorie_id,
    sc.libelle as sous_categorie,
    sc.alias,
    c.id as categorie_id,
    c.libelle as categorie
FROM public.etablissement_sous_categorie esc
JOIN public.sous_categories sc ON sc.id = esc.sous_categorie_id
JOIN public.categories c ON c.id = sc.categorie_id
WHERE esc.etablissement_id = '2f2cf469-f657-481b-bc3c-f33f066dd186'
ORDER BY c.libelle, sc.libelle;

-- 3. SERVICES ASSOCIÉS
SELECT 
    'SERVICES' as section,
    s.id as service_id,
    s.libelle as service
FROM public.etablissement_service es
JOIN public.services s ON s.id = es.service_id
WHERE es.etablissement_id = '2f2cf469-f657-481b-bc3c-f33f066dd186'
ORDER BY s.libelle;

-- 4. RÉSUMÉ COMPLET
SELECT 
    'RÉSUMÉ' as section,
    e.nom,
    e.habitat_type,
    -- Sous-catégories (agrégées)
    COALESCE(
        (SELECT string_agg(sc.libelle, ', ' ORDER BY sc.libelle)
         FROM public.etablissement_sous_categorie esc
         JOIN public.sous_categories sc ON sc.id = esc.sous_categorie_id
         WHERE esc.etablissement_id = e.id),
        'Aucune'
    ) as sous_categories,
    -- Catégories parentes (agrégées)
    COALESCE(
        (SELECT string_agg(DISTINCT c.libelle, ', ' ORDER BY c.libelle)
         FROM public.etablissement_sous_categorie esc
         JOIN public.sous_categories sc ON sc.id = esc.sous_categorie_id
         JOIN public.categories c ON c.id = sc.categorie_id
         WHERE esc.etablissement_id = e.id),
        'Aucune'
    ) as categories_parentes,
    -- Services (agrégés)
    COALESCE(
        (SELECT string_agg(s.libelle, ', ' ORDER BY s.libelle)
         FROM public.etablissement_service es
         JOIN public.services s ON s.id = es.service_id
         WHERE es.etablissement_id = e.id),
        'Aucun'
    ) as services
FROM public.etablissements e
WHERE e.id = '2f2cf469-f657-481b-bc3c-f33f066dd186';

-- 5. DIAGNOSTIC POUR CLASSIFICATION "HABITAT ALTERNATIF"
SELECT 
    'DIAGNOSTIC' as section,
    CASE 
        WHEN e.habitat_type IS NULL THEN '❌ habitat_type non défini'
        WHEN e.habitat_type = 'logement_independant' THEN '✅ Logement indépendant'
        WHEN e.habitat_type = 'residence' THEN '✅ Résidence'
        WHEN e.habitat_type = 'habitat_partage' THEN '✅ Habitat partagé'
        ELSE '⚠️ habitat_type inconnu: ' || e.habitat_type::text
    END as status_habitat_type,
    
    CASE 
        WHEN NOT EXISTS (
            SELECT 1 FROM public.etablissement_sous_categorie esc 
            WHERE esc.etablissement_id = e.id
        ) THEN '❌ Aucune sous-catégorie définie'
        ELSE '✅ Sous-catégories définies'
    END as status_sous_categories,
    
    -- Vérifier si classé en "habitat alternatif" via les catégories
    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM public.etablissement_sous_categorie esc
            JOIN public.sous_categories sc ON sc.id = esc.sous_categorie_id
            JOIN public.categories c ON c.id = sc.categorie_id
            WHERE esc.etablissement_id = e.id 
            AND (c.libelle ILIKE '%alternatif%' OR sc.libelle ILIKE '%alternatif%')
        ) THEN '⚠️ Classé en habitat alternatif via catégories/sous-catégories'
        ELSE '✅ Pas de classification "alternatif" via catégories'
    END as classification_alternatif

FROM public.etablissements e
WHERE e.id = '2f2cf469-f657-481b-bc3c-f33f066dd186';