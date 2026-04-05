-- Pilotage du schema V2 exhaustif
-- Prerequis : migration 19_schema_signaux_v2_exhaustif.sql appliquee

-- 1. KPI couverture globale hors SAA
WITH elig AS (
    SELECT g.*
    FROM public.finess_gestionnaire g
    WHERE EXISTS (
        SELECT 1
        FROM public.finess_etablissement e
        WHERE e.id_gestionnaire = g.id_gestionnaire
          AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
    )
)
SELECT
    COUNT(*) AS nb_total,
    COUNT(*) FILTER (WHERE signal_v2_statut_couverture = 'a_traiter') AS nb_a_traiter,
    COUNT(*) FILTER (WHERE signal_v2_statut_couverture = 'no_signal_public') AS nb_no_signal_public,
    COUNT(*) FILTER (WHERE signal_v2_statut_couverture = 'signal_public_non_tension') AS nb_signal_public_non_tension,
    COUNT(*) FILTER (WHERE signal_v2_statut_couverture = 'signal_tension_probable') AS nb_signal_tension_probable,
    COUNT(*) FILTER (WHERE signal_v2_statut_couverture = 'signal_ambigu_review') AS nb_signal_ambigu_review,
    COUNT(*) FILTER (WHERE signal_v2_niveau_suspicion = 'possible') AS nb_possible,
    COUNT(*) FILTER (WHERE signal_v2_niveau_suspicion = 'probable') AS nb_probable,
    COUNT(*) FILTER (WHERE signal_v2_niveau_suspicion = 'certain') AS nb_certain,
    COUNT(*) FILTER (WHERE signal_v2_review_required) AS nb_review_required
FROM elig;

-- 2. Pipeline coverage par phase
SELECT
    COALESCE(signal_v2_phase, 'non_traite') AS phase,
    COALESCE(signal_v2_statut_couverture, 'null') AS statut_couverture,
    COUNT(*) AS nb_gestionnaires
FROM public.finess_gestionnaire g
WHERE EXISTS (
    SELECT 1
    FROM public.finess_etablissement e
    WHERE e.id_gestionnaire = g.id_gestionnaire
      AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
)
GROUP BY COALESCE(signal_v2_phase, 'non_traite'), COALESCE(signal_v2_statut_couverture, 'null')
ORDER BY phase, statut_couverture;

-- 3. File d'attente G0 : gestionnaires pas encore couverts
SELECT
    g.id_gestionnaire,
    g.raison_sociale,
    g.sigle,
    g.secteur_activite_principal,
    g.nb_etablissements,
    g.signal_v2_statut_couverture,
    g.signal_v2_phase,
    g.signal_v2_methode
FROM public.finess_gestionnaire g
WHERE EXISTS (
    SELECT 1
    FROM public.finess_etablissement e
    WHERE e.id_gestionnaire = g.id_gestionnaire
      AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
)
  AND COALESCE(g.signal_v2_statut_couverture, 'a_traiter') = 'a_traiter'
ORDER BY COALESCE(g.nb_etablissements, 0) DESC, g.id_gestionnaire ASC
LIMIT 200;

-- 4. File d'attente G1/G2 : tous les cas avec suspicion a approfondir
SELECT
    g.id_gestionnaire,
    g.raison_sociale,
    g.sigle,
    g.secteur_activite_principal,
    g.nb_etablissements,
    g.signal_v2_niveau_suspicion,
    g.signal_v2_imputabilite,
    g.signal_v2_review_required,
    g.signal_v2_scope_issue,
    g.signal_v2_phase,
    g.signal_v2_queries_count,
    g.signal_v2_snippets_count,
    g.signal_v2_last_query_at
FROM public.finess_gestionnaire g
WHERE EXISTS (
    SELECT 1
    FROM public.finess_etablissement e
    WHERE e.id_gestionnaire = g.id_gestionnaire
      AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
)
  AND COALESCE(g.signal_v2_niveau_suspicion, 'aucun') IN ('possible', 'probable', 'certain')
ORDER BY
    CASE COALESCE(g.signal_v2_niveau_suspicion, 'aucun')
        WHEN 'certain' THEN 0
        WHEN 'probable' THEN 1
        WHEN 'possible' THEN 2
        ELSE 9
    END,
    COALESCE(g.nb_etablissements, 0) DESC,
    g.id_gestionnaire ASC
LIMIT 500;

-- 5. Cas ambigus a relire en priorite
SELECT
    g.id_gestionnaire,
    g.raison_sociale,
    g.sigle,
    g.nb_etablissements,
    g.signal_v2_niveau_suspicion,
    g.signal_v2_imputabilite,
    g.signal_v2_scope_issue,
    g.signal_v2_decision_detail,
    g.signal_v2_run_id,
    g.signal_v2_last_query_at
FROM public.finess_gestionnaire g
WHERE g.signal_v2_review_required = TRUE
ORDER BY g.signal_v2_last_query_at DESC NULLS LAST, COALESCE(g.nb_etablissements, 0) DESC
LIMIT 200;

-- 6. Volumetrie snippets stockes sur 90 jours
SELECT
    phase,
    COALESCE(scope_label, 'null') AS scope_label,
    COALESCE(imputabilite, 'null') AS imputabilite,
    COALESCE(suspicion_level, 'null') AS suspicion_level,
    COUNT(*) AS nb_snippets,
    COUNT(*) FILTER (WHERE used_for_decision) AS nb_used,
    COUNT(DISTINCT id_gestionnaire) AS nb_gestionnaires
FROM public.finess_signal_v2_snippet
WHERE retrieved_at >= NOW() - INTERVAL '90 days'
GROUP BY phase, COALESCE(scope_label, 'null'), COALESCE(imputabilite, 'null'), COALESCE(suspicion_level, 'null')
ORDER BY phase, nb_snippets DESC;

-- 7. Reuse potentiel par domaine source
SELECT
    COALESCE(domain, 'null') AS domain,
    COUNT(*) AS nb_snippets,
    COUNT(*) FILTER (WHERE used_for_decision) AS nb_used,
    COUNT(DISTINCT id_gestionnaire) AS nb_gestionnaires,
    MAX(retrieved_at) AS last_seen_at
FROM public.finess_signal_v2_snippet
WHERE retrieved_at >= NOW() - INTERVAL '90 days'
GROUP BY COALESCE(domain, 'null')
ORDER BY nb_used DESC, nb_snippets DESC
LIMIT 100;

-- 8. Extrait auditable des snippets d'un sous-lot recent
SELECT
    s.run_id,
    s.phase,
    s.id_gestionnaire,
    g.raison_sociale,
    s.query_text,
    s.domain,
    s.serper_rank,
    s.scope_label,
    s.imputabilite,
    s.suspicion_level,
    s.used_for_decision,
    s.discarded_reason,
    s.url,
    s.title,
    s.snippet,
    s.retrieved_at
FROM public.finess_signal_v2_snippet s
JOIN public.finess_gestionnaire g
  ON g.id_gestionnaire = s.id_gestionnaire
WHERE s.retrieved_at >= NOW() - INTERVAL '7 days'
ORDER BY s.retrieved_at DESC, s.id_gestionnaire ASC
LIMIT 300;