"""Configuration et constantes métier pour le pipeline FINESS.

Ce module centralise :
- Les mappings de catégories FINESS normalisées
- Les secteurs d'activité par catégorie
- Les financeurs par catégorie
- Les tarifications par catégorie
- Les prompts LLM (Gemini)
- Les exclusions de sites web
- Les paramètres Gemini

Usage :
    from enrich_finess_config import (
        CATEGORIE_NORMALISEE,
        SECTEUR_PAR_CATEGORIE,
        FINANCEUR_PAR_CATEGORIE,
        TARIFICATION_PAR_CATEGORIE,
        PROMPT_QUALIFICATION_PUBLIC,
        PROMPT_EXTRACTION_DIRIGEANTS,
        PROMPT_SIGNAUX_TENSION,
        PROMPT_RESEAU_FEDERAL,
        GEMINI_CONFIG,
        SITE_EXCLUSIONS,
        PAGES_CIBLES,
    )
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Catégorie normalisée : libellé FINESS brut → label court
# ---------------------------------------------------------------------------

CATEGORIE_NORMALISEE: dict[str, str] = {
    # =========================================================================
    # Libellés réels extraits du CSV etalab-cs1100502 (colonne 19)
    # =========================================================================

    # --- Handicap enfant ---
    "Institut Médico-Educatif (I.M.E.)": "IME",
    "Centre Action Médico-Sociale Précoce (C.A.M.S.P.)": "CAMSP",
    "Service d'Éducation Spéciale et de Soins à Domicile": "SESSAD",
    "Institut Thérapeutique Éducatif et Pédagogique (I.T.E.P.)": "ITEP",
    "Institut d'éducation motrice": "IEM",
    "Centre Médico-Psycho-Pédagogique (C.M.P.P.)": "CMPP",
    "Etablissement pour Enfants ou Adolescents Polyhandicapés": "EEAP",
    "Institut pour Déficients Auditifs": "IDA",
    "Institut pour Déficients Visuels": "IDV",
    "Institut d'Education Sensorielle Sourd/Aveugle": "IES",
    "Centre d'Accueil Familial Spécialisé": "CAFS",
    "Etablissement d'Accueil Temporaire d'Enfants Handicapés": "EATEH",
    "Foyer Hébergement Enfants et Adolescents Handicapés": "FH-Enfant",
    "Jardin d'Enfants Spécialisé": "JES",
    "Etablissement Expérimental pour Enfance Handicapée": "Expérimental-EH",

    # --- Handicap adulte ---
    "Maison d'Accueil Spécialisée (M.A.S.)": "MAS",
    "Foyer d'Accueil Médicalisé pour Adultes Handicapés (F.A.M.)": "FAM",
    "Etab.Acc.Médicalisé en tout ou partie personnes handicapées": "EAM",
    "Etab.Accueil Non Médicalisé pour personnes handicapées": "FV",
    "Foyer de Vie pour Adultes Handicapés": "FV",
    "Foyer Hébergement Adultes Handicapés": "FH",
    "Foyer d'Accueil Polyvalent pour Adultes Handicapés": "FAP",
    "Etablissement et Service d'Aide par le Travail (E.S.A.T.)": "ESAT",
    "Entreprise adaptée": "EA",
    "Service d'Accompagnement à la Vie Sociale (S.A.V.S.)": "SAVS",
    "Service d'accompagnement médico-social adultes handicapés": "SAMSAH",
    "Etablissement et Service de Réadaptation Professionnelle": "CRP",
    "Etablissement et Service de Préorientation": "CPRO",
    "Service d'Activité de Jour": "SAJ",
    "Etablissement d'Accueil Temporaire pour Adultes Handicapés": "EATAH",
    "Unités Evaluation Réentraînement et d'Orient. Soc. et Pro.": "UEROS",
    "Etablissement Expérimental pour personnes handicapées": "Expérimental-PH",
    "Etablissement Expérimental pour Adultes Handicapés": "Expérimental-AH",

    # --- Personnes âgées ---
    "Etablissement d'hébergement pour personnes âgées dépendantes": "EHPAD",
    "Service de Soins Infirmiers A Domicile (S.S.I.A.D)": "SSIAD",
    "Service autonomie aide (SAA)": "SAA",
    "Service autonomie aide et soins (SAAS)": "SAAS",
    "Résidences autonomie": "RA",
    "EHPA ne percevant pas des crédits d'assurance maladie": "EHPA",
    "EHPA percevant des crédits d'assurance maladie": "EHPA",
    "Etablissement de Soins Longue Durée": "USLD",
    "Centre de Jour pour Personnes Agées": "AJ",
    "Etablissement Expérimental pour Personnes Agées": "Expérimental-PA",

    # --- Protection de l'enfance ---
    "Maison d'Enfants à Caractère Social": "MECS",
    "Foyer de l'Enfance": "FDE",
    "Services AEMO et AED": "AEMO",
    "Service d'Intervention Educative en Milieu Ouvert": "AEMO",
    "Service d'Investigation Educative": "SIE",
    "Lieux de Vie et d'Accueil": "LVA",
    "Pouponnière à Caractère Social": "PCS",
    "Village d'Enfants": "VE",
    "Etablissement de Placement": "EP",
    "Centre Placement Familial Socio-Educatif (C.P.F.S.E.)": "CPFSE",
    "Etablissement d'Accueil Mère-Enfant": "EAME",
    "Centre parental": "CP-Parental",
    "Club Equipe de Prévention": "CP",
    "Etablissement Expérimental Enfance Protégée": "Expérimental-EP",
    "Etab. de mise à l'abri pour les pers. se déclarant M.N.A.": "MNA",
    "Serv. d'éval. minorité et isolement pers. se décl. mineures": "SEMIP",

    # --- Addictologie ---
    "Centre soins accompagnement prévention addictologie (CSAPA)": "CSAPA",
    "Ctre.Accueil/ Accomp.Réduc.Risq.Usag. Drogues (C.A.A.R.U.D.)": "CAARUD",

    # --- Hébergement social / Insertion ---
    "Centre Hébergement & Réinsertion Sociale (C.H.R.S.)": "CHRS",
    "Centre Accueil Demandeurs Asile (C.A.D.A.)": "CADA",
    "Centre Provisoire Hébergement (C.P.H.)": "CPH",
    "Lits Halte Soins Santé (L.H.S.S.)": "LHSS",
    "Lits d'Accueil Médicalisés (L.A.M.)": "LAM",
    "Appartement de Coordination Thérapeutique (A.C.T.)": "ACT",
    "Maisons Relais - Pensions de Famille": "Pension de Famille",
    "Autre Résidence Sociale (hors Maison Relais, Pension de Fami": "Résidence Sociale",
    "Foyer de Jeunes Travailleurs (résidence sociale ou non)": "FJT",
    "Foyer Travailleurs Migrants non transformé en Résidence Soc.": "FTM",
    "Résidence Hôtelière à Vocation Sociale (R.H.V.S.)": "RHVS",
    "Equipe Mobile Médico-Sociale Précarité": "EMMSP",

    # --- Services sociaux divers ---
    "Service mandataire judiciaire à la protection des majeurs": "SMJPM",
    "Service délégué aux prestations familiales": "SDPF",
    "Service d'aide et d'accompagnement à domicile aux familles": "SAAD-Famille",
    "Service dédié mesures d'accompagnement social personnalisé": "MASP",
    "Autre Centre d'Accueil": "Centre d'Accueil",
    "Etablissement Information Consultation Conseil Familial": "EICCF",
}

# ---------------------------------------------------------------------------
# Secteur d'activité par catégorie normalisée
# ---------------------------------------------------------------------------

SECTEUR_PAR_CATEGORIE: dict[str, str] = {
    # Handicap enfant
    "IME":          "Handicap Enfant",
    "SESSAD":       "Handicap Enfant",
    "CAMSP":        "Handicap Enfant",
    "ITEP":         "Handicap Enfant",
    "IEM":          "Handicap Enfant",
    "CMPP":         "Handicap Enfant",
    "EEAP":         "Handicap Enfant",
    "IDA":          "Handicap Enfant",
    "IDV":          "Handicap Enfant",
    "IES":          "Handicap Enfant",
    "CAFS":         "Handicap Enfant",
    "EATEH":        "Handicap Enfant",
    "FH-Enfant":    "Handicap Enfant",
    "JES":          "Handicap Enfant",
    "Expérimental-EH": "Handicap Enfant",
    # Handicap adulte
    "MAS":     "Handicap Adulte",
    "FAM":     "Handicap Adulte",
    "EAM":     "Handicap Adulte",
    "FH":      "Handicap Adulte",
    "FV":      "Handicap Adulte",
    "FAP":     "Handicap Adulte",
    "ESAT":    "Handicap Adulte",
    "EA":      "Handicap Adulte",
    "SAVS":    "Handicap Adulte",
    "SAMSAH":  "Handicap Adulte",
    "CRP":     "Handicap Adulte",
    "CPRO":    "Handicap Adulte",
    "SAJ":     "Handicap Adulte",
    "EATAH":   "Handicap Adulte",
    "UEROS":   "Handicap Adulte",
    "Expérimental-PH": "Handicap Adulte",
    "Expérimental-AH": "Handicap Adulte",
    # Personnes âgées
    "EHPAD":   "Personnes Âgées",
    "SSIAD":   "Personnes Âgées",
    "SAA":     "Personnes Âgées",
    "SAAS":    "Personnes Âgées",
    "RA":      "Personnes Âgées",
    "EHPA":    "Personnes Âgées",
    "USLD":    "Personnes Âgées",
    "AJ":      "Personnes Âgées",
    "Expérimental-PA": "Personnes Âgées",
    # Protection de l'enfance
    "MECS":    "Protection de l'Enfance",
    "FDE":     "Protection de l'Enfance",
    "AEMO":    "Protection de l'Enfance",
    "SIE":     "Protection de l'Enfance",
    "LVA":     "Protection de l'Enfance",
    "PCS":     "Protection de l'Enfance",
    "VE":      "Protection de l'Enfance",
    "EP":      "Protection de l'Enfance",
    "CPFSE":   "Protection de l'Enfance",
    "EAME":    "Protection de l'Enfance",
    "CP-Parental": "Protection de l'Enfance",
    "CP":      "Protection de l'Enfance",
    "Expérimental-EP": "Protection de l'Enfance",
    "MNA":     "Protection de l'Enfance",
    "SEMIP":   "Protection de l'Enfance",
    # Addictologie
    "CSAPA":   "Addictologie",
    "CAARUD":  "Addictologie",
    # Hébergement social / Insertion
    "CHRS":    "Hébergement Social",
    "CADA":    "Hébergement Social",
    "CPH":     "Hébergement Social",
    "LHSS":    "Hébergement Social",
    "LAM":     "Hébergement Social",
    "ACT":     "Hébergement Social",
    "Pension de Famille": "Hébergement Social",
    "Résidence Sociale":  "Hébergement Social",
    "FJT":     "Hébergement Social",
    "FTM":     "Hébergement Social",
    "RHVS":    "Hébergement Social",
    "EMMSP":   "Hébergement Social",
    # Services sociaux divers
    "SMJPM":          "Service Social",
    "SDPF":           "Service Social",
    "SAAD-Famille":   "Service Social",
    "MASP":           "Service Social",
    "Centre d'Accueil": "Service Social",
    "EICCF":          "Service Social",
}

# ---------------------------------------------------------------------------
# Financeur par catégorie normalisée
# ---------------------------------------------------------------------------

FINANCEUR_PAR_CATEGORIE: dict[str, dict[str, str]] = {
    # Handicap enfant
    "IME":    {"principal": "ARS", "secondaire": "Éducation Nationale"},
    "SESSAD": {"principal": "ARS"},
    "CAMSP":  {"principal": "ARS + Assurance Maladie"},
    "ITEP":   {"principal": "ARS"},
    "IEM":    {"principal": "ARS"},
    "CMPP":   {"principal": "Assurance Maladie"},
    "EEAP":   {"principal": "ARS"},
    "IDA":    {"principal": "ARS"},
    "IDV":    {"principal": "ARS"},
    "IES":    {"principal": "ARS"},
    "CAFS":   {"principal": "ARS"},
    "EATEH":  {"principal": "ARS"},
    "FH-Enfant": {"principal": "ARS"},
    "JES":    {"principal": "ARS"},
    "Expérimental-EH": {"principal": "ARS"},
    # Handicap adulte
    "MAS":    {"principal": "Assurance Maladie (100%)"},
    "FAM":    {"principal": "ARS + Conseil Départemental (50/50)"},
    "EAM":    {"principal": "ARS + Conseil Départemental"},
    "FH":     {"principal": "Conseil Départemental"},
    "FV":     {"principal": "Conseil Départemental"},
    "FAP":    {"principal": "Conseil Départemental"},
    "SAVS":   {"principal": "Conseil Départemental"},
    "SAMSAH": {"principal": "ARS + Conseil Départemental"},
    "ESAT":   {"principal": "DREETS (ex-DIRECCTE)"},
    "EA":     {"principal": "DREETS"},
    "CRP":    {"principal": "Assurance Maladie"},
    "CPRO":   {"principal": "Assurance Maladie"},
    "SAJ":    {"principal": "Conseil Départemental"},
    "EATAH":  {"principal": "Conseil Départemental"},
    "UEROS":  {"principal": "Assurance Maladie"},
    "Expérimental-PH": {"principal": "ARS"},
    "Expérimental-AH": {"principal": "ARS"},
    # Personnes âgées
    "EHPAD":  {"principal": "ARS + Conseil Départemental + Usager"},
    "SSIAD":  {"principal": "Assurance Maladie"},
    "SAA":    {"principal": "ARS + Conseil Départemental"},
    "SAAS":   {"principal": "ARS + Conseil Départemental"},
    "RA":     {"principal": "Conseil Départemental + Usager"},
    "EHPA":   {"principal": "Conseil Départemental + Usager"},
    "USLD":   {"principal": "Assurance Maladie"},
    "Expérimental-PA": {"principal": "ARS"},
    # Protection de l'enfance
    "MECS":   {"principal": "Conseil Départemental"},
    "FDE":    {"principal": "Conseil Départemental"},
    "AEMO":   {"principal": "Conseil Départemental"},
    "SIE":    {"principal": "Conseil Départemental"},
    "LVA":    {"principal": "Conseil Départemental"},
    "PCS":    {"principal": "Conseil Départemental"},
    "VE":     {"principal": "Conseil Départemental"},
    "EP":     {"principal": "Conseil Départemental"},
    "CPFSE":  {"principal": "Conseil Départemental"},
    "EAME":   {"principal": "Conseil Départemental"},
    "CP-Parental": {"principal": "Conseil Départemental"},
    "CP":     {"principal": "Conseil Départemental"},
    "Expérimental-EP": {"principal": "Conseil Départemental"},
    "MNA":    {"principal": "État + Conseil Départemental"},
    "SEMIP":  {"principal": "Conseil Départemental"},
    # Addictologie
    "CSAPA":  {"principal": "ARS"},
    "CAARUD": {"principal": "ARS"},
    # Hébergement social / Insertion
    "CHRS":   {"principal": "DREETS"},
    "CADA":   {"principal": "DREETS"},
    "CPH":    {"principal": "DREETS"},
    "LHSS":   {"principal": "ARS"},
    "LAM":    {"principal": "ARS"},
    "ACT":    {"principal": "ARS"},
    "Pension de Famille": {"principal": "DREETS + Conseil Départemental"},
    "Résidence Sociale":  {"principal": "DREETS"},
    "FJT":    {"principal": "Conseil Départemental"},
    "FTM":    {"principal": "DREETS"},
    "RHVS":   {"principal": "DREETS"},
    "EMMSP":  {"principal": "ARS"},
    # Services sociaux divers
    "SMJPM":  {"principal": "DREETS"},
    "SDPF":   {"principal": "Conseil Départemental"},
    "SAAD-Famille": {"principal": "Conseil Départemental"},
    "MASP":   {"principal": "Conseil Départemental"},
    "Centre d'Accueil": {"principal": "Variable"},
    "EICCF":  {"principal": "Conseil Départemental"},
}

# ---------------------------------------------------------------------------
# Tarification par catégorie normalisée
# ---------------------------------------------------------------------------

TARIFICATION_PAR_CATEGORIE: dict[str, str] = {
    # Handicap enfant
    "IME":    "Prix de journée / Dotation globale",
    "SESSAD": "Dotation globale",
    "CAMSP":  "Dotation globale",
    "ITEP":   "Prix de journée / Dotation globale",
    "IEM":    "Prix de journée / Dotation globale",
    "CMPP":   "Dotation globale AM",
    "EEAP":   "Prix de journée / Dotation globale",
    "IDA":    "Prix de journée / Dotation globale",
    "IDV":    "Prix de journée / Dotation globale",
    "IES":    "Prix de journée / Dotation globale",
    # Handicap adulte
    "MAS":    "Dotation globale (100% AM)",
    "FAM":    "Tarification ternaire (ARS+CD+Usager)",
    "EAM":    "Tarification ternaire (ARS+CD+Usager)",
    "FH":     "Prix de journée (APL)",
    "FV":     "Prix de journée",
    "ESAT":   "Dotation globale DREETS",
    "EA":     "Libre (droit commun)",
    "SAVS":   "Dotation globale CD",
    "SAMSAH": "Dotation globale (ARS+CD)",
    "CRP":    "Prix de journée AM",
    "CPRO":   "Prix de journée AM",
    # Personnes âgées
    "EHPAD":  "Tarification ternaire (Soins+Dépendance+Hébergement)",
    "SSIAD":  "Dotation globale ARS",
    "SAA":    "Dotation globale ARS + CD",
    "SAAS":   "Dotation globale ARS + CD",
    "RA":     "Forfait soins + Hébergement usager",
    "EHPA":   "Hébergement usager",
    "USLD":   "Tarification ternaire (Soins+Dépendance+Hébergement)",
    # Protection de l'enfance
    "MECS":   "Prix de journée CD",
    "FDE":    "Prix de journée CD",
    "AEMO":   "Prix de mesure CD",
    "LVA":    "Prix de journée CD",
    "PCS":    "Prix de journée CD",
    # Addictologie
    "CSAPA":  "Dotation globale ARS",
    "CAARUD": "Dotation globale ARS",
    # Insertion
    "CHRS":   "Dotation globale BOP 177",
    "CADA":   "Dotation globale BOP 303",
    "CPH":    "Dotation globale BOP 303",
    "LHSS":   "Dotation globale ARS",
    "LAM":    "Dotation globale ARS",
    "ACT":    "Dotation globale ARS",
    "Pension de Famille": "Forfait résidentialisation",
}

# ---------------------------------------------------------------------------
# Paramètres Gemini
# ---------------------------------------------------------------------------

GEMINI_CONFIG: dict[str, object] = {
    "model": "gemini-2.0-flash",
    "temperature": 0.15,
    "max_output_tokens": 1200,
    "timeout_s": 60,
    "max_retries": 5,
    "retry_backoff": 2.0,
    "retry_backoff_429": 15.0,  # Rate limit needs longer backoff
}

# ---------------------------------------------------------------------------
# Paramètres Mistral / Ministral
# ---------------------------------------------------------------------------

MISTRAL_CONFIG: dict[str, object] = {
    "model": "mistral-small-latest",
    "temperature": 0.15,
    "max_output_tokens": 1200,
    "timeout_s": 60,
    "max_retries": 5,
    "retry_backoff": 2.0,
    "retry_backoff_429": 10.0,
}

# ---------------------------------------------------------------------------
# Sites à exclure lors de l'identification du site officiel
# ---------------------------------------------------------------------------

SITE_EXCLUSIONS: list[str] = [
    # Réseaux sociaux
    "facebook.com", "linkedin.com", "twitter.com", "youtube.com",
    "instagram.com", "tiktok.com", "viadeo.com",
    # Annuaires / portails de données officiels
    "finess.sante.gouv.fr", "annuaire.action-sociale.org",
    "sanitaire-social.com", "sante.fr", "has-sante.fr", "atih.sante.fr",
    "ars.sante.fr", "legifrance.gouv.fr", "data.gouv.fr",
    "solidarites-sante.gouv.fr", "social-sante.gouv.fr",
    "ses-perinat.org", "creai", "cnsa.fr", "onisep.fr",
    "annuaire-mairie.fr", "communes.com", "cartesfrance.fr",
    # Gouvernement / institutions
    "assemblee-nationale.fr", "senat.fr", "service-public.fr",
    "gouv.fr",  # catch-all .gouv.fr subdomains
    "education.fr", "ameli.fr", "caf.fr", "cpam.fr",
    "departement.fr", "region.fr",
    # Annuaires entreprises / juridique
    "pagesjaunes.fr", "societe.com", "infogreffe.fr", "pappers.fr",
    "manageo.fr", "verif.com", "bodacc.fr", "sirene.fr",
    "net-entreprises.fr", "journal-officiel.gouv.fr",
    # Annuaires santé / médico-social
    "mondocteur.fr", "doctolib.fr", "ordoclic.fr",
    "sanitaire-social.com", "action-sociale.org",
    "capgeris.com", "pour-les-personnes-agees.gouv.fr",
    "mdph.fr", "place-handicap.fr",
    # Emploi / RH
    "indeed.com", "emploi-collectivites.fr",
    "hellowork.com", "meteojob.com", "pole-emploi.fr",
    "francetravail.fr", "staffsante.fr", "appel-medical.com",
    "adecco.fr", "randstad.fr", "manpower.fr",
    # Généralistes
    "google.com", "google.fr", "wikipedia.org", "bing.com",
    # Média nationaux
    "lefigaro.fr", "lemonde.fr", "bfmtv.com", "francetvinfo.fr",
    "20minutes.fr", "liberation.fr", "leparisien.fr", "lexpress.fr",
    "tf1info.fr", "europe1.fr", "rtl.fr", "francebleu.fr",
    # Média locaux / PQR (presse quotidienne régionale)
    "ladepeche.fr", "sudouest.fr", "midilibre.fr", "nrpyrenees.fr",
    "ledauphine.com", "leprogres.fr", "lalsace.fr", "dna.fr",
    "estrepublicain.fr", "lavoixdunord.fr", "ouestfrance.fr",
    "ouest-france.fr", "courrier-picard.fr", "lunion.fr",
    "lamontagne.fr", "centrefrance.com", "lanouvellerepublique.fr",
    "leparisien.fr", "actu.fr", "maville.com", "lechorepublicain.fr",
    "lejsl.com", "bienpublic.com", "infos.fr",
    # Hébergeurs / PDF / documents
    "calameo.com", "issuu.com", "scribd.com", "slideshare.net",
    "archive.org",
]

# ---------------------------------------------------------------------------
# Pages cibles pour le scraping
# ---------------------------------------------------------------------------

PAGES_CIBLES: list[str] = [
    "",                    # Page d'accueil
    "/qui-sommes-nous",
    "/nos-missions",
    "/public-accueilli",
    "/equipe",
    "/direction",
    "/organigramme",
    "/contact",
    "/a-propos",
    "/l-etablissement",
    "/presentation",
]

# ---------------------------------------------------------------------------
# Synonymes de fonctions ciblées (prospection)
# ---------------------------------------------------------------------------

DAF_SYNONYMES: set[str] = {
    "DAF",
    "Directeur Administratif et Financier",
    "RAF",
    "Responsable Administratif et Financier",
    "Directeur Financier",
    "Directeur des Finances",
    "Secrétaire Général",
}

# ---------------------------------------------------------------------------
# Requêtes Serper supplémentaires par type de catégorie
# ---------------------------------------------------------------------------

EXTRA_QUERIES_EHPAD: list[str] = [
    '"{nom}" tarif hébergement prix journée',
    '"{nom}" GIR PMP Pathos taux occupation',
    '"{nom}" avis familles résidents',
]

EXTRA_QUERIES_HANDICAP_ENFANT: list[str] = [
    '"{nom}" projet établissement autisme TSA déficience',
    '"{nom}" internat semi-internat SESSAD',
    '"{nom}" unité enseignement UEMA UEEA',
]

EXTRA_QUERIES_PROTECTION_ENFANCE: list[str] = [
    '"{nom}" habilitation justice ASE conseil départemental',
    '"{nom}" accueil urgence placement',
]

# ---------------------------------------------------------------------------
# Prompts LLM
# ---------------------------------------------------------------------------

PROMPT_QUALIFICATION_PUBLIC = """
Tu es un expert du secteur social et médico-social français (ESSMS).

À partir des informations suivantes sur un établissement :
- Nom : {raison_sociale}
- Catégorie FINESS : {categorie_libelle} ({categorie_normalisee})
- Département : {departement_nom}
- Commune : {commune}
- Contenu des pages web : 
{texte_pages_web}

Ta tâche : extraire et structurer les informations sur le public accueilli et le fonctionnement.

Réponds STRICTEMENT en JSON valide :
{{
  "type_public": "<libellé principal normalisé, ex: Enfants et adolescents déficients intellectuels>",
  "type_public_synonymes": ["<synonyme1>", "<synonyme2>"],
  "specificites_public": "<résumé 1-2 phrases des spécificités distinctives du public : particularités, profils atypiques, agréments spéciaux, spécialisations, modalités d'accueil originales, projets innovants. null si rien de notable>",
  "pathologies_specifiques": ["<TSA>", "<Polyhandicap>", ...] ou [],
  "age_min": <entier ou null>,
  "age_max": <entier ou null>,
  "tranche_age_label": "<libellé humain ex: Enfants 6-16 ans>",
  "type_accueil": ["<Internat>", "<Semi-internat>", "<Accueil de jour>", "<Ambulatoire>"],
  "periode_ouverture": "<365 jours / 210 jours / Semaine / etc.>",
  "ouverture_365": <true/false>,
  "places_info": "<info trouvée sur la capacité, ou null>",
  "site_web_officiel": "<URL du site officiel si confirmé, ou null>",
  "email_contact": "<email de contact trouvé, ou null>",
  "telephone": "<téléphone trouvé, ou null>",
  "confidence": <0.0 à 1.0>
}}

Règles :
- Si une info n'est pas trouvée dans le texte, mettre null (pas d'invention).
- Pour type_public, être précis : "Enfants et adolescents autistes (TSA)" plutôt que juste "handicap".
- Pour specificites_public, noter ce qui DISTINGUE cet établissement : unité spécialisée (UHR, PASA, UPG...), agrément MNA, accueil mère-enfant, double diagnostic, habitat inclusif, PCPE, PFR, plateforme TND, accueil temporaire/séquentiel, etc. Ne pas reformuler type_public.
- Les pathologies_specifiques doivent être normalisées : TSA, Polyhandicap, Déficience intellectuelle, Trouble du comportement, Handicap moteur, Déficience visuelle, Déficience auditive, Handicap psychique, Alzheimer, etc.
- Pour ouverture_365 : STRICTEMENT null sauf si tu trouves une mention EXPLICITE dans le texte ("ouvert 365 jours", "ouvert toute l'année", "accueil permanent 365j", "ouverture continue", "pas de fermeture annuelle"). NE JAMAIS DÉDUIRE ou inférer, même si ça semble logique selon le type d'établissement. Beaucoup d'établissements handicap enfant ferment pendant les vacances scolaires : ne mettre true que si c'est clairement dit. Pour les EHPAD/Résidences personnes âgées : ne pas automatiquement supposer 365j, vérifier dans le texte qu'il s'agit bien d'un hébergement permanent sans fermeture annuelle mentionnée.
- confidence = estimation de ta fiabilité globale.

Réponds uniquement en JSON valide, sans commentaire ni markdown.
"""

PROMPT_EXTRACTION_DIRIGEANTS = """
Tu es un assistant d'extraction de données sur les dirigeants d'organisations sociales et médico-sociales.

Organisation : {raison_sociale}
Commune : {commune}
Département : {departement_nom}

Texte des pages web (site officiel + résultats de recherche) :
{texte_combine}

Extrais TOUS les dirigeants et responsables mentionnés. Pour chaque personne, retourne :

{{
  "dirigeants": [
    {{
      "civilite": "<M.|Mme|null>",
      "nom": "<NOM>",
      "prenom": "<Prénom>",
      "fonction_brute": "<texte exact trouvé>",
      "fonction_normalisee": "<une parmi : Président / DG / Directeur / DSI / DRH / Directeur Innovation / Directeur Adjoint / Directeur Administratif et Financier / Médecin Directeur / Chef de Service>",
      "source_url": "<URL de la page où l'info a été trouvée, ou null>",
      "confiance": "<haute/moyenne/basse>"
    }}
  ]
}}

Règles :
- Ne pas inventer. Si tu n'es pas sûr du nom complet, indiquer confiance "basse".
- Normaliser la fonction vers les catégories listées.
- Distinguer le président (CA associatif) du directeur général (opérationnel).
- Si le texte mentionne "le directeur M. Dupont", extraire nom="DUPONT" prenom=null.

Réponds uniquement en JSON valide.
"""

PROMPT_SIGNAUX_TENSION = """
Tu es un analyste du secteur social et médico-social.

Établissement : {raison_sociale} ({categorie_normalisee})
Commune : {commune}, {departement_nom}

Extraits d'actualités et résultats de recherche :
{texte_actualites}

Identifie les signaux de tension ou d'opportunité stratégique parmi :
- Recrutement massif / difficultés de recrutement
- Fermeture / menace de fermeture
- Fusion / regroupement avec un autre établissement
- Projet de construction / extension / rénovation
- Grève / conflit social
- Inspection / rapport critique ARS
- Attribution nouveau CPOM
- Transformation de l'offre (virage inclusif, habitat inclusif...)
- Appel à projets ARS remporté
- Changement de direction

Réponds en JSON :
{{
  "signaux": [
    {{
      "type": "<recrutement|fermeture|fusion|extension|conflit|inspection|cpom|transformation|appel_projet|changement_direction>",
      "resume": "<résumé en 1-2 phrases>",
      "date_approx": "<YYYY-MM ou null>",
      "source_url": "<URL source>",
      "impact": "<positif|négatif|neutre>"
    }}
  ],
  "signal_tension": <true/false>,
  "signal_tension_detail": "<résumé global en 1 phrase, ou null>"
}}

Si aucun signal trouvé, retourner {{"signaux": [], "signal_tension": false, "signal_tension_detail": null}}.
Réponds uniquement en JSON valide.
"""

PROMPT_RESEAU_FEDERAL = """
À partir de ces informations sur l'organisme gestionnaire :
- Nom : {raison_sociale}
- Forme juridique : {forme_juridique_libelle}
- Extraits web : {texte_web}

Identifie le réseau fédéral / tête de réseau d'appartenance parmi :
NEXEM, FEHAP, Croix-Rouge française, UNIOPSS, URIOPSS, APF France handicap, 
UNAPEI, Adapei, ADMR, Mutualité Française, Les PEP, Apprentis d'Auteuil,
Fondation de France, Armée du Salut, Emmaüs, Coallia, AGEFIPH, LADAPT,
ou autre (préciser).

Réponds en JSON :
{{
  "reseau_federal": "<nom du réseau ou null>",
  "confiance": "<haute/moyenne/basse>",
  "source": "<élément qui t'a permis de déduire>"
}}

Si aucun réseau identifié, retourner {{"reseau_federal": null, "confiance": null, "source": null}}.
"""

# ---------------------------------------------------------------------------
# SQL — Création des tables (Annexe B de la doc)
# ---------------------------------------------------------------------------

SQL_CREATE_TABLES = """
-- 1. Table gestionnaire
CREATE TABLE IF NOT EXISTS finess_gestionnaire (
    id_gestionnaire TEXT PRIMARY KEY,
    siren TEXT,
    raison_sociale TEXT NOT NULL,
    sigle TEXT,
    forme_juridique_code TEXT,
    forme_juridique_libelle TEXT,
    reseau_federal TEXT,
    adresse_numero TEXT,
    adresse_type_voie TEXT,
    adresse_lib_voie TEXT,
    adresse_complement TEXT,
    adresse_complete TEXT,
    code_postal TEXT,
    commune TEXT,
    departement_code TEXT,
    departement_nom TEXT,
    region TEXT,
    telephone TEXT,
    site_web TEXT,
    domaine_mail TEXT,
    structure_mail TEXT,
    linkedin_url TEXT,
    nb_etablissements INTEGER DEFAULT 0,
    nb_essms INTEGER DEFAULT 0,
    budget_consolide_estime NUMERIC,
    categorie_taille TEXT,
    dominante_type TEXT,
    secteur_activite_principal TEXT,
    signal_tension BOOLEAN DEFAULT FALSE,
    signal_tension_detail TEXT,
    signaux_recents JSONB,
    latitude NUMERIC,
    longitude NUMERIC,
    geocode_precision TEXT,
    daf_nom TEXT,
    daf_prenom TEXT,
    daf_email TEXT,
    daf_telephone TEXT,
    daf_linkedin_url TEXT,
    daf_source TEXT,
    daf_confiance TEXT DEFAULT 'moyenne',
    deja_prospecte_250 BOOLEAN DEFAULT FALSE,
    deja_prospecte_250_date TIMESTAMP,
    date_ingestion TIMESTAMP DEFAULT NOW(),
    date_enrichissement TIMESTAMP,
    source_enrichissement TEXT,
    enrichissement_statut TEXT DEFAULT 'brut',
    enrichissement_log JSONB,
    CONSTRAINT finess_gestionnaire_statut_check CHECK (enrichissement_statut IN ('brut','en_cours','enrichi','erreur'))
);
CREATE INDEX IF NOT EXISTS idx_finess_gest_dept ON finess_gestionnaire(departement_code);
CREATE INDEX IF NOT EXISTS idx_finess_gest_statut ON finess_gestionnaire(enrichissement_statut);
CREATE INDEX IF NOT EXISTS idx_finess_gest_taille ON finess_gestionnaire(categorie_taille);
CREATE INDEX IF NOT EXISTS idx_finess_gest_prospecte ON finess_gestionnaire(deja_prospecte_250) WHERE deja_prospecte_250 = TRUE;
CREATE INDEX IF NOT EXISTS idx_finess_gest_geo ON finess_gestionnaire(latitude, longitude) WHERE latitude IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_finess_gest_secteur ON finess_gestionnaire(secteur_activite_principal);

-- 2. Table établissement
CREATE TABLE IF NOT EXISTS finess_etablissement (
    id_finess TEXT PRIMARY KEY,
    id_gestionnaire TEXT REFERENCES finess_gestionnaire(id_gestionnaire),
    raison_sociale TEXT,
    sigle TEXT,
    categorie_code TEXT,
    categorie_libelle TEXT,
    categorie_normalisee TEXT,
    groupe_code TEXT,
    groupe_libelle TEXT,
    secteur_activite TEXT,
    type_public TEXT,
    type_public_synonymes TEXT[],
    specificites_public TEXT,
    pathologies_specifiques TEXT[],
    tranches_age TEXT,
    age_min INTEGER,
    age_max INTEGER,
    type_accueil TEXT[],
    periode_ouverture TEXT,
    ouverture_365 BOOLEAN,
    places_autorisees INTEGER,
    places_installees INTEGER,
    taux_occupation NUMERIC,
    financeur_principal TEXT,
    financeur_secondaire TEXT,
    type_tarification TEXT,
    cpom BOOLEAN,
    cpom_date_echeance DATE,
    adresse_numero TEXT,
    adresse_type_voie TEXT,
    adresse_lib_voie TEXT,
    adresse_complement TEXT,
    adresse_complete TEXT,
    code_postal TEXT,
    commune TEXT,
    departement_code TEXT,
    departement_nom TEXT,
    region TEXT,
    telephone TEXT,
    email TEXT,
    site_web TEXT,
    latitude NUMERIC,
    longitude NUMERIC,
    geocode_precision TEXT,
    zone_dotation TEXT,
    signaux_recents JSONB,
    date_ingestion TIMESTAMP DEFAULT NOW(),
    date_enrichissement TIMESTAMP,
    source_enrichissement TEXT,
    enrichissement_statut TEXT DEFAULT 'brut',
    enrichissement_log JSONB,
    CONSTRAINT finess_etab_statut_check CHECK (enrichissement_statut IN ('brut','en_cours','enrichi','erreur'))
);
CREATE INDEX IF NOT EXISTS idx_finess_etab_gest ON finess_etablissement(id_gestionnaire);
CREATE INDEX IF NOT EXISTS idx_finess_etab_dept ON finess_etablissement(departement_code);
CREATE INDEX IF NOT EXISTS idx_finess_etab_cat ON finess_etablissement(categorie_normalisee);
CREATE INDEX IF NOT EXISTS idx_finess_etab_statut ON finess_etablissement(enrichissement_statut);
CREATE INDEX IF NOT EXISTS idx_finess_etab_365 ON finess_etablissement(ouverture_365) WHERE ouverture_365 = TRUE;
CREATE INDEX IF NOT EXISTS idx_finess_etab_secteur ON finess_etablissement(secteur_activite);

-- 3. Table dirigeant
CREATE TABLE IF NOT EXISTS finess_dirigeant (
    id SERIAL PRIMARY KEY,
    id_gestionnaire TEXT REFERENCES finess_gestionnaire(id_gestionnaire),
    id_finess_etablissement TEXT REFERENCES finess_etablissement(id_finess),
    civilite TEXT,
    nom TEXT,
    prenom TEXT,
    fonction_brute TEXT,
    fonction_normalisee TEXT,
    email_reconstitue TEXT,
    email_verifie BOOLEAN DEFAULT FALSE,
    email_organisation TEXT,
    telephone_direct TEXT,
    linkedin_url TEXT,
    source_url TEXT,
    source_type TEXT,
    confiance TEXT DEFAULT 'moyenne',
    date_enrichissement TIMESTAMP DEFAULT NOW(),
    CONSTRAINT finess_dirigeant_confiance_check CHECK (confiance IN ('haute','moyenne','basse'))
);
CREATE INDEX IF NOT EXISTS idx_finess_dir_gest ON finess_dirigeant(id_gestionnaire);
CREATE INDEX IF NOT EXISTS idx_finess_dir_etab ON finess_dirigeant(id_finess_etablissement);
CREATE INDEX IF NOT EXISTS idx_finess_dir_fonction ON finess_dirigeant(fonction_normalisee);

-- 4. Table log d'enrichissement
CREATE TABLE IF NOT EXISTS finess_enrichissement_log (
    id SERIAL PRIMARY KEY,
    id_finess TEXT,
    entite_type TEXT,
    etape TEXT,
    statut TEXT,
    details JSONB,
    serper_requetes INTEGER DEFAULT 0,
    gemini_tokens INTEGER DEFAULT 0,
    duree_ms INTEGER,
    date_execution TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_finess_log_finess ON finess_enrichissement_log(id_finess);
CREATE INDEX IF NOT EXISTS idx_finess_log_etape ON finess_enrichissement_log(etape);
CREATE INDEX IF NOT EXISTS idx_finess_log_statut ON finess_enrichissement_log(statut);

-- 5. Table cache Serper
CREATE TABLE IF NOT EXISTS finess_cache_serper (
    id SERIAL PRIMARY KEY,
    query_hash TEXT UNIQUE NOT NULL,
    query_text TEXT,
    results JSONB,
    nb_results INTEGER,
    date_requete TIMESTAMP DEFAULT NOW(),
    expire_at TIMESTAMP DEFAULT (NOW() + INTERVAL '30 days')
);
CREATE INDEX IF NOT EXISTS idx_finess_cache_hash ON finess_cache_serper(query_hash);
CREATE INDEX IF NOT EXISTS idx_finess_cache_expire ON finess_cache_serper(expire_at);
"""
