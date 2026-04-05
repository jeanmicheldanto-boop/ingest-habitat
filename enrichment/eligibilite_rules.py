"""
Règles métier pour l'éligibilité AVP (Aide à la Vie Partagée)
"""
from typing import Optional


def deduce_eligibilite_statut(
    sous_categorie: str, 
    mention_avp_explicite: bool,
    eligibilite_csv: Optional[str] = None
) -> str:
    """
    Déduire le statut d'éligibilité AVP selon les règles métier EXACTES
    
    Args:
        sous_categorie: Catégorie de l'établissement
        mention_avp_explicite: True si mention AVP détectée dans le contenu
        eligibilite_csv: Valeur déjà présente dans le CSV (si existe)
    
    Returns:
        'avp_eligible', 'non_eligible' ou 'a_verifier'
    
    Règles:
    1. JAMAIS éligibles: résidence services seniors, résidence autonomie, 
       accueil familial, MARPA, béguinage, village seniors
    2. Habitat inclusif: 
       - Si eligibilite_csv = 'avp_eligible' → GARDER (déjà vérifié)
       - Sinon → 'a_verifier'
    3. Autres catégories: 'avp_eligible' si mention AVP, sinon 'non_eligible'
    """
    if not sous_categorie:
        return eligibilite_csv if eligibilite_csv else 'a_verifier'
    
    sous_cat_clean = sous_categorie.lower().strip()
    
    # Catégories JAMAIS éligibles AVP (liste exhaustive)
    jamais_eligibles = [
        'résidence services seniors', 'résidence services',
        'résidence autonomie',
        'accueil familial',
        'marpa',
        'béguinage', 'beguinage',
        'village seniors', 'village séniors'
    ]
    
    for pattern in jamais_eligibles:
        if pattern in sous_cat_clean:
            return 'non_eligible'
    
    # CAS SPÉCIAL : Habitat inclusif
    if 'habitat inclusif' in sous_cat_clean:
        # Si déjà marqué avp_eligible dans le CSV, on garde (déjà vérifié)
        if eligibilite_csv == 'avp_eligible':
            return 'avp_eligible'
        # Sinon, à vérifier
        return 'a_verifier'
    
    # Catégories éligibles SI mention AVP explicite
    eligibles_si_mention = [
        'habitat intergénérationnel', 'habitat intergenerationnel',
        'colocation avec services',
        'habitat alternatif',
        "maison d'accueil familial", "maison d'accueil familial"
    ]
    
    for pattern in eligibles_si_mention:
        if pattern in sous_cat_clean:
            return 'avp_eligible' if mention_avp_explicite else 'non_eligible'
    
    # Par défaut
    return eligibilite_csv if eligibilite_csv else 'a_verifier'


def is_avp_eligible(eligibilite_statut: str) -> bool:
    """Vérifier si un établissement est éligible AVP"""
    return eligibilite_statut == 'avp_eligible'


def should_enrich_avp_data(eligibilite_statut: str) -> bool:
    """
    Détermine si on doit enrichir les données AVP spécifiques
    Seulement pour les établissements avp_eligible
    """
    return eligibilite_statut == 'avp_eligible'
