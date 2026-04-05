"""
Module d'enrichissement des données habitat senior
"""
from .normalizer import DataNormalizer
from .eligibilite_rules import deduce_eligibilite_statut

__all__ = ['DataNormalizer', 'deduce_eligibilite_statut']
