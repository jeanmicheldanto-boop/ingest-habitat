"""
Module de déduplication intelligente pour le pipeline d'extraction
"""
from .similarity_metrics import SimilarityMetrics
from .intelligent_deduplicator import IntelligentDeduplicator

__all__ = ['SimilarityMetrics', 'IntelligentDeduplicator']
