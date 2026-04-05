import pytest

from mvp.scrapers.mistral_extractor import MistralExtractor
from mvp.scrapers.snippet_classifier import SearchResult


def test_ph_medicosocial_rejected_without_habitat_signal():
    extractor = MistralExtractor(mistral_api_key=None, serper_api_key=None, scrapingbee_api_key=None)

    candidate = SearchResult(
        title="Foyer d'hébergement - Association X",
        url="https://example.com/foyer",
        snippet="FAM / foyer de vie pour adultes en situation de handicap",
        is_relevant=True,
    )

    page_content = "Bienvenue au Foyer d'hébergement. FAM, MAS, ESAT."
    assert extractor._should_reject_medicosocial_ph(candidate, page_content) is True


def test_ph_medicosocial_allowed_when_habitat_inclusif_signal_present():
    extractor = MistralExtractor(mistral_api_key=None, serper_api_key=None, scrapingbee_api_key=None)

    candidate = SearchResult(
        title="Habitat inclusif pour personnes handicapées",
        url="https://example.com/habitat-inclusif",
        snippet="Aide à la vie partagée (AVP) - habitat inclusif",
        is_relevant=True,
    )

    page_content = "Ce projet d'habitat inclusif bénéficie d'une aide à la vie partagée (AVP)."
    assert extractor._should_reject_medicosocial_ph(candidate, page_content) is False
