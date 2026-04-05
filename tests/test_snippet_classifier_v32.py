import pytest

from mvp.scrapers.snippet_classifier import SnippetClassifier, SearchResult


@pytest.mark.parametrize(
    "title,snippet,dept_name,dept_code,expected",
    [
        (
            "Maison partagée Alzheimer - Beauvais (60000)",
            "Maison partagée Alzheimer située à Beauvais 60000.",
            "Oise",
            "60",
            "IN_DEPT",
        ),
        (
            "Maison partagée Coincy (02210)",
            "Maison partagée à Coincy 02210.",
            "Oise",
            "60",
            "OUT_DEPT",
        ),
        (
            "Habitat inclusif - projet", 
            "Présentation générale de l'habitat inclusif.",
            "Oise",
            "60",
            "UNKNOWN",
        ),
    ],
)
def test_geo_hint_from_snippet(title, snippet, dept_name, dept_code, expected):
    cls = SnippetClassifier(groq_api_key=None, serper_api_key=None)
    r = SearchResult(title=title, url="https://example.com", snippet=snippet)
    assert cls._geo_hint_from_snippet(r, dept_name, dept_code) == expected
