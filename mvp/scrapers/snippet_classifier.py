"""
Module 2 - Snippet Classifier v3.0
Classification intelligente des résultats Serper AVANT scraping coûteux
Objectif : Filtrer 80% du bruit avec LLM 8B léger
"""

import requests
import time
import os
import re
from typing import List, Dict, Optional, Union
from dataclasses import dataclass
from datetime import datetime


@dataclass
class SearchResult:
    """Résultat de recherche Serper avec classification"""
    title: str
    url: str
    snippet: str

    # décision du module 2
    is_relevant: bool = False
    classification_confidence: float = 0.0
    classification_reason: str = ""

    # nouvelle sortie v3.2
    intent: str = "UNKNOWN"  # ETABLISSEMENT_LOCAL|RESEAU_NATIONAL|ARTICLE|LISTE|HORS_SCOPE|UNKNOWN
    geo_hint: str = "UNKNOWN"  # IN_DEPT|OUT_DEPT|UNKNOWN
    needs_scrape: bool = True


class SnippetClassifier:
    """
    Classificateur intelligent de snippets Google
    Filtre 80% du bruit AVANT scraping coûteux
    """
    
    def __init__(self, groq_api_key: Optional[str] = None, serper_api_key: Optional[str] = None):
        """
        Initialise le classificateur
        
        Args:
            groq_api_key: Clé API Groq (ou depuis .env)
            serper_api_key: Clé API Serper (ou depuis .env)
        """
        self.groq_api_key = groq_api_key or os.getenv('GROQ_API_KEY')
        self.serper_api_key = serper_api_key or os.getenv('SERPER_API_KEY')
        
        # Modèle léger pour classification binaire
        self.model = "llama-3.1-8b-instant"
        
        # Pricing Groq
        self.pricing = {
            "input": 0.05,   # $/1M tokens
            "output": 0.08   # $/1M tokens
        }
        
        # Sites à exclure (contamination connue)
        # NB: on exclut les annuaires/plateformes, mais on ne veut pas exclure la presse ici,
        # car elle peut servir de point d'entrée (identification), même si elle ne doit pas devenir un site officiel.
        self.excluded_sites = [
            'essentiel-autonomie.com',
            'papyhappy.com',
            'papy-happy.com',
            'pour-les-personnes-agees.gouv.fr',
            'retraiteplus.fr',
            'logement-seniors.com',
            'capresidencesseniors.com',
            'capgeris.com',
            'france-maison-de-retraite.org',
            'lesmaisonsderetraite.fr',
            'ascellianceresidence.fr',
            'ascelliance-retraite.fr',
            'conseildependance.fr',
            'tarif-senior.com',
            # annuaires rencontrés en prod
            'cohabiting-seniors.com',
            'capretraite.fr',
            'trouver-maison-de-retraite.fr',
            'sanitaire-social.com',
            'annuaire-retraite.com',
            # réseaux sociaux (bruit)
            'linkedin.com',
            'facebook.com',
            'twitter.com',
            'instagram.com',
            'youtube.com',
            'tiktok.com',
            # divers bruit
            'banq.qc.ca',
            'basededonnees-habitatparticipatif-oasis.fr',
            'co-living-et-co-working.com',
            'villesetvillagesouilfaitbonvivre.com'
        ]

        # Liste courte de villes clés (gating géo précoce) — on peut l'étendre plus tard.
        # Objectif: éviter d'enrichir du hors-département puis supprimer à la fin.
        self.department_geo_hints = {
            '60': ['beauvais', 'compiegne', 'senlis', 'creil', 'nogent-sur-oise', 'noyon', 'clermont', 'meru'],
        }
        
        # Statistiques
        self.stats = {
            'total_results': 0,
            'classified_relevant': 0,
            'classified_irrelevant': 0,
            'excluded_by_domain': 0,
            'classification_cost': 0.0
        }
    
    def search_and_classify(self, department_name: str, department_code: str, extra_queries: Optional[List[str]] = None) -> List[SearchResult]:
        """
        Recherche Serper + Classification
        
        Args:
            department_name: Nom du département (ex: "Aube")
            department_code: Code département (ex: "10")
            extra_queries: Liste de requêtes supplémentaires spécifiques au département
            
        Returns:
            Liste des résultats classifiés comme pertinents
        """
        print(f"\n🔍 === MODULE 2 - SNIPPET CLASSIFIER V3.0 ===")
        print(f"📍 Département: {department_name} ({department_code})")
        
        # Étape 1: Requêtes Serper standard
        all_results = self._execute_serper_queries(department_name, department_code)
        print(f"\n📊 Total résultats Serper: {len(all_results)}")
        
        # Étape 1b: Requêtes supplémentaires spécifiques (si fournies)
        if extra_queries:
            print(f"\n🎯 Requêtes supplémentaires spécifiques ({len(extra_queries)} requêtes)...")
            extra_results = self._execute_extra_queries(extra_queries)
            all_results.extend(extra_results)
            print(f"   → +{len(extra_results)} résultats supplémentaires")
            print(f"📊 Total après requêtes spécifiques: {len(all_results)}")
        
        # Étape 2: Classification des snippets
        classified_results = self._classify_batch(all_results, department_name, department_code)

        # Filtre géo précoce: OUT_DEPT sort tout de suite
        kept = []
        dropped_out = 0
        for r in classified_results:
            if r.geo_hint == 'OUT_DEPT':
                dropped_out += 1
                continue
            kept.append(r)

        if dropped_out:
            print(f"\n🧭 Filtre géo précoce: {dropped_out} résultat(s) rejeté(s) OUT_DEPT avant scraping")

        classified_results = kept
        
        # Statistiques finales
        self._print_stats()
        
        return classified_results
    
    def _execute_serper_queries(self, department_name: str, department_code: str) -> List[SearchResult]:
        """
        Exécute les requêtes Serper v3.1 - 3 requêtes distinctes
        
        Returns:
            Liste de tous les résultats bruts
        """
        print(f"\n🌐 Requêtes Serper v3.2 (4 requêtes optimisées)...")
        
        all_results = []
        
        # REQUÊTE 1: Habitat inclusif simple (20 résultats)
        query1 = f'habitat inclusif {department_name}'
        print(f"\n   📋 Requête 1 (Habitat inclusif): {query1[:80]}...")
        results1 = self._search_with_serper(query1, num_results=20)
        all_results.extend(results1)
        print(f"      → {len(results1)} résultats")
        
        time.sleep(1)  # Rate limiting
        
        # REQUÊTE 2: Colocation seniors / Maisons partagées (20 résultats)
        query2 = f'(colocation seniors OR maison partagée) {department_name}'
        print(f"\n   📋 Requête 2 (Colocation/Maisons partagées): {query2[:80]}...")
        results2 = self._search_with_serper(query2, num_results=20)
        all_results.extend(results2)
        print(f"      → {len(results2)} résultats")
        
        time.sleep(1)  # Rate limiting
        
        # REQUÊTE 3: Béguinage / Village seniors (20 résultats)
        query3 = f'(béguinage OR village seniors) {department_name}'
        print(f"\n   📋 Requête 3 (Béguinage/Village): {query3[:80]}...")
        results3 = self._search_with_serper(query3, num_results=20)
        all_results.extend(results3)
        print(f"      → {len(results3)} résultats")
        
        time.sleep(1)  # Rate limiting
        
        # REQUÊTE 4: Habitat intergénérationnel (15 résultats)
        query4 = f'habitat intergénérationnel {department_name}'
        print(f"\n   📋 Requête 4 (Intergénérationnel): {query4[:80]}...")
        results4 = self._search_with_serper(query4, num_results=15)
        all_results.extend(results4)
        print(f"      → {len(results4)} résultats")
        
        return all_results
    
    def _execute_extra_queries(self, queries: List[Union[str, tuple, dict]]) -> List[SearchResult]:
        """
        Exécute des requêtes supplémentaires spécifiques.

        Accepts either a list of query strings (default 3 results each), or a list
        of tuples/dicts specifying per-query `num_results` for finer control.

        Examples accepted:
          - ["query a", "query b"]  -> each uses 3 results
          - [("query a", 5), ("query b", 3)] -> per-query num_results
          - [{"query": "query a", "num_results": 5}, ...]

        Args:
            queries: Liste de requêtes (str) ou (query,num_results) ou dict

        Returns:
            Liste de résultats bruts
        """
        all_results = []

        for i, q in enumerate(queries, 1):
            # Détecter format: str | (query, num) | {"query":..., "num_results":...}
            if isinstance(q, (list, tuple)) and len(q) >= 2:
                query_text = str(q[0])
                try:
                    num = int(q[1])
                except Exception:
                    num = 3
            elif isinstance(q, dict):
                query_text = str(q.get('query', ''))
                try:
                    num = int(q.get('num_results', 3))
                except Exception:
                    num = 3
            else:
                query_text = str(q)
                num = 3

            print(f"\n   📋 Requête spécifique {i}/{len(queries)}: {query_text[:80]}... (num_results={num})")
            results = self._search_with_serper(query_text, num_results=num)
            all_results.extend(results)
            print(f"      → {len(results)} résultats")

            if i < len(queries):  # Rate limiting sauf pour la dernière
                time.sleep(1)

        return all_results
    
    def _build_main_query(self, department_name: str) -> str:
        """Construit la requête principale OR groupée"""
        
        # Termes d'habitat groupés avec OR
        habitat_terms = [
            '"habitat inclusif"',
            '"habitat partagé"',
            '"habitat intergénérationnel"',
            '"colocation seniors"',
            '"béguinage"',
            '"village seniors"',
            '"maison partagée"'
        ]
        
        # Construction requête OR
        or_terms = ' OR '.join(habitat_terms)
        query = f"({or_terms}) {department_name}"
        
        return query
    
    def _search_with_serper(self, query: str, num_results: int = 10) -> List[SearchResult]:
        """
        Recherche via Serper.dev avec exclusions
        
        Args:
            query: Requête de recherche
            num_results: Nombre de résultats souhaités
            
        Returns:
            Liste de SearchResult
        """
        if not self.serper_api_key:
            print("⚠️ SERPER_API_KEY non configurée")
            return []
        
        # Ajouter exclusions à la requête
        exclusions = ' '.join(f'-site:{site}' for site in self.excluded_sites)
        query_with_exclusions = f'{query} {exclusions}'
        
        url = "https://google.serper.dev/search"
        
        payload = {
            "q": query_with_exclusions,
            "gl": "fr",
            "hl": "fr", 
            "lr": "lang_fr",
            "num": num_results
        }
        
        headers = {
            "X-API-KEY": self.serper_api_key,
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            results = []
            
            for item in data.get("organic", []):
                # Vérification supplémentaire domaines exclus
                if any(excluded in item['link'].lower() for excluded in self.excluded_sites):
                    self.stats['excluded_by_domain'] += 1
                    continue
                
                result = SearchResult(
                    title=item.get('title', ''),
                    url=item.get('link', ''),
                    snippet=item.get('snippet', '')
                )
                results.append(result)
            
            return results
            
        except Exception as e:
            print(f"❌ Erreur Serper: {e}")
            return []
    
    def _classify_batch(self, results: List[SearchResult], department_name: str, department_code: str) -> List[SearchResult]:
        """
        Classifie un batch de résultats
        
        Args:
            results: Liste de SearchResult à classifier
            
        Returns:
            Liste des résultats pertinents seulement
        """
        print(f"\n🤖 Classification des snippets...")
        
        self.stats['total_results'] = len(results)
        relevant_results = []
        
        for i, result in enumerate(results, 1):
            print(f"\n   [{i:02d}/{len(results)}] {result.title[:60]}...")
            
            # Pré-gating heuristique géo rapide (sans LLM)
            result.geo_hint = self._geo_hint_from_snippet(result, department_name, department_code)

            # Classification LLM (3 classes)
            is_relevant, confidence, reason, intent = self._classify_single(result, department_name, department_code)

            result.is_relevant = is_relevant
            result.classification_confidence = confidence
            result.classification_reason = reason
            result.intent = intent

            # politique needs_scrape: éviter scraping sur articles/listes (mais on peut garder pour identification)
            result.needs_scrape = intent in ['ETABLISSEMENT_LOCAL', 'RESEAU_NATIONAL']

            if is_relevant:
                relevant_results.append(result)
                self.stats['classified_relevant'] += 1
                print(f"      ✅ PERTINENT ({confidence:.0f}%) [{intent}/{result.geo_hint}] - {reason}")
            else:
                self.stats['classified_irrelevant'] += 1
                print(f"      ❌ NON PERTINENT ({confidence:.0f}%) [{intent}/{result.geo_hint}] - {reason}")
            
            # Rate limiting
            time.sleep(0.5)
        
        return relevant_results
    
    def _classify_single(self, result: SearchResult, department_name: str, department_code: str) -> tuple[bool, float, str, str]:
        """
        Classifie un seul résultat avec LLM
        
        Returns:
            (is_relevant, confidence, reason)
        """
        if not self.groq_api_key:
            print("⚠️ GROQ_API_KEY non configurée, classification par défaut")
            # intent inconnu mais on garde si geo_hint pas OUT_DEPT
            return True, 50.0, "Pas de clé API", "UNKNOWN"
        
        prompt = self._build_classification_prompt(result, department_name, department_code)
        
        try:
            response = self._call_groq_api(prompt)

            if not response:
                # Fallback heuristique au lieu de rejeter tout
                intent = self._heuristic_intent(result)
                is_relevant = intent in ['ETABLISSEMENT_LOCAL', 'RESEAU_NATIONAL']
                conf = 55.0 if is_relevant else 40.0
                return is_relevant, conf, "Fallback heuristique (API indisponible)", intent

            # Parser la réponse JSON
            parsed = self._parse_llm_json(response['content'])
            intent = parsed.get('intent', 'UNKNOWN')
            confidence = float(parsed.get('confidence', 50))
            reason = parsed.get('reason', 'LLM')

            # Décision
            is_relevant = intent in ['ETABLISSEMENT_LOCAL', 'RESEAU_NATIONAL']
            return is_relevant, confidence, reason, intent
            
        except Exception as e:
            print(f"      ⚠️ Erreur classification: {e}")
            intent = self._heuristic_intent(result)
            is_relevant = intent in ['ETABLISSEMENT_LOCAL', 'RESEAU_NATIONAL']
            conf = 55.0 if is_relevant else 40.0
            return is_relevant, conf, f"Erreur: {str(e)}", intent
    
    def _build_classification_prompt(self, result: SearchResult, department_name: str, department_code: str) -> str:
        """Construit le prompt v3.2 (3 classes + indice géographique)."""

        geo_hints = self.department_geo_hints.get(str(department_code), [])
        geo_hint_text = ', '.join(geo_hints[:8]) if geo_hints else ''

        return f"""Tu es un classificateur de résultats Google pour détecter de l'habitat alternatif (seniors en priorité, ou mixte PA/PH, et PH seulement si habitat inclusif/AVP).

DEPARTEMENT CIBLE:
- Nom: {department_name}
- Code: {department_code}
- Villes indicatives: {geo_hint_text}

Ta sortie doit être du JSON strict.

CATEGORIES (intent) possibles:
- ETABLISSEMENT_LOCAL: une structure / maison / béguinage / habitat inclusif localisable (ville/CP ou mention locale)
- RESEAU_NATIONAL: opérateur national (Ages & Vie, CetteFamille, Domani, Petits Frères...) avec page potentiellement déclinable localement
- ARTICLE: article d'actualité / presse (peut aider à identifier un lieu mais pas un site officiel)
- LISTE: liste/annuaire/agrégateur
- HORS_SCOPE: EHPAD/USLD ou sujet non habitat
- UNKNOWN

IMPORTANT:
- Si tu vois clairement une localisation hors département cible -> geo_hint = OUT_DEPT.
- Si tu vois une localisation dans le département cible -> geo_hint = IN_DEPT.
- Sinon geo_hint = UNKNOWN.

TITRE: {result.title}
URL: {result.url}
SNIPPET: {result.snippet[:350]}

Réponds UNIQUEMENT en JSON:
{{"intent":"ETABLISSEMENT_LOCAL|RESEAU_NATIONAL|ARTICLE|LISTE|HORS_SCOPE|UNKNOWN","geo_hint":"IN_DEPT|OUT_DEPT|UNKNOWN","confidence":0-100,"reason":"..."}}"""
    
    def _call_groq_api(self, prompt: str) -> Optional[Dict]:
        """Appel API Groq pour classification"""
        
        headers = {
            "Authorization": f"Bearer {self.groq_api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "messages": [{"role": "user", "content": prompt}],
            "model": self.model,
            "temperature": 0.1,
            "max_tokens": 180
        }
        
        try:
            response = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers=headers,
                json=payload,
                timeout=60
            )
            
            if response.status_code == 200:
                data = response.json()
                content = data["choices"][0]["message"]["content"]
                usage = data.get("usage", {})
                
                # Calcul coût
                input_tokens = usage.get("prompt_tokens", 0)
                output_tokens = usage.get("completion_tokens", 0)
                cost = (input_tokens * self.pricing["input"] + 
                       output_tokens * self.pricing["output"]) / 1_000_000
                
                self.stats['classification_cost'] += cost
                
                return {
                    "content": content,
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "cost": cost
                }
            else:
                print(f"      ❌ Erreur Groq API: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"      ❌ Erreur appel Groq: {e}")
            return None
    
    def _geo_hint_from_snippet(self, result: SearchResult, department_name: str, department_code: str) -> str:
        """Heuristique rapide sans LLM pour éviter du hors-département.

        - IN_DEPT si mention du code dept ("60"), du nom dept, ou d'une ville clé.
        - OUT_DEPT si mention explicite d'un autre code postal (5 chiffres) dont préfixe != dept.
        - UNKNOWN sinon.
        """
        text = f"{result.title} {result.snippet}".lower()

        dept = str(department_code).zfill(2)
        if dept in ['2a', '2b']:
            # simplification : non géré ici
            return 'UNKNOWN'

        if department_name.lower() in text:
            return 'IN_DEPT'

        # mention du numéro seul peut être ambigu, on privilégie CP
        # mais on considère "(60)" ou "oise (60)"
        if f"({dept})" in text or f" {dept} " in text:
            return 'IN_DEPT'

        for city in self.department_geo_hints.get(str(department_code), []):
            if city in text:
                return 'IN_DEPT'

        # Si un CP est trouvé et ne match pas le dept -> OUT_DEPT
        m = re.search(r'\b(\d{5})\b', text)
        if m:
            cp = m.group(1)
            if not cp.startswith(dept):
                return 'OUT_DEPT'

        return 'UNKNOWN'

    def _heuristic_intent(self, result: SearchResult) -> str:
        """Fallback simple si LLM down."""
        u = (result.url or '').lower()
        t = (result.title or '').lower()
        s = (result.snippet or '').lower()
        combined = f"{u} {t} {s}"

        if any(k in combined for k in ['ehpad', 'usld']):
            return 'HORS_SCOPE'
        if any(k in combined for k in ['annuaire', 'compare', 'tarif', 'devis', 'gratuit', 'sans engagement']):
            return 'LISTE'
        if any(k in combined for k in ['habitat inclusif', 'maison partagée', 'beguinage', 'béguinage', 'colocation']):
            return 'ETABLISSEMENT_LOCAL'
        return 'UNKNOWN'

    def _parse_llm_json(self, content: str) -> Dict:
        import json
        try:
            c = content.strip()
            if '```json' in c:
                c = c.split('```json', 1)[1].split('```', 1)[0]
            elif '```' in c:
                c = c.split('```', 1)[1].split('```', 1)[0]
            return json.loads(c.strip())
        except Exception:
            return {}

    def _print_stats(self):
        """Affiche les statistiques de classification"""
        
        print(f"\n📊 === STATISTIQUES SNIPPET CLASSIFIER ===")
        print(f"   Résultats Serper: {self.stats['total_results']}")
        print(f"   Exclus par domaine: {self.stats['excluded_by_domain']}")
        print(f"   Classifiés pertinents: {self.stats['classified_relevant']}")
        print(f"   Classifiés non pertinents: {self.stats['classified_irrelevant']}")
        
        if self.stats['total_results'] > 0:
            filter_rate = (self.stats['classified_irrelevant'] / self.stats['total_results']) * 100
            print(f"   Taux de filtrage: {filter_rate:.1f}%")
        
        print(f"   Coût classification: €{self.stats['classification_cost']:.6f}")
        print("=" * 50)


if __name__ == "__main__":
    """Test du module"""
    
    classifier = SnippetClassifier()
    
    # Test sur Aube
    results = classifier.search_and_classify("Aube", "10")
    
    print(f"\n✅ Résultats pertinents: {len(results)}")
    for i, result in enumerate(results, 1):
        print(f"   {i}. {result.title[:60]}...")
        print(f"      URL: {result.url}")
        print(f"      Confiance: {result.classification_confidence:.0f}%")
