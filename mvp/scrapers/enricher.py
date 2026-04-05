"""
Module 4 - Enricher v3.1
Enrichissement conditionnel et génération de présentations
Objectif : Compléter les données manquantes et générer présentations 150-200 mots
+ Utilisation du contenu du site officiel quand disponible
"""

import requests
import os
import re
from typing import Dict, Optional
from dataclasses import asdict
from bs4 import BeautifulSoup


class Enricher:
    """
    Enrichisseur intelligent
    - Génère présentations 150-200 mots si manquantes
    - Complète données uniquement si nécessaire (≥2 champs manquants)
    """
    
    def __init__(self, groq_api_key: Optional[str] = None, scrapingbee_api_key: Optional[str] = None, serper_api_key: Optional[str] = None):
        """
        Initialise l'enrichisseur
        
        Args:
            groq_api_key: Clé API Groq (ou depuis .env)
            scrapingbee_api_key: Clé API ScrapingBee (ou depuis .env)
            serper_api_key: Clé API Serper (ou depuis .env)
        """
        self.groq_api_key = groq_api_key or os.getenv('GROQ_API_KEY')
        self.scrapingbee_api_key = scrapingbee_api_key or os.getenv('SCRAPINGBEE_API_KEY')
        self.serper_api_key = serper_api_key or os.getenv('SERPER_API_KEY')
        
        # Modèle léger pour enrichissement
        self.model = "llama-3.1-8b-instant"
        
        # Pricing Groq
        self.pricing = {
            "input": 0.05,   # $/1M tokens
            "output": 0.08   # $/1M tokens
        }
        
        # Statistiques
        self.stats = {
            'establishments_processed': 0,
            'presentations_generated': 0,
            'enrichment_cost': 0.0
        }
    
    def enrich_establishments(self, establishments: list, department_name: str) -> list:
        """
        Enrichit une liste d'établissements
        
        Args:
            establishments: Liste d'ExtractedEstablishment
            department_name: Nom du département
            
        Returns:
            Liste d'établissements enrichis
        """
        print(f"\n✨ === MODULE 4 - ENRICHER V3.0 ===")
        print(f"📊 {len(establishments)} établissements à enrichir")
        
        enriched = []
        
        for i, est in enumerate(establishments, 1):
            print(f"\n   [{i}/{len(establishments)}] {est.nom}")
            
            self.stats['establishments_processed'] += 1
            
            try:
                # Génération présentation si nécessaire
                if self._needs_presentation(est):
                    print(f"      📝 Génération présentation...")
                    
                    # Détecter si c'est une résidence autonomie (source officielle)
                    is_official_source = self._is_official_source(est)
                    
                    # Pour résidences autonomie: utiliser Serper pour contexte enrichi
                    if is_official_source:
                        print(f"      🔍 Recherche Serper pour contexte enrichi...")
                        serper_snippets = self._search_context_snippets(est)
                        if serper_snippets:
                            print(f"      ✅ {len(serper_snippets)} snippets récupérés")
                            presentation = self._generate_presentation(est, department_name, serper_content=serper_snippets)
                        else:
                            print(f"      ⚠️ Serper échoué, génération standard")
                            presentation = self._generate_presentation(est, department_name)
                    else:
                        # Pour habitats alternatifs: comportement actuel (scraping site)
                        site_content = None
                        if est.site_web and est.site_web.strip():
                            print(f"      🌐 Scraping site officiel pour contexte...")
                            site_content = self._scrape_site_content(est.site_web)
                            if site_content:
                                print(f"      ✅ Contenu récupéré ({len(site_content)} caractères)")
                            else:
                                print(f"      ⚠️ Scraping échoué, génération sans contexte site")
                        
                        presentation = self._generate_presentation(est, department_name, site_content)
                    if presentation:
                        est.presentation = presentation
                        self.stats['presentations_generated'] += 1
                        print(f"      ✅ Présentation générée ({len(presentation)} caractères)")
                    else:
                        print(f"      ⚠️ Échec génération présentation")
                else:
                    print(f"      ℹ️ Présentation existante ({len(est.presentation)} caractères)")
                
                enriched.append(est)
                
            except Exception as e:
                print(f"      ❌ Erreur enrichissement: {e}")
                # Garder l'établissement même en cas d'erreur
                enriched.append(est)
                continue
        
        # Statistiques finales
        self._print_stats()
        
        return enriched
    
    def _needs_presentation(self, establishment) -> bool:
        """Vérifie si une présentation doit être générée"""
        
        # Pas de présentation ou trop courte
        if not establishment.presentation or len(establishment.presentation) < 200:
            return True
        
        return False
    
    def _is_official_source(self, establishment) -> bool:
        """Détecte si l'établissement provient de l'annuaire officiel"""
        return 'pour-les-personnes-agees.gouv.fr' in establishment.source
    
    def _search_context_snippets(self, establishment) -> Optional[str]:
        """
        Recherche des snippets contextuels via Serper pour enrichir la présentation
        
        Args:
            establishment: ExtractedEstablishment
            
        Returns:
            String combinant les snippets pertinents ou None
        """
        if not self.serper_api_key:
            return None
        
        # Construire la query ciblée
        query = f'"{establishment.nom}" {establishment.commune} résidence autonomie services'
        
        try:
            response = requests.post(
                "https://google.serper.dev/search",
                headers={
                    "X-API-KEY": self.serper_api_key,
                    "Content-Type": "application/json"
                },
                json={
                    "q": query,
                    "num": 3  # Limiter à 3 résultats max
                },
                timeout=10
            )
            
            if response.status_code != 200:
                return None
            
            data = response.json()
            organic_results = data.get('organic', [])
            
            if not organic_results:
                return None
            
            # Combiner les snippets pertinents
            snippets = []
            for result in organic_results[:3]:  # Max 3 résultats
                snippet = result.get('snippet', '')
                if snippet and len(snippet) > 50:
                    snippets.append(snippet)
            
            if snippets:
                return "\n\n".join(snippets)
            
            return None
            
        except Exception as e:
            print(f"         ❌ Erreur Serper: {e}")
            return None
    
    def _generate_presentation(self, establishment, department_name: str, 
                              site_content: Optional[str] = None,
                              serper_content: Optional[str] = None) -> Optional[str]:
        """
        Génère une présentation 150-200 mots
        
        Args:
            establishment: ExtractedEstablishment
            department_name: Nom du département
            site_content: Contenu optionnel du site officiel (premiers 2000 caractères)
            serper_content: Snippets Serper pour contexte enrichi (résidences autonomie)
            
        Returns:
            Présentation générée ou None
        """
        prompt = self._build_presentation_prompt(establishment, department_name, site_content, serper_content)
        
        try:
            response = self._call_groq_api(prompt, max_tokens=400)

            if response and response.get('content'):
                presentation = response['content'].strip()

                # Nettoyage
                presentation = self._clean_presentation(presentation)

                return presentation
            else:
                # Aucun résultat LLM — utiliser fallback
                self.stats.setdefault('presentations_fallback', 0)
                self.stats['presentations_fallback'] += 1
                return self._fallback_generate_presentation(establishment, department_name)

        except Exception as e:
            print(f"         ❌ Erreur génération: {e}")
            self.stats.setdefault('presentations_fallback', 0)
            self.stats['presentations_fallback'] += 1
            return self._fallback_generate_presentation(establishment, department_name)

    def _fallback_generate_presentation(self, establishment, department_name: str) -> str:
        """Génère une présentation basique et factuelle si le LLM n'est pas disponible."""
        # Construire phrases à partir des champs disponibles
        parts = []
        # Intro: type et localisation
        type_part = establishment.sous_categories if getattr(establishment, 'sous_categories', None) else ''
        commune = establishment.commune or ''
        cp = establishment.code_postal or ''
        intro = ''
        if type_part:
            intro = f"{type_part}"
        if commune:
            intro = (intro + " ") if intro else intro
            intro += f"situé(e) à {commune}"
            if cp:
                intro += f" ({cp})"
        if intro:
            parts.append(intro + '.')

        # Gestionnaire
        gestion = establishment.gestionnaire or ''
        if gestion:
            parts.append(f"Géré par {gestion}.")

        # Services / public
        public = getattr(establishment, 'public_cible', '') or ''
        if public:
            parts.append(f"Destiné principalement aux {public}.")

        # Fallback final: limitation de longueur et nettoyage
        presentation = ' '.join(parts).strip()
        if not presentation:
            presentation = f"Établissement {establishment.nom or ''} situé(e) à {commune or department_name}."

        # Nettoyage minimal (pas de phrases intro interdites)
        return presentation.strip()
    
    def _build_presentation_prompt(self, establishment, department_name: str, 
                                   site_content: Optional[str] = None,
                                   serper_content: Optional[str] = None) -> str:
        """Construit le prompt de génération de présentation"""
        
        # Bloc contexte - prioriser Serper pour résidences autonomie
        context = ""
        if serper_content:
            context = f"""

CONTEXTE RECHERCHÉ (sources web):
{serper_content}

✅ Utilise ces informations pour enrichir la présentation avec des détails sur les services, équipements, public accueilli
✅ Privilégie les informations factuelles et concrètes
⚠️ Reste objectif et professionnel, reformule avec tes mots"""
        elif site_content:
            context = f"""

CONTEXTE DU SITE OFFICIEL:
{site_content[:1500]}

✅ Utilise ces informations du site officiel pour enrichir la présentation
✅ Privilégie les informations factuelles (services, public accueilli, spécificités)
⚠️ Reste objectif et professionnel, pas de copié-collé direct"""
        
        return f"""Rédige une présentation claire, factuelle et professionnelle de 150 à 200 mots pour cet établissement.

ÉTABLISSEMENT:
Nom: {establishment.nom}
Type: {establishment.sous_categories}
Commune: {establishment.commune} ({establishment.code_postal})
Département: {department_name}
Gestionnaire: {establishment.gestionnaire if establishment.gestionnaire else 'Non précisé'}
Adresse: {establishment.adresse_l1 if establishment.adresse_l1 else 'Non précisée'}
Téléphone: {establishment.telephone if establishment.telephone else 'Non précisé'}
Email: {establishment.email if establishment.email else 'Non précisé'}{context}

RÈGLES STRICTES:
✅ Sois factuel, neutre et professionnel
✅ Ne mentionne JAMAIS de sites web d'agrégateurs (Essentiel Autonomie, etc.)
✅ Si gestionnaire manquant, écris "géré par l'établissement {establishment.nom}"
✅ 2-3 paragraphes maximum
✅ Si le site mentionne des services ou activités spécifiques, tu peux les mentionner

❌ N'invente AUCUNE information non présente dans les données
❌ Ne cite pas de sources ou URLs
❌ Pas de phrases commerciales ou promotionnelles
❌ Pas de coordonnées dans la présentation (elles sont dans les champs dédiés)
❌ NE COMMENCE JAMAIS la présentation par une phrase introductive du type "Voici une présentation...", "Voici une présentation claire et professionnelle...", "Présentation :" ou toute formule équivalente. Commence directement par la description (première phrase descriptive).

STRUCTURE SOUHAITÉE:
1. Introduction: Type d'établissement et localisation
2. Gestionnaire et public accueilli
3. Services ou spécificités (si info disponible dans contexte site)

Écris directement la présentation:"""
    
    def _clean_presentation(self, presentation: str) -> str:
        """Nettoie la présentation générée"""
        
        # Supprimer mentions de sites agrégateurs
        forbidden_terms = [
            'essentiel autonomie',
            'papyhappy',
            'pour-les-personnes-agees.gouv.fr',
            'source:',
            'selon',
            'd\'après'
        ]
        
        # Supprimer formules introductives courantes générées par le LLM
        presentation = re.sub(r"^\s*(?:Voici\s+(?:une\s+)?présentation(?:\s+claire)?(?:\s+et\s+professionnelle)?\s*[:\-–—]?\s*)", '', presentation, flags=re.IGNORECASE)
        presentation = re.sub(r"^\s*(?:Présentation\s*[:\-–—]\s*)", '', presentation, flags=re.IGNORECASE)

        presentation_lower = presentation.lower()
        for term in forbidden_terms:
            if term in presentation_lower:
                # Si contamination détectée, retourner présentation générique
                return self._generate_generic_presentation(presentation)
        
        # Limiter la longueur
        if len(presentation) > 600:
            # Couper au dernier point avant 600 caractères
            truncated = presentation[:600]
            last_period = truncated.rfind('.')
            if last_period > 400:
                presentation = truncated[:last_period + 1]
        
        return presentation.strip()
    
    def _generate_generic_presentation(self, original: str) -> str:
        """Génère une présentation générique en cas de contamination"""
        
        # Extraire juste la première phrase si possible
        sentences = original.split('.')
        if sentences and len(sentences[0]) > 50:
            return sentences[0] + '.'
        
        return original[:200] if len(original) > 200 else original
    
    def _call_groq_api(self, prompt: str, max_tokens: int = 400) -> Optional[Dict]:
        """Appel API Groq"""
        
        if not self.groq_api_key:
            return None
        
        headers = {
            "Authorization": f"Bearer {self.groq_api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "messages": [{"role": "user", "content": prompt}],
            "model": self.model,
            "temperature": 0.3,  # Un peu de créativité pour les présentations
            "max_tokens": max_tokens
        }
        
        try:
            response = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers=headers,
                json=payload,
                timeout=30
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
                
                self.stats['enrichment_cost'] += cost
                
                return {
                    "content": content,
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "cost": cost
                }
            else:
                print(f"         ❌ Erreur Groq API: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"         ❌ Erreur appel Groq: {e}")
            return None
    
    def _scrape_site_content(self, url: str) -> Optional[str]:
        """
        Scrape le contenu du site officiel pour enrichir la présentation
        
        Args:
            url: URL du site officiel
            
        Returns:
            Contenu texte nettoyé (premiers 2000 caractères) ou None
        """
        if not self.scrapingbee_api_key:
            return None
        
        # Rejeter les PDFs
        if url.lower().endswith('.pdf'):
            print(f"         ⚠️ PDF détecté, scraping ignoré")
            return None
        
        try:
            response = requests.get(
                'https://app.scrapingbee.com/api/v1/',
                params={
                    'api_key': self.scrapingbee_api_key,
                    'url': url,
                    'render_js': 'false',
                    'wait': '1000'
                },
                timeout=15
            )
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Supprimer scripts, styles, nav, footer
                for element in soup(['script', 'style', 'nav', 'footer', 'header']):
                    element.decompose()
                
                # Extraire le texte principal
                text = soup.get_text()
                
                # Nettoyer
                lines = (line.strip() for line in text.splitlines())
                text = ' '.join(line for line in lines if line)
                
                # Limiter à 2000 caractères pour le prompt
                return text[:2000]
            else:
                return None
                
        except Exception as e:
            print(f"         ⚠️ Erreur scraping site: {e}")
            return None
    
    def _print_stats(self):
        """Affiche les statistiques d'enrichissement"""
        
        print(f"\n📊 === STATISTIQUES ENRICHER ===")
        print(f"   Établissements traités: {self.stats['establishments_processed']}")
        print(f"   Présentations générées: {self.stats['presentations_generated']}")
        print(f"   Coût enrichissement: €{self.stats['enrichment_cost']:.6f}")
        print("=" * 50)


if __name__ == "__main__":
    """Test du module"""
    from mixtral_extractor import ExtractedEstablishment
    
    # Simuler un établissement pour test
    test_est = ExtractedEstablishment(
        nom="Habitat Inclusif LADAPT Troyes",
        commune="Troyes",
        code_postal="10000",
        gestionnaire="LADAPT",
        sous_categories="Habitat inclusif",
        habitat_type="habitat_partage",
        departement="Aube (10)",
        presentation="",  # Vide pour tester la génération
        source="https://example.com",
        date_extraction="2025-12-04"
    )
    
    enricher = Enricher()
    enriched = enricher.enrich_establishments([test_est], "Aube")
    
    if enriched:
        est = enriched[0]
        print(f"\n✅ Établissement enrichi:")
        print(f"   Nom: {est.nom}")
        print(f"   Présentation ({len(est.presentation)} car.):")
        print(f"   {est.presentation}")
