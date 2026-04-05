"""
Module 4.5 - Enrichisseur Adaptatif
Enrichissement automatique des établissements avec données manquantes
via recherche ciblée et extraction LLM
"""

import requests
import time
import json
import re
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import sys
import os

# Configuration
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from config_mvp import scraping_config, ai_config

@dataclass
class EnrichmentStage:
    """Résultat d'une étape d'enrichissement"""
    establishment_id: str
    missing_fields_before: List[str]
    found_fields: List[str]
    input_tokens: int
    output_tokens: int
    cost_euros: float
    duration_seconds: float
    success: bool
    reason: str

class AdaptiveEnricher:
    """
    Enrichissement adaptatif pour établissements avec données manquantes
    Déclenché si ≥3 champs manquants parmi: gestionnaire, adresse_l1, commune, email, telephone
    """
    
    def __init__(self):
        self.serper_api_key = scraping_config.serper_api_key
        self.scrapingbee_api_key = scraping_config.scrapingbee_api_key
        self.groq_api_key = ai_config.groq_api_key
        self.model = "llama-3.1-8b-instant"
        
        # Pricing Groq
        self.pricing = {
            "input": 0.05,  # $/1M tokens
            "output": 0.08  # $/1M tokens
        }
        
        # Champs prioritaires selon spécifications utilisateur
        self.priority_fields = ["commune", "gestionnaire", "adresse_l1", "email", "telephone"]
        self.enrichment_threshold = 3  # Seuil de déclenchement
        
        # Logs d'enrichissement
        self.enrichment_logs: List[EnrichmentStage] = []
        self.total_cost = 0.0
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Cache pour validation géographique (éviter appels API répétés)
        self._geo_cache = {}
        
        # Statistiques génération de présentations
        self.presentations_generated = 0
        self.presentations_total_words = 0

    def process_establishments(self, establishments: List, department: str) -> Tuple[List, Dict]:
        """
        Traite une liste d'établissements et enrichit ceux avec données manquantes
        
        Args:
            establishments: Liste des établissements du Module 4 V2
            department: Département pour le contexte
            
        Returns:
            Tuple[établissements_enrichis, statistiques]
        """
        
        print(f"\n🔧 === MODULE 4.5 - ENRICHISSEMENT ADAPTATIF ===")
        print(f"📊 Analyse de {len(establishments)} établissements")
        
        enriched_establishments = []
        stats = {
            "total_processed": len(establishments),
            "enrichment_triggered": 0,
            "enrichment_successful": 0,
            "fields_recovered": 0,
            "cost_total": 0.0,
            "duration_total": 0.0
        }
        
        for i, establishment in enumerate(establishments, 1):
            
            # Vérification si enrichissement nécessaire
            missing_fields = self._identify_missing_fields(establishment)
            needs_enrichment = len(missing_fields) >= self.enrichment_threshold
            
            print(f"\n📋 {i:02d}/{len(establishments)}: {establishment.nom}")
            print(f"   📍 Commune: {establishment.commune or 'N/A'}")
            print(f"   🏢 Gestionnaire: {establishment.gestionnaire or 'N/A'}")
            print(f"   ⚠️ Champs manquants: {len(missing_fields)}/5 {missing_fields}")
            
            if not needs_enrichment:
                print(f"   ✅ Données suffisantes - pas d'enrichissement")
                enriched_establishments.append(establishment)
                continue
                
            # Déclenchement enrichissement
            print(f"   🔍 Enrichissement déclenché ({len(missing_fields)} champs manquants)")
            stats["enrichment_triggered"] += 1
            
            enriched_establishment = self._enrich_establishment(establishment, missing_fields, department)
            
            if enriched_establishment:
                print(f"   ✅ Enrichissement réussi")
                stats["enrichment_successful"] += 1
                enriched_establishments.append(enriched_establishment)
            else:
                print(f"   ❌ Enrichissement échoué - données originales conservées")
                enriched_establishments.append(establishment)
        
        # Calcul statistiques finales
        stats["cost_total"] = self.total_cost
        stats["duration_total"] = sum(log.duration_seconds for log in self.enrichment_logs)
        stats["fields_recovered"] = sum(len(log.found_fields) for log in self.enrichment_logs)
        stats["presentations_generated"] = self.presentations_generated
        stats["presentations_avg_words"] = (self.presentations_total_words / self.presentations_generated 
                                            if self.presentations_generated > 0 else 0)
        
        print(f"\n📊 === RÉSULTATS ENRICHISSEMENT ===")
        print(f"   • Établissements traités: {stats['total_processed']}")
        print(f"   • Enrichissements déclenchés: {stats['enrichment_triggered']}")
        print(f"   • Enrichissements réussis: {stats['enrichment_successful']}")
        print(f"   • Champs récupérés: {stats['fields_recovered']}")
        print(f"   • Présentations générées: {self.presentations_generated}")
        if self.presentations_generated > 0:
            avg_words = self.presentations_total_words / self.presentations_generated
            print(f"   • Longueur moyenne présentations: {avg_words:.0f} mots")
        print(f"   • Coût total: €{stats['cost_total']:.4f}")
        print(f"   • Durée totale: {stats['duration_total']:.1f}s")
        
        return enriched_establishments, stats

    def enrich_establishment(self, establishment, department: str):
        """
        Méthode publique pour enrichir un établissement individuel
        """
        missing_fields = self._identify_missing_fields(establishment)
        
        if len(missing_fields) >= self.enrichment_threshold:
            print(f"      🔍 Enrichissement nécessaire: {len(missing_fields)} champs manquants")
            return self._enrich_establishment(establishment, missing_fields, department)
        else:
            print(f"      ✅ Établissement suffisamment complet")
            return establishment

    def _normalize_establishment_name(self, name: str) -> str:
        """
        Normalise le nom d'établissement pour recherche Google optimale
        Gère majuscules, symboles spéciaux, formats particuliers
        """
        if not name:
            return ""
        
        # Cas spécial Ages & Vie: format correct
        if "AGES" in name.upper() and "VIE" in name.upper():
            # Remplacer par format propre
            name = re.sub(r'AGES?\s*&?\s*VIE', 'Ages et Vie', name, flags=re.IGNORECASE)
        
        # Capitalisation correcte par mot (Title Case)
        # Sauf pour mots courts courants
        words = name.split()
        normalized_words = []
        
        lowercase_words = ['de', 'du', 'la', 'le', 'les', 'des', 'et', 'sous', 'sur']
        
        for i, word in enumerate(words):
            # Premier mot toujours capitalisé
            if i == 0 or word.lower() not in lowercase_words:
                normalized_words.append(word.capitalize())
            else:
                normalized_words.append(word.lower())
        
        return ' '.join(normalized_words)

    def _identify_missing_fields(self, establishment) -> List[str]:
        """Identifie les champs manquants selon priorités utilisateur"""
        
        missing = []
        
        # Vérification selon ordre de priorité
        if not establishment.commune or establishment.commune.strip() == "":
            missing.append("commune")
        if not establishment.gestionnaire or establishment.gestionnaire.strip() == "":
            missing.append("gestionnaire")
        if not establishment.adresse_l1 or establishment.adresse_l1.strip() == "":
            missing.append("adresse_l1")
        if not establishment.email or establishment.email.strip() in ["", "N/A", "null", "None"]:
            missing.append("email")
        if not establishment.telephone or establishment.telephone.strip() in ["", "N/A", "null", "None"]:
            missing.append("telephone")
            
        return missing

    def _enrich_establishment(self, establishment, missing_fields: List[str], department: str):
        """Enrichit un établissement via recherche ciblée + LLM"""
        
        start_time = time.time()
        
        try:
            # 1. Recherche ciblée avec nom + commune + département (sécurité géographique)
            dept_name = department.split("(")[0].strip()
            
            # Normaliser le nom (capitalisation correcte)
            nom_normalized = self._normalize_establishment_name(establishment.nom)
            
            # Construction requête équilibrée: nom (sans guillemets excessifs) + contexte géographique strict
            if establishment.commune:
                # Cas optimal: nom + commune + département
                search_query = f'{nom_normalized} {establishment.commune} {dept_name}'
            else:
                # Cas dégradé: nom + département seulement
                search_query = f'{nom_normalized} {dept_name}'
            
            print(f"      🔍 Recherche: {search_query}")
            
            # Si première recherche échoue, tenter version alternative avec opérateur
            self._search_alternative_query = None
            if "ages" in nom_normalized.lower() and "vie" in nom_normalized.lower():
                # Pour Ages & Vie: recherche alternative avec "Ages et Vie"
                self._search_alternative_query = search_query.replace("Ages & Vie", "Ages et Vie")
            
            # Recherche avec Serper (3 résultats max)
            search_results = self._search_targeted_content(search_query)
            
            if not search_results:
                print(f"      ❌ Aucun résultat de recherche")
                return None
                
            # 2. Scraping contenu détaillé
            detailed_content = self._scrape_detailed_content(search_results)
            
            if not detailed_content:
                print(f"      ❌ Échec scraping contenu")
                return None
                
            # 3. Extraction LLM données manquantes
            extracted_data = self._extract_missing_data_llm(
                establishment, missing_fields, detailed_content
            )
            
            # 4. Génération présentation si nécessaire
            presentation_generated = False
            if extracted_data:
                # Créer un établissement temporaire avec les données enrichies
                temp_establishment = self._apply_enrichment(establishment, extracted_data, department)
                
                # Vérifier si présentation manquante ou trop courte
                presentation_len = len(temp_establishment.presentation or '')
                if presentation_len < 200:
                    print(f"      📝 Génération présentation (actuelle: {presentation_len} chars)")
                    
                    generated_presentation = self._generate_presentation_llm(
                        temp_establishment, 
                        detailed_content
                    )
                    
                    if generated_presentation:
                        # Ajouter la présentation aux données enrichies
                        extracted_data["presentation"] = generated_presentation
                        presentation_generated = True
                        print(f"      ✅ Présentation générée: {len(generated_presentation)} chars ({len(generated_presentation.split())} mots)")
                    else:
                        print(f"      ⚠️ Échec génération présentation")
            
            duration = time.time() - start_time
            
            if extracted_data:
                # Application des données enrichies avec validation géographique
                enriched_establishment = self._apply_enrichment(establishment, extracted_data, department)
                
                # Log succès
                found_fields = [field for field in missing_fields 
                              if getattr(enriched_establishment, field, None) 
                              and str(getattr(enriched_establishment, field)).strip() not in ["", "N/A", "null", "None"]]
                
                stage_log = EnrichmentStage(
                    establishment_id=establishment.nom,
                    missing_fields_before=missing_fields,
                    found_fields=found_fields,
                    input_tokens=extracted_data.get("input_tokens", 0),
                    output_tokens=extracted_data.get("output_tokens", 0),
                    cost_euros=extracted_data.get("cost", 0.0),
                    duration_seconds=duration,
                    success=True,
                    reason=f"Enrichi {len(found_fields)} champs"
                )
                
                self.enrichment_logs.append(stage_log)
                self.total_cost += extracted_data.get("cost", 0.0)
                
                print(f"      ✅ Champs enrichis: {found_fields}")
                return enriched_establishment
            else:
                # Log échec
                stage_log = EnrichmentStage(
                    establishment_id=establishment.nom,
                    missing_fields_before=missing_fields,
                    found_fields=[],
                    input_tokens=0,
                    output_tokens=0,
                    cost_euros=0.0,
                    duration_seconds=duration,
                    success=False,
                    reason="Échec extraction LLM"
                )
                self.enrichment_logs.append(stage_log)
                return None
                
        except Exception as e:
            print(f"      ❌ Erreur enrichissement: {str(e)}")
            return None

    def _search_targeted_content(self, query: str) -> Optional[List[Dict]]:
        """Recherche ciblée avec Serper - 3 résultats max, exclut les sites contaminés"""
        
        try:
            # Sites à exclure (contamination connue)
            excluded_sites = [
                'essentiel-autonomie.com',
                'papyhappy.com',
                'pour-les-personnes-agees.gouv.fr',
                'villesetvillagesouilfaitbonvivre.com'
            ]
            
            # Ajouter exclusions à la requête
            exclusions = ' '.join(f'-site:{site}' for site in excluded_sites)
            query_with_exclusions = f'{query} {exclusions}'
            
            url = "https://google.serper.dev/search"
            payload = {
                "q": query_with_exclusions,
                "gl": "fr",
                "hl": "fr", 
                "num": 3  # Limite à 3 résultats
            }
            headers = {
                "X-API-KEY": self.serper_api_key,
                "Content-Type": "application/json"
            }
            
            response = requests.post(url, json=payload, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            results = data.get("organic", [])
            
            # Filtrer les résultats pour exclure les sites contaminés
            excluded_domains = ['essentiel-autonomie.com', 'papyhappy.com', 'pour-les-personnes-agees.gouv.fr', 'co-living-et-co-working.com', 'villesetvillagesouilfaitbonvivre.com']
            filtered_results = []
            for result in results:
                url = result.get('link', '')
                if not any(domain in url for domain in excluded_domains):
                    filtered_results.append(result)
            
            return filtered_results
            
        except Exception as e:
            print(f"      ❌ Erreur recherche Serper: {str(e)}")
            return None

    def _scrape_detailed_content(self, search_results: List[Dict]) -> Optional[str]:
        """Scrape le contenu détaillé des résultats de recherche"""
        
        combined_content = []
        
        for i, result in enumerate(search_results[:3], 1):  # Max 3 résultats
            url = result.get("link", "")
            title = result.get("title", "")
            
            if not url:
                continue
                
            try:
                print(f"      📄 Scraping {i}/3: {title[:50]}...")
                
                # Scraping avec ScrapingBee
                api_url = "https://app.scrapingbee.com/api/v1/"
                params = {
                    "api_key": self.scrapingbee_api_key,
                    "url": url,
                    "render_js": "false",
                    "timeout": 15000
                }
                
                response = requests.get(api_url, params=params, timeout=20)
                response.raise_for_status()
                
                content = response.text
                if content and len(content) > 100:
                    # Limiter à 2000 caractères par source
                    content_cleaned = content[:2000]
                    combined_content.append(f"SOURCE {i} ({title}):\n{content_cleaned}")
                    
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                print(f"      ⚠️ Erreur scraping {url}: {str(e)}")
                continue
                
        return "\n\n".join(combined_content) if combined_content else None

    def _extract_missing_data_llm(self, establishment, missing_fields: List[str], content: str) -> Optional[Dict]:
        """Extraction LLM des données manquantes depuis le contenu scrapé"""
        
        missing_str = ", ".join(missing_fields)
        
        prompt = f"""Tu es un expert en extraction de données d'établissements. 
Extrait UNIQUEMENT les informations manquantes pour cet établissement depuis le contenu fourni.

ÉTABLISSEMENT CIBLE:
- Nom: {establishment.nom}
- Commune actuelle: {establishment.commune or "NON_RENSEIGNÉ"}
- Gestionnaire actuel: {establishment.gestionnaire or "NON_RENSEIGNÉ"}

CHAMPS À ENRICHIR: {missing_str}

CONTENU À ANALYSER:
{content[:4000]}

INSTRUCTIONS CRITIQUES:
- Réponds UNIQUEMENT en JSON valide
- Ne complète QUE les champs demandés et trouvés dans le contenu
- IMPORTANT: Retourne SEULEMENT la valeur exacte, PAS de texte explicatif
- Si une info n'est pas dans le contenu → NE PAS inclure le champ dans le JSON
- Commune: ville seule (sans code postal)
- Email: juste l'email (ex: contact@example.com)
- Téléphone: juste le numéro (ex: 03 25 92 00 00)

EXEMPLES CORRECTS:
{{
  "telephone": "03 25 92 00 00",
  "email": "contact@agesetvie.com"
}}

EXEMPLES INCORRECTS (À ÉVITER):
{{
  "telephone": "non trouvé",
  "telephone": "Trouvé dans la source 2 : 03 25 92 00 00",
  "email": "pas trouvé"
}}

FORMAT FINAL:
{{
  "commune": "Troyes",
  "gestionnaire": "Ages & Vie", 
  "adresse_l1": "12 rue Example",
  "email": "contact@example.com",
  "telephone": "03 25 92 00 00"
}}

Si AUCUNE info trouvée: {{"found": false}}"""

        try:
            response_data = self._call_llm_groq(prompt, max_tokens=200)
            
            if response_data and response_data.get("content"):
                # Parse JSON response
                content_str = response_data["content"].strip()
                
                # Nettoyage JSON amélioré
                # Supprime texte explicatif avant JSON
                lines = content_str.split('\n')
                json_start = -1
                json_end = -1
                
                for i, line in enumerate(lines):
                    if line.strip().startswith('{'):
                        json_start = i
                    elif line.strip().endswith('}') and json_start != -1:
                        json_end = i
                        break
                
                if json_start != -1 and json_end != -1:
                    json_lines = lines[json_start:json_end+1]
                    content_str = '\n'.join(json_lines)
                
                # Supprime blocs markdown si présents
                if "```json" in content_str:
                    content_str = content_str.split("```json")[1]
                if "```" in content_str:
                    content_str = content_str.split("```")[0]
                
                content_str = content_str.strip()
                
                # Nettoyer les commentaires JSON (// ... ou /* ... */)
                # Le LLM ajoute parfois des commentaires malgré les instructions
                import re
                # Supprimer commentaires //
                content_str = re.sub(r'//.*', '', content_str)
                # Supprimer commentaires /* */
                content_str = re.sub(r'/\*.*?\*/', '', content_str, flags=re.DOTALL)
                # Nettoyer les virgules en trop (après suppression commentaires)
                content_str = re.sub(r',(\s*[}\]])', r'\1', content_str)
                
                try:
                    extracted = json.loads(content_str)
                    
                    # Vérifier si données trouvées
                    if extracted.get("found") == False:
                        return None
                    
                    # Nettoyer gestionnaire suspect si trouvé
                    if "gestionnaire" in extracted:
                        if self._is_suspicious_gestionnaire(extracted["gestionnaire"]):
                            print(f"      ⚠️ Gestionnaire suspect '{extracted['gestionnaire']}' détecté dans enrichissement -> supprimé")
                            del extracted["gestionnaire"]
                    
                    # Nettoyer commune suspecte si trouvée
                    if "commune" in extracted:
                        if self._is_suspicious_commune(extracted["commune"]):
                            print(f"      ⚠️ Commune suspecte '{extracted['commune']}' détectée dans enrichissement -> supprimée")
                            del extracted["commune"]
                    
                    # Nettoyer adresse suspecte si trouvée
                    if "adresse_l1" in extracted:
                        if self._is_suspicious_address(extracted["adresse_l1"]):
                            print(f"      ⚠️ Adresse suspecte '{extracted['adresse_l1']}' détectée dans enrichissement -> supprimée")
                            del extracted["adresse_l1"]
                    
                    # Nettoyer téléphone suspect si trouvé
                    if "telephone" in extracted:
                        if self._is_suspicious_phone(extracted["telephone"]):
                            print(f"      ⚠️ Téléphone suspect '{extracted['telephone']}' détecté dans enrichissement -> supprimé")
                            del extracted["telephone"]
                    
                    # Nettoyer email suspect si trouvé
                    if "email" in extracted:
                        if self._is_suspicious_email(extracted["email"]):
                            print(f"      ⚠️ Email suspect '{extracted['email']}' détecté dans enrichissement -> supprimé")
                            del extracted["email"]
                        
                    # Ajouter métadonnées coût
                    extracted["input_tokens"] = response_data.get("input_tokens", 0)
                    extracted["output_tokens"] = response_data.get("output_tokens", 0)
                    extracted["cost"] = response_data.get("cost", 0.0)
                    
                    return extracted
                    
                except json.JSONDecodeError as e:
                    print(f"      ❌ Erreur parsing JSON: {str(e)}")
                    print(f"      📋 Contenu à parser: '{content_str[:200]}...'")
                    return None
            else:
                return None
                
        except Exception as e:
            print(f"      ❌ Erreur LLM extraction: {str(e)}")
            return None

    def _generate_presentation_llm(self, establishment, scraped_content: str) -> Optional[str]:
        """
        Génère une présentation synthétique de 300-400 mots via LLM
        
        Args:
            establishment: Établissement avec données de base
            scraped_content: Contenu web scrapé (sources multiples)
        
        Returns:
            Présentation synthétique ou None si échec
        """
        
        # Construire le contexte de l'établissement
        context_parts = []
        context_parts.append(f"- Nom: {establishment.nom}")
        context_parts.append(f"- Type: {establishment.habitat_type or 'Non spécifié'}")
        
        if establishment.commune:
            context_parts.append(f"- Commune: {establishment.commune}")
        if establishment.gestionnaire and not self._is_suspicious_gestionnaire(establishment.gestionnaire):
            context_parts.append(f"- Gestionnaire: {establishment.gestionnaire}")
        if establishment.adresse_l1:
            context_parts.append(f"- Adresse: {establishment.adresse_l1}")
        if establishment.telephone:
            context_parts.append(f"- Téléphone: {establishment.telephone}")
        if establishment.email:
            context_parts.append(f"- Email: {establishment.email}")
        if establishment.site_web:
            context_parts.append(f"- Site web: {establishment.site_web}")
        
        establishment_context = "\n".join(context_parts)
        
        # Limiter le contenu scrapé à 3000 caractères
        content_excerpt = scraped_content[:3000] if scraped_content else "Aucun contenu supplémentaire disponible."
        
        prompt = f"""Rédige une présentation de 150-200 mots pour cet établissement.

DONNÉES DISPONIBLES:
{establishment_context}

CONTENU SCRAPÉ:
{content_excerpt}

RÈGLES:
- Utilise SEULEMENT les informations fournies ci-dessus
- Ne mentionne JAMAIS "Essentiel Autonomie" ou sites web
- Si gestionnaire manquant, écris "géré par [nom établissement]"
- Sois factuel et concis
- 2 paragraphes maximum
- Ne commence PAS par "Voici une présentation..."

Écris directement la présentation:"""

        try:
            # Appel LLM avec max_tokens réduit pour 150-200 mots (~300 tokens)
            response_data = self._call_llm_groq(prompt, max_tokens=400)
            
            if response_data and response_data.get("content"):
                presentation = response_data["content"].strip()
                
                # Nettoyer la présentation (supprimer markdown, titres, etc.)
                # Supprimer les titres markdown
                presentation = re.sub(r'^#{1,6}\s+.*$', '', presentation, flags=re.MULTILINE)
                # Supprimer les puces
                presentation = re.sub(r'^\s*[-*•]\s+', '', presentation, flags=re.MULTILINE)
                # Supprimer les lignes vides multiples
                presentation = re.sub(r'\n\s*\n+', ' ', presentation)
                # Nettoyer les espaces multiples
                presentation = re.sub(r'\s+', ' ', presentation)
                presentation = presentation.strip()
                # Supprimer formules introductives courantes générées par le LLM
                presentation = re.sub(r"^\s*(?:Voici\s+(?:une\s+)?présentation(?:\s+claire)?(?:\s+et\s+professionnelle)?\s*[:\-–—]?\s*)", '', presentation, flags=re.IGNORECASE)
                presentation = re.sub(r"^\s*(?:Présentation\s*[:\-–—]\s*)", '', presentation, flags=re.IGNORECASE)
                
                # Nettoyer les mentions d'Essentiel Autonomie et sites suspects
                suspicious_mentions = [
                    "Essentiel Autonomie", "essentiel autonomie", "essentiel-autonomie",
                    "pour-les-personnes-agees.gouv.fr", "Papyhappy", "papy happy",
                    "géré par Essentiel Autonomie", "l'Essentiel Autonomie",
                    "gestionnaire Essentiel Autonomie", "gestionnaire, l'Essentiel Autonomie"
                ]
                
                for mention in suspicious_mentions:
                    # Remplacer les mentions de gestionnaire suspect
                    presentation = presentation.replace(f"géré par {mention}", "géré par l'organisme gestionnaire")
                    presentation = presentation.replace(f"Le gestionnaire {mention}", "Le gestionnaire")
                    presentation = presentation.replace(f"Le gestionnaire, {mention},", "Le gestionnaire")
                    presentation = presentation.replace(mention, "l'organisme gestionnaire")
                
                # Nettoyer les doublons de mots après remplacement
                presentation = re.sub(r'\b(l\'organisme gestionnaire)\s+\1\b', r'\1', presentation, flags=re.IGNORECASE)
                presentation = re.sub(r'\s+', ' ', presentation).strip()
                
                # Vérifier longueur (en mots) - limites assouplies
                word_count = len(presentation.split())
                
                if word_count < 50:
                    print(f"      ⚠️ Présentation trop courte ({word_count} mots), rejetée")
                    return None
                
                if word_count > 300:
                    print(f"      ⚠️ Présentation trop longue ({word_count} mots), tronquée")
                    # Tronquer à environ 250 mots
                    words = presentation.split()[:250]
                    presentation = ' '.join(words)
                    # S'assurer qu'on termine par une phrase complète
                    if not presentation.endswith('.'):
                        last_period = presentation.rfind('.')
                        if last_period > 0:
                            presentation = presentation[:last_period + 1]
                    word_count = len(presentation.split())
                
                # Mettre à jour les statistiques
                self.presentations_generated += 1
                self.presentations_total_words += word_count
                
                # Ajouter le coût de génération au total
                self.total_cost += response_data.get("cost", 0.0)
                
                return presentation
            else:
                return None
                
        except Exception as e:
            print(f"      ❌ Erreur génération présentation: {str(e)}")
            return None

    def _apply_enrichment(self, original_establishment, enrichment_data: Dict, department: str):
        """Applique les données d'enrichissement à l'établissement avec validation géographique"""
        
        # Copie de l'établissement original
        enriched = type(original_establishment)(**asdict(original_establishment))
        
        # Validation géographique préalable
        if not self._validate_geographic_coherence(enrichment_data, department):
            print(f"      ❌ Validation géographique échouée - enrichissement rejeté")
            return original_establishment
        
        # Application sélective des enrichissements avec validation
        for field in ["commune", "gestionnaire", "adresse_l1", "email", "telephone", "presentation"]:
            if field in enrichment_data and enrichment_data[field]:
                value = enrichment_data[field].strip()
                
                # Filtrage de contenu aberrant
                if self._is_aberrant_content(value):
                    print(f"      ⚠️ Contenu aberrant détecté pour {field}: '{value[:50]}...'")
                    continue
                    
                # Validation spécifique par type de champ
                if field == "email":
                    if self._is_valid_email(value):
                        setattr(enriched, field, value)
                    else:
                        print(f"      ⚠️ Email invalide ignoré: '{value}'")
                elif field == "telephone":
                    if self._is_valid_phone(value):
                        setattr(enriched, field, value)
                    else:
                        print(f"      ⚠️ Téléphone invalide ignoré: '{value}'")
                elif field == "presentation":
                    # Validation longueur présentation (doit être substantielle)
                    if len(value) >= 200:
                        setattr(enriched, field, value)
                    else:
                        print(f"      ⚠️ Présentation trop courte ignorée: {len(value)} chars")
                else:
                    # Validation basique pour autres champs
                    if value and value not in ["", "N/A", "null", "None", "Non spécifié", "Non disponible"]:
                        setattr(enriched, field, value)
        
        return enriched
    
    def _is_aberrant_content(self, content: str) -> bool:
        """Détecte le contenu aberrant (binaire, symboles, etc.)"""
        if not content or len(content) < 2:
            return True
            
        # Contenu trop court (mais pas de limite supérieure pour les présentations)
        # Note: Cette fonction est utilisée pour valider emails, téléphones, adresses, etc.
        # Les présentations sont validées séparément
        if len(content) > 5000:  # Limite très haute pour éviter du contenu vraiment aberrant
            return True
            
        # Pourcentage de caractères non-ASCII trop élevé
        non_ascii_count = sum(1 for c in content if ord(c) > 127)
        if non_ascii_count > len(content) * 0.3:
            return True
            
        # Contenu principalement des symboles
        symbol_count = sum(1 for c in content if not c.isalnum() and c not in ' .,;:!?-_@()[]{}"\'')
        if symbol_count > len(content) * 0.5:
            return True
            
        # Patterns suspects
        suspect_patterns = ['\\x', '<?', '<!', 'binary', 'encoding', 'charset']
        if any(pattern in content.lower() for pattern in suspect_patterns):
            return True
            
        return False
    
    def _is_valid_email(self, email: str) -> bool:
        """Valide le format email"""
        import re
        if not email or len(email) < 5:
            return False
            
        # Pattern email basique mais robuste
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(pattern, email):
            return False
            
        # Rejeter emails génériques/inventés
        invalid_patterns = [
            'exemple', 'example', 'test', 'admin', 'info@info',
            'contact@contact', 'noreply', 'no-reply', 'fake',
            '.com.com', '@gmail.gmail', 'xxx', '000'
        ]
        email_lower = email.lower()
        if any(pattern in email_lower for pattern in invalid_patterns):
            return False
            
        return True
    
    def _is_valid_phone(self, phone: str) -> bool:
        """Valide le format téléphone"""
        import re
        if not phone:
            return False
            
        # Nettoyer le numéro
        clean_phone = re.sub(r'[^0-9+]', '', phone)
        
        # Vérifier longueur et format
        if len(clean_phone) < 9 or len(clean_phone) > 15:
            return False
            
        # Rejeter numéros manifestement faux
        invalid_patterns = [
            '0000000000', '1111111111', '9999999999',
            '0123456789', '1234567890', '0000000001'
        ]
        if clean_phone in invalid_patterns:
            return False
            
        # Vérifier format français basique
        if clean_phone.startswith('0') and len(clean_phone) == 10:
            return True
        elif clean_phone.startswith('+33') and len(clean_phone) == 12:
            return True
        elif clean_phone.startswith('33') and len(clean_phone) == 11:
            return True
            
        return False

    def _call_llm_groq(self, prompt: str, max_tokens: int = 200) -> Optional[Dict]:
        """Appel LLM Groq avec calcul du coût"""
        
        try:
            headers = {
                "Authorization": f"Bearer {self.groq_api_key}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "messages": [{"role": "user", "content": prompt}],
                "model": self.model,
                "temperature": 0.1,
                "max_tokens": max_tokens
            }
            
            response = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers=headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            usage = data.get("usage", {})
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            
            # Calcul coût
            input_tokens = usage.get("prompt_tokens", 0)
            output_tokens = usage.get("completion_tokens", 0)
            cost = (input_tokens * self.pricing["input"] + output_tokens * self.pricing["output"]) / 1_000_000
            
            return {
                "content": content,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cost": cost
            }
            
        except Exception as e:
            print(f"      ❌ Erreur appel LLM: {str(e)}")
            return None

    def _validate_geographic_coherence(self, enrichment_data: Dict, department: str) -> bool:
        """
        Valide la cohérence géographique via API geo.data.gouv.fr
        Solution industrialisable pour toute la France
        """
        
        # Extraire code département (ex: "Aube (10)" -> "10")
        dept_code = None
        if "(" in department and ")" in department:
            dept_code = department.split("(")[1].split(")")[0].strip()
        
        if not dept_code:
            return True  # Pas de validation possible
        
        # Vérification commune enrichie
        if "commune" in enrichment_data and enrichment_data["commune"]:
            commune_name = enrichment_data["commune"].strip()
            
            # Vérifier via API geo.data.gouv.fr
            if not self._is_commune_in_department(commune_name, dept_code):
                print(f"      ❌ Commune '{commune_name}' n'appartient pas au département {dept_code}")
                return False
        
        return True
    
    def _is_commune_in_department(self, commune_name: str, dept_code: str) -> bool:
        """
        Vérifie qu'une commune appartient à un département via API officielle
        Utilise cache pour éviter appels répétés
        """
        
        # Clé cache
        cache_key = f"{commune_name.lower()}_{dept_code}"
        
        # Vérifier cache
        if cache_key in self._geo_cache:
            return self._geo_cache[cache_key]
        
        try:
            # API geo.data.gouv.fr (données officielles INSEE)
            url = "https://geo.api.gouv.fr/communes"
            params = {
                "nom": commune_name,
                "fields": "nom,codeDepartement",
                "limit": 5
            }
            
            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            
            communes = response.json()
            
            # Vérifier si une commune correspond au département
            for commune in communes:
                if commune.get("codeDepartement") == dept_code:
                    self._geo_cache[cache_key] = True
                    return True
            
            # Aucune correspondance trouvée
            self._geo_cache[cache_key] = False
            return False
            
        except Exception as e:
            print(f"      ⚠️ Erreur validation géo API: {str(e)}")
            # En cas d'erreur API, on accepte par défaut (fail-safe)
            self._geo_cache[cache_key] = True
            return True

    def _is_suspicious_gestionnaire(self, gestionnaire: str) -> bool:
        """Détecte gestionnaires suspects (même logique que LLM validator)"""
        if not gestionnaire:
            return False
        
        gest_lower = gestionnaire.lower()
        suspects = [
            'non renseigné', 'non précisé', 'inconnu', 'n/a', 'na',
            'particulier anonyme', 'privé', 'à définir',
            # Sites web confondus avec gestionnaires
            'essentiel autonomie', 'essentiel-autonomie',
            'pour-les-personnes-agees.gouv.fr', 'pour les personnes agees',
            'papyhappy', 'papy happy', 'papy-happy',
            'malakoff humanis',  # Éditeur du site Essentiel Autonomie, pas gestionnaire d'établissement
            'annuaire', 'site web', 'plateforme', 'portail'
        ]
        return any(s in gest_lower for s in suspects)
    
    def _is_suspicious_address(self, address: str) -> bool:
        """Détecte adresses suspectes de sites agrégateurs"""
        if not address:
            return False
        
        addr_lower = address.lower()
        # Adresses typiques des sites agrégateurs parisiens
        suspicious_patterns = [
            '21 rue laffitte',  # Essentiel Autonomie
            'rue laffitte',     # Variantes
            '75009',           # Code postal Paris 9e (Essentiel Autonomie)
            'malakoff humanis' # Siège social pas établissement
        ]
        return any(pattern in addr_lower for pattern in suspicious_patterns)
    
    def _is_suspicious_commune(self, commune: str) -> bool:
        """Détecte communes suspectes pour établissements locaux"""
        if not commune:
            return False
        
        commune_lower = commune.lower()
        # Paris est suspect pour des établissements censés être dans d'autres départements
        suspicious_communes = ['paris']
        return commune_lower in suspicious_communes
    
    def _is_suspicious_phone(self, phone: str) -> bool:
        """Détecte téléphones suspects de sites agrégateurs"""
        if not phone:
            return False
        
        # Nettoyer le numéro pour comparaison (enlever espaces, +33, tirets, etc.)
        clean_phone = ''.join(c for c in phone if c.isdigit())
        
        # Normaliser +33 vers 0
        if clean_phone.startswith('33'):
            clean_phone = '0' + clean_phone[2:]
        
        # Numéros typiques des sites agrégateurs
        suspicious_numbers = [
            '0156033456',  # +33 1 56 03 34 56 (Essentiel Autonomie/Malakoff Humanis)
            '0144903456',  # Autres numéros parisiens typiques
        ]
        return clean_phone in suspicious_numbers
    
    def _is_suspicious_email(self, email: str) -> bool:
        """Détecte emails suspects de sites agrégateurs"""
        if not email:
            return False
        
        email_lower = email.lower()
        suspicious_domains = [
            'essentiel-autonomie.com',
            'papyhappy.com',
            'pour-les-personnes-agees.gouv.fr'
        ]
        return any(domain in email_lower for domain in suspicious_domains)

if __name__ == "__main__":
    """Test rapide du module"""
    
    from dataclasses import dataclass
    
    @dataclass
    class TestEstablishment:
        nom: str = "Maison Test"
        commune: str = ""
        gestionnaire: str = ""
        adresse_l1: str = ""
        email: str = ""
        telephone: str = ""
    
    # Test
    enricher = AdaptiveEnricher()
    test_est = TestEstablishment()
    
    missing = enricher._identify_missing_fields(test_est)
    print(f"Champs manquants détectés: {missing}")
    print(f"Enrichissement nécessaire: {len(missing) >= enricher.enrichment_threshold}")
