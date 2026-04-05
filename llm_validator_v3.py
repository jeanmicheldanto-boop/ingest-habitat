#!/usr/bin/env python3
"""
LLM Validator V3 avec support OpenAI et Groq
"""

import json
import os
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from dotenv import load_dotenv

# Charger variables d'environnement
load_dotenv()

@dataclass
class ExtractedEstablishmentV2:
    """Structure établissement extrait Module 4 V2"""
    nom: str
    commune: str
    code_postal: str
    gestionnaire: str
    adresse_l1: str
    telephone: str
    email: str
    site_web: str
    sous_categories: str
    public_cible: str
    presentation: str
    habitat_type: str
    eligibilite_avp: str
    departement: str
    source: str
    date_extraction: str
    confidence_score: float
    validation_timestamp: str

class LLMValidatorV3:
    """Validator avec support OpenAI et Groq"""
    
    def __init__(self, provider="openai"):
        self.provider = provider.lower()
        self.total_cost = 0.0
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.validation_logs = []
        
        # Configuration modèles
        if self.provider == "openai":
            self.model = "gpt-4o-mini"
            self.pricing = {
                "input": 0.15,   # $0.15 per 1M tokens
                "output": 0.60   # $0.60 per 1M tokens  
            }
            self._setup_openai()
        elif self.provider == "groq":
            self.model = "llama-3.3-70b-versatile"
            self.pricing = {
                "light": {
                    "input": 0.59,   # $0.59 per 1M tokens
                    "output": 0.79   # $0.79 per 1M tokens
                }
            }
            self._setup_groq()
        
        # Sous-catégories valides
        self.sous_categories_valides = [
            "Béguinage", "Village seniors", "Colocation avec services",
            "Habitat inclusif", "Accueil familial", "Maison d'accueil familial",
            "Habitat intergénérationnel", "Habitat regroupé"
        ]
    
    def _setup_openai(self):
        """Configuration OpenAI"""
        try:
            import openai
            self.openai_client = openai.Client(api_key=os.getenv('OPENAI_API_KEY'))
            print(f"🤖 LLM Validator V3 configuré avec OpenAI ({self.model})")
        except ImportError:
            raise ImportError("pip install openai requis pour provider='openai'")
    
    def _setup_groq(self):
        """Configuration Groq"""
        self.groq_api_key = os.getenv('GROQ_API_KEY')
        if not self.groq_api_key:
            raise ValueError("GROQ_API_KEY manquant dans .env")
        print(f"🤖 LLM Validator V3 configuré avec Groq ({self.model})")
    
    def _call_llm(self, prompt: str, max_tokens: int = 500) -> Optional[Dict]:
        """Appel LLM unifié"""
        if self.provider == "openai":
            return self._call_openai(prompt, max_tokens)
        elif self.provider == "groq":
            return self._call_groq(prompt, max_tokens)
    
    def _call_openai(self, prompt: str, max_tokens: int = 500) -> Optional[Dict]:
        """Appel OpenAI"""
        try:
            response = self.openai_client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=max_tokens
            )
            
            content = response.choices[0].message.content
            input_tokens = response.usage.prompt_tokens
            output_tokens = response.usage.completion_tokens
            cost = (input_tokens * self.pricing["input"] + output_tokens * self.pricing["output"]) / 1_000_000
            
            # Parse JSON
            try:
                cleaned_content = self._clean_json_response(content)
                parsed = json.loads(cleaned_content)
                parsed.update({
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "cost": cost
                })
                return parsed
            except json.JSONDecodeError as e:
                print(f"❌ OpenAI JSON error: {e}")
                print(f"📋 Content: '{content[:200]}...'")
                return None
                
        except Exception as e:
            print(f"❌ OpenAI error: {e}")
            return None
    
    def _call_groq(self, prompt: str, max_tokens: int = 500) -> Optional[Dict]:
        """Appel Groq (existant)"""
        headers = {
            "Authorization": f"Bearer {self.groq_api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.0,
            "max_tokens": max_tokens
        }
        
        try:
            response = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers=headers,
                json=data,
                timeout=30
            )
            
            if response.status_code == 200:
                response_data = response.json()
                content = response_data["choices"][0]["message"]["content"]
                
                usage = response_data.get("usage", {})
                input_tokens = usage.get("prompt_tokens", 0)
                output_tokens = usage.get("completion_tokens", 0)
                cost = (input_tokens * self.pricing["light"]["input"] + 
                       output_tokens * self.pricing["light"]["output"]) / 1_000_000
                
                try:
                    cleaned_content = self._clean_json_response(content)
                    parsed = json.loads(cleaned_content)
                    parsed.update({
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                        "cost": cost
                    })
                    return parsed
                except json.JSONDecodeError as e:
                    print(f"❌ Groq JSON error: {e}")
                    return None
            else:
                print(f"❌ Groq API error: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Groq error: {e}")
            return None
    
    def _clean_json_response(self, content: str) -> str:
        """Nettoyage JSON robuste"""
        import re
        
        if not content:
            return ""
        
        # Supprime les marqueurs markdown  
        content = re.sub(r'```json\s*', '', content)
        content = re.sub(r'```\s*', '', content)
        
        # Supprime les phrases d'introduction courantes SEULEMENT si elles existent
        intro_patterns = [
            r'Voici les établissements.*?format CSV.*?:',
            r'Voici les établissements.*?extraits.*?:',
            r'Établissements.*?extraits.*?:',
            r'Les établissements.*?sont.*?:',
            r'Voici.*?JSON.*?:',
            r'Résultats?.*?format.*?:'
        ]
        for pattern in intro_patterns:
            if re.search(pattern, content, flags=re.IGNORECASE | re.DOTALL):
                content = re.sub(pattern, '', content, flags=re.IGNORECASE | re.DOTALL)
                break
        
        # Trouve la première accolade ouvrante
        start_idx = content.find('{')
        if start_idx == -1:
            return ""
        
        # Trouve la dernière accolade fermante
        end_idx = content.rfind('}')
        if end_idx == -1 or end_idx <= start_idx:
            return ""
        
        # Extrait le JSON entre les accolades
        json_content = content[start_idx:end_idx+1]
        
        # Supprime tous types de commentaires
        json_content = re.sub(r'//.*?(?=\n|$)', '', json_content)
        json_content = re.sub(r'/\*.*?\*/', '', json_content, flags=re.DOTALL)
        
        # Nettoie les caractères de contrôle invalides
        json_content = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', json_content)
        
        # Corrige les valeurs "null" en null JSON
        json_content = re.sub(r'"null"', 'null', json_content)
        json_content = re.sub(r'"None"', 'null', json_content)
        
        # Nettoie les lignes vides multiples mais préserve la structure
        json_content = re.sub(r'\n\s*\n+', '\n', json_content)
        
        # Validation JSON avant retour
        try:
            json.loads(json_content.strip())
        except json.JSONDecodeError:
            json_content = content[start_idx:end_idx+1]
        
        return json_content.strip()
    
    def _build_extraction_csv_prompt(self, candidate, department: str) -> str:
        """Prompt d'extraction optimisé"""
        categories_str = ", ".join(self.sous_categories_valides)
        
        return f"""Extrait TOUS les établissements d'habitat seniors de cette source au format CSV EXACT:

DÉPARTEMENT CIBLE: {department}
URL: {candidate.url}
TITRE: {candidate.nom}
CONTENU: {candidate.snippet}

FORMAT CSV OBLIGATOIRE (16 colonnes exactes):
{{
  "establishments": [
    {{
      "nom": "Nom exact établissement",
      "commune": "Ville seule (sans CP)",
      "code_postal": "44000",
      "gestionnaire": "CCAS/Commune/Association/Groupe (libellé court)",
      "adresse_l1": "Numéro + voie (sans ville)",
      "telephone": "02 XX XX XX XX ou null",
      "email": "contact@etablissement.fr ou null (uniquement si publique)",
      "site_web": "URL page spécifique",
      "sous_categories": "UNE valeur exacte parmi: {categories_str}",
      "public_cible": "personnes_agees ou personnes_handicapees ou mixtes (+ alzheimer_accessible si mentionné)",
      "presentation": "Description neutre 300-500 caractères, factuelle, sans extrapolation",
      "confidence_score": 95
    }}
  ],
  "confidence": 90
}}

RÈGLES CRITIQUES:
- Réponds UNIQUEMENT en JSON valide (pas de texte avant/après)
- sous_categories: EXACTEMENT une valeur de la liste
- email/telephone: seulement si visible publiquement, sinon null
- presentation: neutre, factuel, max 500 caractères
- NE RIEN INVENTER - laisser null si info absente
- JAMAIS de commentaires // dans le JSON

Si aucun établissement: {{"establishments": [], "confidence": 0}}"""