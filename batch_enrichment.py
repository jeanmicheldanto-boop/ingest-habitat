"""
Service d'enrichissement par lots pour optimiser les performances Groq
Traite plusieurs établissements en une seule requête API
"""

import json
import requests
from typing import List, Dict, Any
import time

class BatchEnrichmentService:
    """Service d'enrichissement par lots optimisé pour Groq"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
    def enrich_batch_groq(self, establishments: List[Dict], batch_size: int = 3) -> List[Dict]:
        """
        Enrichissement par lots via Groq API
        
        Args:
            establishments: Liste des établissements à enrichir
            batch_size: Nombre d'établissements par batch (3-5 recommandé)
        
        Returns:
            Liste des résultats d'enrichissement
        """
        results = []
        
        # Traiter par lots
        for i in range(0, len(establishments), batch_size):
            batch = establishments[i:i + batch_size]
            
            try:
                batch_result = self._process_batch(batch)
                results.extend(batch_result)
                
                # Pause entre les lots
                if i + batch_size < len(establishments):
                    time.sleep(0.2)  # Pause réduite grâce aux lots
                    
            except Exception as e:
                # En cas d'échec du lot, fallback individuel
                print(f"⚠️ Échec batch {i//batch_size + 1}, fallback individuel...")
                for establishment in batch:
                    try:
                        individual_result = self._process_individual(establishment)
                        results.append(individual_result)
                    except Exception as individual_error:
                        results.append({
                            'establishment_index': establishment.get('index'),
                            'nom': establishment.get('nom'),
                            'error': f'Erreur enrichissement: {str(individual_error)}',
                            'batch_failed': True
                        })
                        
        return results
    
    def _process_batch(self, batch: List[Dict]) -> List[Dict]:
        """Traite un lot d'établissements en une requête"""
        
        # Construire le prompt optimisé pour le lot
        batch_info = []
        for idx, etab in enumerate(batch, 1):
            info = f"{idx}. {etab['nom']}, {etab['commune']}"
            if etab.get('site_web'):
                info += f" - {etab['site_web']}"
            batch_info.append(info)
        
        prompt = f"""Analysez ces {len(batch)} établissements seniors/habitat:

{chr(10).join(batch_info)}

Retournez un JSON array avec exactement {len(batch)} objets:
[
  {{"type_public": "personnes_agees|mixtes", "restauration": {{"kitchenette": true/false, "resto_collectif": true/false}}, "tarifs": {{"prix_min": number, "fourchette_prix": "euro|deux_euros|trois_euros"}}, "services": {{"activites_organisees": true/false, "espace_partage": true/false}}, "eligibilite_statut": "avp_eligible|non_eligible|a_verifier"}},
  ...
]

Tarifs: euro<750€, deux_euros=750-1500€, trois_euros>1500€"""

        # Appel API Groq
        api_url = self.config.get('openai_compatible_url', 'https://api.groq.com/openai/v1/chat/completions')
        api_key = self.config.get('openai_compatible_key', '')
        
        if not api_key:
            raise Exception('Clé API Groq requise')
        
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
        
        payload = {
            'model': self.config.get('openai_compatible_model', 'llama-3.3-70b-versatile'),
            'messages': [{'role': 'user', 'content': prompt}],
            'temperature': 0.1,
            'max_tokens': 400 + (len(batch) * 50)  # Tokens adaptés au nombre d'établissements
        }
        
        # Retry avec backoff
        max_retries = 2
        for attempt in range(max_retries):
            try:
                response = requests.post(api_url, headers=headers, json=payload, timeout=60)
                
                if response.status_code == 200:
                    result = response.json()
                    content = result['choices'][0]['message']['content']
                    
                    # Parser le JSON array
                    try:
                        # Extraire le JSON array de la réponse
                        json_start = content.find('[')
                        json_end = content.rfind(']') + 1
                        
                        if json_start >= 0 and json_end > json_start:
                            json_data = json.loads(content[json_start:json_end])
                            
                            # Vérifier que nous avons le bon nombre de résultats
                            if len(json_data) != len(batch):
                                raise ValueError(f"Reçu {len(json_data)} résultats, attendu {len(batch)}")
                            
                            # Associer les résultats aux établissements
                            batch_results = []
                            for idx, (etab, data) in enumerate(zip(batch, json_data)):
                                batch_results.append({
                                    'establishment_index': etab.get('index'),
                                    'nom': etab.get('nom'),
                                    'commune': etab.get('commune'),
                                    'data': data,
                                    'source': 'groq_batch',
                                    'success': True,
                                    'batch_size': len(batch)
                                })
                            
                            return batch_results
                            
                    except (json.JSONDecodeError, ValueError) as e:
                        if attempt == max_retries - 1:
                            raise Exception(f"Erreur parsing JSON batch: {str(e)}")
                        continue
                
                elif response.status_code == 429:
                    wait_time = min(2 ** attempt, 8)
                    time.sleep(wait_time)
                    continue
                    
                else:
                    raise Exception(f"Erreur API Groq: {response.status_code}")
                    
            except requests.exceptions.Timeout:
                if attempt == max_retries - 1:
                    raise Exception(f"Timeout API Groq après {max_retries} essais")
                time.sleep(2 + attempt)
                continue
        
        raise Exception("Échec après tous les retry")
    
    def _process_individual(self, establishment: Dict) -> Dict:
        """Fallback: traitement individuel si le lot échoue"""
        # Utiliser l'ancienne méthode optimisée pour un seul établissement
        from web_enrichment import WebEnrichmentService
        
        service = WebEnrichmentService()
        service.config = self.config
        
        result = service.enrich_with_openai_compatible(
            establishment.get('nom', ''),
            establishment.get('commune', ''),
            establishment.get('site_web')
        )
        
        result['establishment_index'] = establishment.get('index')
        result['fallback_individual'] = True
        
        return result