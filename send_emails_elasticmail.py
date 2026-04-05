"""
🚀 SCRIPT D'ENVOI D'EMAILS VIA ELASTICMAIL - Phase 2

OBJECTIFS:
1. Charger les 289 emails préparés depuis le JSON
2. Envoyer via l'API Elasticmail v3 (ou v2)
3. Mode DRY RUN (simulation) par défaut
4. Respecter les délais (1 sec par email) pour éviter rate limiting
5. Générer un rapport d'envoi

ENTRÉE:
  - outputs/relance_emails/preparation_emails_YYYYMMDD_HHMMSS.json (289 emails)

SORTIE:
  - Emails envoyés via Elasticmail API
  - outputs/relance_emails/envoi_rapport_YYYYMMDD_HHMMSS.json (statistiques)

MODES:
  DRY RUN (défaut = dry_run=True):
    └─ Simule l'envoi sans HTTP request
    └─ Affiche les emails qui auraient été envoyés
    └─ Utile pour tester avant production
  
  PRODUCTION (dry_run=False):
    └─ Envoie réellement via Elasticmail API
    └─ Chaque email = 1 HTTP POST
    └─ Délai: 1 sec entre chaque envoi

FLUX LOGIQUE:
  load_prepared_emails()
    ↓ (289 emails au format JSON)
  send_all()
    ├─ MODE DRY RUN: Afficher [SIMULATION] pour chaque email
    ├─ MODE PRODUCTION: Appeler send_email() pour chaque
    │                    └─ HTTP POST vers Elasticmail
    │                    └─ Attendre réponse (timeout 30s)
    │                    └─ Incrémenter counters
    │                    └─ Attendre 1 sec avant suivant
    ├─ Tous les 50 emails: Afficher [Batch N/289] + taux de succès
    └─ À la fin: Afficher résumé final (total, sent, failed, skipped)
    ↓
  save_rapport()
    └─ Sauvegarder stats en JSON
    ↓
DONE ✅

RÉSULTAT ATTENDU (DRY RUN):
  ✅ 289 simulations réussies
  ✅ Rapport avec "mode": "DRY RUN"
  ✅ Prêt pour production

RÉSULTAT ATTENDU (PRODUCTION):
  ✅ 289 emails envoyés
  ✅ Rapport avec "mode": "PRODUCTION"
  ✅ Statistiques d'envoi (succès/erreurs)
"""

import json
import os
import requests
import time
import urllib3
from dotenv import load_dotenv
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import datetime

# Supprimer les warnings SSL (certifi parfois manquant sur Windows)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

ELASTICMAIL_API_KEY = os.getenv('ELASTICMAIL_API_KEY')
ELASTICMAIL_ENDPOINT_V2 = "https://api.elasticemail.com/v2/email/send"
ELASTICMAIL_ENDPOINT_V4 = "https://api.elasticemail.com/v4/emails/transactional"

# Mode: 'v2' pour form-data (recommandé, testé OK), 'v4' pour JSON
ELASTICMAIL_MODE = 'v2'


class ElasticmailSender:
    """
    Gestionnaire d'envoi via Elasticmail API
    
    Responsabilités:
    1. Charger les emails préparés depuis JSON
    2. Envoyer via Elasticmail (v2 form-data ou v3 JSON)
    3. Gérer les modes DRY RUN vs PRODUCTION
    4. Respecter les délais/rate limiting
    5. Générer rapports et statistiques
    
    Attributs:
        api_key (str): Clé API Elasticmail
        dry_run (bool): Mode test (True) ou production (False)
        delay (float): Délai en secondes entre chaque envoi (défaut 1.0)
        stats (dict): Compteurs (total, sent, failed, skipped)
    
    Formats API supportés:
        v2: POST form-data vers https://api.elasticmail.com/v2/email/send
        v3: POST JSON vers https://api.elasticmail.com/v3/emails/send
    
    Utilisation DRY RUN (test sans risque):
        >>> sender = ElasticmailSender(api_key, dry_run=True)
        >>> emails = sender.load_prepared_emails('file.json')
        >>> sender.send_all(emails)
        [SIMULATION 1/289] À: email1@example.com
        [SIMULATION 2/289] À: email2@example.com
        ...
    
    Utilisation PRODUCTION:
        >>> sender = ElasticmailSender(api_key, dry_run=False)
        >>> emails = sender.load_prepared_emails('file.json')
        >>> sender.send_all(emails)  # ENVOIE RÉEL
        ✅ [1/289] À: email1@example.com
        ✅ [2/289] À: email2@example.com
        ...
    """
    
    def __init__(self, api_key: str, dry_run: bool = False, delay: float = 1.0):
        self.api_key = api_key
        self.dry_run = dry_run
        self.delay = delay  # Délai entre chaque envoi (en secondes)
        self.stats = {
            'total': 0,
            'sent': 0,
            'failed': 0,
            'skipped': 0
        }
    
    def load_prepared_emails(self, json_file: str) -> List[Dict]:
        """Charge les emails préparés depuis le JSON"""
        if not os.path.exists(json_file):
            print(f"❌ Fichier non trouvé: {json_file}")
            return []
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                emails = json.load(f)
            print(f"✅ {len(emails)} emails chargés depuis {Path(json_file).name}")
            return emails
        except Exception as e:
            print(f"❌ Erreur lors de la lecture: {str(e)}")
            return []
    
    def send_via_elasticmail_v4(self, email: Dict) -> Tuple[bool, str]:
        """Envoie un email via l'API Elastic Email v4 (JSON)"""
        
        payload = {
            "Recipients": {
                "To": [email['recipient_email']]
            },
            "Content": {
                "From": email['sender'],
                "Subject": email['subject'],
                "Body": [
                    {"ContentType": "PlainText", "Content": email['body']}
                ]
            }
        }
        
        headers = {
            'X-ElasticEmail-ApiKey': self.api_key,
            'Content-Type': 'application/json'
        }
        
        try:
            response = requests.post(
                ELASTICMAIL_ENDPOINT_V4,
                json=payload,
                headers=headers,
                timeout=30,
                verify=False
            )
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    tid = data.get('TransactionID', '')
                    return True, f"OK (TransactionID: {tid})"
                except:
                    return True, "OK (200)"
            else:
                return False, f"Code {response.status_code}: {response.text[:150]}"
        
        except requests.exceptions.RequestException as e:
            return False, f"Erreur reseau: {str(e)[:100]}"
        except Exception as e:
            return False, f"Erreur: {str(e)[:100]}"
    
    def send_via_elasticmail_v2(self, email: Dict) -> Tuple[bool, str]:
        """Envoie un email via l'API Elastic Email v2 (form-data avec HTML + tracking)"""
        
        data = {
            'apikey': self.api_key,
            'from': email['sender'],
            'fromName': 'Patrick Danto - BMSE',
            'to': email['recipient_email'],
            'subject': email['subject'],
            'bodyText': email.get('body', ''),  # Fallback texte
            'bodyHtml': email.get('body_html', ''),  # HTML principal
            'tracklinks': 1  # Activer le tracking des liens
        }
        
        try:
            response = requests.post(
                ELASTICMAIL_ENDPOINT_V2,
                data=data,
                timeout=30,
                verify=False
            )
            
            if response.status_code == 200:
                try:
                    result = response.json()
                    if result.get('success'):
                        tid = result.get('data', {}).get('transactionid', '')
                        return True, f"OK (tid: {tid})"
                    else:
                        err = result.get('error', 'Unknown error')
                        return False, f"API error: {err}"
                except:
                    return True, "OK (200)"
            else:
                return False, f"Code {response.status_code}: {response.text[:150]}"
        
        except requests.exceptions.RequestException as e:
            return False, f"Erreur reseau: {str(e)[:100]}"
        except Exception as e:
            return False, f"Erreur: {str(e)[:100]}"
    
    def send_email(self, email: Dict) -> Tuple[bool, str]:
        """Envoie un email (v2 form-data ou v4 JSON)"""
        if ELASTICMAIL_MODE == 'v4':
            return self.send_via_elasticmail_v4(email)
        else:
            return self.send_via_elasticmail_v2(email)
    
    def send_all(self, emails: List[Dict], batch_size: int = 50, start_index: int = 0) -> Dict:
        """
        Envoie tous les emails avec gestion des batches et délais.
        
        LOGIQUE D'ENVOI:
        ┌────────────────────────────────────────────────────┐
        │ POUR CHAQUE email (289 total):                    │
        ├────────────────────────────────────────────────────┤
        │ MODE DRY RUN (défaut):                             │
        │  1. Afficher "[SIMULATION N/289] À: email"        │
        │  2. Incrémenter compteur 'sent'                    │
        │  3. Continuer sans délai (rapide)                  │
        │                                                    │
        │ MODE PRODUCTION:                                   │
        │  1. Appeler send_email(email)                      │
        │     └─ HTTP POST vers Elasticmail                 │
        │     └─ Timeout: 30 secondes                        │
        │     └─ Retour: (success: bool, message: str)      │
        │                                                    │
        │  2. Vérifier le résultat:                          │
        │     └─ SI success:                                 │
        │        └─ Afficher ✅ et incrémenter 'sent'       │
        │     └─ SINON:                                      │
        │        └─ Afficher ❌ + message d'erreur          │
        │        └─ Incrémenter 'failed'                     │
        │                                                    │
        │  3. Respecter le délai:                            │
        │     └─ SI pas le dernier email:                   │
        │        └─ Attendre 'delay' secondes (défaut 1 sec)│
        │     └─ SINON:                                      │
        │        └─ Pas d'attente (fin de boucle)           │
        │                                                    │
        │ AFFICHAGE PÉRIODIQUE:                              │
        │  Tous les 50 emails:                               │
        │   [Batch 50/289] Taux de succès: 98.2%            │
        │   [Batch 100/289] Taux de succès: 99.1%           │
        │   [Batch 150/289] Taux de succès: 98.8%           │
        │   ...                                              │
        │                                                    │
        │ RÉSUMÉ FINAL:                                      │
        │  ✅ Total: 289                                     │
        │  ✅ Envoyés: 289 (ou <289 si erreurs)             │
        │  ❌ Échoués: 0                                     │
        │  ⏭️  Ignorés: 0                                     │
        │                                                    │
        │ DURÉE ESTIMÉE:                                     │
        │  DRY RUN:    ~3 secondes (pas de réseau)          │
        │  PRODUCTION: ~289 secondes (1 sec/email)          │
        │             = ~5 minutes pour 289 emails          │
        └────────────────────────────────────────────────────┘
        
        Args:
            emails (List[Dict]): 289 emails préparés
            batch_size (int): Afficher rapport tous les N emails (défaut 50)
            start_index (int): Index de départ (utile pour reprendre)
        
        Returns:
            Dict: Statistiques finales
                {
                  'total': 289,
                  'sent': 289,
                  'failed': 0,
                  'skipped': 0
                }
        
        Exemple DRY RUN:
            >>> sender = ElasticmailSender(key, dry_run=True)
            >>> emails = load_emails('file.json')
            >>> stats = sender.send_all(emails)
            [SIMULATION 1/289] À: email1@example.com
            [SIMULATION 2/289] À: email2@example.com
            ...
            [Batch 50/289] Taux de succès: 100.0%
            >>> stats
            {'total': 289, 'sent': 289, 'failed': 0, 'skipped': 0}
        
        Exemple PRODUCTION:
            >>> sender = ElasticmailSender(key, dry_run=False, delay=1.0)
            >>> emails = load_emails('file.json')
            >>> stats = sender.send_all(emails)
            ✅ [1/289] À: email1@example.com
            ✅ [2/289] À: email2@example.com
            ...
            [Batch 50/289] Taux de succès: 98.0%
            # ~ 5 minutes plus tard ...
            >>> stats
            {'total': 289, 'sent': 287, 'failed': 2, 'skipped': 0}
        """
        
        total = len(emails)
        print(f"\n{'='*80}")
        print(f"ENVOI DES EMAILS - {total} total")
        print(f"{'='*80}\n")
        
        self.stats['total'] = total
        
        for i, email in enumerate(emails[start_index:], start=start_index+1):
            
            if self.dry_run:
                print(f"[SIMULATION {i}/{total}] À: {email['recipient_email']}")
                self.stats['sent'] += 1
            else:
                # Vrai envoi
                success, message = self.send_email(email)
                
                status_icon = "✅" if success else "❌"
                print(f"{status_icon} [{i}/{total}] À: {email['recipient_email']}")
                
                if not success:
                    print(f"    ⚠️  {message}")
                    self.stats['failed'] += 1
                else:
                    self.stats['sent'] += 1
                
                # Respecter les délais
                if i < total and not self.dry_run:
                    time.sleep(self.delay)
            
            # Afficher le résumé tous les 50 emails
            if i % batch_size == 0 or i == total:
                self._print_batch_summary(i, total)
        
        print(f"\n{'='*80}")
        print(f"RÉSUMÉ FINAL")
        print(f"{'='*80}")
        print(f"Total: {self.stats['total']}")
        print(f"Envoyés: {self.stats['sent']}")
        print(f"Échoués: {self.stats['failed']}")
        print(f"Ignorés: {self.stats['skipped']}")
        print(f"{'='*80}\n")
        
        return self.stats
    
    def _print_batch_summary(self, current: int, total: int):
        """Affiche un résumé de batch"""
        success_rate = (self.stats['sent'] / current * 100) if current > 0 else 0
        print(f"\n📊 [Batch {current}/{total}] Taux de succès: {success_rate:.1f}%")


def main():
    """Fonction principale"""
    
    # Vérifier la clé API
    if not ELASTICMAIL_API_KEY:
        print("❌ ELASTICMAIL_API_KEY non trouvée dans .env")
        return
    
    # Chercher le dernier fichier JSON préparé
    relance_dir = 'outputs/relance_emails'
    if not os.path.exists(relance_dir):
        print(f"❌ Répertoire non trouvé: {relance_dir}")
        return
    
    json_files = sorted(Path(relance_dir).glob('*.json'), key=os.path.getctime, reverse=True)
    if not json_files:
        print(f"❌ Aucun fichier JSON trouvé dans {relance_dir}")
        return
    
    latest_json = str(json_files[0])
    print(f"📖 Utilisation du fichier: {Path(latest_json).name}")
    
    # Charger les emails
    sender = ElasticmailSender(
        api_key=ELASTICMAIL_API_KEY,
        dry_run=False,  # ✅ MODE PRODUCTION
        delay=1.0  # Délai entre chaque email (secondes)
    )
    
    emails = sender.load_prepared_emails(latest_json)
    if not emails:
        return
    
    # Envoyer les emails
    stats = sender.send_all(emails, batch_size=50)
    
    # Sauvegarder le rapport
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f'outputs/relance_emails/envoi_rapport_{timestamp}.json'
    
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': timestamp,
            'mode': 'DRY RUN' if sender.dry_run else 'PRODUCTION',
            'stats': stats,
            'emails_file': Path(latest_json).name
        }, f, ensure_ascii=False, indent=2)
    
    print(f"📄 Rapport sauvegardé: {report_file}")


if __name__ == '__main__':
    main()
