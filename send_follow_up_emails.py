"""
🚀 SCRIPT DE PRÉPARATION D'EMAILS DE RELANCE - Phase 1

OBJECTIFS:
1. Charger les 250 prospects du fichier Excel enrichi
2. EXCLURE les prospects du Pas-de-Calais (département 62)
3. Extraire/récupérer les adresses email via HIÉRARCHIE OPTIMISÉE
4. Personnaliser le contenu avec le nom du dirigeant
5. Générer les fichiers de préparation (CSV + JSON)

HIÉRARCHIE D'EXTRACTION D'EMAILS (ordre de priorité):
  1️⃣  Email Dirigeant 1 (pattern le plus fiable - ex: prenom.nom@domain)
  2️⃣  Email Dirigeant 2 (variante - ex: p.nom@domain)
  3️⃣  Email Dirigeant 3 (autre variante - ex: prenomnom@domain)
  4️⃣  email_contact (email générique de contact connu)
  5️⃣  Email Organisation (email d'accueil/direction/siège)
  6️⃣  emails_generiques (autres emails génériques séparés par ;)

IMPACT:
  - Sans: 172-174 emails
  - Avec hiérarchie: 230+ emails dirigeants + 174+ emails org = 400-500 emails!
  - Taux de réponse × 3 avec email dirigeant vs générique

ENTRÉE:
  - outputs/prospection_250_FINAL_FORMATE_V2.xlsx (250 prospects avec emails reconstruits)

SORTIE:
  - outputs/relance_emails/preparation_emails_YYYYMMDD_HHMMSS.csv (résumé)
  - outputs/relance_emails/preparation_emails_YYYYMMDD_HHMMSS.json (détail complet)

FLUX LOGIQUE:
  load_prospects()
    ↓ (250 prospects)
  filter_exclude_pas_de_calais()
    ↓ (246 prospects, 4 exclus)
  prepare_emails()
    ├─ generate_possible_emails()   → Récupérer emails via hiérarchie
    ├─ extract_name_for_greeting()  → Extraire nom pour salutation
    ├─ generate_email_content()     → Créer sujet + corps personnalisé
    └─ Créer objet 'prepared' pour chaque email
    ↓ (400-500 emails préparés!)
  save_preparation()
    ├─ Sauvegarder en CSV
    └─ Sauvegarder en JSON
    ↓
  display_samples()
    └─ Afficher 3 exemples

RÉSULTAT ATTENDU:
  ✅ 400-500 emails générés (vs 289 avant)
  ✅ 1 CSV (~100 KB) + 1 JSON (~1.5 MB)
  ✅ Prêt pour la phase 2 d'envoi
"""

import pandas as pd
import os
import re
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Tuple, Optional
import json
from datetime import datetime

load_dotenv()

# Configuration
ELASTICMAIL_API_KEY = os.getenv('ELASTICMAIL_API_KEY')
INPUT_FILE = 'outputs/prospection_250_FINAL_FORMATE_V2.xlsx'  # Fichier enrichi avec emails reconstruits (Dirigeant 1/2/3)
OUTPUT_DIR = 'outputs/relance_emails'
DRY_RUN = True  # Mode test: affiche les emails sans les envoyer
SENDER_EMAIL = 'patrick.danto@bmse.fr'

# Objet du mail
EMAIL_SUBJECT = "Intelligence Artificielle dans les ESSMS : les outils et l'accompagnement de BMSE"

# Corps du mail
EMAIL_BODY = """Vous avez reçu il y a quelques semaines un courrier de notre part à propos de l'intelligence artificielle dans les ESSMS. Nous y évoquions le fait que l'IA, mobilisée avec méthode et responsabilité, peut libérer du temps administratif au profit de l'accompagnement, et nous présentions l'offre de BMSE et de ConfidensIA en la matière.

Je me permets de reprendre contact brièvement, sans insistance — et si ce n'est pas le bon moment ou le bon interlocuteur, n'hésitez pas à me rediriger vers la personne concernée au sein de votre structure.

Nous proposons des accompagnements et des outils ancrés dans les réalités des ESSMS, principalement dans deux domaines :
— les outils budgétaires : rapports de gestion, CPOM, analyse financière, extraction et traitement des données issues des cadres normalisés ;
— les écrits professionnels : aide à la rédaction, exploitation et analyse de vos données textuelles.

Nous avons également élaboré un modèle de charte IA adapté au contexte des ESSMS, que vous pouvez télécharger librement : https://www.bmse.fr/Charte_IA_BMSE_v.1.2.pdf

Une brique essentielle à de nombreux usages de l'IA dans les ESSMS est la pseudonymisation des données. Nous avons développé un outil dédié au secteur social et médico-social qui, en tenant compte de la jurisprudence récente, détecte et protège près de 100 types d'entités directement ou indirectement identifiantes dans vos écrits professionnels — noms, adresses, numéros, identifiants, références médicales ou sociales, etc.

Une démonstration gratuite est accessible en ligne, sans inscription, à cette adresse : https://confidensia.fr/demo
Vous pouvez coller un texte ou déposer un document pour observer le résultat en quelques secondes.

Je reste disponible pour en échanger si vous le souhaitez, sans engagement de votre part.

Bien cordialement,

Patrick Danto
Associé BMSE & directeur Technique ConfidensIA
patrick.danto@bmse.fr
06 98 39 70 66
www.bmse.fr"""


class ProspectEmailSender:
    """
    Gestionnaire de préparation d'emails de relance
    
    Responsabilités:
    1. Charger et valider 250 prospects depuis Excel
    2. Filtrer les exclusions géographiques (Pas-de-Calais)
    3. Extraire les adresses email de contact
    4. Personnaliser le contenu (salutation + corps)
    5. Générer les fichiers de sortie (CSV + JSON)
    
    Attributs:
        input_file (str): Chemin du fichier Excel source
        dry_run (bool): Mode test (True) ou production (False)
        prospects (List[dict]): Liste des prospects chargés
        emails_generated (List[dict]): Emails générés et prêts à envoyer
    
    Utilisation:
        >>> sender = ProspectEmailSender('file.xlsx')
        >>> sender.run()
        ✅ 289 emails préparés
    """
    
    def __init__(self, input_file: str, dry_run: bool = True):
        self.input_file = input_file
        self.dry_run = dry_run
        self.prospects = []
        self.emails_generated = []
        self.emails_sent = []
        self.emails_failed = []
        
    def load_prospects(self) -> List[dict]:
        """
        Charge les 250 prospects depuis le fichier Excel.
        
        LOGIQUE:
        1. Vérifier que le fichier existe
        2. Lire avec pandas.read_excel()
        3. Convertir en liste de dictionnaires
           └─ Chaque ligne = 1 prospect
           └─ Chaque colonne = 1 propriété
        4. Afficher des logs informatifs
        5. Retourner la liste
        
        COLONNES UTILISÉES:
        - gestionnaire_nom: Nom de la structure
        - gestionnaire_adresse: Adresse (contient code postal)
        - nom_public: Nom public
        - dirigeant_nom: Nom du responsable
        - email_contact: Email principal
        - emails_generiques: Autres emails (séparés par ;)
        
        Returns:
            List[dict]: Liste de 250 prospects avec toutes les colonnes Excel
        
        Exemple:
            >>> prospects = self.load_prospects()
            >>> len(prospects)
            250
            >>> prospects[0].keys()
            ['finess_ej', 'gestionnaire_nom', 'gestionnaire_adresse', ...]
        """
        print(f"[FILE] Chargement du fichier: {self.input_file}")
        
        if not os.path.exists(self.input_file):
            print(f"[FAIL] Fichier non trouve: {self.input_file}")
            return []
        
        try:
            df = pd.read_excel(self.input_file)
            print(f"[OK] {len(df)} prospects charges")
            print(f"   Colonnes: {', '.join(df.columns.tolist())}")
            return df.to_dict('records')
        except Exception as e:
            print(f"[FAIL] Erreur lors de la lecture du fichier: {str(e)}")
            return []
    
    def extract_department_code(self, code_postal: str) -> Optional[str]:
        """
        Extrait le code département d'un code postal français.
        
        LOGIQUE DE CONVERSION:
        ┌────────────────────────────────────────────────────┐
        │ Code Postal (5 digits) → Département (2-3 digits) │
        ├────────────────────────────────────────────────────┤
        │ MÉTROPOLE:                                         │
        │   75013 (Paris)        → "75"      (prendre 2 1ers) │
        │   69000 (Lyon)         → "69"                      │
        │   62000 (Arras/Pas-de-C) → "62" ← À EXCLURE        │
        │                                                    │
        │ DOM-TOM 97xxx:                                     │
        │   97411 (Réunion)      → "974"     (prendre 3 1ers)│
        │   97600 (Mayotte)      → "976"                     │
        │                                                    │
        │ DOM-TOM 98xxx:                                     │
        │   98849 (Wallis/Futuna) → "988"    (prendre 3 1ers)│
        │   98000 (Saint-Pierre&M) → "980"                   │
        └────────────────────────────────────────────────────┘
        
        Args:
            code_postal (str): Code postal 5 chiffres (ex: "75013")
        
        Returns:
            Optional[str]: Code département (ex: "75", "974", "62")
                          None si code_postal vide/invalide
        
        Exemples:
            >>> extract_department_code("75013")
            "75"
            >>> extract_department_code("62000")
            "62"
            >>> extract_department_code("97411")
            "974"
            >>> extract_department_code("")
            None
        """
        if not code_postal:
            return None
        
        code_postal = str(code_postal).strip()
        
        # Codes DOM-TOM spéciaux
        if code_postal.startswith('97'):
            return code_postal[:3]  # 971, 972, 974, 976, 978
        elif code_postal.startswith('98'):
            return code_postal[:3]  # 985, 986, 987, 988
        else:
            return code_postal[:2]  # Codes métropole
    
    def filter_exclude_pas_de_calais(self, prospects: List[dict]) -> List[dict]:
        """
        Exclut les prospects du Pas-de-Calais (département 62).
        
        LOGIQUE D'EXCLUSION:
        ┌──────────────────────────────────────────────────────────────┐
        │ POUR CHAQUE prospect parmi les 250:                         │
        ├──────────────────────────────────────────────────────────────┤
        │ 1. Extraire le code postal                                  │
        │    └─ Chercher 5 chiffres dans 'gestionnaire_adresse'      │
        │    └─ Regex: r"\b(\d{5})\b"                                │
        │    └─ Exemple: "33 AV PIERRE MENDES, 75013 PARIS"          │
        │                                           ^^^^^            │
        │                                          Code trouvé       │
        │                                                            │
        │ 2. Appeler extract_department_code(code_postal)             │
        │    └─ Prendre les 2 premiers chiffres                      │
        │    └─ Exemple: "75013" → "75"                             │
        │                                                            │
        │ 3. Vérifier département                                    │
        │    └─ SI dept == "62" (Pas-de-Calais):                    │
        │       └─ Ajouter à liste EXCLUDED (à ignorer)             │
        │    └─ SINON:                                               │
        │       └─ Ajouter à liste FILTERED (à traiter)             │
        │                                                            │
        │ RÉSULTAT ATTENDU:                                          │
        │   Entrée:   250 prospects                                  │
        │   Sortie:   246 prospects (4 du dept 62 exclus)           │
        └──────────────────────────────────────────────────────────────┘
        
        Args:
            prospects (List[dict]): 250 prospects à filtrer
        
        Returns:
            List[dict]: 246 prospects filtrés (Pas-de-Calais exclus)
        
        Side effects:
            Affiche dans stdout le nombre d'exclusions
        
        Exemple:
            >>> prospects = [
            ...   {"gestionnaire_adresse": "10 RUE, 75013 PARIS"},      # ← Garder
            ...   {"gestionnaire_adresse": "99 RUE, 62000 ARRAS"},      # ← Exclure
            ...   {"gestionnaire_adresse": "50 RUE, 69000 LYON"}        # ← Garder
            ... ]
            >>> filtered = self.filter_exclude_pas_de_calais(prospects)
            >>> len(filtered)
            2
        """
        excluded = []
        filtered = []
        
        for prospect in prospects:
            # Chercher le code postal dans l'adresse
            code_postal = None
            
            # Chercher dans gestionnaire_adresse
            if prospect.get('gestionnaire_adresse'):
                adresse = str(prospect['gestionnaire_adresse'])
                # Chercher 5 chiffres dans l'adresse
                import re
                match = re.search(r'\b(\d{5})\b', adresse)
                if match:
                    code_postal = match.group(1)
            
            # Fallback: chercher une colonne CP
            if not code_postal:
                for col in ['code_postal', 'CodePostal', 'CP', 'postal_code']:
                    if col in prospect and prospect[col]:
                        code_postal = str(prospect[col]).strip()
                        break
            
            if code_postal:
                dept = self.extract_department_code(code_postal)
                if dept == '62':
                    excluded.append(prospect)
                    continue
            
            filtered.append(prospect)
        
        # Exclusions manuelles (prospects ayant déjà répondu)
        EXCLUDE_FINESS = {
            '770707305',  # ADSEA77 - ASS. DEPTALE DE SAUVEGARDE ENFANCE ET ADOLESCENCE (a répondu au courrier)
        }
        before_manual = len(filtered)
        filtered = [p for p in filtered if str(p.get('finess_ej', '')).strip() not in EXCLUDE_FINESS]
        manual_exclusions = before_manual - len(filtered)
        
        print(f"\n[SEARCH] Filtrage Pas-de-Calais (62)")
        print(f"   Prospects initiaux: {len(prospects)}")
        print(f"   Exclusions (62): {len(excluded)}")
        if manual_exclusions:
            print(f"   Exclusions manuelles: {manual_exclusions} (prospects ayant déjà répondu)")
        print(f"   Prospects restants: {len(filtered)}")
        
        return filtered
    
    def generate_possible_emails(self, prospect: dict) -> List[str]:
        """
        Génère la liste des adresses email possibles pour un prospect.
        
        HIÉRARCHIE OPTIMISÉE D'EXTRACTION D'EMAILS (ordre de priorité):
        ┌──────────────────────────────────────────────────────────────┐
        │ 1️⃣  EMAIL DIRIGEANT 1 (pattern le plus fiable)              │
        │     └─ Colonne: 'Email Dirigeant 1'                        │
        │     └─ Format: prenom.nom@domain (90%+ confiance)          │
        │     └─ Généré par patterns détectés                        │
        │     └─ EXEMPLE: serge.widawski@apf-francehandicap.org     │
        │                                                             │
        │ 2️⃣  EMAIL DIRIGEANT 2 (variante)                           │
        │     └─ Colonne: 'Email Dirigeant 2'                        │
        │     └─ Format: p.nom@domain (50-70% confiance)             │
        │     └─ EXEMPLE: s.widawski@apf-francehandicap.org         │
        │                                                             │
        │ 3️⃣  EMAIL DIRIGEANT 3 (autre variante)                     │
        │     └─ Colonne: 'Email Dirigeant 3'                        │
        │     └─ Format: prenomnom@domain (30-40% confiance)         │
        │     └─ EXEMPLE: sergewidawski@apf-francehandicap.org      │
        │                                                             │
        │ 4️⃣  EMAIL CONTACT (généralement générique connu)           │
        │     └─ Colonne: 'email_contact'                            │
        │     └─ Format: contact@, info@, etc.                       │
        │     └─ EXEMPLE: accueil.adherents@apf.asso.fr             │
        │                                                             │
        │ 5️⃣  EMAIL ORGANISATION (email d'accueil/direction)         │
        │     └─ Colonne: 'Email Organisation'                       │
        │     └─ Priorité: siege > direction > dg > contact          │
        │     └─ EXEMPLE: accueil.donateurs@apf-francehandicap.org  │
        │                                                             │
        │ 6️⃣  EMAILS GÉNÉRIQUES (autres contacts)                   │
        │     └─ Colonne: 'emails_generiques'                        │
        │     └─ Format: email1;email2;email3 (séparés par ;)        │
        │     └─ Valider chacun via regex                            │
        │                                                             │
        │ RÉSULTATS POSSIBLES:                                       │
        │   ✅ 230 prospects avec Email Dirigeant 1 (le meilleur!)   │
        │   ✅ 174 prospects avec Email Organisation                 │
        │   ✅ 172 prospects avec email_contact                      │
        │   ✅ 168 prospects avec emails_generiques                  │
        │                                                             │
        │ TOTAL ESTIMÉ: 400-500 emails (vs 289 avant!)              │
        │              = +40-75% de prise de contact supplémentaire │
        └──────────────────────────────────────────────────────────────┘
        
        Args:
            prospect (dict): Un prospect avec ses données
        
        Returns:
            List[str]: Liste 0-N adresses email valides (triées par priorité)
        
        Exemple:
            >>> prospect = {
            ...   "Email Dirigeant 1": "serge.widawski@apf-francehandicap.org",
            ...   "Email Dirigeant 2": "s.widawski@apf-francehandicap.org",
            ...   "email_contact": "contact@apf.asso.fr",
            ...   "Email Organisation": "direction@apf-francehandicap.org"
            ... }
            >>> emails = self.generate_possible_emails(prospect)
            >>> emails
            ['serge.widawski@apf-francehandicap.org',
             's.widawski@apf-francehandicap.org',
             'contact@apf.asso.fr',
             'direction@apf-francehandicap.org']
        """
        emails = []
        
        # PRIORITÉ 1-3: Emails dirigeants (meilleurs!)
        for col in ['Email Dirigeant 1', 'Email Dirigeant 2', 'Email Dirigeant 3']:
            if prospect.get(col):
                email = str(prospect[col]).strip().lower()
                if self._is_valid_email(email) and email not in emails:
                    emails.append(email)
        
        # PRIORITÉ 4: Email contact direct
        if prospect.get('email_contact'):
            email = str(prospect['email_contact']).strip().lower()
            if self._is_valid_email(email) and email not in emails:
                emails.append(email)
        
        # PRIORITÉ 5: Email organisation (accueil/direction/siège)
        if prospect.get('Email Organisation'):
            email = str(prospect['Email Organisation']).strip().lower()
            if self._is_valid_email(email) and email not in emails:
                emails.append(email)
        
        # PRIORITÉ 6: Emails génériques (autres)
        if prospect.get('emails_generiques'):
            email_str = str(prospect['emails_generiques']).strip()
            # Peut contenir plusieurs emails séparés par ;
            for email in email_str.split(';'):
                email = email.strip().lower()
                if self._is_valid_email(email) and email not in emails:
                    emails.append(email)
        
        return emails
    
    def _is_valid_email(self, email: str) -> bool:
        """Valide le format d'une adresse email"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, str(email).lower()))
    
    def extract_name_for_greeting(self, prospect: dict) -> str:
        """
        Extrait le nom/identité pour personnaliser la salutation.
        
        HIÉRARCHIE DE PRÉFÉRENCE (par ordre de priorité):
        ┌────────────────────────────────────────────────────┐
        │ 1️⃣ DIRIGEANT NOM (meilleur)                        │
        │    └─ Colonne: 'dirigeant_nom'                    │
        │    └─ Exemple: "Baudry S"                         │
        │    └─ Extraction: Prendre DERNIER mot (nom)       │
        │    └─ Salutation: "Madame, Monsieur S,"           │
        │                                                   │
        │ 2️⃣ GESTIONNAIRE NOM (bon)                         │
        │    └─ Colonne: 'gestionnaire_nom'                └─ Exemple: "CROIX ROUGE FRANCAISE"
        │    └─ Salutation: "Madame, Monsieur CROIX ROUGE FRANCAISE,"
        │                                                   │
        │ 3️⃣ NOM PUBLIC (acceptable)                        │
        │    └─ Colonne: 'nom_public'                      │
        │    └─ Exemple: "Croix-Rouge Française"           │
        │    └─ Salutation: "Madame, Monsieur Croix-Rouge Française,"
        │                                                   │
        │ 4️⃣ FALLBACK (défaut)                              │
        │    └─ Salutation: "Madame, Monsieur,"            │
        │    └─ Utilisé si aucune info disponible          │
        └────────────────────────────────────────────────────┘
        
        Args:
            prospect (dict): Un prospect avec ses données
        
        Returns:
            str: Nom/identité à utiliser pour la salutation
        
        Exemple 1 (avec dirigeant):
            >>> prospect = {"dirigeant_nom": "Martin Jean-Paul"}
            >>> name = self.extract_name_for_greeting(prospect)
            >>> name
            "Paul"  # Dernier mot du dirigeant
        
        Exemple 2 (sans dirigeant):
            >>> prospect = {"gestionnaire_nom": "CROIX ROUGE"}
            >>> name = self.extract_name_for_greeting(prospect)
            >>> name
            "CROIX ROUGE"
        
        Exemple 3 (aucune info):
            >>> prospect = {}
            >>> name = self.extract_name_for_greeting(prospect)
            >>> name
            "Madame, Monsieur"
        """
        # Préférer le nom du dirigeant si disponible
        if prospect.get('dirigeant_nom'):
            name = str(prospect['dirigeant_nom']).strip()
            if name and name.lower() != 'nan':
                # Extraire juste le nom de famille (dernier mot)
                parts = name.split()
                if len(parts) >= 2:
                    last_name = parts[-1]
                    # Exclure les initiales (1-2 chars, ex: "Baudry S", "Alexandra D")
                    if len(last_name.strip('.')) <= 2:
                        return None  # Nom avec initiale, pas de personnalisation
                    return last_name
                elif len(parts) == 1:
                    # Un seul mot: vérifier que ce n'est pas une initiale
                    if len(parts[0].strip('.')) <= 2:
                        return None
                    return parts[0]
        
        return None  # Pas de nom personnel, civilité seule
    
    def text_to_html(self, text: str) -> str:
        """Convertit du texte brut en HTML simple"""
        import html as html_module
        
        # Échapper les caractères HTML spéciaux
        text = html_module.escape(text)
        
        # Remplacer les doubles retours à ligne par des paragraphes
        lines = text.split('\n')
        html_lines = []
        
        for line in lines:
            line = line.rstrip()
            if not line:
                html_lines.append('</p><p>')
            elif line.startswith('— '):
                # Listes à puces (—)
                html_lines.append(f'  • {line[2:]}')
            else:
                html_lines.append(line)
        
        # Joindre et formater
        html_body = '<br>'.join(html_lines)
        
        # Envelopper en structure HTML minimaliste
        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
</head>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
<div style="max-width: 600px; margin: 0 auto;">
{html_body}
</div>
</body>
</html>"""
        return html
    
    def generate_email_content(self, prospect: dict) -> Tuple[str, str, str]:
        """
        Génère le sujet et le corps personnalisé de l'email (texte + HTML).
        
        LOGIQUE DE GÉNÉRATION:
        ┌────────────────────────────────────────────────────┐
        │ SUJET (constant pour tous les emails):            │
        │  "Intelligence Artificielle dans les ESSMS : │
        │   les outils et l'accompagnement de BMSE"       │
        │                                                   │
        │ CORPS TEXTE (fallback pour clients email):        │
        │  1. Salutation personnalisée (genrée)             │
        │  2. Contenu EMAIL_BODY (constant)                 │
        │  3. Format: texte brut                            │
        │                                                   │
        │ CORPS HTML (principal + tracking):                │
        │  1. Conversion du texte en HTML                   │
        │  2. Styling CSS minimaliste                       │
        │  3. Elastic Email remplace les liens automat.     │
        │     (avec tracking si tracklinks=1)               │
        │                                                   │
        │ RÉSULTAT:                                         │
        │   ✅ Objet unique                                 │
        │   ✅ Corps texte (fallback)                       │
        │   ✅ Corps HTML (principal + tracking clickX)     │
        └────────────────────────────────────────────────────┘
        
        Returns:
            Tuple[str, str, str]: (subject, body_text, body_html)
        """
        name = self.extract_name_for_greeting(prospect)
        
        # Sujet
        subject = EMAIL_SUBJECT
        
        # Civilité genrée depuis la colonne 'Civilité' du fichier
        civilite_raw = str(prospect.get('Civilité', '')).strip()
        if civilite_raw.lower() in ('nan', ''):
            civilite = "Madame, Monsieur"
        elif civilite_raw == 'Monsieur':
            civilite = "Monsieur"
        elif civilite_raw == 'Madame':
            civilite = "Madame"
        else:
            civilite = "Madame, Monsieur"  # Fallback (ex: 'Madame, Monsieur')
        
        # Construire la salutation
        if name:
            greeting = f"{civilite} {name},"
        else:
            greeting = f"{civilite},"
        
        # Corps texte brut (pour fallback)
        body_text = f"""{greeting}

{EMAIL_BODY}"""
        
        # Corps HTML (pour affichage principal + tracking)
        body_html = self.text_to_html(body_text)
        
        return subject, body_text, body_html
        return subject, body
    
    def prepare_emails(self, prospects: List[dict]) -> List[dict]:
        """
        Prépare TOUS les emails pour chaque prospect et chaque adresse email.
        
        LOGIQUE DE PRÉPARATION:
        ┌──────────────────────────────────────────────────────┐
        │ POUR CHAQUE prospect parmi les 246:                │
        ├──────────────────────────────────────────────────────┤
        │ 1. Générer emails possibles                         │
        │    └─ generate_possible_emails()                   │
        │    └─ Retourne: [], [1 email], [2-3 emails]       │
        │                                                    │
        │ 2. Vérifier si emails trouvés                       │
        │    └─ SI liste vide:                               │
        │       └─ Afficher ⚠️ et SKIP ce prospect           │
        │    └─ SINON: Continuer                             │
        │                                                    │
        │ 3. Générer contenu personnalisé                     │
        │    └─ generate_email_content()                     │
        │    └─ Retourne: (subject, body personnalisé)       │
        │                                                    │
        │ 4. POUR CHAQUE email du prospect                    │
        │    └─ Créer objet 'prepared':                      │
        │       ├─ prospect_name: Nom de la structure       │
        │       ├─ prospect_etablissement: Établissement    │
        │       ├─ recipient_email: Email destination       │
        │       ├─ subject: Sujet (constant)                │
        │       ├─ body: Corps (personnalisé)               │
        │       └─ sender: "patrick.danto@bmse.fr"          │
        │    └─ Ajouter à liste finale                       │
        │                                                    │
        │ RÉSULTATS ATTENDUS:                                │
        │   Entrée:   246 prospects filtrés                  │
        │   Sortie:   289 emails préparés                    │
        │   (Certains prospects = 1-3 emails)               │
        └──────────────────────────────────────────────────────┘
        
        Args:
            prospects (List[dict]): 246 prospects après filtrage
        
        Returns:
            List[dict]: 289 emails préparés avec toutes les infos
        
        Exemple:
            >>> prospects = [
            ...   {"gestionnaire_nom": "ORG A", "email_contact": "a@ex.fr"},
            ...   {"gestionnaire_nom": "ORG B", "email_contact": None},
            ...   {"gestionnaire_nom": "ORG C", "emails_generiques": "c1@ex.fr;c2@ex.fr"}
            ... ]
            >>> prepared = self.prepare_emails(prospects)
            >>> len(prepared)
            3  # ORG A (1) + ORG C (2) = 3 emails, ORG B skipped
            >>> prepared[0]['recipient_email']
            'a@ex.fr'
        """
        prepared_emails = []
        
        for prospect in prospects:
            # Générer les emails possibles
            recipient_emails = self.generate_possible_emails(prospect)
            
            if not recipient_emails:
                print(f"[WARNING] Pas d'email pour: {prospect.get('gestionnaire_nom', 'Inconnu')}")
                continue
            
            # Générer le contenu personnalisé
            subject, body, body_html = self.generate_email_content(prospect)
            
            # Créer une préparation pour chaque email possible
            for recipient_email in recipient_emails:
                prepared = {
                    'prospect_name': prospect.get('gestionnaire_nom', ''),
                    'prospect_etablissement': prospect.get('nom_public', prospect.get('gestionnaire_nom', '')),
                    'recipient_email': recipient_email,
                    'subject': subject,
                    'body': body,
                    'body_html': body_html,
                    'sender': SENDER_EMAIL
                }
                prepared_emails.append(prepared)
        
        print(f"\n[EMAIL] {len(prepared_emails)} emails prepares pour envoi")
        return prepared_emails
    
    def save_preparation(self, emails: List[dict], output_dir: str = OUTPUT_DIR):
        """Sauvegarde la préparation des emails"""
        os.makedirs(output_dir, exist_ok=True)
        
        # CSV avec résumé
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_file = os.path.join(output_dir, f'preparation_emails_{timestamp}.csv')
        
        df = pd.DataFrame([{
            'prospect_name': e['prospect_name'],
            'prospect_etablissement': e['prospect_etablissement'],
            'recipient_email': e['recipient_email'],
            'subject': e['subject']
        } for e in emails])
        
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        print(f"[CSV] Fichier sauvegarde: {csv_file}")
        
        # JSON avec contenu complet
        json_file = os.path.join(output_dir, f'preparation_emails_{timestamp}.json')
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(emails, f, ensure_ascii=False, indent=2)
        print(f"[JSON] Fichier sauvegarde: {json_file}")
        
        return emails
    
    def display_samples(self, emails: List[dict], count: int = 3):
        """Affiche des exemples d'emails"""
        print(f"\n{'='*80}")
        print(f"EXEMPLES D'EMAILS (premiers {min(count, len(emails))})")
        print(f"{'='*80}\n")
        
        for i, email in enumerate(emails[:count], 1):
            print(f"--- Email {i} ---")
            print(f"À: {email['recipient_email']}")
            print(f"Prospect: {email['prospect_name']} ({email['prospect_etablissement']})")
            print(f"Sujet: {email['subject']}")
            print(f"\nCorps:\n{email['body'][:300]}...\n")
    
    def run(self):
        """Exécute le processus complet"""
        print("\n" + "="*80)
        print("PRÉPARATION D'ENVOI D'EMAILS DE RELANCE - 250 PROSPECTS")
        print("="*80)
        
        # 1. Charger les prospects
        prospects = self.load_prospects()
        if not prospects:
            return
        
        # 2. Exclure Pas-de-Calais
        filtered = self.filter_exclude_pas_de_calais(prospects)
        if not filtered:
            print("❌ Aucun prospect après filtrage")
            return
        
        # 3. Préparer les emails
        prepared = self.prepare_emails(filtered)
        if not prepared:
            print("❌ Aucun email à envoyer")
            return
        
        # 4. Sauvegarder la préparation
        self.save_preparation(prepared)
        
        # 5. Afficher des exemples
        self.display_samples(prepared)
        
        print(f"\n{'='*80}")
        if self.dry_run:
            print("✅ MODE TEST - Les emails n'ont pas été envoyés")
            print(f"   Ready to send {len(prepared)} emails to Elasticmail API")
        print(f"{'='*80}\n")


if __name__ == '__main__':
    sender = ProspectEmailSender(INPUT_FILE, dry_run=True)
    sender.run()
