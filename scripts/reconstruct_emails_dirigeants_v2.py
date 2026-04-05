"""
Script PASSE 2 V2 : Reconstruction intelligente et optimisée des emails dirigeants.

AMÉLIORATIONS V2 :
1. Filtre les emails génériques (contact@, info@, etc.) pour la détection de pattern
2. Priorise les emails de personnes réelles (prénom.nom détectables)
3. Email dirigeant n°1 = pattern le plus fiable appliqué au dirigeant
4. Email organisation n°1 = email public avec mots-clés (siege, direction, dg, contact)
5. Colonne Civilité ajoutée pour publipostage
6. Adresse formatée pour publipostage (sans département)
"""

from __future__ import annotations

import argparse
import os
import re
import time
from collections import Counter
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv(override=True)


# === EMAILS GÉNÉRIQUES À EXCLURE POUR LA DÉTECTION DE PATTERN ===
GENERIC_EMAIL_PATTERNS = [
    'contact@', 'info@', 'accueil@', 'secretariat@', 'administration@',
    'direction@', 'siege@', 'dg@', 'communication@', 'presse@',
    'rh@', 'recrutement@', 'commercial@', 'service@', 'support@',
    'bonjour@', 'hello@', 'salut@', 'webmaster@', 'admin@',
    'noreply@', 'no-reply@', 'donotreply@'
]


def is_generic_email(email: str) -> bool:
    """Vérifie si un email est générique (à exclure pour pattern)."""
    email_lower = email.lower()
    local_part = email_lower.split('@')[0] if '@' in email_lower else email_lower
    
    # Patterns génériques exacts
    for pattern in GENERIC_EMAIL_PATTERNS:
        if email_lower.startswith(pattern):
            return True
    
    # Emails trop courts (probablement génériques)
    if len(local_part) <= 4:
        return True
    
    return False


def is_person_email(email: str) -> bool:
    """
    Détecte si un email semble être celui d'une personne (pas générique).
    
    Critères :
    - Contient un point ou underscore (séparateur prénom/nom)
    - Ou format prenomnom détectable (> 6 caractères)
    - Pas dans la liste des emails génériques
    """
    if is_generic_email(email):
        return False
    
    local_part = email.split('@')[0] if '@' in email else email
    
    # Présence de séparateur (fort indicateur)
    if '.' in local_part or '_' in local_part or '-' in local_part:
        return True
    
    # Format prenomnom (sans séparateur mais > 6 chars)
    if len(local_part) > 6:
        return True
    
    return False


def serper_search(query: str, num: int = 10) -> list[dict[str, Any]]:
    """Recherche Serper."""
    api_key = os.getenv("SERPER_API_KEY", "")
    if not api_key:
        raise RuntimeError("SERPER_API_KEY missing in environment")

    try:
        resp = requests.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
            json={"q": query, "gl": "fr", "hl": "fr", "num": num},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("organic", []) or []
    except Exception as e:
        print(f"    ⚠️ Serper error: {e}")
        return []


def extract_emails_from_text(text: str, domain: str) -> list[str]:
    """Extrait tous les emails d'un domaine donné depuis un texte."""
    pattern = rf'\b([a-zA-Z0-9._-]+)@{re.escape(domain)}\b'
    matches = re.findall(pattern, text, re.IGNORECASE)
    return [f"{m}@{domain}".lower() for m in matches]


def analyze_email_pattern(emails: list[str], verbose: bool = True) -> dict[str, Any]:
    """
    Analyse les emails pour déduire le pattern de construction.
    
    V2: Filtre les emails génériques et priorise les emails de personnes.
    
    Patterns détectés :
    - prenom.nom@domain (ex: john.doe@)
    - p.nom@domain (ex: j.doe@)
    - prenomnom@domain (ex: johndoe@)
    - prenom_nom@domain (ex: john_doe@)
    - nom.prenom@domain (ex: doe.john@)
    """
    if not emails:
        return {"pattern": "unknown", "confidence": 0, "examples": [], "person_emails": []}
    
    # FILTRAGE V2: Séparer emails de personnes et emails génériques
    person_emails = [e for e in emails if is_person_email(e)]
    generic_emails = [e for e in emails if is_generic_email(e)]
    
    if verbose:
        print(f"      📧 Total emails trouvés: {len(emails)}")
        print(f"      👤 Emails de personnes: {len(person_emails)}")
        print(f"      🏢 Emails génériques (exclus): {len(generic_emails)}")
    
    # Si aucun email de personne, on ne peut pas détecter de pattern
    if not person_emails:
        if verbose:
            print(f"      ⚠️ Aucun email de personne détecté, pattern impossible")
        return {
            "pattern": "unknown",
            "confidence": 0,
            "examples": emails[:3],
            "person_emails": [],
            "generic_emails": generic_emails[:3]
        }
    
    patterns = []
    
    for email in person_emails:
        local = email.split('@')[0]
        
        # Détecter le pattern
        if '.' in local:
            parts = local.split('.')
            if len(parts) == 2:
                # Vérifier si c'est p.nom ou prenom.nom
                if len(parts[0]) == 1:
                    patterns.append("p.nom")
                elif len(parts[1]) == 1:
                    patterns.append("nom.p")
                else:
                    # Heuristique : prenom.nom est plus courant que nom.prenom
                    # On pourrait affiner en vérifiant la longueur relative
                    patterns.append("prenom.nom")
        elif '_' in local:
            patterns.append("prenom_nom")
        elif '-' in local:
            patterns.append("prenom-nom")
        else:
            # Pas de séparateur
            patterns.append("prenomnom")
    
    # Compter les patterns
    pattern_counts = Counter(patterns)
    most_common = pattern_counts.most_common(1)[0] if pattern_counts else ("unknown", 0)
    
    # Calculer confidence
    total = len(patterns)
    confidence = int((most_common[1] / total * 100)) if total > 0 else 0
    
    return {
        "pattern": most_common[0],
        "confidence": confidence,
        "total_emails": len(emails),
        "person_emails_count": len(person_emails),
        "examples": person_emails[:5],  # Exemples de personnes uniquement
        "person_emails": person_emails,
        "generic_emails": generic_emails[:3]
    }


def generate_email_variants(prenom: str, nom: str, domain: str, pattern: str) -> list[str]:
    """
    Génère des variantes d'emails selon le pattern détecté.
    
    V2: Email n°1 = pattern détecté, emails 2-3 = variantes standard
    """
    # Normaliser prenom et nom (enlever accents, lowercase)
    def normalize(s: str) -> str:
        s = s.lower()
        replacements = {
            'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
            'à': 'a', 'â': 'a', 'ä': 'a',
            'ô': 'o', 'ö': 'o',
            'û': 'u', 'ü': 'u', 'ù': 'u',
            'ç': 'c',
            'î': 'i', 'ï': 'i',
            'ñ': 'n',
            'æ': 'ae', 'œ': 'oe'
        }
        for old, new in replacements.items():
            s = s.replace(old, new)
        # Enlever caractères spéciaux (garder lettres et chiffres)
        s = re.sub(r'[^a-z0-9]', '', s)
        return s
    
    prenom_clean = normalize(prenom)
    nom_clean = normalize(nom)
    
    variants = []
    
    # EMAIL N°1 : Selon le pattern détecté
    if pattern == "prenom.nom":
        variants.append(f"{prenom_clean}.{nom_clean}@{domain}")
    elif pattern == "p.nom":
        variants.append(f"{prenom_clean[0]}.{nom_clean}@{domain}")
    elif pattern == "prenomnom":
        variants.append(f"{prenom_clean}{nom_clean}@{domain}")
    elif pattern == "prenom_nom":
        variants.append(f"{prenom_clean}_{nom_clean}@{domain}")
    elif pattern == "prenom-nom":
        variants.append(f"{prenom_clean}-{nom_clean}@{domain}")
    elif pattern == "nom.prenom":
        variants.append(f"{nom_clean}.{prenom_clean}@{domain}")
    elif pattern == "nom.p":
        variants.append(f"{nom_clean}.{prenom_clean[0]}@{domain}")
    else:
        # Unknown : format le plus courant (prenom.nom)
        variants.append(f"{prenom_clean}.{nom_clean}@{domain}")
    
    # EMAILS N°2 et N°3 : Variantes standard
    # Toujours proposer les 2 formats les plus courants si pas déjà présent
    standard_formats = [
        f"{prenom_clean}.{nom_clean}@{domain}",
        f"{prenom_clean[0]}.{nom_clean}@{domain}",
        f"{prenom_clean}{nom_clean}@{domain}"
    ]
    
    for fmt in standard_formats:
        if fmt not in variants:
            variants.append(fmt)
        if len(variants) >= 3:
            break
    
    return variants[:3]


def find_organization_email(person_emails: list[str], generic_emails: list[str], 
                            domain: str) -> dict[str, Any]:
    """
    Trouve l'email organisation n°1 selon ordre de priorité :
    1. Emails contenant "siege", "direction", "dg"
    2. Email "contact@"
    3. Premier email générique disponible
    """
    all_emails = generic_emails + person_emails
    
    # Ordre de priorité des mots-clés
    priority_keywords = [
        ['siege', 'siège'],
        ['direction', 'dir.'],
        ['dg', 'd.g'],
        ['contact']
    ]
    
    for keywords in priority_keywords:
        for email in all_emails:
            local = email.split('@')[0].lower()
            if any(kw in local for kw in keywords):
                # Déterminer le type
                email_type = "siege" if any(k in local for k in ['siege', 'siège']) else \
                            "direction" if 'direction' in local or 'dir' in local else \
                            "dg" if 'dg' in local or 'd.g' in local else \
                            "contact"
                return {
                    "email": email,
                    "type": email_type,
                    "priority": priority_keywords.index(keywords) + 1
                }
    
    # Fallback : premier email générique ou personne
    if all_emails:
        return {
            "email": all_emails[0],
            "type": "fallback",
            "priority": 99
        }
    
    return {
        "email": "",
        "type": "none",
        "priority": 0
    }


def search_domain_email_pattern(domain: str) -> dict[str, Any]:
    """
    Recherche le pattern d'emails pour un domaine donné.
    
    V2: Recherche élargie avec emails de personnes réelles
    """
    # Requête 1: Emails généraux du domaine
    query1 = f'"{domain}"'
    
    # Requête 2: Emails de personnes (directeur, responsable, etc.)
    query2 = f'"{domain}" (directeur OR directrice OR responsable OR président OR présidente)'
    
    all_results = []
    all_results.extend(serper_search(query1, num=10))
    time.sleep(0.3)  # Rate limiting
    all_results.extend(serper_search(query2, num=10))
    
    # Collecter tout le texte des résultats
    all_text = ""
    for item in all_results:
        all_text += item.get("title", "") + " "
        all_text += item.get("snippet", "") + " "
    
    # Extraire emails
    emails = extract_emails_from_text(all_text, domain)
    
    # Analyser pattern (V2 avec filtrage)
    pattern_info = analyze_email_pattern(emails, verbose=True)
    
    # Trouver email organisation
    org_email_info = find_organization_email(
        pattern_info.get("person_emails", []),
        pattern_info.get("generic_emails", []),
        domain
    )
    
    pattern_info["org_email"] = org_email_info["email"]
    pattern_info["org_email_type"] = org_email_info["type"]
    
    return pattern_info


def reconstruct_dirigeant_emails(
    prenom: str,
    nom: str,
    domain: str,
    pattern_info: dict[str, Any]
) -> dict[str, Any]:
    """
    Reconstruit les emails probables d'un dirigeant.
    
    V2: Email n°1 basé sur pattern fiable, emails 2-3 = variantes standard
    """
    pattern = pattern_info["pattern"]
    pattern_confidence = pattern_info["confidence"]
    
    # Générer variantes (V2)
    variants = generate_email_variants(prenom, nom, domain, pattern)
    
    result = {
        "email_1": variants[0] if len(variants) > 0 else "",
        "email_2": variants[1] if len(variants) > 1 else "",
        "email_3": variants[2] if len(variants) > 2 else "",
        "email_pattern": pattern,
        "email_confidence": pattern_confidence,
        "pattern_examples": ", ".join(pattern_info.get("examples", [])[:3]),
        "email_org": pattern_info.get("org_email", ""),
        "email_org_type": pattern_info.get("org_email_type", "")
    }
    
    return result


def extract_prenom_nom(dirigeant_nom: str) -> tuple[str, str]:
    """
    Extrait prénom et nom depuis "Prénom NOM" ou "Prénom Nom".
    
    Format attendu : "Mathilde DELEVAL" ou "Daniel D" (initiale)
    """
    parts = dirigeant_nom.strip().split()
    
    if len(parts) >= 2:
        prenom = parts[0]
        nom = " ".join(parts[1:])  # Cas des noms composés
        return prenom, nom
    
    return "", ""


def determine_civilite(prenom: str, fonction: str = "") -> str:
    """
    Détermine la civilité à partir du prénom et/ou de la fonction.
    
    Heuristique :
    - Fonction contient "Directrice", "Présidente" → Madame
    - Fonction contient "Directeur", "Président" → Monsieur
    - Prénoms féminins courants → Madame
    - Prénoms masculins courants → Monsieur
    - Défaut → Madame, Monsieur
    """
    # Vérifier fonction d'abord (gérer NaN/None)
    if fonction and pd.notna(fonction):
        fonction_lower = fonction.lower()
        if any(f in fonction_lower for f in ['directrice', 'présidente', 'responsable']):
            if 'directrice' in fonction_lower or 'présidente' in fonction_lower:
                return "Madame"
        if any(f in fonction_lower for f in ['directeur', 'président']):
            return "Monsieur"
    
    # Prénoms féminins courants
    prenoms_feminins = [
        'marie', 'sophie', 'catherine', 'isabelle', 'nathalie', 'christine',
        'anne', 'françoise', 'sylvie', 'martine', 'brigitte', 'laurence',
        'valérie', 'sandrine', 'véronique', 'florence', 'monique', 'dominique',
        'pascale', 'claire', 'stéphanie', 'emmanuelle', 'hélène', 'cécile',
        'caroline Caroline', 'mathilde', 'charlotte', 'camille', 'julie', 'sarah'
    ]
    
    # Prénoms masculins courants
    prenoms_masculins = [
        'jean', 'pierre', 'michel', 'philippe', 'alain', 'bernard', 'patrick',
        'christian', 'daniel', 'françois', 'laurent', 'david', 'nicolas',
        'olivier', 'thierry', 'eric', 'frédéric', 'christophe', 'stéphane',
        'marc', 'pascal', 'vincent', 'serge', 'jacques', 'thomas', 'julien',
        'alexandre', 'guillaume', 'sébastien', 'maxime', 'antoine'
    ]
    
    prenom_lower = prenom.lower()
    
    # Cas spéciaux (prénoms mixtes)
    if prenom_lower in ['dominique', 'camille', 'claude']:
        return "Madame, Monsieur"
    
    if any(p in prenom_lower for p in prenoms_feminins):
        return "Madame"
    
    if any(p in prenom_lower for p in prenoms_masculins):
        return "Monsieur"
    
    # Défaut
    return "Madame, Monsieur"


def format_adresse_publipostage(adresse_complete: str) -> str:
    """
    Formate l'adresse pour le publipostage (sans département).
    
    Entrée : "33 AV PIERRE MENDES FRANCE, 75013 PARIS, PARIS"
    Sortie : "33 avenue Pierre Mendes France, 75013 Paris"
    """
    if not adresse_complete or pd.isna(adresse_complete):
        return ""

    txt = str(adresse_complete).strip()

    # helper: capitalization
    def capitalize_proper(text: str) -> str:
        words = text.lower().split()
        result = []
        for i, word in enumerate(words):
            if i == 0 or word not in ['de', 'du', 'la', 'le', 'les', 'd', 'l', 'au', "d'", "l'"]:
                result.append(word.capitalize())
            else:
                result.append(word)
        return ' '.join(result)

    # Normalize common abbreviations (word boundaries, case-insensitive)
    def expand_abbrev(s: str) -> str:
        s = re.sub(r'\bAV\b\.?', 'Avenue', s, flags=re.IGNORECASE)
        s = re.sub(r'\bBD\b\.?', 'Boulevard', s, flags=re.IGNORECASE)
        s = re.sub(r'\bR\b\.?', 'Rue', s, flags=re.IGNORECASE)
        s = re.sub(r'\bPL\b\.?', 'Place', s, flags=re.IGNORECASE)
        s = re.sub(r'\bCHE\b\.?', 'Chemin', s, flags=re.IGNORECASE)
        s = re.sub(r'\bIMP\b\.?', 'Impasse', s, flags=re.IGNORECASE)
        s = re.sub(r'\bBP\b\.?', 'BP', s, flags=re.IGNORECASE)
        s = re.sub(r'\bCS\b\.?', 'CS', s, flags=re.IGNORECASE)
        return s

    txt = expand_abbrev(txt)

    # Chercher un code postal 5 chiffres
    m = re.search(r"\b(\d{5})\b", txt)
    if m:
        cp = m.group(1)
        # Rue = tout avant le code postal
        rue_part = txt[: m.start()].strip(' ,')

        # Ville = tente de prendre le token qui suit le CP
        after = txt[m.end():].strip(' ,')
        ville_candidate = ''
        if after:
            # prendre jusqu'à la prochaine virgule ou fin
            ville_candidate = after.split(',')[0].strip()
            # si ville commence par des indicateurs comme 'Cedex' ou 'Siège', essayer la partie précédente
            if re.match(r'^(?:Cedex|Cede?x|Si[eè]ge|Si[eè]ge Social|Siège Social)$', ville_candidate, flags=re.IGNORECASE):
                ville_candidate = ''

        # if no city after cp, fallback to last comma-separated token before or after
        if not ville_candidate:
            parts = [p.strip() for p in txt.split(',') if p.strip()]
            if len(parts) >= 2:
                # prefer the part that contains the CP, else the last
                for part in parts:
                    if cp in part:
                        # try to extract city from this part
                        tokens = part.split()
                        # city likely after the CP
                        try:
                            idx = tokens.index(cp)
                            ville_candidate = ' '.join(tokens[idx + 1 :])
                        except ValueError:
                            continue
                if not ville_candidate:
                    ville_candidate = parts[-1]

        # Clean city (remove words like 'Cedex', 'BP', 'CS' trailing tokens if they look like boxes)
        ville_candidate = re.sub(r'\b(?:Cedex|Cede?x|BP|CS)\b[:\s]*\w*', '', ville_candidate, flags=re.IGNORECASE).strip()

        # Capitalize properly
        rue_clean = capitalize_proper(re.sub(r'\s+', ' ', rue_part)) if rue_part else ''
        ville_clean = capitalize_proper(re.sub(r'\s+', ' ', ville_candidate)) if ville_candidate else ''

        # Handle cases where rue_part ends with BP/CS tokens (we want '..., BP 30824')
        m_token = re.search(r"\b(BP|CS|Bp|Cs)\b\s*$", rue_part)
        if m_token:
            token = m_token.group(1).upper()
            # remove the token from the rue base
            rue_base = re.sub(r"\b(BP|CS|Bp|Cs)\b\s*$", '', rue_part).strip(' ,')
            rue_base_clean = capitalize_proper(re.sub(r'\s+', ' ', rue_base)) if rue_base else ''
            if ville_clean:
                return f"{rue_base_clean}\n{token} {cp} {ville_clean}"
            return f"{rue_base_clean}\n{token} {cp}"

        if rue_clean and ville_clean:
            return f"{rue_clean}\n{cp} {ville_clean}"
        if rue_clean:
            return f"{rue_clean}\n{cp}"
        if ville_clean:
            return f"{cp} {ville_clean}"

    # No CP found: try to split on last comma -> treat last part as ville
    parts = [p.strip() for p in txt.split(',') if p.strip()]
    if len(parts) >= 2:
        rue = ', '.join(parts[:-1])
        ville = parts[-1]
        return f"{capitalize_proper(re.sub(r'\s+', ' ', expand_abbrev(rue)))}\n{capitalize_proper(ville)}"

    # Fallback: normalize abbreviations and capitalize
    return capitalize_proper(expand_abbrev(txt))


def main():
    parser = argparse.ArgumentParser(description="PASSE 2 V2: Reconstruction emails dirigeants optimisée")
    parser.add_argument("--input", default="outputs/prospection_250_dirigeants_complet_v2.xlsx")
    parser.add_argument("--output", default="outputs/prospection_250_FINAL_FORMATE_V2.xlsx")
    parser.add_argument("--sleep", type=float, default=0.5, help="Délai entre requêtes (secondes)")
    parser.add_argument("--test", action="store_true", help="Mode test (10 premiers)")
    args = parser.parse_args()
    
    print("=" * 80)
    print("🚀 RECONSTRUCTION EMAILS DIRIGEANTS V2 - VERSION OPTIMISÉE")
    print("=" * 80)
    print("\n📋 AMÉLIORATIONS V2 :")
    print("  ✅ Filtre des emails génériques pour la détection de pattern")
    print("  ✅ Priorisation des emails de personnes réelles")
    print("  ✅ Email dirigeant n°1 = pattern le plus fiable")
    print("  ✅ Email organisation n°1 (siege/direction/dg/contact)")
    print("  ✅ Colonne Civilité pour publipostage")
    print("  ✅ Adresse formatée pour publipostage\n")
    
    # Charger données
    df = pd.read_excel(args.input)
    print(f"📁 Chargé: {len(df)} gestionnaires")
    
    # Filtrer : dirigeants trouvés ET domaine disponible
    mask = df["dirigeant_nom"].notna() & df["domaine"].notna()
    to_process = df[mask].copy()
    
    print(f"✅ Trouvé: {len(to_process)} gestionnaires avec dirigeant + domaine\n")
    
    if args.test:
        to_process = to_process.head(10)
        print(f"🧪 MODE TEST: Traitement de {len(to_process)} cas seulement\n")
    
    # Initialiser colonnes V2
    new_columns = {
        "Email Dirigeant 1": "",
        "Email Dirigeant 2": "",
        "Email Dirigeant 3": "",
        "Email Organisation": "",
        "Type Email Org": "",
        "Pattern Email": "",
        "Conf. Email": 0,
        "Exemples Pattern": "",
        "Civilité": "",
        "Adresse Publipostage": ""
    }
    
    for col, default in new_columns.items():
        if col not in df.columns:
            df[col] = default
    
    # Cache des patterns par domaine
    domain_patterns = {}
    
    # Traiter chaque cas
    success_count = 0
    
    for idx, row in to_process.iterrows():
        i = to_process.index.get_loc(idx) + 1
        
        gestionnaire_nom = row["nom_public"]
        dirigeant_nom = row["dirigeant_nom"]
        fonction = row.get("dirigeant_titre", "")
        domain = row["domaine"]
        adresse = row.get("gestionnaire_adresse", "")
        
        print(f"\n{'='*80}")
        print(f"[{i}/{len(to_process)}] {gestionnaire_nom[:60]}")
        print(f"{'='*80}")
        print(f"  👤 Dirigeant: {dirigeant_nom}")
        print(f"  💼 Fonction: {fonction}")
        print(f"  🌐 Domaine: {domain}")
        
        # Extraire prénom et nom
        prenom, nom = extract_prenom_nom(dirigeant_nom)
        
        if not prenom or not nom:
            print(f"  ❌ Impossible d'extraire prénom/nom")
            continue
        
        print(f"  ✅ Prénom: {prenom} | Nom: {nom}")
        
        # Si initiale uniquement
        if len(nom) == 1:
            print(f"  ⚠️ Initiale seulement ({nom}), emails incomplets possibles")
        
        # Chercher pattern du domaine (cache)
        if domain not in domain_patterns:
            print(f"\n  🔍 Recherche pattern emails pour {domain}...")
            pattern_info = search_domain_email_pattern(domain)
            domain_patterns[domain] = pattern_info
            
            total = pattern_info.get('total_emails', 0)
            person_count = pattern_info.get('person_emails_count', 0)
            
            print(f"\n  📊 RÉSULTATS:")
            print(f"     Pattern: {pattern_info['pattern']} (confiance: {pattern_info['confidence']}%)")
            print(f"     Total emails: {total} | Personnes: {person_count}")
            
            if pattern_info.get('examples'):
                print(f"     Exemples: {', '.join(pattern_info['examples'][:3])}")
            
            if pattern_info.get('org_email'):
                print(f"     📧 Email org: {pattern_info['org_email']} (type: {pattern_info['org_email_type']})")
        else:
            pattern_info = domain_patterns[domain]
            print(f"  📧 Pattern (cache): {pattern_info['pattern']} (conf: {pattern_info['confidence']}%)")
        
        # Reconstruire emails
        email_result = reconstruct_dirigeant_emails(prenom, nom, domain, pattern_info)
        
        # Déterminer civilité
        civilite = determine_civilite(prenom, fonction)
        
        # Formater adresse
        adresse_publi = format_adresse_publipostage(adresse)
        
        # Mettre à jour DataFrame
        df.at[idx, "Email Dirigeant 1"] = email_result["email_1"]
        df.at[idx, "Email Dirigeant 2"] = email_result["email_2"]
        df.at[idx, "Email Dirigeant 3"] = email_result["email_3"]
        df.at[idx, "Email Organisation"] = email_result["email_org"]
        df.at[idx, "Type Email Org"] = email_result["email_org_type"]
        df.at[idx, "Pattern Email"] = email_result["email_pattern"]
        df.at[idx, "Conf. Email"] = email_result["email_confidence"]
        df.at[idx, "Exemples Pattern"] = email_result["pattern_examples"]
        df.at[idx, "Civilité"] = civilite
        df.at[idx, "Adresse Publipostage"] = adresse_publi
        
        print(f"\n  ✅ RÉSULTAT:")
        print(f"     Email dirigeant n°1: {email_result['email_1']}")
        print(f"     Variantes: {email_result['email_2']}, {email_result['email_3']}")
        print(f"     Email organisation: {email_result['email_org']}")
        print(f"     Civilité: {civilite}")
        print(f"     Adresse publipostage: {adresse_publi[:50]}...")
        
        success_count += 1
        
        # Sleep
        time.sleep(args.sleep)
    
    # Sauvegarder
    df.to_excel(args.output, index=False)
    
    print("\n" + "=" * 80)
    print("✅ TERMINÉ")
    print("=" * 80)
    print(f"📊 Traités: {len(to_process)} dirigeants")
    print(f"✅ Emails reconstruits: {success_count}")
    print(f"🌐 Domaines uniques analysés: {len(domain_patterns)}")
    print(f"\n💾 Fichier de sortie: {args.output}")
    print("\n📋 Nouvelles colonnes ajoutées:")
    print("   - Email Organisation (siege/direction/dg/contact)")
    print("   - Type Email Org")
    print("   - Civilité")
    print("   - Adresse Publipostage")
    print("=" * 80)


if __name__ == "__main__":
    main()
