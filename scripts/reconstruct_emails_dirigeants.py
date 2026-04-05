"""
Script PASSE 2 : Reconstruction intelligente des emails des dirigeants.

Stratégie de l'utilisateur (SMART !) :
1. Pour chaque domaine, chercher "@domain.fr" sur Serper
2. Analyser les emails trouvés pour déduire le FORMAT réel utilisé
3. Générer 2-3 variantes d'emails pour chaque dirigeant selon le pattern
4. Scorer la confiance selon la cohérence des patterns observés

Avantage : on ne devine pas, on observe les vrais emails !
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
    # Pattern email : word@domain
    pattern = rf'\b([a-zA-Z0-9._-]+)@{re.escape(domain)}\b'
    matches = re.findall(pattern, text, re.IGNORECASE)
    return [f"{m}@{domain}".lower() for m in matches]


def analyze_email_pattern(emails: list[str]) -> dict[str, Any]:
    """
    Analyse les emails pour déduire le pattern de construction.
    
    Patterns détectés :
    - prenom.nom@domain (ex: john.doe@)
    - p.nom@domain (ex: j.doe@)
    - prenomnom@domain (ex: johndoe@)
    - prenom_nom@domain (ex: john_doe@)
    - nom.prenom@domain (ex: doe.john@)
    """
    if not emails:
        return {"pattern": "unknown", "confidence": 0, "examples": []}
    
    patterns = []
    
    for email in emails:
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
                    # Déterminer si prenom.nom ou nom.prenom
                    # Heuristique : prenom.nom est plus courant
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
        "total_emails": total,
        "examples": emails[:5]  # Garder 5 exemples
    }


def generate_email_variants(prenom: str, nom: str, domain: str, pattern: str) -> list[str]:
    """
    Génère des variantes d'emails selon le pattern détecté.
    
    Args:
        prenom: Prénom du dirigeant
        nom: Nom du dirigeant
        domain: Domaine de l'organisation
        pattern: Pattern détecté (prenom.nom, p.nom, etc.)
    """
    # Normaliser prenom et nom (enlever accents, lowercase)
    prenom_clean = prenom.lower().replace('é', 'e').replace('è', 'e').replace('ê', 'e')
    prenom_clean = prenom_clean.replace('à', 'a').replace('â', 'a')
    prenom_clean = prenom_clean.replace('ô', 'o').replace('ö', 'o')
    prenom_clean = prenom_clean.replace('û', 'u').replace('ü', 'u')
    prenom_clean = prenom_clean.replace('ç', 'c')
    prenom_clean = prenom_clean.replace('î', 'i').replace('ï', 'i')
    
    nom_clean = nom.lower().replace('é', 'e').replace('è', 'e').replace('ê', 'e')
    nom_clean = nom_clean.replace('à', 'a').replace('â', 'a')
    nom_clean = nom_clean.replace('ô', 'o').replace('ö', 'o')
    nom_clean = nom_clean.replace('û', 'u').replace('ü', 'u')
    nom_clean = nom_clean.replace('ç', 'c')
    nom_clean = nom_clean.replace('î', 'i').replace('ï', 'i')
    
    variants = []
    
    if pattern == "prenom.nom":
        variants.append(f"{prenom_clean}.{nom_clean}@{domain}")
        # Variante : initiale.nom (au cas où)
        variants.append(f"{prenom_clean[0]}.{nom_clean}@{domain}")
    
    elif pattern == "p.nom":
        variants.append(f"{prenom_clean[0]}.{nom_clean}@{domain}")
        # Variante : prenom complet (au cas où)
        variants.append(f"{prenom_clean}.{nom_clean}@{domain}")
    
    elif pattern == "prenomnom":
        variants.append(f"{prenom_clean}{nom_clean}@{domain}")
        # Variante avec point
        variants.append(f"{prenom_clean}.{nom_clean}@{domain}")
    
    elif pattern == "prenom_nom":
        variants.append(f"{prenom_clean}_{nom_clean}@{domain}")
        # Variante avec point
        variants.append(f"{prenom_clean}.{nom_clean}@{domain}")
    
    elif pattern == "prenom-nom":
        variants.append(f"{prenom_clean}-{nom_clean}@{domain}")
        # Variante avec point
        variants.append(f"{prenom_clean}.{nom_clean}@{domain}")
    
    elif pattern == "nom.prenom":
        variants.append(f"{nom_clean}.{prenom_clean}@{domain}")
        # Variante inverse
        variants.append(f"{prenom_clean}.{nom_clean}@{domain}")
    
    elif pattern == "nom.p":
        variants.append(f"{nom_clean}.{prenom_clean[0]}@{domain}")
        # Variante normale
        variants.append(f"{prenom_clean[0]}.{nom_clean}@{domain}")
    
    else:
        # Unknown : proposer les 3 formats les plus courants
        variants.append(f"{prenom_clean}.{nom_clean}@{domain}")
        variants.append(f"{prenom_clean[0]}.{nom_clean}@{domain}")
        variants.append(f"{prenom_clean}{nom_clean}@{domain}")
    
    return variants[:3]  # Max 3 variantes


def search_domain_email_pattern(domain: str) -> dict[str, Any]:
    """
    Recherche le pattern d'emails pour un domaine donné.
    
    Stratégie : chercher "@domain" sur Serper et analyser les emails trouvés.
    """
    # Requête : "@domain.fr" pour trouver des emails
    query = f'"{domain}"'
    
    results = serper_search(query, num=10)
    
    # Collecter tout le texte des résultats
    all_text = ""
    for item in results:
        all_text += item.get("title", "") + " "
        all_text += item.get("snippet", "") + " "
    
    # Extraire emails
    emails = extract_emails_from_text(all_text, domain)
    
    # Analyser pattern
    pattern_info = analyze_email_pattern(emails)
    
    return pattern_info


def reconstruct_dirigeant_emails(
    prenom: str,
    nom: str,
    domain: str,
    pattern_info: dict[str, Any]
) -> dict[str, Any]:
    """
    Reconstruit les emails probables d'un dirigeant.
    
    Returns:
        Dict avec email_1, email_2, email_3, confidence, pattern
    """
    pattern = pattern_info["pattern"]
    pattern_confidence = pattern_info["confidence"]
    
    # Générer variantes
    variants = generate_email_variants(prenom, nom, domain, pattern)
    
    result = {
        "email_1": variants[0] if len(variants) > 0 else "",
        "email_2": variants[1] if len(variants) > 1 else "",
        "email_3": variants[2] if len(variants) > 2 else "",
        "email_pattern": pattern,
        "email_confidence": pattern_confidence,
        "pattern_examples": ", ".join(pattern_info.get("examples", [])[:3])
    }
    
    return result


def extract_prenom_nom(dirigeant_nom: str) -> tuple[str, str]:
    """
    Extrait prénom et nom depuis "Prénom NOM".
    
    Format attendu : "Mathilde DELEVAL" ou "Daniel D" (initiale)
    """
    parts = dirigeant_nom.strip().split()
    
    if len(parts) >= 2:
        prenom = parts[0]
        nom = " ".join(parts[1:])  # Cas des noms composés
        return prenom, nom
    
    return "", ""


def main():
    parser = argparse.ArgumentParser(description="PASSE 2: Reconstruction emails dirigeants")
    parser.add_argument("--input", default="outputs/prospection_250_dirigeants_complet_v2.xlsx")
    parser.add_argument("--output", default="outputs/prospection_250_dirigeants_final.xlsx")
    parser.add_argument("--sleep", type=float, default=0.5, help="Délai entre requêtes (secondes)")
    parser.add_argument("--test", action="store_true", help="Mode test (10 premiers)")
    args = parser.parse_args()
    
    # Charger données
    df = pd.read_excel(args.input)
    print(f"Loaded: {len(df)} gestionnaires")
    
    # Filtrer : dirigeants trouvés ET domaine disponible
    mask = df["dirigeant_nom"].notna() & df["domaine"].notna()
    to_process = df[mask].copy()
    
    print(f"\nFound: {len(to_process)} gestionnaires with dirigeant + domain")
    
    if args.test:
        to_process = to_process.head(10)
        print(f"TEST MODE: Processing only {len(to_process)} cases\n")
    
    # Initialiser colonnes
    if "email_dirigeant_1" not in df.columns:
        df["email_dirigeant_1"] = ""
        df["email_dirigeant_2"] = ""
        df["email_dirigeant_3"] = ""
        df["email_pattern"] = ""
        df["email_confidence"] = 0
        df["pattern_examples"] = ""
    
    # Cache des patterns par domaine
    domain_patterns = {}
    
    # Traiter chaque cas
    success_count = 0
    
    for idx, row in to_process.iterrows():
        i = to_process.index.get_loc(idx) + 1
        
        gestionnaire_nom = row["gestionnaire_nom"]
        dirigeant_nom = row["dirigeant_nom"]
        domain = row["domaine"]
        
        print(f"[{i}/{len(to_process)}] {gestionnaire_nom[:50]}...")
        print(f"  Dirigeant: {dirigeant_nom}")
        print(f"  Domain: {domain}")
        
        # Extraire prénom et nom
        prenom, nom = extract_prenom_nom(dirigeant_nom)
        
        if not prenom or not nom:
            print(f"  ❌ Cannot extract prenom/nom")
            continue
        
        # Si initiale uniquement, on peut quand même essayer
        if len(nom) == 1:
            print(f"  ⚠️ Initiale only ({nom}), emails will be incomplete")
        
        # Chercher pattern du domaine (cache)
        if domain not in domain_patterns:
            print(f"  🔍 Searching email pattern for {domain}...")
            pattern_info = search_domain_email_pattern(domain)
            domain_patterns[domain] = pattern_info
            total = pattern_info.get('total_emails', 0)
            print(f"  📧 Pattern: {pattern_info['pattern']} (conf. {pattern_info['confidence']}%, {total} emails found)")
            if pattern_info.get('examples'):
                print(f"     Examples: {', '.join(pattern_info['examples'][:3])}")
        else:
            pattern_info = domain_patterns[domain]
            print(f"  📧 Pattern (cached): {pattern_info['pattern']} (conf. {pattern_info['confidence']}%)")
        
        # Reconstruire emails
        email_result = reconstruct_dirigeant_emails(prenom, nom, domain, pattern_info)
        
        # Mettre à jour DataFrame
        df.at[idx, "email_dirigeant_1"] = email_result["email_1"]
        df.at[idx, "email_dirigeant_2"] = email_result["email_2"]
        df.at[idx, "email_dirigeant_3"] = email_result["email_3"]
        df.at[idx, "email_pattern"] = email_result["email_pattern"]
        df.at[idx, "email_confidence"] = email_result["email_confidence"]
        df.at[idx, "pattern_examples"] = email_result["pattern_examples"]
        
        print(f"  ✅ Emails: {email_result['email_1']}, {email_result['email_2']}")
        success_count += 1
        
        # Sleep
        time.sleep(args.sleep)
    
    # Sauvegarder
    df.to_excel(args.output, index=False)
    
    print("\n" + "=" * 60)
    print("✅ COMPLETED")
    print("=" * 60)
    print(f"Processed: {len(to_process)} dirigeants")
    print(f"Emails reconstructed: {success_count}")
    print(f"Unique domains analyzed: {len(domain_patterns)}")
    print(f"\nOutput: {args.output}")


if __name__ == "__main__":
    main()
