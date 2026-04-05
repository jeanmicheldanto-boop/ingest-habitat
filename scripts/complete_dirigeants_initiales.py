"""
Script PASSE 1bis : Compléter les noms de dirigeants avec initiales (Prénom I).

Problème détecté : 56/117 (47.9%) des noms extraits de LinkedIn ont juste une initiale
au lieu du nom complet (ex: "Baudry S" au lieu de "Baudry SURNAME").

Stratégie :
1. Filtrer les lignes avec dirigeant_source='linkedin_snippet' ET pattern initiale
2. Pour chaque cas :
   - Recherche ciblée : "{nom_organisation}" "{prénom}" + termes directeur
   - Chercher sur pages officielles (rapports, communiqués, gouvernance)
   - Extraction LLM systématique avec plus de contexte
3. Mettre à jour avec noms complets et confidence ajustée
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv(override=True)


def serper_search(query: str, num: int = 10) -> list[dict[str, Any]]:
    """Recherche Serper avec plus de résultats pour meilleure couverture."""
    api_key = os.getenv("SERPER_API_KEY", "")
    if not api_key:
        raise RuntimeError("SERPER_API_KEY missing in environment")

    try:
        resp = requests.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
            json={"q": query, "gl": "fr", "hl": "fr", "num": num},
            timeout=10,  # Réduire timeout de 15s à 10s
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("organic", []) or []
    except Exception as e:
        print(f"    ⚠️ Serper error: {e}")
        return []


def fetch_html_content(url: str, max_chars: int = 8000) -> str:
    """Récupère contenu HTML d'une page (version simplifiée)."""
    try:
        resp = requests.get(url, timeout=3, headers={"User-Agent": "Mozilla/5.0"})  # Timeout réduit à 3s
        resp.raise_for_status()
        
        # Utiliser resp.text pour éviter problèmes d'encodage
        # Requests gère automatiquement l'encodage
        text = resp.text[:50000]  # Limiter à 50KB avant parsing
        
        soup = BeautifulSoup(text, "html.parser")
        
        # Supprimer scripts, styles
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        
        clean_text = soup.get_text(separator=" ", strip=True)
        return clean_text[:max_chars]
    except Exception:
        # Silently fail - snippets suffisent souvent
        return ""


def search_full_name_with_llm(
    gestionnaire_nom: str,
    nom_public: str,
    prenom: str,
    initiale: str,
    groq_model: str = "llama-3.1-8b-instant"
) -> dict[str, Any]:
    """
    Recherche approfondie du nom complet avec LLM.
    
    Stratégie :
    1. Requête ciblée : "{nom_public}" "{prenom}" directeur
    2. Chercher sur pages officielles, rapports, communiqués
    3. Extraction LLM avec contexte enrichi
    """
    api_key = os.getenv("GROQ_API_KEY", "")
    if not api_key:
        return {"director_name": "", "director_title": "", "confidence": 0}
    
    # Requête 1 : Page officielle avec prénom
    query1 = f'"{nom_public}" "{prenom}" (directeur OR directrice OR président OR présidente)'
    
    # Requête 2 : Rapports et gouvernance
    query2 = f'"{nom_public}" "{prenom}" (rapport annuel OR gouvernance OR équipe OR direction)'
    
    # Requête 3 : Communiqués et presse
    query3 = f'"{nom_public}" "{prenom}" {initiale}* (communiqué OR nomination OR presse)'
    
    all_content = []
    
    for i, query in enumerate([query1, query2, query3], 1):
        try:
            results = serper_search(query, num=5)
            
            for item in results[:3]:  # Top 3 par requête
                link = item.get("link", "")
                snippet = item.get("snippet", "")
                title = item.get("title", "")
                
                # Collecter snippet + titre (toujours)
                all_content.append(f"Source: {link}\nTitre: {title}\n{snippet}")
        except Exception as e:
            print(f"    ⚠️ Query {i}/3 failed: {e}")
            pass
    
    if not all_content:
        return {"director_name": "", "director_title": "", "confidence": 0}
    
    # Extraction LLM avec contexte enrichi
    context_text = "\n\n".join(all_content[:8])  # Max 8 sources
    
    prompt = f"""Tu es un assistant d'extraction d'information pour identifier des dirigeants d'organisations.

ORGANISATION: {gestionnaire_nom}
NOM PUBLIC: {nom_public}
PRÉNOM PARTIEL: {prenom} {initiale}.

CONTEXTE (résultats de recherche):
{context_text}

MISSION:
Identifier le NOM DE FAMILLE COMPLET du dirigeant (Directeur Général / Directrice Générale / Président(e)).

Le prénom est "{prenom}" et le nom commence par "{initiale}".

RÈGLES STRICTES:
1. Extraire UNIQUEMENT si le nom complet est clairement mentionné
2. Format: Prénom NOM (avec NOM en majuscules)
3. Le nom doit correspondre à l'initiale "{initiale}"
4. Donner le titre exact (Directeur général, Présidente, etc.)
5. Confidence: 90-100 si nom complet trouvé plusieurs fois, 70-85 si une seule mention

RÉPONSE (JSON strict):
{{
  "director_name": "Prénom NOM" ou "",
  "director_title": "Directeur général" ou "",
  "confidence": 0-100
}}"""

    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": groq_model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.0,
                "max_tokens": 200,
            },
            timeout=20,  # Réduire timeout de 30s à 20s
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        
        # Parse JSON
        content = content.strip()
        if content.startswith("```"):
            content = content.strip("`")
            content = re.sub(r"^\s*json\s*", "", content, flags=re.IGNORECASE).strip()
        
        try:
            result = json.loads(content)
        except Exception:
            m = re.search(r"\{.*\}", content, flags=re.DOTALL)
            if m:
                result = json.loads(m.group(0))
            else:
                result = {}
        
        # Vérifier que le nom correspond à l'initiale
        director_name = result.get("director_name", "")
        if director_name:
            # Extraire nom de famille (dernier mot en majuscules)
            nom_famille_match = re.search(r"\b([A-ZÉÈÊË]{2,})\b", director_name)
            if nom_famille_match:
                nom_famille = nom_famille_match.group(1)
                # Vérifier que ça commence par l'initiale
                if nom_famille[0] == initiale:
                    return {
                        "director_name": director_name,
                        "director_title": result.get("director_title", ""),
                        "confidence": result.get("confidence", 75)
                    }
        
        return {"director_name": "", "director_title": "", "confidence": 0}
    
    except Exception as e:
        print(f"    ❌ LLM error: {e}")
        return {"director_name": "", "director_title": "", "confidence": 0}


def is_initiale_pattern(nom: str) -> bool:
    """Détecte si un nom suit le pattern 'Prénom I' (initiale)."""
    if pd.isna(nom) or not nom:
        return False
    return bool(re.match(r"^[A-ZÉÈÊË][a-zéèêëàâäôö]+\s[A-ZÉÈÊË]$", nom.strip()))


def extract_prenom_initiale(nom: str) -> tuple[str, str]:
    """Extrait prénom et initiale depuis 'Prénom I'."""
    match = re.match(r"^([A-ZÉÈÊË][a-zéèêëàâäôö]+)\s([A-ZÉÈÊË])$", nom.strip())
    if match:
        return match.group(1), match.group(2)
    return "", ""


def main():
    parser = argparse.ArgumentParser(description="PASSE 1bis: Compléter noms avec initiales")
    parser.add_argument("--input", default="outputs/prospection_250_dirigeants_complet.xlsx")
    parser.add_argument("--output", default="outputs/prospection_250_dirigeants_complet_v2.xlsx")
    parser.add_argument("--sleep", type=float, default=0.5, help="Délai entre requêtes (secondes)")
    parser.add_argument("--test", action="store_true", help="Mode test (5 premiers)")
    args = parser.parse_args()
    
    # Charger données
    df = pd.read_excel(args.input)
    print(f"Loaded: {len(df)} gestionnaires")
    
    # Filtrer : dirigeant_source='linkedin_snippet' ET pattern initiale
    mask = (df["dirigeant_source"] == "linkedin_snippet") & df["dirigeant_nom"].apply(is_initiale_pattern)
    to_process = df[mask].copy()
    
    print(f"\nFound: {len(to_process)} dirigeants with initiale pattern")
    
    if args.test:
        to_process = to_process.head(5)
        print(f"TEST MODE: Processing only {len(to_process)} cases\n")
    
    # Traiter chaque cas
    found_count = 0
    
    for idx, row in to_process.iterrows():
        i = to_process.index.get_loc(idx) + 1
        
        gestionnaire_nom = row["gestionnaire_nom"]
        nom_public = row.get("nom_public", gestionnaire_nom)
        dirigeant_nom_actuel = row["dirigeant_nom"]
        
        prenom, initiale = extract_prenom_initiale(dirigeant_nom_actuel)
        
        print(f"[{i}/{len(to_process)}] {gestionnaire_nom[:50]}...")
        print(f"  Current: {dirigeant_nom_actuel} (initiale)")
        
        # Recherche approfondie
        result = search_full_name_with_llm(
            gestionnaire_nom=gestionnaire_nom,
            nom_public=nom_public,
            prenom=prenom,
            initiale=initiale
        )
        
        if result["director_name"]:
            # Nom complet trouvé !
            df.at[idx, "dirigeant_nom"] = result["director_name"]
            df.at[idx, "dirigeant_titre"] = result["director_title"]
            df.at[idx, "dirigeant_confidence"] = result["confidence"]
            df.at[idx, "dirigeant_source"] = "linkedin_llm_complete"
            
            print(f"  ✅ FOUND: {result['director_name']} - {result['director_title']} (conf. {result['confidence']})")
            found_count += 1
        else:
            print(f"  ❌ Not found (keeping initiale)")
        
        # Sleep
        time.sleep(args.sleep)
    
    # Sauvegarder
    df.to_excel(args.output, index=False)
    
    print("\n" + "=" * 60)
    print("✅ COMPLETED")
    print("=" * 60)
    print(f"Processed: {len(to_process)} dirigeants with initiales")
    print(f"Found full names: {found_count} ({found_count/len(to_process)*100:.1f}%)")
    print(f"\nOutput: {args.output}")


if __name__ == "__main__":
    main()
