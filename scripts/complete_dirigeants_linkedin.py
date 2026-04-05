"""
Script de complétion des dirigeants manquants via LinkedIn (Serper).

Objectif : Passer de 116/250 (46%) à 230+/250 (92%) dirigeants identifiés.

Stratégie :
1. Lire le fichier enrichi existant
2. Filtrer les lignes sans dirigeant (dirigeant_nom vide ou NaN)
3. Pour chaque ligne :
   - Requête Serper : site:linkedin.com/in "{nom_public}" (directeur OR directrice OR président)
   - Extraire nom + titre depuis les snippets LinkedIn
   - Parser avec LLM Groq si nécessaire
4. Écrire résultats avec colonnes supplémentaires : dirigeant_source, dirigeant_linkedin_url
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
from dotenv import load_dotenv

load_dotenv(override=True)


def serper_search(query: str, num: int = 5) -> list[dict[str, Any]]:
    api_key = os.getenv("SERPER_API_KEY", "")
    if not api_key:
        raise RuntimeError("SERPER_API_KEY missing in environment")

    resp = requests.post(
        "https://google.serper.dev/search",
        headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
        json={"q": query, "gl": "fr", "hl": "fr", "num": num},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("organic", []) or []


def extract_director_from_linkedin_snippets(
    snippets: list[dict[str, Any]],
    gestionnaire_nom: str,
    groq_model: str = ""
) -> dict[str, Any]:
    """Extrait nom du dirigeant depuis snippets LinkedIn."""
    
    # Essayer d'abord extraction simple depuis les snippets
    for item in snippets:
        title = (item.get("title") or "").strip()
        snippet = (item.get("snippet") or "").strip()
        link = (item.get("link") or "").strip()
        
        # Vérifier que c'est bien un profil LinkedIn
        if "linkedin.com/in/" not in link:
            continue
        
        # Pattern simple : "Prénom NOM - Titre"
        # Exemple: "Serge WIDAWSKI - Directeur Général chez APF France Handicap"
        match = re.search(r"([A-ZÉÈÊË][a-zéèêëàâäôö]+(?:\s+[A-ZÉÈÊË][a-zéèêëàâäôö]+)?)\s+([A-ZÉÈÊË]+)", title)
        if match:
            prenom = match.group(1).strip()
            nom = match.group(2).strip()
            
            # Extraire le titre
            titre_match = re.search(r"(Directeur|Directrice|Président|Présidente|Délégué|Déléguée)(?:\s+[Gg]énéral|e)?", title + " " + snippet, re.IGNORECASE)
            titre = titre_match.group(0).strip() if titre_match else ""
            
            return {
                "director_name": f"{prenom} {nom}",
                "director_title": titre,
                "linkedin_url": link,
                "confidence": 80,
                "source": "linkedin_snippet"
            }
    
    # Si extraction simple échoue, utiliser LLM Groq
    if groq_model:
        api_key = os.getenv("GROQ_API_KEY", "")
        if api_key and snippets:
            try:
                snippets_text = "\n".join([
                    f"- {item.get('title', '')}: {item.get('snippet', '')}"
                    for item in snippets[:5]
                ])
                
                prompt = f"""Tu es un assistant d'extraction d'information depuis LinkedIn.

ORGANISATION: {gestionnaire_nom}

RÉSULTATS LINKEDIN:
{snippets_text}

MISSION:
Extraire le nom du Directeur Général / Directrice Générale / Président(e) depuis ces résultats LinkedIn.

RÈGLES:
1. N'extraire QUE si clairement mentionné
2. Format: Prénom NOM (avec majuscules NOM)
3. Donner le titre exact (Directeur général, Présidente, etc.)

RÉPONSE (JSON strict):
{{
  "director_name": "Prénom NOM" ou "",
  "director_title": "Directeur général" ou "",
  "confidence": 0-100
}}"""

                resp = requests.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    json={
                        "model": groq_model,
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": 0.0,
                        "max_tokens": 200,
                    },
                    timeout=30,
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
                
                if result.get("director_name"):
                    return {
                        "director_name": result["director_name"],
                        "director_title": result.get("director_title", ""),
                        "linkedin_url": snippets[0].get("link", "") if snippets else "",
                        "confidence": result.get("confidence", 70),
                        "source": "linkedin_llm"
                    }
            except Exception as e:
                print(f"  ⚠️ LLM extraction failed: {e}")
    
    return {
        "director_name": "",
        "director_title": "",
        "linkedin_url": "",
        "confidence": 0,
        "source": ""
    }


def search_director_linkedin(
    gestionnaire_nom: str,
    nom_public: str,
    groq_model: str,
    sleep_s: float = 0.3
) -> dict[str, Any]:
    """Recherche le dirigeant via LinkedIn (Serper)."""
    
    # Utiliser nom_public si disponible, sinon gestionnaire_nom
    search_name = nom_public if nom_public and str(nom_public) != "nan" else gestionnaire_nom
    
    # Requête LinkedIn ciblée
    query = f'site:linkedin.com/in "{search_name}" (directeur OR directrice OR président OR présidente OR "directeur général" OR "directrice générale")'
    
    try:
        results = serper_search(query, num=10)
        if sleep_s:
            time.sleep(sleep_s)
        
        if not results:
            return {
                "director_name": "",
                "director_title": "",
                "linkedin_url": "",
                "confidence": 0,
                "source": ""
            }
        
        # Extraire depuis snippets
        extraction = extract_director_from_linkedin_snippets(results, search_name, groq_model)
        return extraction
        
    except Exception as e:
        print(f"  ❌ Search failed: {e}")
        return {
            "director_name": "",
            "director_title": "",
            "linkedin_url": "",
            "confidence": 0,
            "source": ""
        }


def main():
    parser = argparse.ArgumentParser(description="Complete missing directors via LinkedIn search")
    parser.add_argument("--in", dest="in_path", default="outputs/prospection_250_gestionnaires.xlsx")
    parser.add_argument("--out", dest="out_path", default="outputs/prospection_250_dirigeants_complet.xlsx")
    parser.add_argument("--groq-model", dest="groq_model", default=os.getenv("GROQ_MODEL", "llama-3.1-8b-instant"))
    parser.add_argument("--sleep", dest="sleep_s", type=float, default=0.3)
    parser.add_argument("--limit", type=int, default=0, help="Limit number of rows to process (0=all)")
    parser.add_argument("--test", action="store_true", help="Test mode: process only first 5 missing")
    
    args = parser.parse_args()
    
    # Lire fichier existant
    df = pd.read_excel(args.in_path)
    print(f"📊 Loaded {len(df)} gestionnaires")
    
    # Filtrer les lignes sans dirigeant
    missing_mask = df["dirigeant_nom"].isna() | (df["dirigeant_nom"] == "")
    missing = df[missing_mask].copy()
    print(f"🔍 Found {len(missing)} gestionnaires without director")
    
    if args.test:
        missing = missing.head(5)
        print(f"🧪 TEST MODE: Processing only {len(missing)} rows")
    elif args.limit > 0:
        missing = missing.head(args.limit)
        print(f"⚠️ LIMIT: Processing only {len(missing)} rows")
    
    # Initialiser nouvelles colonnes si nécessaire
    if "dirigeant_source" not in df.columns:
        df["dirigeant_source"] = ""
    if "dirigeant_linkedin_url" not in df.columns:
        df["dirigeant_linkedin_url"] = ""
    
    # Traiter chaque ligne manquante
    found_count = 0
    for i, (idx, row) in enumerate(missing.iterrows(), start=1):
        gestionnaire_nom = row["gestionnaire_nom"]
        nom_public = row.get("nom_public", gestionnaire_nom)
        
        print(f"[{i}/{len(missing)}] Searching LinkedIn for {gestionnaire_nom[:60]}...")
        
        result = search_director_linkedin(
            gestionnaire_nom=gestionnaire_nom,
            nom_public=nom_public,
            groq_model=args.groq_model,
            sleep_s=args.sleep_s
        )
        
        if result["director_name"]:
            df.at[idx, "dirigeant_nom"] = result["director_name"]
            df.at[idx, "dirigeant_titre"] = result["director_title"]
            df.at[idx, "dirigeant_confidence"] = result["confidence"]
            df.at[idx, "dirigeant_source"] = result["source"]
            df.at[idx, "dirigeant_linkedin_url"] = result["linkedin_url"]
            found_count += 1
            print(f"  ✅ Found: {result['director_name']} - {result['director_title']} (conf. {result['confidence']})")
        else:
            print(f"  ❌ Not found")
    
    # Sauvegarder
    df.to_excel(args.out_path, index=False)
    
    print(f"\n{'='*60}")
    print(f"✅ COMPLETED")
    print(f"{'='*60}")
    print(f"Processed: {len(missing)} gestionnaires")
    print(f"Found: {found_count} directors ({found_count/len(missing)*100:.1f}%)")
    print(f"Total directors now: {df['dirigeant_nom'].notna().sum()}/{len(df)} ({df['dirigeant_nom'].notna().sum()/len(df)*100:.1f}%)")
    print(f"\nOutput: {args.out_path}")


if __name__ == "__main__":
    main()
