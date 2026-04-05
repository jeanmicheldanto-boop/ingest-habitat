#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Habitat Intermédiaire – Import enrichi (avec prompts intégrés)
- Prompts IA éditables (extraction champs) et requêtes de recherche adaptées aux sources courantes (communes/CCAS/CIAS/associations/OPH).
- Websearch par défaut: Tavily (souvent le moins cher/gratuit pour dev) avec fallback optionnel SerpAPI.
"""
import os, re, json, time
from typing import Any, Dict, List, Optional, Tuple, Iterable

import pandas as pd
import numpy as np
import streamlit as st

try:
    import requests
    from bs4 import BeautifulSoup
except Exception:
    requests = None
    BeautifulSoup = None

try:
    import psycopg2
    from psycopg2.extras import execute_values
except Exception:
    psycopg2 = None

# IA clients
try:
    from openai import OpenAI
except Exception:
    OpenAI = None

try:
    from groq import Groq as GroqClient
except Exception:
    GroqClient = None

# ---------- UI ----------
st.set_page_config(page_title="Habitat Intermédiaire – Import enrichi", layout="wide")
st.title("🏗️ Habitat Intermédiaire – Import enrichi")

with st.expander("ℹ️ Guide rapide"):
    st.markdown("""
1) Chargez **un CSV unique** (ex: `data_40.csv`).  
2) Choisissez **Webscraping / IA / Websearch+IA**.  
3) Les **prompts** ci-dessous sont **intégrés et éditables**.  
4) Géocodez, normalisez, importez (statut `draft` ou `publie`) et exportez les CSV (y compris erreurs).
""")

# ---------- Sidebar: Connexion / Providers / Prompts ----------
with st.sidebar:
    st.header("🔐 PostgreSQL (Supabase)")
    host = st.text_input("Host", value="db.minwoumfgutampcgrcbr.supabase.co", placeholder="db.xxxxx.supabase.co")
    port = st.number_input("Port", 1, 65535, 5432)
    dbname = st.text_input("Database", value="postgres")
    user = st.text_input("User", value="postgres")
    password = st.text_input("Password", value="", type="password")
    sslmode = st.selectbox("SSL mode", ["require","verify-full","prefer","disable"], index=0)

    def build_dsn():
        return f"host={host} port={port} dbname={dbname} user={user} password={password} sslmode={sslmode}"

    st.divider()
    st.header("🧠 Enrichissement")
    enrich_mode = st.selectbox("Mode", ["Aucun","Webscraping","IA seule","Websearch + IA"], index=0)

    ai_vendor = st.selectbox("Fournisseur IA", ["OpenAI","Groq"], index=0)
    openai_key = st.text_input("OPENAI_API_KEY", type="password")
    groq_key = st.text_input("GROQ_API_KEY", type="password")
    default_model = "gpt-4o-mini" if ai_vendor == "OpenAI" else "llama-3-8b"
    ai_model = st.text_input("Modèle IA", value=default_model)

    st.subheader("🔎 Websearch (par défaut: Tavily)")
    search_vendor = st.selectbox("Search API", ["Tavily","SerpAPI"], index=0)  # Tavily par défaut
    tavily_key = st.text_input("TAVILY_API_KEY", type="password")
    serpapi_key = st.text_input("SERPAPI_KEY", type="password")

    st.subheader("📍 Géocodage")
    geocode_provider = st.selectbox("Provider", ["Nominatim (OSM)","Google","Mapbox"], index=0)
    google_key = st.text_input("GOOGLE_MAPS_API_KEY", type="password")
    mapbox_key = st.text_input("MAPBOX_API_KEY", type="password")

    st.divider()
    st.header("✍️ Prompts (éditables)")
    default_ai_prompt = """Tu es chargé d'extraire des faits fiables depuis sites officiels (mairie/ville/CCAS/CIAS, associations type ADMR, APAJH, APEI, GIHP, gestionnaires médico-sociaux), OPH/ESH, et pages d'établissements seniors.
Contexte: on alimente une base d'habitat intermédiaire pour seniors/PH en France.

Contraintes de sortie (JSON uniquement, valide, sans texte hors JSON):
{
    "gestionnaire": "texte",            // 'particulier' si personne physique (pas de nom perso), sinon nom d'organisme ou commune/CCAS
    "presentation": "texte 200-600c",   // synthèse neutre (pas marketing), sans emoji
    "email": "contact@domaine.tld",     // laisser null si inconnu
    "telephone": "01 23 45 67 89",      // format FR standard ou null
    "site_web": "https://...",
    "mention_avp": true|false,           // true si AVP / 'aide à la vie partagée' explicitement mentionnée
    "restauration": {
        "kitchenette": true|false,
        "resto_collectif_midi": true|false,
        "resto_collectif": true|false,
        "portage_repas": true|false
    },
    "logements_types": [
        {
            "libelle": "T1|T2|T3",
            "surface_min": number,
            "surface_max": number,
            "meuble": true|false,
            "pmr": true|false,
            "domotique": true|false,
            "nb_unit": integer,
            "plain_pied": true|false
        }
    ],
    "tarification": {
        "fourchette_prix": "euro|deux_euros|trois_euros",
        "prix_min": number,
        "prix_max": number
    },
    "services": ["activités organisées","espace_partage","conciergerie","personnel de nuit","commerces à pied","médecin intervenant"]
}

Rappels:
- Ne jamais inventer d'email/téléphone. Si plusieurs, privilégier contact/accueil ou établissement.
- Si page indique clairement 'aide à la vie partagée' ou 'AVP' pour cet habitat → mention_avp = true.
- Si gestionnaire semble un particulier (accueil familial, personne physique) → "gestionnaire": "particulier".
- Pour restauration, logements_types, tarification et services, ne renseigner que si l'information est explicitement présente ou déductible du site ou de la page.
"""
    ai_prompt = st.text_area("Prompt extraction IA", value=default_ai_prompt, height=220)

    default_search_query = """{nom} {commune} site officiel CCAS CIAS mairie association résidence autonomie habitat inclusif MARPA EHPAD résidence services seniors gestionnaire contact téléphone email"""
    search_query_template = st.text_input("Template requête websearch", value=default_search_query)

# ---------- Helpers ----------
def clean_text(x: Any) -> Optional[str]:
    if x is None: return None
    if isinstance(x, float) and np.isnan(x): return None
    s = str(x).strip()
    return s or None

def normalize_phone_fr(s: Optional[str]) -> Optional[str]:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s)
    digits = re.sub(r"\D", "", s)
    if digits.startswith("33") and len(digits) == 11:
        digits = "0" + digits[2:]
    if len(digits) == 10:
        return " ".join([digits[i:i+2] for i in range(0, 10, 2)])
    return ""

def normalize_email(s: Optional[str]) -> Optional[str]:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    s = str(s).strip()
    return s if re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", s) else None

DEPARTEMENT_TO_REGION = {
    "40":"Nouvelle-Aquitaine","32":"Occitanie","65":"Occitanie","64":"Nouvelle-Aquitaine","09":"Occitanie","33":"Nouvelle-Aquitaine",
}

ALLOWED_SOUS_CATEGORIES = {
    "colocation avec services":"colocation avec services",
    "habitat intergénérationnel":"habitat intergénérationnel",
    "accueil familial":"accueil familial",
    "maison d’accueil familial":"maison d’accueil familial",
    "résidence services seniors":"résidence services seniors",
    "résidence service seniors":"résidence services seniors",
    "residence services seniors":"résidence services seniors",
    "habitat inclusif":"habitat inclusif",
    "béguinage":"béguinage",
    "beguinage":"béguinage",
    "village seniors":"village seniors",
    "résidence autonomie":"résidence autonomie",
    "residence autonomie":"résidence autonomie",
    "marpa":"MARPA","MARPA":"MARPA",
    "habitat regroupé":"habitat regroupé",
    "habitat alternatif":"habitat alternatif",
    "logement adapté":"logement adapté",
}

SERVICES_WHITELIST = ["activités organisées","espace_partage","conciergerie","personnel de nuit","commerces à pied","médecin intervenant"]

PUBLIC_CIBLE_MAP = {
    "personnes_agees":"personnes_agees",
    "personnes âgées":"personnes_agees",
    "seniors":"personnes_agees",
    "personnes_handicapees":"personnes_handicapees",
    "personnes handicapées":"personnes_handicapees",
    "alzheimer":"alzheimer_accessible",
    "alzheimer_accessible":"alzheimer_accessible",
    "mixte":"mixtes","mixtes":"mixtes","intergénérationnel":"mixtes","intergenerationnel":"mixtes",
}

NON_ELIGIBLE_SET = {"résidence services seniors","résidence autonomie","accueil familial","béguinage","village seniors"}

def normalize_public_cible(csv_value: Optional[str]) -> Optional[str]:
    if not csv_value: return None
    parts = re.split(r"[;,/|]", str(csv_value))
    out = []
    for p in parts:
        k = PUBLIC_CIBLE_MAP.get(p.strip().lower())
        if k and k not in out:
            out.append(k)
    return ",".join(out) if out else None

def parse_departement(code_postal: Optional[str], departement_field: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    dep_code = None; dep_label = None
    if departement_field and re.search(r"\(\d{2,3}\)", str(departement_field)):
        m = re.search(r"\((\d{2,3})\)", str(departement_field)); dep_code = m.group(1).zfill(2); dep_label = departement_field
    if not dep_code and code_postal and len(code_postal)>=2:
        dep_code = code_postal[:2]; dep_label = f"{dep_code}"
    region = DEPARTEMENT_TO_REGION.get(dep_code)
    return dep_label, region

def normalize_sous_categorie(x: Optional[str]) -> Optional[str]:
    if not x: return None
    return ALLOWED_SOUS_CATEGORIES.get(str(x).strip().lower())

def deduce_habitat_type(sous_cat: Optional[str], csv_default: Optional[str]) -> Optional[str]:
    if sous_cat in ["résidence autonomie","résidence services seniors","MARPA"]:
        return "residence"
    if sous_cat in ["colocation avec services","habitat intergénérationnel","habitat inclusif","habitat alternatif","accueil familial","maison d’accueil familial"]:
        return "habitat_partage"
    if sous_cat in ["béguinage","village seniors","logement adapté","habitat regroupé"]:
        return "logement_independant"
    if csv_default in ["habitat_partage","residence","logement_independant"]:
        return csv_default
    return None

def deduce_eligibilite(sous_cat: Optional[str], mention_avp: bool, csv_hint: Optional[str]) -> Optional[str]:
    if csv_hint in ["avp_eligible","non_eligible","a_verifier"]:
        return csv_hint
    if sous_cat in NON_ELIGIBLE_SET:
        return "non_eligible"
    if mention_avp:
        return "avp_eligible"
    if sous_cat == "habitat inclusif":
        return "a_verifier"
    return "a_verifier"

def looks_like_particulier(gestionnaire: Optional[str], sous_cat: Optional[str]) -> bool:
    if not gestionnaire: return False
    if sous_cat in ["accueil familial","maison d’accueil familial"]:
        if not re.search(r"(mairie|commune|ccas|cias|asso|association|sarl|sas|sa|oph|office|habitat|metropole|communaut[eé])", gestionnaire.lower()):
            if re.search(r"^[A-Za-zÀ-ÖØ-öø-ÿ'\-]+\s+[A-Za-zÀ-ÖØ-öø-ÿ'\-]+$", gestionnaire):
                return True
    return False

# ---------- IA & Search ----------
def ai_extract_fields(prompt_text: str, vendor: str, model: str, openai_key: Optional[str], groq_key: Optional[str]) -> Optional[Dict[str,Any]]:
    schema_hint = {
        "type":"object",
        "properties":{
            "gestionnaire":{"type":"string"},
            "presentation":{"type":"string"},
            "email":{"type":"string"},
            "telephone":{"type":"string"},
            "site_web":{"type":"string"},
            "mention_avp":{"type":"boolean"},
            "restauration":{
                "type":"object",
                "properties":{
                    "kitchenette":{"type":"boolean"},
                    "resto_collectif_midi":{"type":"boolean"},
                    "resto_collectif":{"type":"boolean"},
                    "portage_repas":{"type":"boolean"}
                }
            },
            "logements_types":{
                "type":"array",
                "items":{
                    "type":"object",
                    "properties":{
                        "libelle":{"type":"string"},
                        "surface_min":{"type":"number"},
                        "surface_max":{"type":"number"},
                        "meuble":{"type":"boolean"},
                        "pmr":{"type":"boolean"},
                        "domotique":{"type":"boolean"},
                        "nb_unit":{"type":"integer"},
                        "plain_pied":{"type":"boolean"}
                    }
                }
            },
            "tarification":{
                "type":"object",
                "properties":{
                    "fourchette_prix":{"type":"string"},
                    "prix_min":{"type":"number"},
                    "prix_max":{"type":"number"}
                }
            },
            "services":{
                "type":"array",
                "items":{"type":"string"}
            }
        },
        "additionalProperties": False
    }
    try:
        if vendor=="OpenAI" and openai_key and OpenAI is not None:
            client = OpenAI(api_key=openai_key)
            out = client.chat.completions.create(
                model=model, temperature=0,
                messages=[
                    {"role":"system","content":ai_prompt},
                    {"role":"user","content":prompt_text + "\nRéponds en JSON strict: " + json.dumps(schema_hint)}
                ]
            )
            text = out.choices[0].message.content
            return json.loads(text)
        if vendor=="Groq" and groq_key and GroqClient is not None:
            client = GroqClient(api_key=groq_key)
            out = client.chat.completions.create(
                model=model, temperature=0,
                messages=[
                    {"role":"system","content":ai_prompt},
                    {"role":"user","content":prompt_text + "\nRéponds en JSON strict: " + json.dumps(schema_hint)}
                ]
            )
            text = out.choices[0].message.content
            return json.loads(text)
    except Exception as e:
        st.write(f"IA error: {e}")
    return None

def search_urls(query: str, vendor: str, tavily_key: Optional[str], serpapi_key: Optional[str]) -> List[str]:
    urls: List[str] = []
    if requests is None: return urls
    try:
        if vendor=="Tavily" and tavily_key:
            r = requests.post("https://api.tavily.com/search", json={"api_key":tavily_key,"query":query,"include_answer":False,"max_results":6}, timeout=20)
            if r.ok:
                for item in r.json().get("results", []):
                    u = item.get("url"); 
                    if u: urls.append(u)
        elif vendor=="SerpAPI" and serpapi_key:
            r = requests.get("https://serpapi.com/search.json", params={"q":query,"engine":"google","api_key":serpapi_key,"num":6}, timeout=20)
            if r.ok:
                for item in r.json().get("organic_results", []):
                    u = item.get("link")
                    if u: urls.append(u)
    except Exception as e:
        st.write(f"Search error: {e}")
    # Filtre léger pour privilégier sources courantes
    preferred = []
    others = []
    for u in urls:
        if re.search(r"(mairie|\.fr/ccas|ccas|cias|\.gouv\.fr|\.fr/ville|\.fr/commune|admr|apajh|apei|gihp|\.oph|office\-public|esh|\.asso\.fr|\.org)", u, flags=re.I):
            preferred.append(u)
        else:
            others.append(u)
    return preferred + others

def scrape_extract_basic(url: str) -> Dict[str, Optional[str]]:
    out = {
        "email":None, "telephone":None, "site_web":None, "mention_avp":False, "presentation":None, "gestionnaire":None,
        "restauration":{"kitchenette":False,"resto_collectif_midi":False,"resto_collectif":False,"portage_repas":False},
        "logements_types":[],
        "tarification":{"fourchette_prix":None,"prix_min":None,"prix_max":None},
        "services":[]
    }
    if requests is None or BeautifulSoup is None: return out
    try:
        r = requests.get(url, timeout=20, headers={"User-Agent":"Mozilla/5.0"})
        if not r.ok: return out
        html = r.text
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)
        # telephone
        m = re.findall(r"(?:\+33\s?|\b0)(?:\d[\s\.\-]?){9}\b", text)
        if m: out["telephone"] = normalize_phone_fr(m[0])
        # email
        m = re.findall(r"[A-Za-z0-9\.\-_]+@[A-Za-z0-9\.\-]+\.[A-Za-z]{2,}", text)
        if m: out["email"] = normalize_email(m[0])
        out["site_web"] = url
        # gestionnaire heuristique
        if re.search(r"\b(CCAS|CIAS|Mairie|Ville de|Commune de|Office Public|OPH|Office de l'Habitat|ADMR|APAJH|APEI|GIHP)\b", text, flags=re.I):
            title = soup.title.get_text(strip=True) if soup.title else ""
            out["gestionnaire"] = title[:120] or "commune/CCAS"
        # AVP mention
        if re.search(r"\b(aide à la vie partagée|AVP)\b", text, flags=re.I):
            out["mention_avp"] = True
        # présentation (tronquée)
        if len(text)>80: out["presentation"] = text[:800]
        # restauration
        for key in out["restauration"]:
            if re.search(key.replace("_"," "), text, flags=re.I):
                out["restauration"][key] = True
        # logements_types
        for match in re.finditer(r"(T1|T2|T3|studio|f1|f2|f3|deux pièces|trois pièces|t1bis|t2bis)", text, flags=re.I):
            lib = match.group(1).lower()
            if lib in ["t1","studio","f1","t1bis"]: lib = "T1"
            elif lib in ["t2","deux pièces","f2","t2bis"]: lib = "T2"
            elif lib in ["t3","trois pièces","f3"]: lib = "T3"
            out["logements_types"].append({"libelle":lib})
        # tarification
        prixs = re.findall(r"(\d{3,5}) ?€", text)
        prixs = [int(p) for p in prixs if p.isdigit()]
        if prixs:
            out["tarification"]["prix_min"] = min(prixs)
            out["tarification"]["prix_max"] = max(prixs)
            ref = min(prixs)
            if ref < 750: out["tarification"]["fourchette_prix"] = "euro"
            elif 750 <= ref <= 1500: out["tarification"]["fourchette_prix"] = "deux_euros"
            else: out["tarification"]["fourchette_prix"] = "trois_euros"
        # services
        whitelist = ["activités organisées","espace_partage","conciergerie","personnel de nuit","commerces à pied","médecin intervenant"]
        for s in whitelist:
            if re.search(s, text, flags=re.I):
                out["services"].append(s)
    except Exception as e:
        st.write(f"Scrape error({url}): {e}")
    return out

# ---------- Geocode ----------
def geocode(address: str, provider: str, google_key: Optional[str], mapbox_key: Optional[str]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    if requests is None: return None, None, None
    try:
        if provider.startswith("Nominatim"):
            r = requests.get("https://nominatim.openstreetmap.org/search", params={"q":address,"format":"json","addressdetails":1,"limit":1}, headers={"User-Agent":"HI-import/1.0"}, timeout=20)
            if r.ok and r.json():
                j = r.json()[0]; return float(j["lon"]), float(j["lat"]), "street"
        if provider=="Google" and google_key:
            r = requests.get("https://maps.googleapis.com/maps/api/geocode/json", params={"address":address,"key":google_key}, timeout=20)
            if r.ok and r.json().get("results"):
                loc = r.json()["results"][0]["geometry"]["location"]; lt = r.json()["results"][0]["geometry"]["location_type"]
                return float(loc["lng"]), float(loc["lat"]), "rooftop" if "ROOFTOP" in lt else "street"
        if provider=="Mapbox" and mapbox_key:
            r = requests.get(f"https://api.mapbox.com/geocoding/v5/mapbox.places/{address}.json", params={"access_token":mapbox_key,"limit":1}, timeout=20)
            if r.ok and r.json().get("features"):
                coords = r.json()["features"][0]["center"]; return float(coords[0]), float(coords[1]), "street"
    except Exception as e:
        st.write(f"Géocodage error: {e}")
    return None, None, None

# ---------- Load CSV ----------
st.header("📥 CSV d'entrée")
csv_file = st.file_uploader("Charger le CSV source", type=["csv"])

if csv_file is not None:
    df_src = pd.read_csv(csv_file)
    st.dataframe(df_src.head(10), use_container_width=True)

    # Ajout automatique des colonnes manquantes pour enrichissement et import conforme
    required_cols = [
        "gestionnaire", "presentation", "adresse_l1", "code_postal", "commune", "telephone", "region", "departement",
        "public_cible", "email", "site_web", "habitat_type", "eligibilite_statut", "sous_categories",
        "kitchenette", "resto_collectif_midi", "resto_collectif", "portage_repas",
        "libelle", "surface_min", "surface_max", "meuble", "pmr", "domotique", "nb_unit", "plain_pied",
        "fourchette_prix", "prix_min", "prix_max", "services"
    ]
    for col in required_cols:
        if col not in df_src.columns:
            df_src[col] = None

    # Column names
    col_nom="nom"; col_commune="commune"; col_cp="code_postal"; col_gestionnaire="gestionnaire"
    col_adresse="adresse_l1"; col_tel="telephone"; col_email="email"; col_site="site_web"
    col_souscat="sous_categories"; col_habitat_type="habitat_type"; col_elig="eligibilite_avp"
    col_presentation="presentation"; col_departement="departement"; col_source="source"; col_public_cible="public_cible"

    work = df_src.copy()
    work[col_nom] = work.get(col_nom, pd.Series(dtype=str)).apply(clean_text)
    work[col_adresse] = work.get(col_adresse, pd.Series(dtype=str)).apply(clean_text)
    work[col_commune] = work.get(col_commune, pd.Series(dtype=str)).apply(lambda x: clean_text(x).upper() if clean_text(x) else None)
    work[col_cp] = work.get(col_cp, pd.Series(dtype=str)).astype(str).str.replace(r"\D","", regex=True).str[:5]
    work[col_tel] = work.get(col_tel, pd.Series(dtype=str)).apply(normalize_phone_fr)
    work[col_email] = work.get(col_email, pd.Series(dtype=str)).apply(normalize_email)
    work[col_site] = work.get(col_site, pd.Series(dtype=str)).apply(clean_text)
    work[col_public_cible] = work.get(col_public_cible, pd.Series(dtype=str)).apply(normalize_public_cible)

    dep_list=[]; reg_list=[]
    for _, row in work.iterrows():
        dep_label, region = parse_departement(row.get(col_cp), row.get(col_departement))
        dep_list.append(dep_label if dep_label else row.get(col_departement))
        reg_list.append(region if region else None)
    work["departement_norm"]=dep_list; work["region_norm"]=reg_list

    def build_query(row: pd.Series) -> str:
        template = search_query_template
        return template.format(nom=row.get(col_nom,""), commune=row.get(col_commune,""))

    st.header("🧩 Enrichissement")
    do_geocode = st.checkbox("Activer le géocodage", value=True)
    import_status = st.selectbox("Statut d'import", ["draft","publie"], index=0)

    enriched_rows=[]; errors=[]
    # Nouvelle logique d'enrichissement et mapping secondaire
    enriched_rows = []
    rest_rows = []
    tar_rows = []
    logt_rows = []
    svc_rows = []
    errors = []
    for idx, row in work.iterrows():
        rec = {
            "nom": row.get(col_nom),
            "presentation": row.get(col_presentation),
            "adresse_l1": row.get(col_adresse),
            "code_postal": row.get(col_cp),
            "commune": row.get(col_commune),
            "departement": row.get("departement_norm") or row.get(col_departement),
            "region": row.get("region_norm"),
            "telephone": row.get(col_tel),
            "email": row.get(col_email),
            "site_web": row.get(col_site),
            "gestionnaire": row.get(col_gestionnaire),
            "public_cible": row.get(col_public_cible),
            "habitat_type_csv": row.get(col_habitat_type),
            "sous_categories_csv": row.get(col_souscat),
            "eligibilite_avp_csv": row.get(col_elig),
            "source": row.get(col_source),
    }
    mention_avp = False
    enrich_data = {}
    try:
                    # Scraping enrichissement
                    if enrich_mode in ["Webscraping","Websearch + IA"] and rec["site_web"]:
                        sc = scrape_extract_basic(rec["site_web"])
                        for k in ["email","telephone","site_web","presentation","gestionnaire"]:
                            rec[k] = rec[k] or sc.get(k)
                        mention_avp = mention_avp or bool(sc.get("mention_avp"))
                        enrich_data = sc

                    # Websearch enrichissement
                    if enrich_mode in ["Websearch + IA"]:
                        urls = search_urls(build_query(row), search_vendor, tavily_key, serpapi_key)
                        for u in urls[:3]:
                            sc = scrape_extract_basic(u)
                            rec["email"] = rec["email"] or sc.get("email")
                            rec["telephone"] = rec["telephone"] or sc.get("telephone")
                            rec["site_web"] = rec["site_web"] or sc.get("site_web")
                            rec["presentation"] = rec["presentation"] or sc.get("presentation")
                            rec["gestionnaire"] = rec["gestionnaire"] or sc.get("gestionnaire")
                            mention_avp = mention_avp or bool(sc.get("mention_avp"))
                            # merge enrich_data
                            for key in ["restauration","logements_types","tarification","services"]:
                                if key in sc and sc[key]:
                                    enrich_data[key] = sc[key]

                    # IA enrichissement
                    if enrich_mode in ["IA seule","Websearch + IA"]:
                        prompt_user = f"""Nom: {rec['nom']}\nCommune: {rec['commune']}\nAdresse: {rec.get('adresse_l1') or ''}\nSite: {rec.get('site_web') or ''}\nObjectif: extraire gestionnaire/presentation/email/téléphone/site, mention_avp, restauration, logements_types, tarification, services pour cet établissement (sources publiques probables: mairie/ville/CCAS/CIAS, associations médico-sociales/gest., OPH/ESH)."""
                        ai_json = ai_extract_fields(prompt_user, ai_vendor, ai_model, openai_key, groq_key)
                        if ai_json:
                            for k in ["gestionnaire","presentation","email","telephone","site_web"]:
                                v = ai_json.get(k)
                                if k=="telephone": v = normalize_phone_fr(v)
                                if k=="email": v = normalize_email(v)
                                rec[k] = rec[k] or v
                            mention_avp = mention_avp or bool(ai_json.get("mention_avp", False))
                            for key in ["restauration","logements_types","tarification","services"]:
                                if key in ai_json and ai_json[key]:
                                    enrich_data[key] = ai_json[key]

                    sous_cat = normalize_sous_categorie(rec.get("sous_categories_csv"))
                    rec["sous_categorie"] = sous_cat or None
                    rec["habitat_type"] = deduce_habitat_type(rec["sous_categorie"], rec.get("habitat_type_csv"))
                    rec["eligibilite_statut"] = deduce_eligibilite(rec["sous_categorie"], mention_avp, rec.get("eligibilite_avp_csv"))

                    if looks_like_particulier(rec.get("gestionnaire"), rec.get("sous_categorie")):
                        rec["gestionnaire"] = "particulier"

                    lon, lat, prec = None, None, None
                    if do_geocode and rec.get("adresse_l1") and rec.get("code_postal") and rec.get("commune"):
                        addr = f"{rec['adresse_l1']}, {rec['code_postal']} {rec['commune']}, France"
                        lon, lat, prec = geocode(addr, geocode_provider, google_key, mapbox_key)
                    # Correction nan pour géoloc
                    try:
                        lon = float(lon) if lon is not None and not pd.isna(lon) else None
                        lat = float(lat) if lat is not None and not pd.isna(lat) else None
                    except Exception:
                        lon, lat = None, None
                    rec["lon"] = lon; rec["lat"] = lat; rec["geocode_precision"] = prec

                    rec["telephone"] = normalize_phone_fr(rec.get("telephone"))
                    rec["email"] = normalize_email(rec.get("email"))
                    rec["site_web"] = clean_text(rec.get("site_web"))
                    rec["presentation"] = clean_text(rec.get("presentation"))
                    rec["gestionnaire"] = clean_text(rec.get("gestionnaire"))
                    rec["public_cible"] = normalize_public_cible(rec.get("public_cible"))

                    # Mapping secondaire enrichi
                    # Restauration
                    ro = enrich_data.get("restauration", {})
                    ro_row = {"nom": rec["nom"]}
                    for k in ["kitchenette","resto_collectif_midi","resto_collectif","portage_repas"]:
                        ro_row[k] = bool(ro.get(k, False))
                    rest_rows.append(ro_row)
                    # Tarification
                    tar = enrich_data.get("tarification", {})
                    tar_row = {"nom": rec["nom"], "fourchette_prix": tar.get("fourchette_prix"), "prix_min": tar.get("prix_min"), "prix_max": tar.get("prix_max")}
                    tar_rows.append(tar_row)
                    # Logements types
                    logts = enrich_data.get("logements_types", [])
                    for logt in logts:
                        logt_row = {"nom": rec["nom"]}
                        for k in ["libelle","surface_min","surface_max","meuble","pmr","domotique","nb_unit","plain_pied"]:
                            logt_row[k] = logt.get(k)
                        logt_rows.append(logt_row)
                    # Services
                    svcs = enrich_data.get("services", [])
                    for s in svcs:
                        svc_rows.append({"nom": rec["nom"], "libelle": s, "present": True})

                    enriched_rows.append(rec)
    except Exception as e:
        errors.append({"row_index":int(idx),"nom":rec.get("nom"),"erreur":str(e)})

    # Créer le DataFrame enrichi
    df_enriched = pd.DataFrame(enriched_rows) if enriched_rows else pd.DataFrame()

    # ---------- Split tables ----------
    etab_cols = ["nom","presentation","adresse_l1","code_postal","commune","departement","region","telephone","email","site_web","gestionnaire","public_cible","habitat_type","eligibilite_statut","source"]
    df_etablissements = df_enriched[etab_cols].copy() if not df_enriched.empty else pd.DataFrame(columns=etab_cols)
    df_etablissements["statut_editorial"] = import_status
    df_etablissements["pays"] = "FR"

    df_sous_categories = df_enriched[["nom","sous_categorie"]].dropna().rename(columns={"sous_categorie":"libelle"})

    # Restauration (bools)
    rest_cols = ["kitchenette","resto_collectif_midi","resto_collectif","portage_repas"]
    rest_rows = []
    for _, r in df_src.iterrows():
        nm = r.get(col_nom)
        row = {"nom":nm}
        for c in rest_cols:
            val = r.get(c)
            if isinstance(val, str):
                vb = val.strip().lower() in ["1","true","vrai","oui","y","yes"]
            else:
                vb = bool(val) if pd.notna(val) else False
            row[c]=vb
        rest_rows.append(row)
    df_restauration = pd.DataFrame(rest_rows)

    # Tarifs
    def deduce_fourchette(pm, px, f):
        if f in ["euro","deux_euros","trois_euros"]: return f
        ref = pm if pd.notna(pm) else (px if pd.notna(px) else None)
        if ref is None: return None
        if ref < 750: return "euro"
        if 750 <= ref <= 1500: return "deux_euros"
        return "trois_euros"
    tar_rows=[]
    for _, r in df_src.iterrows():
        nm = r.get(col_nom)
        pm = pd.to_numeric(r.get("prix_min"), errors="coerce")
        px = pd.to_numeric(r.get("prix_max"), errors="coerce")
        f = clean_text(r.get("fourchette_prix"))
        tar_rows.append({"nom":nm,"fourchette_prix":deduce_fourchette(pm,px,f),"prix_min":pm,"prix_max":px})
    df_tarifications = pd.DataFrame(tar_rows)

    # Logements types
    if {"libelle","surface_min","surface_max","meuble","pmr","domotique","nb_unit","plain_pied"}.issubset(df_src.columns):
        df_logements = df_src[["nom","libelle","surface_min","surface_max","meuble","pmr","domotique","nb_unit","plain_pied"]].copy()
        def norm_libelle(x):
            if not pd.notna(x): return None
            s=str(x).strip().lower()
            if s in ["t1","studio","f1","t1bis"]: return "T1"
            if s in ["t2","deux pièces","f2","t2bis"]: return "T2"
            if s in ["t3","trois pièces","f3"]: return "T3"
            return None
        df_logements["libelle"]=df_logements["libelle"].apply(norm_libelle)
        df_logements=df_logements.rename(columns={"nb_unit":"nb_unites"})
    else:
        df_logements=pd.DataFrame(columns=["nom","libelle","surface_min","surface_max","meuble","pmr","domotique","nb_unites","plain_pied"])

    # Services depuis CSV si colonne "services" existe (liste séparée)
    if "services" in df_src.columns:
        svc_rows=[]
        for i, r in df_src.iterrows():
            nm = r.get(col_nom)
            items = [s.strip().lower() for s in str(r.get("services","")).split(",") if s]
            for s in items:
                if s in ["activites organisees","activités organisées","activites","activités"]:
                    svc_rows.append({"nom":nm,"libelle":"activités organisées","present":True})
                elif s in ["espace partage","espace_partage"]:
                    svc_rows.append({"nom":nm,"libelle":"espace_partage","present":True})
                elif s in ["conciergerie"]:
                    svc_rows.append({"nom":nm,"libelle":"conciergerie","present":True})
                elif s in ["personnel de nuit","nuit"]:
                    svc_rows.append({"nom":nm,"libelle":"personnel de nuit","present":True})
                elif s in ["commerces a pied","commerces à pied","commerces"]:
                    svc_rows.append({"nom":nm,"libelle":"commerces à pied","present":True})
                elif s in ["medecin intervenant","médecin intervenant","medecin"]:
                    svc_rows.append({"nom":nm,"libelle":"médecin intervenant","present":True})
        df_services = pd.DataFrame(svc_rows) if svc_rows else pd.DataFrame(columns=["nom","libelle","present"])
    else:
        df_services = pd.DataFrame(columns=["nom","libelle","present"])

    # ---------- Stats ----------
    st.header("📊 Stats")
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Etablissements", len(df_etablissements))
    c2.metric("Emails manquants", int(df_etablissements["email"].isna().sum()))
    c3.metric("Téléphones manquants", int(df_etablissements["telephone"].isna().sum()))
    c4.metric("'a_verifier'", int((df_etablissements["eligibilite_statut"]=="a_verifier").sum()) if "eligibilite_statut" in df_etablissements else 0)

    st.write("Par sous_catégorie:")
    st.dataframe(df_sous_categories["libelle"].value_counts(dropna=False).rename_axis("sous_categorie").to_frame("nb"))

    st.write("Par eligibilite_statut:")
    if "eligibilite_statut" in df_etablissements:
        st.dataframe(df_etablissements["eligibilite_statut"].value_counts(dropna=False).rename_axis("eligibilite_statut").to_frame("nb"))

    # ---------- Exports ----------
    st.header("⬇️ Exports CSV")
    st.download_button("CSV enrichi (all)", df_enriched.to_csv(index=False).encode("utf-8"), "enriched_all.csv", "text/csv")
    st.download_button("etablissements.csv", df_etablissements.to_csv(index=False).encode("utf-8"), "etablissements.csv","text/csv")
    st.download_button("sous_categories.csv", df_sous_categories.to_csv(index=False).encode("utf-8"), "sous_categories.csv","text/csv")
    st.download_button("services.csv", df_services.to_csv(index=False).encode("utf-8"), "services.csv","text/csv")
    st.download_button("restauration.csv", df_restauration.to_csv(index=False).encode("utf-8"), "restauration.csv","text/csv")
    st.download_button("tarifications.csv", df_tarifications.to_csv(index=False).encode("utf-8"), "tarifications.csv","text/csv")
    st.download_button("logements_types.csv", df_logements.to_csv(index=False).encode("utf-8"), "logements_types.csv","text/csv")

    if errors:
        df_err = pd.DataFrame(errors)
        st.error("Erreurs d'enrichissement:")
        st.dataframe(df_err, use_container_width=True)
        st.download_button("Journal erreurs enrichissement", df_err.to_csv(index=False).encode("utf-8"), "enrichment_errors.csv","text/csv")
else:
    st.info("Chargez un CSV pour démarrer.")
