
# 🏗️ Pipeline Intelligent d’Extraction des Habitats Intermédiaires Seniors  
### **Version 3.0 — Haute précision Mixtral — Extraction multipasse — Normalisation robuste**

---

## 📘 1. Objectif du Pipeline v3.0

Ce pipeline permet d’extraire automatiquement **tous les établissements d’habitat intermédiaire seniors** d’un département donné, avec une fiabilité maximale et un coût minimal.

Il combine :

- **Web Search Serper**  
- **Classification IA sur snippet**  
- **Scraping ciblé**  
- **Extraction multipasse stricte (Mixtral‑8x7B‑32768)**  
- **Normalisation catégorielle v3.0**  
- **Enrichissement : email, téléphone, site, présentation 150–200 mots**  
- **Déduplication intelligente**  
- **Export CSV normalisé**

Objectifs :

- 0 hallucination  
- < 0,01 € / département  
- < 3 min / département  
- F1 extraction → très élevé (Mixtral multipasse)

---

# 🧱 2. Architecture Globale

```
┌──────────────────────────────────────────┐
│ MODULE 1 — SERPER (requêtes OR v3.0)     │
└──────────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────┐
│ MODULE 2 — SNIPPET CLASSIFIER (LLM 8B)   │
└──────────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────┐
│ MODULE 3 — SCRAPING ciblé (ScrapingBee)  │
└──────────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────┐
│ MODULE 4 — EXTRACTION MULTIPASSE         │
│     Mixtral‑8x7B‑32768                   │
└──────────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────┐
│ MODULE 5 — NORMALISATION CATÉGORIELLE    │
└──────────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────┐
│ MODULE 6 — ENRICHISSEMENT + PRÉSENTATION │
└──────────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────┐
│ MODULE 7 — DÉDUPLICATION INTELLIGENTE    │
└──────────────────────────────────────────┘
                     ↓
┌──────────────────────────────────────────┐
│ EXPORT CSV                               │
└──────────────────────────────────────────┘
```

---

# 🌐 3. MODULE 1 — Requêtes SERPER v3.0

## 3.1 Requête principale OR (habitats alternatifs)

```
("habitat inclusif" OR "habitat partagé" OR "habitat intergénérationnel"
OR "colocation seniors" OR "béguinage" OR "village seniors"
OR "maison partagée" OR "résidence services seniors"
OR "MARPA")
{departement_name}
```

Paramètres :

```
gl=fr
hl=fr
lr=lang_fr
num=10
```

---

## 3.2 Requête dédiée “service d'accueil familial ”
*(limitée à 2 résultats)*

```
("service accueil familial") {departement_name}
```

Paramètres :

```
num=2
```

---

# 🟣 4. MODULE 2 — Classification sur Snippet (LLM 8B)

Ce module évite 80% du bruit **avant** tout scraping.

### Modèle :
**llama‑3.1‑8B‑instant**

### Prompt complet :

```
Tu es un classificateur strict.  
Tu dois dire si ce résultat Google décrit PROBABLEMENT 
un établissement concret relevant de l’habitat intermédiaire pour seniors.

Catégories recherchées :
- Habitat inclusif
- Habitat intergénérationnel  
- Béguinage
- Village seniors
- Colocation seniors
- Maison d'accueil familial (type CetteFamille)
- Maison partagée
- Résidence services seniors (hors EHPAD)
- MARPA

Réponds OUI uniquement si le snippet laisse entendre 
qu’un établissement concret (lieu, structure, maison, résidence) est décrit.

Réponds NON si :
- c’est un article généraliste
- une actualité
- une page institutionnelle
- un PDF, rapport ou fiche non locale
- le snippet n’évoque aucun lieu concret

TITRE : {title}
SNIPPET : {snippet}

Réponds strictement : OUI ou NON.
```

---

# 🔵 5. MODULE 3 — Scraping ciblé (ScrapingBee)

On scrape **uniquement** les résultats classés OUI.

Contenu retourné :

```
- texte principal
- HTML nettoyé
- liens utiles
- metadata
```

Ce contenu devient le **contexte du module 4**.

---

# 🟠 6. MODULE 4 — Extraction multipasse (Mixtral‑8x7B‑32768)

Chaque champ est extrait via **une requête dédiée**, afin de minimiser les hallucinations.

---

## 6.1 Extraction du nom

```
Extrait UNIQUEMENT le nom précis de l’établissement décrit dans ce texte.

RÈGLES :
- N'invente rien.
- Ne reformule pas.
- Le nom doit apparaître explicitement.
- Si plusieurs établissements : garde celui décrit en détail.
- Si doute : renvoie "".

Texte :
{page_content}

Réponds :
{"nom": "...", "confidence": 0-100}
```

---

## 6.2 Extraction code postal + commune

### Étape 1 — regex

```
(\d{5})\s+([A-Z][A-Za-zÀ-ÿ\-\s']+)
```

### Étape 2 — fallback LLM

```
Extrait la commune EXACTE de l’établissement.  
Si imprécis → "".

Texte :
{page_content}

Réponse :
{"commune": "...", "confidence": 0-100}
```

---

## 6.3 Extraction du gestionnaire

```
Extrait le gestionnaire UNIQUEMENT s'il est explicitement mentionné.

VALIDES :
- Ages & Vie
- CetteFamille
- CCAS de {ville}
- Habitat & Humanisme

INTERDIT :
- déduire depuis URL
- confondre propriétaire du site / auteur
- inférer depuis un réseau non mentionné

Texte :
{page_content}

Réponse :
{"gestionnaire": "...", "confidence": 0-100}
```

---

## 6.4 Validation établissement

```
nom.conf ≥ 50  
commune.conf ≥ 50 ou regex OK  
code_postal présent  
gestionnaire.conf ≥ 20 (ou vide acceptable)
```

Score global :

```
confidence_score = moyenne(nom, commune, gestionnaire)
```

---

# 🧩 7. MODULE 5 — Normalisation Catégorielle v3.0

## Catégories autorisées

### habitat_type = residence
- Résidence autonomie  
- MARPA  
- Résidence services seniors  

### habitat_type = habitat_partage
- Habitat inclusif  
- Habitat intergénérationnel  
- Maison d’accueil familial (CetteFamille)  
- Maison partagée  

### habitat_type = logement_independant
- Béguinage  
- Village seniors  
- Colocation avec services (Ages & Vie)

---

## Mapping automatique (pseudo‑code)

```python
def normalize_category(name, description, gestionnaire):

    text = f"{name} {description} {gestionnaire}".lower()

    if "résidence autonomie" in text or "marpa" in text:
        return "Résidence autonomie", "residence"

    if "résidence services" in text:
        return "Résidence services seniors", "residence"

    if "béguinage" in text:
        return "Béguinage", "logement_independant"

    if "village seniors" in text:
        return "Village seniors", "logement_independant"

    if gestionnaire.strip().lower() == "ages & vie":
        return "Colocation avec services", "logement_independant"

    if "habitat inclusif" in text:
        return "Habitat inclusif", "habitat_partage"

    if "intergénérationnel" in text:
        return "Habitat intergénérationnel", "habitat_partage"

    if "maison d'accueil familial" in text or "cettefamille" in text:
        return "Maison d'accueil familial", "habitat_partage"

    if "maison partagée" in text:
        return "Maison partagée", "habitat_partage"

    return "Habitat inclusif", "habitat_partage"
```

---

# 🟡 8. MODULE 6 — Enrichissement (présentation incluse)

## Modèle :
**llama‑3.1‑8B‑instant**

## Actions :
- recherche email  
- recherche téléphone  
- site Web  
- **Génération de la présentation 150–200 mots**  

---

## Prompt Présentation v3.0

```
Rédige une présentation claire, factuelle, professionnelle, 
de 150 à 200 mots, pour décrire l’établissement suivant.

N'utilise que les informations fournies.  
N’ajoute aucune donnée non citée dans le texte.  
Ne mentionne jamais d’agrégateurs.

Données :
Nom : {nom}
Commune : {commune} ({code_postal})
Gestionnaire : {gestionnaire}
Type : {sous_categorie} / {habitat_type}

Contenu extrait :
{page_excerpt}

Structure souhaitée :
- Introduction courte
- Fonctionnement / services / public
- Phrase finale sur l’ancrage territorial

Réponds uniquement avec la présentation.
```

---

# 🟢 9. MODULE 7 — Déduplication intelligente

Matching multi‑critères :

- nom (distance)  
- commune  
- gestionnaire  
- CP  
- contact (email/téléphone)  

Fusion selon la meilleure confiance.

---

# 📦 10. Export CSV

## Champs :

| Champ | Description |
|-------|-------------|
| nom | Nom établissement |
| commune | Commune |
| code_postal | CP |
| gestionnaire | Gestionnaire (ou vide) |
| adresse_l1 | Optionnel |
| telephone | Enrichissement |
| email | Enrichissement |
| site_web | Enrichissement |
| sous_categories | Catégorie normalisée |
| habitat_type | Catégorie normalisée |
| presentation | Texte 150–200 mots |
| confidence_score | Score moyen |
| departement | Code |
| source | URL |
| date_extraction | YYYY-MM-DD |

---

# 🎯 11. Performances attendues

| Étape | Durée | Coût | Qualité |
|------|--------|--------|---------|
| Serper | 1 s | 0 | Couverture |
| Snippet classifier | 0.3 s | quasi 0 | Filtre 80% |
| Scraping | 1.5–2 s | ~0.003€ | Pages utiles |
| Extraction Mixtral | ~1.2 s | ~0.0004€ | Très haute précision |
| Normalisation | instant | 0 | Cohérente |
| Présentation | 0.6 s | ~0.0002€ | Propre |
| Dedup | instant | 0 | Dataset clean |

---

# ✔️ README v3.0 complet  
Prêt pour GitHub.
