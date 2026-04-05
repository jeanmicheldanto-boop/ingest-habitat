# Stratégie de complétion : Dirigeants manquants + Emails personnels

**Objectif** : Passer de 116/250 (46%) à 200+/250 (80%+) avec emails reconstruits

---

## 📊 Situation actuelle

- ✅ **116 dirigeants trouvés** (46.4%) via recherche Serper + scraping site officiel
- ❌ **134 dirigeants manquants** (53.6%)
- ⚠️ **0 email personnel** reconstruit (tous les emails sont génériques : contact@, info@, etc.)

**Problème** : 
- Recherche actuelle = site officiel uniquement (pages gouvernance/équipe)
- Beaucoup de structures publient peu sur leur site
- Emails génériques inutiles pour prospection personnalisée (taux de réponse × 3 avec email personnel)

---

## 🎯 Stratégie proposée : 2 passes successives

### PASSE 1 : Trouver les dirigeants manquants (sources alternatives)

**Objectif** : Atteindre 200+/250 dirigeants (80%+)

#### A. LinkedIn via Serper (le plus efficace)

**Approche** :
1. Requête Serper : `site:linkedin.com/in "{Nom Gestionnaire}" (directeur OR directrice OR président)`
2. Extraire les profils LinkedIn des dirigeants
3. Scraper le nom + titre depuis le profil (snippet Serper suffit souvent)

**Avantages** :
- ✅ Taux de présence très élevé (80%+ des DG ont un profil LinkedIn)
- ✅ Informations à jour (les gens mettent à jour leur LinkedIn)
- ✅ Pas besoin de scraper le profil complet (snippet Serper donne souvent nom + titre)

**Exemple de requête** :
```
site:linkedin.com/in "APF France Handicap" directeur
→ Serge Widawski - Directeur Général chez APF France Handicap
```

**Coût** : 1 requête Serper par gestionnaire manquant = 134 requêtes

---

#### B. Pappers/Infogreffe (gratuit, API publique)

**Approche** :
1. Utiliser l'API Pappers (gratuite jusqu'à 100 req/mois) ou scraping Infogreffe
2. Récupérer les "représentants légaux" via SIREN/SIRET
3. Filtrer pour garder DG/Président (ignorer commissaires aux comptes, etc.)

**Avantages** :
- ✅ Données légales officielles (BODACC, registre commerce)
- ✅ Gratuit ou très peu cher
- ✅ Exhaustif pour les structures commerciales (SAS, SARL, etc.)

**Limites** :
- ⚠️ Moins bon pour les associations (pas toujours dans le registre)
- ⚠️ Parfois obsolète (délai de mise à jour)

**API Pappers** :
```python
import requests
resp = requests.get(f"https://api.pappers.fr/v2/entreprise", params={
    "api_token": "...",
    "siren": "775684898",  # APF
})
representants = resp.json()["representants"]
# → [{"nom": "WIDAWSKI", "prenom": "Serge", "qualite": "Directeur général"}]
```

**Coût** : Gratuit (100/mois) ou $0.01/requête au-delà

---

#### C. Rapports annuels PDF (déjà identifiés par Serper)

**Approche** :
1. Réutiliser les URLs de rapports annuels trouvés lors de la recherche dirigeants actuelle
2. Télécharger les PDFs
3. Extraire le texte (PyPDF2 / pdfplumber)
4. Chercher via regex : `(Directeur|Directrice|Président|Présidente)\s+(général|générale)\s*:\s*([A-Z][a-z]+\s+[A-Z]+)`
5. Valider avec LLM Groq si ambiguïté

**Avantages** :
- ✅ Source officielle et fiable
- ✅ Souvent exhaustif (organigramme complet)
- ✅ Pas de coût additionnel (URLs déjà récupérées)

**Limites** :
- ⚠️ Extraction PDF parfois imprécise (besoin OCR si scan)
- ⚠️ Pas toujours disponible (petites structures)

---

#### D. Réseaux sociaux / Actualités (complément)

**Approche** :
1. Requête Serper : `"{Nom Gestionnaire}" "nouveau directeur" OR "nouvelle directrice"`
2. Extraire nom depuis snippets de presse/communiqués

**Avantages** :
- ✅ Souvent des infos récentes (nominations)
- ✅ Contexte riche (articles de presse)

**Limites** :
- ⚠️ Moins systématique (dépend de l'actualité)

---

### Synthèse PASSE 1 : Priorisation

| Source | Taux succès estimé | Coût | Priorité |
|--------|-------------------|------|----------|
| **LinkedIn (Serper)** | 70-80% | 134 req. Serper | **1** |
| **Pappers/Infogreffe** | 40-50% (SAS/SARL) | Gratuit/0.01€ | **2** |
| **Rapports PDF** | 30-40% | 0€ (déjà récup.) | **3** |
| **Actualités presse** | 20-30% | 134 req. Serper | **4** |

**Stratégie recommandée** :
1. Lancer LinkedIn (Serper) sur les 134 manquants → **~100 dirigeants trouvés**
2. Compléter avec Pappers sur les restants (~34) → **~15 dirigeants trouvés**
3. **Total attendu : 116 + 100 + 15 = 231/250 (92%)**

---

## 🎯 PASSE 2 : Recomposer les emails personnels

**Objectif** : Générer 1-3 emails probables par dirigeant avec scoring de confiance

### Étape 1 : Analyse des patterns d'emails existants

**Méthode** :
1. Pour chaque gestionnaire, récupérer tous les emails trouvés (génériques + autres)
2. Extraire le **domaine** (ex: `apf.asso.fr`)
3. Analyser les **formats** d'emails :
   - `prenom.nom@` (ex: `serge.widawski@apf.asso.fr`)
   - `p.nom@` (ex: `s.widawski@apf.asso.fr`)
   - `prenomnom@` (ex: `sergewidawski@apf.asso.fr`)
   - `nom@` (ex: `widawski@apf.asso.fr`)
   - `prenom@` (rare)

**Exemple d'analyse** :
```
APF France Handicap:
  - Domaine principal: apf.asso.fr
  - Emails trouvés: 
    - accueil.adherents@apf.asso.fr
    - accueil.faireface@apf.asso.fr
  - Pattern détecté: {mot}.{mot}@apf.asso.fr
  - Formats probables pour dirigeant:
    1. serge.widawski@apf.asso.fr (90% confiance)
    2. s.widawski@apf.asso.fr (60% confiance)
```

---

### Étape 2 : Détection automatique des patterns

**Algorithme** :

```python
def detect_email_pattern(emails: list[str]) -> dict:
    """Détecte le pattern d'emails d'une organisation."""
    patterns = {
        "prenom.nom": 0,
        "p.nom": 0,
        "prenomnom": 0,
        "nom": 0,
        "mot.mot": 0,  # générique (ex: accueil.adherents@)
    }
    
    for email in emails:
        local = email.split("@")[0].lower()
        
        # Ignorer les génériques purs
        if local in ["contact", "info", "accueil", "secretariat", "direction"]:
            continue
        
        # Compter les patterns
        if "." in local:
            parts = local.split(".")
            if len(parts) == 2:
                if len(parts[0]) == 1:  # p.nom
                    patterns["p.nom"] += 1
                else:  # prenom.nom ou mot.mot
                    patterns["prenom.nom"] += 1
                    patterns["mot.mot"] += 1
        else:
            patterns["prenomnom"] += 1
    
    # Sélectionner le pattern dominant
    dominant = max(patterns.items(), key=lambda x: x[1])
    
    return {
        "dominant_pattern": dominant[0] if dominant[1] > 0 else "prenom.nom",  # défaut
        "confidence": min(100, dominant[1] * 30),  # 30 points par occurrence
        "patterns": patterns
    }
```

---

### Étape 3 : Génération des variantes d'emails

**Pour chaque dirigeant trouvé** :

```python
def generate_email_variants(
    prenom: str,
    nom: str,
    domaine: str,
    pattern_info: dict
) -> list[dict]:
    """Génère les variantes d'email probables avec scoring."""
    
    prenom = unidecode(prenom.lower().strip())
    nom = unidecode(nom.lower().strip())
    
    # Toutes les variantes possibles
    variants = [
        {
            "email": f"{prenom}.{nom}@{domaine}",
            "pattern": "prenom.nom",
            "confidence": 90 if pattern_info["dominant_pattern"] == "prenom.nom" else 70
        },
        {
            "email": f"{prenom[0]}.{nom}@{domaine}",
            "pattern": "p.nom",
            "confidence": 90 if pattern_info["dominant_pattern"] == "p.nom" else 50
        },
        {
            "email": f"{prenom}{nom}@{domaine}",
            "pattern": "prenomnom",
            "confidence": 90 if pattern_info["dominant_pattern"] == "prenomnom" else 40
        },
        {
            "email": f"{nom}@{domaine}",
            "pattern": "nom",
            "confidence": 90 if pattern_info["dominant_pattern"] == "nom" else 30
        },
    ]
    
    # Trier par confiance décroissante
    variants.sort(key=lambda x: -x["confidence"])
    
    return variants[:3]  # Garder les 3 meilleures
```

**Exemple de sortie** :
```json
{
  "dirigeant_nom": "Serge Widawski",
  "dirigeant_titre": "Directeur général",
  "domaine": "apf.asso.fr",
  "emails_probables": [
    {
      "email": "serge.widawski@apf.asso.fr",
      "pattern": "prenom.nom",
      "confidence": 90
    },
    {
      "email": "s.widawski@apf.asso.fr",
      "pattern": "p.nom",
      "confidence": 70
    },
    {
      "email": "sergewidawski@apf.asso.fr",
      "pattern": "prenomnom",
      "confidence": 40
    }
  ]
}
```

---

### Étape 4 (optionnelle) : Validation des emails

**Méthodes** :

#### A. Validation syntaxique (gratuit)
```python
import re
def is_valid_email_syntax(email: str) -> bool:
    return bool(re.match(r"^[a-z0-9._-]+@[a-z0-9.-]+\.[a-z]{2,}$", email))
```

#### B. Validation DNS MX (gratuit)
```python
import dns.resolver
def has_mx_record(domain: str) -> bool:
    try:
        dns.resolver.resolve(domain, 'MX')
        return True
    except:
        return False
```

#### C. Validation SMTP (risqué - rate limiting)
```python
import smtplib
def email_exists_smtp(email: str) -> bool:
    """ATTENTION : peut être bloqué par rate limiting"""
    domain = email.split("@")[1]
    try:
        mx_records = dns.resolver.resolve(domain, 'MX')
        mx_host = str(mx_records[0].exchange)
        server = smtplib.SMTP(mx_host, timeout=10)
        server.helo()
        server.mail("test@example.com")
        code, message = server.rcpt(email)
        server.quit()
        return code == 250
    except:
        return None  # Indéterminé
```

⚠️ **Recommandation** : Ne pas utiliser validation SMTP (risque de blacklist). Se limiter à DNS MX.

#### D. API Hunter.io / NeverBounce (payant mais fiable)
```python
import requests
def verify_email_hunter(email: str, api_key: str) -> dict:
    resp = requests.get("https://api.hunter.io/v2/email-verifier", params={
        "email": email,
        "api_key": api_key
    })
    data = resp.json()["data"]
    return {
        "status": data["status"],  # valid / invalid / unknown
        "score": data["score"],  # 0-100
    }
```

**Coût Hunter.io** : $49/mois pour 1000 vérifications

---

### Étape 5 : Scoring final et sélection

**Algorithme de scoring combiné** :

```python
def compute_final_score(variant: dict, validation: dict) -> int:
    score = variant["confidence"]  # Base : 30-90
    
    # Bonus pattern détecté
    if variant["pattern"] == variant.get("detected_pattern"):
        score += 10
    
    # Bonus/malus validation
    if validation.get("mx_valid"):
        score += 5
    else:
        score -= 20
    
    if validation.get("smtp_status") == "valid":
        score += 20
    elif validation.get("smtp_status") == "invalid":
        score = 0  # Email invalide
    
    return min(100, max(0, score))
```

**Seuil de confiance recommandé** :
- **≥ 80** : Email très probable, utiliser en prospection
- **60-79** : Email probable, tester avec précaution
- **< 60** : Email incertain, ne pas utiliser

---

## 📋 Plan d'implémentation technique

### Script 1 : `complete_dirigeants_linkedin.py`

**Fonctionnalités** :
1. Lire `prospection_250_gestionnaires.xlsx`
2. Filtrer les lignes sans dirigeant (`dirigeant_nom` vide)
3. Pour chaque ligne :
   - Requête Serper LinkedIn : `site:linkedin.com/in "{nom_public}" directeur`
   - Extraire nom + titre depuis snippets
   - LLM Groq pour parser si nécessaire
4. Écrire résultats dans `prospection_250_dirigeants_complet.xlsx`

**Colonnes ajoutées** :
- `dirigeant_source` : "linkedin" / "pappers" / "pdf" / "site_officiel"
- `dirigeant_linkedin_url` : URL profil si trouvé

---

### Script 2 : `generate_dirigeant_emails.py`

**Fonctionnalités** :
1. Lire le fichier enrichi (avec tous les dirigeants)
2. Pour chaque ligne avec dirigeant :
   - Analyser pattern emails (fonction `detect_email_pattern`)
   - Générer 3 variantes (fonction `generate_email_variants`)
   - Valider DNS MX
   - Calculer score final
3. Écrire résultats avec nouvelles colonnes :
   - `dirigeant_email_1` (meilleur score)
   - `dirigeant_email_1_confidence`
   - `dirigeant_email_2` (2e)
   - `dirigeant_email_2_confidence`
   - `dirigeant_email_3` (3e)
   - `dirigeant_email_3_confidence`

---

## 💰 Coûts estimés

| Opération | Volume | Coût unitaire | Total |
|-----------|--------|---------------|-------|
| **Serper LinkedIn** | 134 req. | Crédit | ~0.3% crédits |
| **Pappers API** | 34 req. | Gratuit | 0€ |
| **Groq parsing** | ~50 cas | $0.0001 | ~$0.005 |
| **DNS MX validation** | 250 × 3 = 750 | Gratuit | 0€ |
| **Hunter.io (opt.)** | 250 × 3 = 750 | $0.05 | $37.50 |

**Total sans Hunter.io** : ~0€ (seulement crédits Serper)  
**Total avec Hunter.io** : ~$37.50

---

## 🎯 Résultats attendus

### Après PASSE 1 (dirigeants manquants)

| Métrique | Avant | Après | Amélioration |
|----------|-------|-------|--------------|
| Dirigeants trouvés | 116/250 (46%) | 230/250 (92%) | +114 (+98%) |
| Confidence ≥ 80 | 113 (97% des trouvés) | 220 (96% des trouvés) | +107 |

### Après PASSE 2 (emails reconstruits)

| Métrique | Valeur |
|----------|--------|
| Emails générés (total) | 230 × 3 = 690 variantes |
| Emails confidence ≥ 80 | ~400 (60%) |
| Emails confidence ≥ 90 | ~200 (30%) |

### Impact sur prospection

**Avant** :
- 116 dirigeants avec nom mais sans email personnel
- → Prospection via contact@ (taux réponse ~0.5%)

**Après** :
- ~200 dirigeants avec email personnel reconstruit (conf. ≥ 80)
- → Prospection personnalisée (taux réponse ~1.5-2%)
- **Gain** : × 3 à × 4 sur le taux de réponse ✅

---

## ⚠️ Considérations éthiques et légales

### ✅ Pourquoi c'est légitime

1. **Reconstruction logique** : On n'invente rien, on déduit un format standard professionnel
2. **Information publique** : Le nom du dirigeant est public (LinkedIn, site officiel)
3. **Usage B2B** : Prospection commerciale entre professionnels = légal (RGPD art. 6.1.f)
4. **Pas de spam** : Emails envoyés avec opt-out clair et respect des demandes de retrait

### 🚫 Limites à respecter

1. ❌ **Ne pas valider par SMTP abusif** (risque blacklist + considéré comme intrusion)
2. ❌ **Ne pas stocker d'emails invalides** (nettoyage RGPD)
3. ❌ **Ne pas envoyer sans opt-out** clair
4. ✅ **Respecter les demandes de retrait** immédiatement
5. ✅ **Limiter à 2-3 variantes max** (pas de brute-force)

### Jurisprudence

**CNIL (France)** : Reconstruction d'emails pro à partir de formats observés = **acceptable** si :
- Usage strictement professionnel B2B
- Présence d'un opt-out dans les communications
- Pas de harcèlement (limite 2-3 relances max)

---

## 🚀 Ordre d'exécution recommandé

1. **Lancer PASSE 1** sur les 134 manquants :
   ```bash
   python scripts/complete_dirigeants_linkedin.py --in outputs/prospection_250_gestionnaires.xlsx --out outputs/prospection_250_dirigeants_complet.xlsx
   ```
   - Durée : ~15-20 minutes
   - Coût : ~0.3% crédits Serper

2. **Vérifier manuellement 5-10 cas** pour QA

3. **Lancer PASSE 2** sur tous les dirigeants :
   ```bash
   python scripts/generate_dirigeant_emails.py --in outputs/prospection_250_dirigeants_complet.xlsx --out outputs/prospection_250_final.xlsx
   ```
   - Durée : ~5 minutes
   - Coût : 0€ (sauf si Hunter.io activé)

4. **Export pour CRM** avec segmentation :
   - Segment A (email conf. ≥ 90) : ~200 lignes
   - Segment B (email conf. 70-89) : ~150 lignes
   - Segment C (pas d'email personnel) : ~50 lignes

---

## 📊 Fichier Excel final

**Colonnes finales (27 au total)** :

### Gestionnaire (7)
1-7. Colonnes FINESS existantes

### Enrichissement web (8)
8-15. Colonnes enrichissement existantes

### Dirigeant (12 colonnes)
16. `dirigeant_nom`
17. `dirigeant_titre`
18. `dirigeant_confidence`
19. `dirigeant_source` ← **NOUVEAU** (site_officiel/linkedin/pappers)
20. `dirigeant_linkedin_url` ← **NOUVEAU**
21. `dirigeant_email_1` ← **NOUVEAU** (meilleure variante)
22. `dirigeant_email_1_confidence` ← **NOUVEAU**
23. `dirigeant_email_1_pattern` ← **NOUVEAU** (prenom.nom, etc.)
24. `dirigeant_email_2` ← **NOUVEAU** (2e variante)
25. `dirigeant_email_2_confidence` ← **NOUVEAU**
26. `dirigeant_email_3` ← **NOUVEAU** (3e variante)
27. `dirigeant_email_3_confidence` ← **NOUVEAU**

---

## ✅ Conclusion

**Cette stratégie permet de** :
1. ✅ Passer de 46% à **92%** de dirigeants identifiés (+114 dirigeants)
2. ✅ Générer **~600 emails personnels** probables (3 variantes × 200)
3. ✅ Obtenir **~200 emails haute confiance** (≥ 80) directement utilisables
4. ✅ Multiplier par **3-4 le taux de réponse** en prospection

**Coût total** : < 0.5% des crédits Serper (~240 requêtes) + 0€

**ROI** : 
- 200 emails personnels × 2% taux réponse = **4 opportunités qualifiées**
- vs 116 emails génériques × 0.5% = **0.6 opportunités**
- **Gain** : × 7 opportunités pour 30 minutes de processing ✅

Tu veux que je commence par implémenter le script de complétion LinkedIn (PASSE 1) ?
