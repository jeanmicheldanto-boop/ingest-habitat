# MVP - Automatisation Pipeline Habitat Seniors - Lot-et-Garonne

## 🎯 **OBJECTIF**
Développer un MVP pour automatiser la collecte des habitats intermédiaires seniors du Lot-et-Garonne et atteindre **80% de la qualité** du fichier de référence `data_47.csv` (29 établissements).

## 📊 **ANALYSE FICHIER RÉFÉRENCE**

### Établissements par type dans data_47.csv :
- **Résidences autonomie** : 8 établissements (28%)
- **MARPA** : 8 établissements (28%) 
- **Résidences services seniors** : 6 établissements (21%)
- **Habitat inclusif/partagé** : 7 établissements (24%)

### Qualité actuelle à reproduire :
- ✅ **Excellente** : Noms, communes, téléphones, types
- ✅ **Bonne** : Emails (90% remplis), sites web
- ⚠️ **Variable** : Gestionnaires (parfois incomplets)
- ❌ **Manquante** : Adresses précises, codes postaux, éligibilité AVP, présentation

### Schema cible (d'après le prompt original) :
```
nom,commune,code_postal,gestionnaire,adresse_l1,telephone,email,site_web,
sous_categories,habitat_type,eligibilite_avp,presentation,departement,
source,date_extraction,public_cible
```

## 🔄 **STRATÉGIE HYBRIDE MVP**

### **Phase 1 : Bulk officiel (70% des établissements)**
**Sources** : 
- Annuaire gouvernemental : `https://www.pour-les-personnes-agees.gouv.fr/annuaire-residences-autonomie/lot-et-garonne-47`
- Couvre : Résidences autonomie + MARPA

**Méthode** :
- Scraping simple (BeautifulSoup + requests)
- Parsing direct des données structurées
- ✅ **Avantage** : Données complètes et officielles

### **Phase 2 : Chaînes privées (15% des établissements)**  
**Sources** :
- Sites des grandes chaînes identifiées dans data_47.csv :
  - DOMITYS : `domitys.fr`
  - Espace & Vie : `residences-espaceetvie.fr` 
  - API Résidence : `api-residence.fr`
  - Happy Senior : `residenceshappysenior.fr`
  - Senioriales : `senioriales.com`

**Méthode** :
- Scraping ciblé par département (URL pattern : `/lot-et-garonne-47/` etc.)
- Extraction via IA (Groq) pour normalisation

### **Phase 3 : Structures alternatives (15% des établissements)**
**Sources** :
- Ages & Vie : `agesetvie.com` (habitat inclusif)
- CetteFamille : `cettefamille.com` (accueil familial)  
- UDAF 47 : `udaf47.fr` (habitat inclusif)
- Conseil départemental 47 : `lotetgaronne.fr`

**Méthode** :
- Google Search API pour découverte
- Scraping intelligent + extraction IA
- Validation croisée

## 🛠️ **STACK TECHNIQUE OPTIMISÉ**

### **Scraping :**
- **ScrapingBee** (plan Freelancer €29/mois) : 50k requêtes/mois
- **Fallback** : BeautifulSoup + rotating proxies gratuits

### **IA pour extraction :**
- **Groq (Llama 3.1 70B)** : €0,59/1M tokens
- **Estimation** : ~100k tokens par département = €0,06/département

### **Search API :**
- **Serper.dev** : €50/mois pour 10k recherches (€0,005/requête)
- **Alternative** : Google Custom Search (€5/1k requêtes - plus cher)

### **Proxy/Residential :**
- **Bright Data** : $500/mois (trop cher pour MVP)
- **Solution MVP** : Rotation IP gratuite + ScrapingBee

## 💰 **ESTIMATION COÛTS MVP (1 département)**

| Service | Coût mensuel | Usage MVP | Coût réel MVP |
|---------|--------------|-----------|---------------|
| **ScrapingBee** | €29/mois | ~200 requêtes | €0,12 |
| **Groq API** | Pay-per-use | ~100k tokens | €0,06 |
| **Serper.dev** | €50/mois | ~50 recherches | €0,25 |
| **Total** | | | **€0,43/département** |

*Note : Coûts mensuels d'abonnement amortis sur usage intensif (20+ départements/mois)*

## ⚡ **PERFORMANCES VISÉES**

- **Durée d'exécution** : 15-30 minutes par département
- **Précision** : 80%+ vs fichier référence 
- **Couverture** : 90%+ des établissements du fichier référence
- **Fiabilité** : 95%+ (données sources officielles majoritaires)

## 🎯 **CRITÈRES DE SUCCÈS MVP**

### Quantitatifs :
- [ ] **23+ établissements** retrouvés (80% des 29 de référence)
- [ ] **95%+ noms/téléphones** corrects
- [ ] **80%+ emails** retrouvés  
- [ ] **100% types/sous-catégories** bien classifiés

### Qualitatifs :
- [ ] **Aucune donnée inventée** (principe de précaution)
- [ ] **Sources traçables** (URLs dans colonne source)
- [ ] **Format CSV conforme** au schéma cible
- [ ] **Déduplication effective** (pas de doublons)

## 🚀 **PLAN D'ACTIONS**

### **Étape 1 : Inscriptions outils (J1)**
1. **ScrapingBee** : Plan Freelancer (€29/mois)
2. **Groq** : Compte gratuit → Pay-as-you-go  
3. **Serper.dev** : Plan Basic (€50/mois avec 10k requêtes)

### **Étape 2 : Développement (J2-J4)**
1. **Module scraping annuaire officiel**
2. **Module scraping chaînes privées** 
3. **Module recherche & extraction alternatives**
4. **Pipeline de normalisation et déduplication**

### **Étape 3 : Test & validation (J5)**
1. **Exécution sur Lot-et-Garonne**
2. **Comparaison avec data_47.csv**
3. **Ajustements précision**

### **Étape 4 : Optimisation (J6-J7)**
1. **Réduction coûts API** (cache, optimisation requêtes)
2. **Amélioration extraction IA**
3. **Documentation pipeline**

## ❓ **QUESTIONS EN SUSPENS**

1. **Budget mensuel** acceptable pour les abonnements ?
2. **Priorité** sur la précision vs couverture vs coût ?
3. **Validation humaine** souhaitée sur quels segments ?
4. **Format final** : CSV pur ou base de données intermédiaire ?
5. **Fréquence de mise à jour** envisagée (mensuelle, trimestrielle) ?

## 📝 **NOTES IMPORTANTES**

- **Pas de websearch ChatGPT/Claude** : éviter les limitations
- **Sources officielles privilégiées** : fiabilité maximum
- **Traçabilité complète** : URLs sources pour chaque établissement
- **Respect robots.txt** : scraping éthique et légal
- **Rate limiting** : pas de surcharge des serveurs cibles

---
*Document créé le 2025-12-02 - Version MVP 1.0*