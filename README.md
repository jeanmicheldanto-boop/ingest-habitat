# 🏠 Application d'Ingestion Habitat Intermédiaire

Cette application permet d'alimenter la base de données PostgreSQL de l'habitat intermédiaire à partir de fichiers CSV récupérés via recherches web.

## 🚀 Fonctionnalités

### ✨ Fonctionnalités principales
- **📁 Lecture CSV** : Import avec détection automatique d'encodage
- **🔄 Mapping colonnes** : Reconnaissance automatique des colonnes
- **🔍 Détection données manquantes** : Identification des champs obligatoires/optionnels
- **✏️ Correction manuelle** : Interface de saisie pour données manquantes obligatoires
- **🌐 Enrichissement web** : Récupération automatique d'infos via scraping des sites web
- **📍 Géolocalisation** : Géocodage automatique des adresses (Nominatim/Google Maps)
- **📸 Upload images** : Gestion des photos par drag & drop
- **✅ Validation** : Contrôles de format et cohérence
- **📊 Tableaux de bord** : Visualisation et statistiques d'import

### 🎯 Workflow complet
1. **Upload CSV** → Analyse automatique du fichier et mapping des colonnes
2. **Correction** → Interface pour compléter les données obligatoires manquantes
3. **Géolocalisation** → Récupération automatique des coordonnées GPS
4. **Enrichissement** → Extraction d'infos depuis les sites web des établissements
5. **Import final** → Insertion en base avec validation complète

## 📋 Prérequis

- Python 3.8+
- PostgreSQL avec la base de données habitat
- Accès internet (pour géolocalisation et enrichissement web)

## ⚙️ Installation

1. **Cloner et accéder au projet**
```bash
cd c:\Users\Lenovo\ingest-habitat
```

2. **Créer un environnement virtuel**
```bash
python -m venv venv
venv\Scripts\activate
```

3. **Installer les dépendances**
```bash
pip install -r requirements.txt
```

4. **Configuration**
```bash
# Copier le fichier d'exemple
copy .env.example .env

# Éditer .env avec vos paramètres
notepad .env
```

### 🔧 Configuration requise (.env)

```env
# Base de données
DB_HOST=localhost
DB_NAME=habitat_db
DB_USER=postgres
DB_PASSWORD=your_password
DB_PORT=5432

# Géolocalisation (optionnel pour Google Maps)
GEOCODING_SERVICE=nominatim
GOOGLE_MAPS_API_KEY=your_api_key

# Application
DEBUG=true
```

## 🚀 Lancement

```bash
# Activer l'environnement virtuel
venv\Scripts\activate

# Lancer l'application Streamlit
streamlit run app.py
```

L'application sera accessible à : http://localhost:8501

## 📊 Structure des données

### 🎯 Mapping des colonnes CSV

L'application reconnaît automatiquement ces colonnes :

| Champ cible | Colonnes CSV acceptées |
|-------------|----------------------|
| **nom** | nom, name, établissement, etablissement |
| **presentation** | presentation, description, présentation |
| **adresse_l1** | adresse_l1, adresse, address, rue |
| **code_postal** | code_postal, cp, postal_code, zipcode |
| **commune** | commune, ville, city |
| **departement** | departement, département, dept |
| **telephone** | telephone, téléphone, phone, tel |
| **email** | email, mail, e-mail |
| **site_web** | site_web, site, website, url |
| **gestionnaire** | gestionnaire, gestionnaire/opérateur, operateur |
| **habitat_type** | habitat_type, type_habitat |

### 🏠 Types d'habitat supportés

- **`residence`** : Résidences autonomie, résidences services seniors, MARPA
- **`habitat_partage`** : Habitat inclusif, habitat partagé, accueil familial
- **`logement_independant`** : Logements indépendants

### ✅ Champs obligatoires

- `nom` : Nom de l'établissement
- `commune` : Commune de localisation
- `code_postal` : Code postal (5 chiffres)
- `gestionnaire` : Gestionnaire/opérateur
- `email` : Email de contact (format valide)
- `habitat_type` : Type d'habitat

### 📋 Champs recommandés

- `presentation` : Description de l'établissement
- `telephone` : Téléphone (format français)
- `site_web` : Site web (URL valide)
- `adresse_l1` : Adresse complète

## 🌍 Services intégrés

### 📍 Géolocalisation
- **Nominatim (OpenStreetMap)** : Service gratuit par défaut
- **Google Maps API** : Service premium (nécessite clé API)
- Géocodage par lots avec gestion des timeouts
- Carte interactive des résultats

### 🌐 Enrichissement web
- Scraping automatique des sites web d'établissements
- Extraction de :
  - Descriptions enrichies
  - Coordonnées de contact
  - Services proposés
  - Informations tarifaires
  - Images représentatives

### ✅ Validation des données
- **Emails** : Validation format RFC
- **Téléphones** : Validation formats français
- **Codes postaux** : Validation 5 chiffres + cohérence départementale
- **URLs** : Validation format HTTP/HTTPS
- **Coordonnées GPS** : Vérification territoire français

## 📈 Tableaux de bord

### 📊 Métriques d'import
- Nombre total d'enregistrements
- Taux de validation
- Répartition par type d'habitat
- Statut de géolocalisation
- Résultats d'enrichissement

### 🗺️ Visualisations
- Carte interactive des établissements géolocalisés
- Graphiques de répartition
- Indicateurs de complétude des données

## 🔄 Exemples d'utilisation

### 📁 Format CSV type

```csv
Département,Commune,Nom,Type,Téléphone,Email,Site,Gestionnaire/Opérateur,Source,habitat_type,sous_categories
Lot-et-Garonne,Marmande,Résidence autonomie Les Glycines,Résidence autonomie,05 53 64 14 19,contact@residence.fr,https://www.residence.fr,CCAS Marmande,web_search,residence,Résidence autonomie
```

### 🎯 Workflow type

1. **Préparer votre CSV** avec les colonnes reconnues
2. **Lancer l'app** et uploader le fichier
3. **Vérifier le mapping** automatique des colonnes
4. **Corriger les données manquantes** via l'interface
5. **Géolocaliser** les établissements
6. **Enrichir** depuis les sites web
7. **Importer** en base après validation

## 🛠️ Dépannage

### ❌ Problèmes courants

**Erreur de connexion base de données**
```bash
# Vérifier les paramètres dans .env
# Tester la connexion PostgreSQL
psql -h localhost -U postgres -d habitat_db
```

**Timeout géolocalisation**
```bash
# Réduire la taille des lots dans l'interface
# Passer de Nominatim à Google Maps API si disponible
```

**Erreur d'encodage CSV**
```bash
# L'app détecte automatiquement l'encodage
# Sauvegarder le CSV en UTF-8 si problème persistant
```

### 📝 Logs et debug

En mode debug, les logs détaillés sont affichés dans l'interface Streamlit.

## 🔧 Architecture technique

```
ingest-habitat/
├── app.py                 # Interface Streamlit principale
├── data_processor.py      # Traitement et validation CSV
├── database.py           # Gestion PostgreSQL
├── geocoding.py          # Services géolocalisation
├── web_enrichment.py     # Enrichissement web/scraping
├── validation.py         # Validation des données
├── config.py            # Configuration et mappings
├── requirements.txt     # Dépendances Python
├── schema.sql          # Structure base de données
├── .env.example        # Template configuration
└── README.md          # Documentation
```

## 🤝 Contribution

Pour contribuer au projet :

1. Fork le repository
2. Créer une branche feature
3. Commit les modifications
4. Push vers la branche
5. Créer une Pull Request

## 📄 License

Ce projet est sous licence MIT. Voir le fichier LICENSE pour plus de détails.

## 📞 Support

Pour tout support ou question :
- Créer une issue sur le repository
- Contacter l'équipe de développement

---

**🏠 Application d'Ingestion Habitat Intermédiaire - Version 1.0**