# 🚀 Configuration IA pour Lenovo T495s - Guide Optimisé

## 💻 **Votre configuration T495s**
- **AMD Ryzen 5/7 PRO** (4-8 cœurs)
- **8-16GB RAM** 
- **SSD** (bon pour l'IA)

## ⚡ **Option 1: Ollama Local (Recommandé)**

### ✅ **Modèles compatibles avec votre T495s :**

| Modèle | RAM requise | Vitesse | Qualité | Recommandation |
|--------|-------------|---------|---------|----------------|
| **phi3:mini** | **2GB** | ⚡⚡⚡ | ⭐⭐⭐⭐ | **🏆 PARFAIT T495s** |
| gemma:2b | 1.4GB | ⚡⚡⚡ | ⭐⭐⭐ | Ultra-léger |
| llama2:7b | 4GB | ⚡⚡ | ⭐⭐⭐⭐ | Possible si 16GB RAM |

### 📦 **Installation rapide :**
```powershell
# Windows - PowerShell
winget install Ollama.Ollama

# Ou téléchargement direct
# https://ollama.ai/download/windows
```

```bash
# Télécharger le modèle recommandé (2GB)
ollama pull phi3:mini

# Démarrer le service
ollama serve

# Test rapide
ollama run phi3:mini "Bonjour, analysez cette résidence senior à Paris"
```

### 🎯 **Performances attendues T495s :**
- **phi3:mini**: ~5-10 secondes par analyse ✅
- **gemma:2b**: ~3-8 secondes par analyse ✅
- **llama2:7b**: ~15-30 secondes (si 16GB RAM) ⚠️

---

## 🌐 **Option 2: API Groq (Gratuite et Rapide)**

### ✅ **Avantages pour T495s :**
- **Aucune installation**
- **Très rapide** (1-3 secondes)
- **Quota généreux gratuit**
- **Modèles puissants**

### 🔑 **Configuration :**
1. **S'inscrire** : https://console.groq.com (gratuit)
2. **Créer une clé API** 
3. **Configurer** dans l'application :
```python
# Dans config.py
WEB_ENRICHMENT_CONFIG = {
    'openai_compatible_key': 'votre_cle_groq_ici',
    # ...
}
```

### 📊 **Quotas gratuits Groq :**
- **14,400 requêtes/minute** (très généreux !)
- **Modèles disponibles** : Llama 2 70B, Mixtral, etc.

---

## 🤗 **Option 3: Hugging Face (Gratuit)**

### 📦 **Installation :**
```bash
pip install transformers torch
```

### 🔑 **Token (optionnel mais recommandé) :**
1. **S'inscrire** : https://huggingface.co
2. **Créer un token** : https://hf.co/settings/tokens
3. **Configurer** dans l'app

---

## 🎯 **Recommandations pour votre T495s :**

### **🏆 Scénario idéal :**
1. **Groq API** pour la rapidité (quotidien)
2. **Ollama phi3:mini** en backup (hors ligne)

### **💡 Scénario économe :**
- **Ollama phi3:mini** uniquement (100% gratuit + privé)

### **⚡ Scénario performance :**
- **Groq API** uniquement (le plus rapide)

---

## 🔧 **Test de votre configuration :**

### **Test RAM disponible :**
```powershell
# Windows
Get-ComputerInfo | Select-Object TotalPhysicalMemory,AvailablePhysicalMemory
```

### **Test Ollama :**
```bash
ollama pull phi3:mini
ollama run phi3:mini "Testez cette résidence senior Les Jardins à Lyon avec restaurant collectif"
```

Si la réponse arrive en **moins de 15 secondes**, votre T495s est **parfait** pour Ollama !

---

## ⚠️ **Si problèmes de performance :**

### **Ollama lent :**
1. **Fermer** autres applications
2. **Utiliser** `gemma:2b` au lieu de `phi3:mini`
3. **Passer** à Groq API

### **RAM insuffisante :**
1. **Groq API** (aucune RAM requise)
2. **Hugging Face** (léger)

---

## 📈 **Résultats attendus :**

Avec votre T495s, vous devriez obtenir :
- ✅ **Analyse de 10 établissements** : 2-5 minutes (Groq) / 5-15 minutes (Ollama)
- ✅ **Extraction** : type_public, restauration, tarifs, services, AVP
- ✅ **Format JSON** directement compatible base

**Votre T495s est largement suffisant pour l'enrichissement IA habitat !** 🚀