# Installation et configuration d'Ollama pour l'enrichissement habitat

## Qu'est-ce qu'Ollama ?
Ollama est un outil qui permet d'exécuter localement des modèles d'IA comme Llama 2, Mistral, etc.
Il est **gratuit** et fonctionne entièrement sur votre machine (pas d'API externe nécessaire).

## Installation

### Windows (PowerShell)
```powershell
# Télécharger et installer Ollama
Invoke-WebRequest -Uri https://ollama.ai/download/windows -OutFile ollama-installer.exe
./ollama-installer.exe
```

### Linux/macOS
```bash
curl -fsSL https://ollama.ai/install.sh | sh
```

## Configuration pour l'application habitat

### 1. Démarrer le service Ollama
```bash
ollama serve
```

### 2. Télécharger un modèle recommandé
```bash
# Modèle léger et rapide (recommandé)
ollama pull llama2

# Ou modèle plus performant (plus lent)
ollama pull llama2:13b

# Ou modèle Mistral (alternative)
ollama pull mistral
```

### 3. Tester l'installation
```bash
# Test simple
ollama run llama2 "Bonjour, tu fonctionnes bien ?"
```

## Utilisation dans l'application

1. **Démarrer Ollama** : `ollama serve`
2. **Lancer l'application habitat** : `streamlit run app.py`
3. **Étape 4** : Cocher "🤖 Analyse IA (Ollama)"
4. **Sélectionner le modèle** approprié
5. **Lancer l'enrichissement** intelligent

## Avantages d'Ollama pour l'habitat

- ✅ **Gratuit** et local (pas d'API externe)
- ✅ **Compréhension contextuelle** du vocabulaire habitat
- ✅ **Extraction structurée** des données
- ✅ **Analyse sans site web** (nom + commune suffisent)
- ✅ **Respect des formats** de base de données
- ✅ **Confidentialité** (tout reste sur votre serveur)

## Modèles recommandés

| Modèle | Taille | Vitesse | Qualité | Recommandation |
|--------|--------|---------|---------|----------------|
| llama2 | ~4GB | ⚡⚡⚡ | ⭐⭐⭐ | **Recommandé** |
| llama2:13b | ~8GB | ⚡⚡ | ⭐⭐⭐⭐⭐ | Si RAM suffisante |
| mistral | ~4GB | ⚡⚡⚡ | ⭐⭐⭐⭐ | Alternative |

## Dépannage

### Erreur "Ollama non accessible"
```bash
# Vérifier si Ollama tourne
curl http://localhost:11434/api/tags

# Redémarrer si nécessaire
ollama serve
```

### Performance lente
- Utiliser `llama2` au lieu de `llama2:13b`
- Fermer d'autres applications gourmandes
- Augmenter la RAM si possible

## Configuration avancée

### Changer le port (si 11434 est occupé)
```bash
OLLAMA_HOST=127.0.0.1:11435 ollama serve
```

Puis modifier dans `config.py`:
```python
WEB_ENRICHMENT_CONFIG = {
    'ollama_url': 'http://localhost:11435',
    # ...
}
```