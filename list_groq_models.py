#!/usr/bin/env python3
"""
Liste tous les modèles Groq disponibles via l'API
"""
import os
import requests
from groq import Groq
from dotenv import load_dotenv

def list_all_groq_models():
    load_dotenv()
    api_key = os.getenv('GROQ_API_KEY')
    
    if not api_key:
        print("❌ Clé API manquante")
        return
        
    try:
        client = Groq(api_key=api_key)
        
        # Liste tous les modèles disponibles
        models = client.models.list()
        
        print("📋 Modèles Groq disponibles:")
        print("-" * 60)
        
        available_models = []
        for model in models.data:
            print(f"🔹 {model.id}")
            print(f"   Owned by: {model.owned_by}")
            if hasattr(model, 'context_window'):
                print(f"   Context: {model.context_window} tokens")
            print()
            available_models.append(model.id)
            
        print(f"📊 Total: {len(available_models)} modèles disponibles")
        
        # Test rapide du modèle principal
        if available_models:
            main_model = available_models[0]
            print(f"\n🧪 Test du modèle principal: {main_model}")
            
            response = client.chat.completions.create(
                messages=[{"role": "user", "content": "Bonjour, dis juste 'OK'"}],
                model=main_model,
                max_tokens=5
            )
            
            print(f"✅ Réponse: {response.choices[0].message.content}")
            
    except Exception as e:
        print(f"❌ Erreur: {e}")

if __name__ == "__main__":
    list_all_groq_models()