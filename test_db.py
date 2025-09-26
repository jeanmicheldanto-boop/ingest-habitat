#!/usr/bin/env python3
"""
Script de test de connexion à la base de données PostgreSQL/Supabase
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import db_manager

def test_connection():
    """Test de connexion à la base"""
    print("🔗 Test de connexion à la base de données...")
    
    try:
        connected, message = db_manager.test_connection()
        
        if connected:
            print(f"✅ {message}")
            
            # Test des tables principales
            print("\n📊 Vérification des tables...")
            
            with db_manager.get_connection() as conn:
                with conn.cursor() as cur:
                    # Vérifier les tables principales
                    tables_to_check = [
                        'etablissements', 
                        'categories', 
                        'sous_categories',
                        'services',
                        'logements_types',
                        'tarifications',
                        'medias'
                    ]
                    
                    for table in tables_to_check:
                        cur.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cur.fetchone()[0]
                        print(f"  - {table}: {count} enregistrements")
                    
                    # Vérifier les types enum
                    print("\n🏷️ Types d'habitat disponibles...")
                    cur.execute("SELECT unnest(enum_range(NULL::habitat_type))")
                    habitat_types = [row[0] for row in cur.fetchall()]
                    for ht in habitat_types:
                        print(f"  - {ht}")
                    
                    # Vérifier les catégories et sous-catégories
                    categories = db_manager.get_categories_and_sous_categories()
                    print(f"\n📂 Catégories et sous-catégories: {len(categories)} disponibles")
                    
                    # Grouper par catégorie
                    cats_dict = {}
                    for cat in categories:
                        cat_name = cat['categorie']
                        if cat_name not in cats_dict:
                            cats_dict[cat_name] = []
                        cats_dict[cat_name].append(cat['sous_categorie'])
                    
                    for cat_name, sous_cats in list(cats_dict.items())[:3]:  # Afficher les 3 premières
                        print(f"  - {cat_name}: {len(sous_cats)} sous-catégories")
                    
                    print(f"\n🎯 Base de données prête pour l'ingestion !")
                    return True
                    
        else:
            print(f"❌ Erreur de connexion: {message}")
            return False
            
    except Exception as e:
        print(f"💥 Erreur lors du test: {e}")
        return False

if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)