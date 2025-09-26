#!/usr/bin/env python3
"""
Script de test simple pour la connexion Supabase
"""
import psycopg2
import psycopg2.extras

# Configuration directe (pour éviter les problèmes de .env)
DB_CONFIG = {
    'host': 'db.minwoumfgutampcgrcbr.supabase.co',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'CHOUchou1803Se!',
    'port': 5432
}

def test_connection():
    """Test de connexion à Supabase"""
    print("🔗 Test de connexion à Supabase...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Test de base
        cur.execute("SELECT version();")
        version = cur.fetchone()['version']
        print(f"✅ Connexion réussie !")
        print(f"📋 Version PostgreSQL : {version[:50]}...")
        
        # Vérification des tables principales
        print("\n📊 Vérification des tables du schéma habitat...")
        
        tables_to_check = [
            'etablissements', 
            'categories', 
            'sous_categories',
            'services'
        ]
        
        for table in tables_to_check:
            try:
                cur.execute(f"SELECT COUNT(*) as count FROM {table}")
                count = cur.fetchone()['count']
                print(f"  ✅ {table}: {count} enregistrements")
            except psycopg2.Error as e:
                print(f"  ❌ {table}: Erreur - {e}")
        
        # Vérifier les types enum
        print("\n🏷️ Types d'habitat disponibles...")
        try:
            cur.execute("SELECT unnest(enum_range(NULL::habitat_type)) as type_habitat")
            habitat_types = [row['type_habitat'] for row in cur.fetchall()]
            for ht in habitat_types:
                print(f"  - {ht}")
        except psycopg2.Error as e:
            print(f"  ❌ Erreur avec les types enum: {e}")
        
        cur.close()
        conn.close()
        
        print(f"\n🎯 Base de données Supabase prête pour l'ingestion !")
        return True
        
    except psycopg2.Error as e:
        print(f"❌ Erreur PostgreSQL: {e}")
        return False
    except Exception as e:
        print(f"💥 Erreur générale: {e}")
        return False

if __name__ == "__main__":
    test_connection()