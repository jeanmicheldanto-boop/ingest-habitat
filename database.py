import psycopg2
import psycopg2.extras
from contextlib import contextmanager
import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
from config import DATABASE_CONFIG

class DatabaseManager:
    """Gestionnaire de connexion et opérations PostgreSQL"""
    
    def __init__(self):
        self.config = DATABASE_CONFIG
    
    @staticmethod
    def create_geometry_point(latitude: float, longitude: float) -> str:
        """Convertit des coordonnées latitude/longitude en géométrie PostGIS"""
        return f"ST_GeomFromText('POINT({longitude} {latitude})', 4326)"
    
    @contextmanager
    def get_connection(self):
        """Context manager pour les connexions à la base"""
        conn = None
        try:
            conn = psycopg2.connect(**self.config)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            st.error(f"Erreur de connexion à la base : {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def test_connection(self):
        """Test de connexion à la base"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    version = cur.fetchone()[0]
                    return True, f"Connexion réussie : {version}"
        except Exception as e:
            return False, f"Erreur de connexion : {e}"
    
    def get_categories_and_sous_categories(self):
        """Récupère toutes les catégories et sous-catégories"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("""
                        SELECT c.libelle as categorie, sc.libelle as sous_categorie, sc.id as sc_id
                        FROM categories c
                        JOIN sous_categories sc ON c.id = sc.categorie_id
                        ORDER BY c.libelle, sc.libelle
                    """)
                    return cur.fetchall()
        except Exception as e:
            st.error(f"Erreur lors de la récupération des catégories : {e}")
            return []
    
    def get_services(self):
        """Récupère tous les services disponibles"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT id, libelle FROM services ORDER BY libelle")
                    return cur.fetchall()
        except Exception as e:
            st.error(f"Erreur lors de la récupération des services : {e}")
            return []
    
    def insert_etablissement(self, data):
        """Insère un nouvel établissement"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Préparer les données géométriques si lat/lon disponibles
                    geom_sql = None
                    if data.get('latitude') and data.get('longitude'):
                        geom_sql = f"ST_SetSRID(ST_MakePoint({data['longitude']}, {data['latitude']}), 4326)"
                    
                    # Requête d'insertion
                    insert_query = """
                        INSERT INTO etablissements (
                            nom, presentation, adresse_l1, adresse_l2, code_postal,
                            commune, departement, region, telephone, email, site_web,
                            gestionnaire, source, habitat_type, geom, statut_editorial,
                            date_observation
                        ) VALUES (
                            %(nom)s, %(presentation)s, %(adresse_l1)s, %(adresse_l2)s, %(code_postal)s,
                            %(commune)s, %(departement)s, %(region)s, %(telephone)s, %(email)s, %(site_web)s,
                            %(gestionnaire)s, %(source)s, %(habitat_type)s, """ + (geom_sql or "NULL") + """,
                            'draft', CURRENT_DATE
                        ) RETURNING id
                    """
                    
                    cur.execute(insert_query, data)
                    etablissement_id = cur.fetchone()[0]
                    conn.commit()
                    
                    return {
                        'success': True,
                        'id': etablissement_id,
                        'message': f'Établissement inséré avec ID {etablissement_id}'
                    }
                    
        except Exception as e:
            return {
                'success': False,
                'message': f'Erreur lors de l\'insertion de l\'établissement : {e}'
            }
    
    def insert_sous_categories_for_etablissement(self, etablissement_id, sous_categorie_ids):
        """Associe des sous-catégories à un établissement"""
        if not sous_categorie_ids:
            return True
            
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Supprimer les anciennes associations
                    cur.execute(
                        "DELETE FROM etablissement_sous_categorie WHERE etablissement_id = %s",
                        (etablissement_id,)
                    )
                    
                    # Insérer les nouvelles associations
                    for sc_id in sous_categorie_ids:
                        cur.execute(
                            "INSERT INTO etablissement_sous_categorie (etablissement_id, sous_categorie_id) VALUES (%s, %s)",
                            (etablissement_id, sc_id)
                        )
                    
                    conn.commit()
                    return True
                    
        except Exception as e:
            st.error(f"Erreur lors de l'association des sous-catégories : {e}")
            return False
    
    def insert_services_for_etablissement(self, etablissement_id, service_ids):
        """Associe des services à un établissement"""
        if not service_ids:
            return True
            
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Supprimer les anciennes associations
                    cur.execute(
                        "DELETE FROM etablissement_service WHERE etablissement_id = %s",
                        (etablissement_id,)
                    )
                    
                    # Insérer les nouvelles associations
                    for service_id in service_ids:
                        cur.execute(
                            "INSERT INTO etablissement_service (etablissement_id, service_id) VALUES (%s, %s)",
                            (etablissement_id, service_id)
                        )
                    
                    conn.commit()
                    return True
                    
        except Exception as e:
            st.error(f"Erreur lors de l'association des services : {e}")
            return False
    
    def get_etablissements_count(self):
        """Retourne le nombre d'établissements par statut"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("""
                        SELECT statut_editorial, COUNT(*) as count
                        FROM etablissements
                        WHERE is_test = false
                        GROUP BY statut_editorial
                    """)
                    return {row['statut_editorial']: row['count'] for row in cur.fetchall()}
        except Exception as e:
            st.error(f"Erreur lors du comptage des établissements : {e}")
            return {}
    
    def search_etablissements(self, search_term="", limit=100):
        """Recherche d'établissements"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    if search_term:
                        cur.execute("""
                            SELECT id, nom, commune, departement, statut_editorial, habitat_type
                            FROM etablissements
                            WHERE (nom ILIKE %s OR commune ILIKE %s OR departement ILIKE %s)
                            AND is_test = false
                            ORDER BY nom
                            LIMIT %s
                        """, (f'%{search_term}%', f'%{search_term}%', f'%{search_term}%', limit))
                    else:
                        cur.execute("""
                            SELECT id, nom, commune, departement, statut_editorial, habitat_type
                            FROM etablissements
                            WHERE is_test = false
                            ORDER BY created_at DESC
                            LIMIT %s
                        """, (limit,))
                    
                    return cur.fetchall()
        except Exception as e:
            st.error(f"Erreur lors de la recherche : {e}")
            return []

# Instance globale
db_manager = DatabaseManager()