import logging
import os
import sys
from contextlib import contextmanager
from datetime import datetime

import psycopg2
import psycopg2.extras

try:  # UI-only
    import streamlit as st  # type: ignore
except Exception:  # pragma: no cover
    st = None

try:  # optional; used by some UI helpers
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None

try:
    from config import DATABASE_CONFIG
except Exception:  # pragma: no cover
    # Cloud Run / batch-friendly fallback (no import of the full config module required)
    DATABASE_CONFIG = {
        "host": os.getenv("DB_HOST", ""),
        "database": os.getenv("DB_NAME", ""),
        "user": os.getenv("DB_USER", ""),
        "password": os.getenv("DB_PASSWORD", ""),
        "port": int(os.getenv("DB_PORT", "5432")),
    }


logger = logging.getLogger(__name__)

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
            msg = f"Erreur de connexion à la base : {e}"
            if st is not None:
                try:
                    st.error(msg)
                except Exception:
                    logger.error(msg)
            else:
                logger.error(msg)
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
    
    def get_sous_categorie_id_by_name(self, sous_categorie_name: str):
        """Trouve l'ID d'une sous-catégorie par son nom"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT id FROM sous_categories 
                        WHERE LOWER(REPLACE(libelle, ' ', '_')) = LOWER(%s)
                        OR LOWER(libelle) ILIKE %s
                        LIMIT 1
                    """, (sous_categorie_name.replace('_', ' '), f'%{sous_categorie_name.replace("_", " ")}%'))
                    result = cur.fetchone()
                    return result[0] if result else None
        except Exception as e:
            st.error(f"Erreur lors de la recherche de sous-catégorie : {e}")
            return None
    
    def get_service_id_by_name(self, service_name: str):
        """Trouve l'ID d'un service par son nom"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Mapping des noms de services de l'IA vers la base
                    service_mapping = {
                        'activites_organisees': 'activités organisées',
                        'personnel_de_nuit': 'personnel de nuit',
                        'commerces_a_pied': 'commerces à pied', 
                        'medecin_intervenant': 'médecin intervenant',
                        'espace_partage': 'espace_partage',
                        'conciergerie': 'conciergerie'
                    }
                    
                    # Utiliser le mapping si disponible, sinon le nom direct
                    db_service_name = service_mapping.get(service_name, service_name)
                    
                    cur.execute("""
                        SELECT id FROM services 
                        WHERE libelle = %s
                        OR LOWER(REPLACE(libelle, ' ', '_')) = LOWER(%s)
                        OR LOWER(libelle) ILIKE %s
                        LIMIT 1
                    """, (db_service_name, service_name.replace('_', ' '), f'%{service_name.replace("_", " ")}%'))
                    result = cur.fetchone()
                    return result[0] if result else None
        except Exception as e:
            st.error(f"Erreur lors de la recherche de service : {e}")
            return None
    
    def get_departement_stats(self, departement):
        """Récupère les statistiques complètes d'un département après import"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Statistiques générales
                    stats_query = """
                        SELECT 
                            COUNT(*) as total_etablissements,
                            COUNT(CASE WHEN statut_editorial = 'publie' THEN 1 END) as publies,
                            COUNT(CASE WHEN statut_editorial = 'draft' THEN 1 END) as brouillons,
                            
                            -- Pourcentages de remplissage des champs clés
                            ROUND(100.0 * COUNT(CASE WHEN public_cible IS NOT NULL AND public_cible != '' THEN 1 END) / COUNT(*), 1) as pct_public_cible,
                            
                            -- Sous-catégories (via table de liaison)
                            ROUND(100.0 * COUNT(CASE WHEN EXISTS(
                                SELECT 1 FROM etablissement_sous_categorie esc 
                                WHERE esc.etablissement_id = e.id
                            ) THEN 1 END) / COUNT(*), 1) as pct_sous_categories,
                            
                            -- Restauration
                            ROUND(100.0 * COUNT(CASE WHEN EXISTS(
                                SELECT 1 FROM restaurations r 
                                WHERE r.etablissement_id = e.id
                            ) THEN 1 END) / COUNT(*), 1) as pct_restauration,
                            
                            -- Services (via table de liaison)
                            ROUND(100.0 * COUNT(CASE WHEN EXISTS(
                                SELECT 1 FROM etablissement_service es 
                                WHERE es.etablissement_id = e.id
                            ) THEN 1 END) / COUNT(*), 1) as pct_services,
                            
                            -- Tarifications
                            ROUND(100.0 * COUNT(CASE WHEN EXISTS(
                                SELECT 1 FROM tarifications t 
                                WHERE t.etablissement_id = e.id
                            ) THEN 1 END) / COUNT(*), 1) as pct_tarifications,
                            
                            -- Logements types
                            ROUND(100.0 * COUNT(CASE WHEN EXISTS(
                                SELECT 1 FROM logements_types lt 
                                WHERE lt.etablissement_id = e.id
                            ) THEN 1 END) / COUNT(*), 1) as pct_logements_types
                            
                        FROM etablissements e
                        WHERE departement = %s
                    """
                    
                    cur.execute(stats_query, (departement,))
                    stats = cur.fetchone()
                    
                    if stats and stats[0] > 0:  # Si on a des résultats
                        total = stats[0]
                        publies = stats[1]
                        pourcentage_publie = round(100.0 * publies / total, 1) if total > 0 else 0
                        
                        return {
                            'total_etablissements': total,
                            'publies': publies, 
                            'brouillons': stats[2],
                            'pourcentage_publie': pourcentage_publie,
                            'pct_public_cible': stats[3] or 0,
                            'pct_sous_categories': stats[4] or 0,
                            'pct_restauration': stats[5] or 0,
                            'pct_services': stats[6] or 0,
                            'pct_tarifications': stats[7] or 0,
                            'pct_logements_types': stats[8] or 0  # Nouveau champ
                        }
                    return {
                        'total_etablissements': 0,
                        'publies': 0,
                        'brouillons': 0,
                        'pourcentage_publie': 0,
                        'pct_public_cible': 0,
                        'pct_sous_categories': 0,
                        'pct_restauration': 0,
                        'pct_services': 0,
                        'pct_tarifications': 0,
                        'pct_logements_types': 0
                    }
                    
        except Exception as e:
            st.error(f"Erreur lors de la récupération des statistiques : {e}")
            return None
                    
        except Exception as e:
            st.error(f"Erreur lors de la récupération des statistiques : {e}")
            return None
    
    def get_etablissements_by_departement(self, departement, limit=None):
        """Récupère la liste des établissements d'un département"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    query = """
                        SELECT 
                            id, nom, commune, statut_editorial, public_cible,
                            'Non' as a_sous_categories,
                            CASE WHEN EXISTS(
                                SELECT 1 FROM restaurations r WHERE r.etablissement_id = e.id
                            ) THEN 'Oui' ELSE 'Non' END as a_restauration,
                            CASE WHEN EXISTS(
                                SELECT 1 FROM etablissement_service es WHERE es.etablissement_id = e.id
                            ) THEN 'Oui' ELSE 'Non' END as a_services,
                            CASE WHEN EXISTS(
                                SELECT 1 FROM tarifications t WHERE t.etablissement_id = e.id
                            ) THEN 'Oui' ELSE 'Non' END as a_tarifications
                        FROM etablissements e
                        WHERE departement = %s
                        ORDER BY nom
                    """
                    
                    if limit:
                        query += f" LIMIT {limit}"
                    
                    cur.execute(query, (departement,))
                    return cur.fetchall()
                    
        except Exception as e:
            st.error(f"Erreur lors de la récupération des établissements : {e}")
            return []
    
    def publish_etablissements_by_departement(self, departement):
        """Passe tous les établissements d'un département en statut 'published'"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Compter les établissements à publier
                    count_query = "SELECT COUNT(*) FROM etablissements WHERE departement = %s AND statut_editorial != 'publie'"
                    cur.execute(count_query, (departement,))
                    count_to_publish = cur.fetchone()[0]
                    
                    if count_to_publish == 0:
                        return {
                            'success': True,
                            'published_count': 0,
                            'message': 'Tous les établissements sont déjà publiés'
                        }
                    
                    # Publier les établissements
                    update_query = """
                        UPDATE etablissements 
                        SET statut_editorial = 'publie',
                            date_observation = CURRENT_DATE
                        WHERE departement = %s AND statut_editorial != 'publie'
                    """
                    
                    cur.execute(update_query, (departement,))
                    published_count = cur.rowcount
                    conn.commit()
                    
                    return {
                        'success': True,
                        'published_count': published_count,
                        'message': f'{published_count} établissement(s) publié(s) avec succès'
                    }
                    
        except Exception as e:
            return {
                'success': False,
                'message': f'Erreur lors de la publication : {e}'
            }
    
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
                            date_observation, code_insee, eligibilite_statut, public_cible
                        ) VALUES (
                            %(nom)s, %(presentation)s, %(adresse_l1)s, %(adresse_l2)s, %(code_postal)s,
                            %(commune)s, %(departement)s, %(region)s, %(telephone)s, %(email)s, %(site_web)s,
                            %(gestionnaire)s, %(source)s, %(habitat_type)s, """ + (geom_sql or "NULL") + """,
                            'draft', CURRENT_DATE, %(code_insee)s, %(eligibilite_statut)s, %(public_cible)s
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
    
    def insert_restauration_data(self, etablissement_id, restauration_data):
        """Insère les données de restauration pour un établissement"""
        if not restauration_data:
            return True
            
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Supprimer les anciennes données
                    cur.execute("DELETE FROM restaurations WHERE etablissement_id = %s", (etablissement_id,))
                    
                    # Insérer les nouvelles données
                    cur.execute("""
                        INSERT INTO restaurations (etablissement_id, kitchenette, resto_collectif, portage_repas)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        etablissement_id,
                        restauration_data.get('kitchenette', False),
                        restauration_data.get('resto_collectif', False),
                        restauration_data.get('portage_repas', False)
                    ))
                    
                    conn.commit()
                    return True
                    
        except Exception as e:
            st.error(f"Erreur lors de l'insertion des données de restauration : {e}")
            return False
    
    def insert_tarification_data(self, etablissement_id, tarifications):
        """Insère les données de tarification pour un établissement"""
        if not tarifications:
            return True
            
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Supprimer les anciennes données
                    cur.execute("DELETE FROM tarifications WHERE etablissement_id = %s", (etablissement_id,))
                    
                    # Insérer les nouvelles données
                    for tarif in tarifications:
                        # Note: Pour l'instant, on insère sans logements_type_id car on n'a pas cette info
                        # On peut ajouter la logique plus tard pour mapper vers logements_types
                        cur.execute("""
                            INSERT INTO tarifications (etablissement_id, fourchette_prix, periode, source)
                            VALUES (%s, %s, %s, %s)
                        """, (
                            etablissement_id,
                            tarif.get('fourchette_prix'),
                            tarif.get('periode', 'mensuel'),
                            tarif.get('source', 'import_csv')
                        ))
                    
                    conn.commit()
                    return True
                    
        except Exception as e:
            st.error(f"Erreur lors de l'insertion des tarifications : {e}")
            return False

    def insert_logements_types_for_etablissement(self, etablissement_id, logements_types):
        """Insère les types de logements pour un établissement"""
        if not logements_types:
            return True
            
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Supprimer les anciens types de logements
                    cur.execute("DELETE FROM logements_types WHERE etablissement_id = %s", (etablissement_id,))
                    
                    # Insérer les nouveaux types
                    for logement in logements_types:
                        cur.execute("""
                            INSERT INTO logements_types (
                                etablissement_id, libelle, surface_min, surface_max, 
                                meuble, pmr, domotique, nb_unites
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            etablissement_id,
                            logement.get('libelle', 'Logement'),
                            logement.get('surface_min'),
                            logement.get('surface_max'),
                            logement.get('meuble', False),
                            logement.get('pmr', False),
                            logement.get('domotique', False),
                            logement.get('nb_unites', 1)
                        ))
                    
                    conn.commit()
                    return True
                    
        except Exception as e:
            st.error(f"Erreur lors de l'insertion des types de logements : {e}")
            return False
    
    def geocode_city_center(self, commune, code_postal=None):
        """Géocoder le centre d'une commune si l'adresse exacte échoue"""
        try:
            from geopy.geocoders import Nominatim
            from geopy.exc import GeocoderTimedOut, GeocoderServiceError
            
            geolocator = Nominatim(user_agent="habitat-intermediaire")
            
            # Construire la requête pour le centre ville
            if code_postal:
                location_query = f"{commune}, {code_postal}, France"
            else:
                location_query = f"{commune}, France"
            
            location = geolocator.geocode(location_query, timeout=10)
            
            if location:
                return {
                    'latitude': location.latitude,
                    'longitude': location.longitude,
                    'geocode_precision': 'city_center',
                    'geocode_address': location.address
                }
            else:
                return None
                
        except (GeocoderTimedOut, GeocoderServiceError, Exception) as e:
            st.warning(f"Erreur géocodage centre ville {commune}: {e}")
            return None

# Instance globale
db_manager = DatabaseManager()