import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time
from data_processor import DataProcessor
from database import DatabaseManager
from geocoding import GeocodingService
from web_enrichment import WebEnrichmentService
from validation import DataValidator
import config

# Configuration de la page
st.set_page_config(
    page_title="Habitat Ingestion Tool",
    page_icon="🏠",
    layout="wide"
)

# CSS personnalisé
st.markdown("""
<style>
    .section-header {
        color: #1f77b4;
        border-bottom: 2px solid #1f77b4;
        padding-bottom: 0.5rem;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# Initialisation de session_state
if 'current_step' not in st.session_state:
    st.session_state.current_step = 1
if 'data_processor' not in st.session_state:
    st.session_state.data_processor = None
if 'processor' not in st.session_state:
    st.session_state.processor = None
if 'geocoding_results' not in st.session_state:
    st.session_state.geocoding_results = []
if 'enrichment_results' not in st.session_state:
    st.session_state.enrichment_results = []
if 'database_manager' not in st.session_state:
    st.session_state.database_manager = None

def step_1_upload():
    """Étape 1: Téléchargement du fichier CSV"""
    st.markdown('<h2 class="section-header">📁 Étape 1: Téléchargement du fichier</h2>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader(
        "Choisissez un fichier CSV",
        type=['csv'],
        help="Le fichier doit contenir les données des établissements à importer"
    )
    
    if uploaded_file is not None:
        try:
            # Initialiser le processeur de données
            st.session_state.data_processor = DataProcessor()
            st.session_state.processor = st.session_state.data_processor
            
            # Charger le fichier
            success, message = st.session_state.data_processor.load_csv(uploaded_file)
            
            if success:
                st.success(message)
                
                # Détecter automatiquement le mapping des colonnes
                mapping = st.session_state.data_processor.detect_column_mapping()
                
                # Afficher le mapping détecté
                if mapping:
                    st.info(f"📋 Mapping automatique détecté pour {len(mapping)} colonnes")
                    with st.expander("Voir le mapping détecté"):
                        for target, source in mapping.items():
                            st.write(f"**{target}** ← `{source}`")
                else:
                    st.warning("⚠️ Aucun mapping automatique détecté. Les données seront affichées telles quelles.")
                
                # Afficher un aperçu
                preview_df = st.session_state.data_processor.get_preview_data()
                st.subheader("📊 Aperçu des données")
                st.dataframe(preview_df, width="stretch")
                
                # Passer à l'étape suivante
                if st.button("✅ Continuer vers l'étape 2", type="primary"):
                    st.session_state.current_step = 2
                    st.rerun()
            else:
                st.error(message)
                
        except Exception as e:
            st.error(f"Erreur lors du chargement: {e}")

def step_2_correction():
    """Étape 2: Correction des données manquantes - Version simplifiée"""
    st.markdown('<h2 class="section-header">✏️ Étape 2: Correction des données</h2>', unsafe_allow_html=True)
    
    if st.session_state.processor.df is None:
        st.error("Aucune donnée chargée. Retournez à l'étape 1.")
        return
    
    # Aperçu des données
    st.subheader("👀 Aperçu des données")
    preview_df = st.session_state.processor.get_preview_data()
    st.dataframe(preview_df, width="stretch")
    
    # Interface de correction pour les champs obligatoires manquants
    missing_data = st.session_state.processor.get_missing_data_summary()
    
    if missing_data:
        st.subheader("🔧 Correction des données manquantes")
        
        # Résumé de progression globale
        total_records = len(missing_data)
        completed_records = sum(1 for data in missing_data.values() 
                              if len(data['missing_required']) == 0)
        excluded_records = st.session_state.processor.get_excluded_count()
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📊 Total", total_records)
        with col2:
            st.metric("✅ Complétés", completed_records)
        with col3:
            st.metric("🚫 Exclus", excluded_records)
        
        # Progress bar
        if total_records > 0:
            progress = (completed_records + excluded_records) / total_records
            st.progress(progress)
            st.write(f"Progression: {progress*100:.1f}%")
        
        # Sélection d'enregistrement simplifiée
        record_options = {}
        for idx, data in missing_data.items():
            nom = data['nom'] or f"Ligne {idx + 1}"
            status = ""
            if st.session_state.processor.is_excluded(idx):
                status = " 🚫 EXCLU"
            elif len(data['missing_required']) == 0:
                status = " ✅"
            else:
                status = f" ❌ ({len(data['missing_required'])} manquant(s))"
            record_options[idx] = f"{nom}{status}"
        
        # Sélection de l'enregistrement à traiter
        selected_idx = st.selectbox(
            "Choisir un enregistrement à traiter:",
            options=list(record_options.keys()),
            format_func=lambda x: record_options[x]
        )
        
        if selected_idx is not None:
            record_data = missing_data[selected_idx]
            current_row = st.session_state.processor.df.iloc[selected_idx]
            
            st.write(f"**Enregistrement:** {record_data['nom']}")
            
            # Interface différente selon le statut
            if st.session_state.processor.is_excluded(selected_idx):
                st.info("🚫 Cet enregistrement est marqué comme EXCLU")
                if st.button("🔄 Réintégrer cet enregistrement"):
                    st.session_state.processor.include_record(selected_idx)
                    st.rerun()
            
            elif len(record_data['missing_required']) == 0:
                st.success("✅ Cet enregistrement est complet!")
                # Option d'exclusion même pour les complets
                if st.button("🚫 Exclure cet enregistrement"):
                    st.session_state.processor.exclude_record(selected_idx, "Marqué pour exclusion")
                    st.rerun()
            
            else:
                # Enregistrement avec données manquantes
                st.warning(f"❌ Données manquantes: {', '.join(record_data['missing_required'])}")
                
                # Formulaire de correction
                with st.form(f"form_{selected_idx}"):
                    st.write("**Correction des données manquantes:**")
                    
                    new_values = {}
                    for field in record_data['missing_required']:
                        new_values[field] = st.text_input(
                            f"{field.replace('_', ' ').title()}:",
                            key=f"input_{selected_idx}_{field}"
                        )
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        submitted = st.form_submit_button("💾 Sauvegarder")
                    with col2:
                        excluded = st.form_submit_button("🚫 Exclure enregistrement")
                    
                    if submitted:
                        # Sauvegarder les nouvelles valeurs
                        for field, value in new_values.items():
                            if value and value.strip():
                                if field in st.session_state.processor.mapped_columns:
                                    col_name = st.session_state.processor.mapped_columns[field]
                                    st.session_state.processor.df.loc[selected_idx, col_name] = value.strip()
                        
                        st.success("✅ Données sauvegardées!")
                        time.sleep(1)
                        st.rerun()
                    
                    if excluded:
                        st.session_state.processor.exclude_record(selected_idx, "Données obligatoires introuvables")
                        st.success("🚫 Enregistrement exclu!")
                        time.sleep(1)
                        st.rerun()
    
    else:
        st.success("✅ Aucune donnée manquante détectée!")
    
    # Bouton pour passer à l'étape suivante
    if st.button("➡️ Continuer vers l'étape 3", type="primary"):
        st.session_state.current_step = 3
        st.rerun()

def step_3_geocoding():
    """Étape 3: Géocodage des adresses"""
    st.markdown('<h2 class="section-header">🗺️ Étape 3: Géocodage</h2>', unsafe_allow_html=True)
    
    if st.session_state.data_processor is None:
        st.error("Aucune donnée chargée. Retournez à l'étape 1.")
        return
    
    df = st.session_state.data_processor.df
    mapping = st.session_state.data_processor.mapped_columns
    
    # Obtenir les enregistrements importables (non exclus)
    importable_df = st.session_state.data_processor.get_importable_records()
    
    st.write(f"📊 {len(importable_df)} enregistrements à géolocaliser")
    
    # Configuration du service de géocodage
    st.subheader("⚙️ Configuration du géocodage")
    
    col1, col2 = st.columns(2)
    
    with col1:
        geocoding_service = st.selectbox(
            "Service de géocodage:",
            ["Nominatim (gratuit)", "Google Maps (API key requise)"],
            help="Nominatim est gratuit mais moins précis que Google Maps"
        )
    
    with col2:
        if geocoding_service == "Google Maps (API key requise)":
            google_api_key = st.text_input(
                "Clé API Google Maps:",
                type="password",
                help="Votre clé API Google Maps"
            )
        else:
            google_api_key = None
    
    # Démarrer le géocodage
    if st.button("🎯 Démarrer le géocodage", type="primary"):
        if geocoding_service == "Google Maps (API key requise)" and not google_api_key:
            st.error("Clé API Google Maps requise!")
            return
        
        # Créer le service de géocodage
        geocoding_svc = GeocodingService()
        
        # Configurer la clé API Google si nécessaire
        if geocoding_service == "Google Maps (API key requise)" and google_api_key:
            geocoding_svc.set_google_api_key(google_api_key)
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        geocoding_results = []
        
        for i, (idx, row) in enumerate(importable_df.iterrows()):
            # Construire l'adresse
            address_parts = []
            
            for field in ['adresse_l1', 'adresse_l2', 'commune', 'code_postal']:
                if field in mapping and mapping[field] in row:
                    value = row[mapping[field]]
                    if pd.notna(value) and str(value).strip():
                        address_parts.append(str(value).strip())
            
            address = ", ".join(address_parts)
            
            if address:
                status_text.text(f"Géocodage... {address}")
                
                # Géocoder l'adresse
                try:
                    lat, lon, precision = geocoding_svc.geocode_address(address)
                    
                    if lat and lon:
                        geocoding_results.append({
                            'index': idx,
                            'address': address,
                            'latitude': lat,
                            'longitude': lon,
                            'geocode_precision': precision
                        })
                    else:
                        geocoding_results.append({
                            'index': idx,
                            'address': address,
                            'latitude': None,
                            'longitude': None,
                            'geocode_precision': 'Failed'
                        })
                except Exception as e:
                    st.error(f"Erreur lors du géocodage de {address}: {e}")
                    geocoding_results.append({
                        'index': idx,
                        'address': address,
                        'latitude': None,
                        'longitude': None,
                        'geocode_precision': 'Error'
                    })
                
                progress_bar.progress((i + 1) / len(importable_df))
                time.sleep(0.5)  # Pause pour éviter la surcharge
        
        st.session_state.geocoding_results = geocoding_results
        status_text.text("✅ Géocodage terminé!")
    
    # Affichage des résultats
    if st.session_state.geocoding_results:
        st.subheader("📊 Résultats du géocodage")
        
        successful_geocoding = [r for r in st.session_state.geocoding_results if r['latitude'] is not None]
        failed_geocoding = [r for r in st.session_state.geocoding_results if r['latitude'] is None]
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("✅ Réussis", len(successful_geocoding))
        with col2:
            st.metric("❌ Échecs", len(failed_geocoding))
        with col3:
            success_rate = len(successful_geocoding) / len(st.session_state.geocoding_results) * 100
            st.metric("📈 Taux de réussite", f"{success_rate:.1f}%")
        
        # Carte des résultats
        if successful_geocoding:
            st.subheader("🗺️ Carte des établissements géolocalisés")
            
            map_df = pd.DataFrame(successful_geocoding)
            
            fig = px.scatter_mapbox(
                map_df,
                lat="latitude",
                lon="longitude",
                hover_name="address",
                zoom=6,
                height=400
            )
            
            fig.update_layout(
                mapbox_style="open-street-map",
                margin={"r": 0, "t": 0, "l": 0, "b": 0}
            )
            
            st.plotly_chart(fig, width="stretch")
    
    # Bouton pour passer à l'étape suivante
    if st.button("➡️ Continuer vers l'étape 4", type="primary"):
        st.session_state.current_step = 4
        st.rerun()

def step_4_enrichment():
    """Étape 4: Enrichissement depuis les sites web"""
    st.markdown('<h2 class="section-header">🌐 Étape 4: Enrichissement Web</h2>', unsafe_allow_html=True)
    
    if st.session_state.data_processor is None:
        st.error("Aucune donnée chargée. Retournez à l'étape 1.")
        return
    
    df = st.session_state.data_processor.df
    mapping = st.session_state.data_processor.mapped_columns
    
    # Obtenir les enregistrements importables
    importable_df = st.session_state.data_processor.get_importable_records()
    
    web_enrichment_service = WebEnrichmentService()
    
    # Configuration de l'enrichissement
    st.subheader("⚙️ Configuration de l'enrichissement")
    
    col1, col2 = st.columns(2)
    
    with col1:
        use_web_scraping = st.checkbox("🌐 Web scraping", value=True, help="Analyse directe des sites web")
        use_ai = st.checkbox("🤖 Analyse IA", value=False, help="Analyse intelligente avec IA")
    
    with col2:
        if use_ai:
            ai_service = st.selectbox(
                "Service IA:",
                ["Ollama (local)", "Groq (API gratuite)", "Hugging Face (gratuit)"],
                help="Choisissez le service d'IA à utiliser"
            )
            
            if ai_service == "Ollama (local)":
                ollama_model = st.selectbox(
                    "Modèle Ollama:",
                    ["phi3:mini (2GB - Recommandé T495s)", "llama2:7b (4GB)", "mistral:7b (4GB)", "gemma:2b (1GB)"],
                    help="Modèle optimisé pour votre configuration"
                )
            elif ai_service == "Groq (API gratuite)":
                groq_key = st.text_input(
                    "Clé API Groq:",
                    type="password",
                    help="Obtenez une clé gratuite sur console.groq.com"
                )
                if groq_key:
                    st.success("✅ Clé API configurée")
            elif ai_service == "Hugging Face (gratuit)":
                hf_token = st.text_input(
                    "Token Hugging Face (optionnel):",
                    type="password",
                    help="Token gratuit pour plus de requêtes (hf.co/settings/tokens)"
                )
        
        max_establishments = st.slider(
            "Nombre max d'établissements:",
            1, 50, 10,
            help="Limiter pour éviter les timeouts"
        )

    # Messages d'information selon le choix
    if use_ai:
        if ai_service == "Ollama (local)":
            st.info("ℹ️ **Ollama local**: Installez avec `ollama pull phi3:mini` puis `ollama serve`")
            st.write("**Avantages**: Gratuit, privé, illimité")
            st.write("**Configuration T495s**: Recommandé `phi3:mini` (2GB RAM)")
        elif ai_service == "Groq (API gratuite)":
            st.info("ℹ️ **Groq**: Service gratuit très rapide - https://console.groq.com")
            st.write("**Avantages**: Très rapide, pas d'installation")
            st.write("**Limites**: Quota gratuit (généreuse)")
        elif ai_service == "Hugging Face (gratuit)":
            st.info("ℹ️ **Hugging Face**: Service gratuit avec quota - https://hf.co")
            st.write("**Avantages**: Nombreux modèles, gratuit")
            st.write("**Limites**: Plus lent, quota limité")
    
    # Sélection des établissements à enrichir
    st.subheader("🎯 Sélection des établissements")
    
    # Tous les établissements (pas seulement ceux avec site web pour Ollama)
    all_establishments = []
    
    for idx, row in importable_df.iterrows():
        nom = row[mapping.get('nom', importable_df.columns[0])] if mapping.get('nom') else f"Ligne {idx + 1}"
        commune = row[mapping.get('commune', '')] if mapping.get('commune') else "Commune inconnue"
        site_web = row[mapping.get('site_web', '')] if mapping.get('site_web') and pd.notna(row[mapping.get('site_web', '')]) else None
        
        all_establishments.append({
            'index': idx,
            'nom': nom,
            'commune': commune,
            'site_web': site_web
        })
    
    # Filtrer selon la méthode choisie
    if use_web_scraping and not use_ollama:
        # Seulement les établissements avec site web
        establishments_to_process = [est for est in all_establishments if est['site_web']]
        if not establishments_to_process:
            st.warning("⚠️ Aucun établissement avec site web détecté pour le web scraping.")
    else:
        # Tous les établissements (Ollama peut fonctionner sans site web)
        establishments_to_process = all_establishments
    
    if establishments_to_process:
        st.write(f"📊 {len(establishments_to_process)} établissements sélectionnés pour enrichissement")
        
        # Aperçu des établissements
        preview_data = []
        for est in establishments_to_process[:10]:  # Limiter l'aperçu
            preview_data.append({
                'Nom': est['nom'],
                'Commune': est['commune'],
                'Site web': est['site_web'] if est['site_web'] else 'Non renseigné'
            })
        
        if preview_data:
            st.dataframe(pd.DataFrame(preview_data), width="stretch")
        
        # Démarrer l'enrichissement
        if st.button("🚀 Démarrer l'enrichissement intelligent", type="primary"):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            enrichment_results = []
            num_to_process = min(max_establishments, len(establishments_to_process))
            
            for i, establishment in enumerate(establishments_to_process[:num_to_process]):
                status_text.text(f"Enrichissement... {establishment['nom']} ({i+1}/{num_to_process})")
                
                try:
                    if use_ollama:
                        # Enrichissement complet avec Ollama
                        result = web_enrichment_service.enrich_establishment_complete(
                            establishment['nom'],
                            establishment['commune'], 
                            establishment['site_web']
                        )
                    else:
                        # Web scraping seulement
                        result = web_enrichment_service.enrich_from_website(establishment['site_web'])
                        result['nom'] = establishment['nom']
                        result['commune'] = establishment['commune']
                    
                    result['establishment_index'] = establishment['index']
                    enrichment_results.append(result)
                    
                except Exception as e:
                    enrichment_results.append({
                        'establishment_index': establishment['index'],
                        'nom': establishment['nom'],
                        'error': f'Erreur traitement: {str(e)}'
                    })
                
                progress_bar.progress((i + 1) / num_to_process)
                time.sleep(1 if use_ollama else 0.5)  # Pause plus longue pour Ollama
            
            st.session_state.enrichment_results = enrichment_results
            status_text.text("✅ Enrichissement terminé!")
    
    else:
        if use_web_scraping and not use_ollama:
            st.warning("⚠️ Aucun établissement avec site web pour le web scraping.")
        else:
            st.info("ℹ️ Aucun établissement trouvé pour l'enrichissement.")
    
    # Affichage des résultats d'enrichissement
    if st.session_state.enrichment_results:
        st.subheader("📊 Résultats de l'enrichissement")
        
        # Statistiques globales
        successful_enrichments = [r for r in st.session_state.enrichment_results if 'error' not in r]
        failed_enrichments = [r for r in st.session_state.enrichment_results if 'error' in r]
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("✅ Succès", len(successful_enrichments))
        with col2:
            st.metric("❌ Échecs", len(failed_enrichments))
        with col3:
            success_rate = len(successful_enrichments) / len(st.session_state.enrichment_results) * 100 if st.session_state.enrichment_results else 0
            st.metric("📈 Taux de réussite", f"{success_rate:.1f}%")
        
        # Détails par établissement
        for result in st.session_state.enrichment_results:
            nom_etablissement = result.get('nom', f"Établissement {result.get('establishment_index', '')}")
            
            with st.expander(f"🏢 {nom_etablissement}"):
                if 'error' in result:
                    st.error(f"❌ Erreur: {result['error']}")
                else:
                    # Afficher les résultats selon le type d'enrichissement
                    if 'combined_data' in result:
                        # Enrichissement complet (Ollama + Web)
                        st.write("🤖 **Enrichissement intelligent (Ollama + Web)**")
                        data = result['combined_data']
                        
                        # Type de public
                        if data.get('type_public'):
                            st.write(f"👥 **Type de public**: {data['type_public']}")
                        
                        # Restauration
                        if data.get('restauration'):
                            resto = data['restauration']
                            resto_services = [k for k, v in resto.items() if v]
                            if resto_services:
                                st.write(f"🍽️ **Services de restauration**: {', '.join(resto_services)}")
                        
                        # Tarifs
                        if data.get('tarifs'):
                            tarifs = data['tarifs']
                            if tarifs.get('prix_min') or tarifs.get('prix_max'):
                                prix_text = f"{tarifs.get('prix_min', '?')}€ - {tarifs.get('prix_max', '?')}€"
                                if tarifs.get('fourchette_prix'):
                                    prix_text += f" ({tarifs['fourchette_prix']})"
                                st.write(f"💰 **Tarifs**: {prix_text}")
                        
                        # Services
                        if data.get('services'):
                            services = data['services']
                            services_actifs = [k.replace('_', ' ').title() for k, v in services.items() if v]
                            if services_actifs:
                                st.write(f"🛎️ **Services**: {', '.join(services_actifs)}")
                        
                        # Éligibilité AVP
                        if data.get('eligibilite_statut'):
                            avp_status = {
                                'avp_eligible': '✅ Éligible AVP',
                                'non_eligible': '❌ Non éligible AVP',
                                'a_verifier': '⚠️ À vérifier'
                            }
                            st.write(f"🏛️ **Éligibilité AVP**: {avp_status.get(data['eligibilite_statut'], data['eligibilite_statut'])}")
                        
                        # Détails techniques
                        with st.expander("🔍 Détails techniques"):
                            if result.get('ollama_analysis'):
                                st.write("**Analyse Ollama:**", result['ollama_analysis'])
                            if result.get('web_scraping'):
                                st.write("**Web scraping:**", result['web_scraping'])
                    
                    else:
                        # Enrichissement web simple
                        st.write("🌐 **Web scraping classique**")
                        
                        for key, value in result.items():
                            if key not in ['establishment_index', 'nom', 'commune', 'url', 'error']:
                                if value:
                                    st.write(f"**{key.replace('_', ' ').title()}**: {value}")
    
    # Bouton pour passer à l'étape suivante
    if st.button("➡️ Continuer vers l'étape 5", type="primary"):
        st.session_state.current_step = 5
        st.rerun()

def step_5_import():
    """Étape 5: Validation et import en base"""
    st.markdown('<h2 class="section-header">🎯 Étape 5: Import en base</h2>', unsafe_allow_html=True)
    
    if st.session_state.data_processor is None:
        st.error("Aucune donnée chargée. Retournez à l'étape 1.")
        return
    
    df = st.session_state.data_processor.df
    mapping = st.session_state.data_processor.mapped_columns
    
    # Obtenir les enregistrements importables
    importable_df = st.session_state.data_processor.get_importable_records()
    
    st.write(f"📊 {len(importable_df)} enregistrements prêts pour validation et import")
    
    # Validation finale
    st.subheader("✅ Validation finale")
    
    data_validator = DataValidator()
    
    validation_summary = {
        'valid_records': 0,
        'invalid_records': 0,
        'warnings': 0
    }
    
    detailed_validation = []
    
    # Valider chaque enregistrement importable
    for idx, row in importable_df.iterrows():
        # Préparer les données de l'enregistrement
        record_data = {}
        
        for target_field, source_col in mapping.items():
            if source_col in importable_df.columns:
                value = row[source_col]
                record_data[target_field] = value if pd.notna(value) else None
        
        # Ajouter les coordonnées GPS si disponibles
        if st.session_state.geocoding_results:
            geocoding_result = next((r for r in st.session_state.geocoding_results if r['index'] == idx), None)
            if geocoding_result:
                record_data['latitude'] = geocoding_result['latitude']
                record_data['longitude'] = geocoding_result['longitude']
                record_data['geocode_precision'] = geocoding_result['geocode_precision']
        
        # Valider l'enregistrement
        validation_result = data_validator.validate_record(record_data, idx)
        
        detailed_validation.append({
            'index': idx,
            'nom': record_data.get('nom', f'Ligne {idx + 1}'),
            'valid': validation_result['valid'],
            'score': validation_result['score'],
            'errors': validation_result['errors'],
            'warnings': validation_result['warnings']
        })
        
        if validation_result['valid']:
            validation_summary['valid_records'] += 1
        else:
            validation_summary['invalid_records'] += 1
        
        validation_summary['warnings'] += len(validation_result['warnings'])
    
    # Affichage du résumé de validation
    total_records = len(df)
    excluded_count = len(st.session_state.data_processor.excluded_records)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("📊 Total", total_records)
    with col2:
        st.metric("✅ Valides", validation_summary['valid_records'])
    with col3:
        st.metric("❌ Invalides", validation_summary['invalid_records'])
    with col4:
        st.metric("🚫 Exclus", excluded_count)
    with col5:
        st.metric("⚠️ Avertissements", validation_summary['warnings'])
    
    # Graphique de répartition
    if total_records > 0:
        fig = go.Figure(data=[go.Pie(
            labels=['Valides', 'Invalides', 'Exclus'],
            values=[validation_summary['valid_records'], validation_summary['invalid_records'], excluded_count],
            hole=.3
        )])
        
        fig.update_layout(title="Répartition des enregistrements")
        st.plotly_chart(fig, width="stretch")
    
    # Import final
    st.subheader("🚀 Import en base de données")
    
    # Configuration de la base de données
    if st.session_state.database_manager is None:
        try:
            st.session_state.database_manager = DatabaseManager()
            connection_status = st.session_state.database_manager.test_connection()
            if connection_status['success']:
                st.success(f"✅ Connexion base réussie: {connection_status['message']}")
            else:
                st.error(f"❌ Erreur connexion base: {connection_status['message']}")
                return
        except Exception as e:
            st.error(f"❌ Erreur de connexion à la base: {e}")
            return
    
    # Options d'import
    col1, col2 = st.columns(2)
    
    with col1:
        import_invalid = st.checkbox(
            "Importer aussi les enregistrements invalides",
            help="Les enregistrements invalides seront importés avec le statut 'draft'"
        )
        test_mode = st.checkbox(
            "Mode test (marquage test_tag)",
            value=True,
            help="Les données seront marquées comme test"
        )
    
    with col2:
        auto_geocode = st.checkbox(
            "Géolocalisation automatique manquante",
            value=True,
            help="Géolocaliser les adresses qui n'ont pas été traitées"
        )
    
    # Démarrer l'import
    records_to_import = detailed_validation if import_invalid else [r for r in detailed_validation if r['valid']]
    
    st.write(f"📊 **{len(records_to_import)}** enregistrements seront importés.")
    st.write(f"🚫 **{excluded_count}** enregistrements exclus ne seront pas importés.")
    
    if st.button("🎯 LANCER L'IMPORT EN BASE", type="primary"):
        if not records_to_import:
            st.error("Aucun enregistrement à importer!")
            return
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        import_results = {
            'success': 0,
            'errors': 0,
            'details': []
        }
        
        try:
            for i, validation_record in enumerate(records_to_import):
                idx = validation_record['index']
                row = importable_df.iloc[importable_df.index.get_loc(idx)]
                
                status_text.text(f"Import en cours... {validation_record['nom']}")
                
                # Préparer les données pour l'import
                import_data = {}
                
                for target_field, source_col in mapping.items():
                    if source_col in row:
                        value = row[source_col]
                        import_data[target_field] = value if pd.notna(value) else None
                
                # Ajouter les coordonnées GPS
                if st.session_state.geocoding_results:
                    geocoding_result = next((r for r in st.session_state.geocoding_results if r['index'] == idx), None)
                    if geocoding_result:
                        import_data['latitude'] = geocoding_result['latitude']
                        import_data['longitude'] = geocoding_result['longitude']
                        import_data['geocode_precision'] = geocoding_result['geocode_precision']
                
                # Statut selon validation
                import_data['status'] = 'active' if validation_record['valid'] else 'draft'
                
                if test_mode:
                    import_data['test_tag'] = 'import_test'
                
                # Import en base
                result = st.session_state.database_manager.insert_etablissement(import_data)
                
                if result['success']:
                    import_results['success'] += 1
                    import_results['details'].append({
                        'nom': validation_record['nom'],
                        'status': 'success',
                        'id': result.get('id'),
                        'message': result.get('message', 'Import réussi')
                    })
                else:
                    import_results['errors'] += 1
                    import_results['details'].append({
                        'nom': validation_record['nom'],
                        'status': 'error',
                        'message': result.get('message', 'Erreur inconnue')
                    })
                
                progress_bar.progress((i + 1) / len(records_to_import))
                time.sleep(0.1)  # Courte pause
            
            # Résultats finaux
            status_text.text("✅ Import terminé!")
            
            st.success(f"🎉 Import terminé avec succès!")
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("✅ Imports réussis", import_results['success'])
            with col2:
                st.metric("❌ Erreurs", import_results['errors'])
            
            # Détails des erreurs si nécessaire
            if import_results['errors'] > 0:
                st.subheader("❌ Détails des erreurs")
                error_details = [d for d in import_results['details'] if d['status'] == 'error']
                for error in error_details:
                    st.error(f"**{error['nom']}**: {error['message']}")
        
        except Exception as e:
            st.error(f"Erreur lors de l'import: {e}")
            import traceback
            st.code(traceback.format_exc())

def main():
    """Application principale"""
    st.title("🏠 Habitat Ingestion Tool")
    st.markdown("Outil d'importation de données pour la base PostgreSQL Habitat")
    
    # Menu de navigation
    steps = {
        1: "📁 Upload",
        2: "✏️ Correction", 
        3: "🗺️ Géocodage",
        4: "🌐 Enrichissement",
        5: "🎯 Import"
    }
    
    # Affichage de l'étape courante
    current_step_name = steps.get(st.session_state.current_step, "Inconnue")
    st.sidebar.title("🧭 Navigation")
    st.sidebar.write(f"**Étape actuelle:** {current_step_name}")
    
    # Navigation manuelle
    for step_num, step_name in steps.items():
        if step_num <= st.session_state.current_step or step_num == 1:
            if st.sidebar.button(f"{step_name}", key=f"nav_{step_num}"):
                st.session_state.current_step = step_num
                st.rerun()
    
    # Exécution de l'étape courante
    if st.session_state.current_step == 1:
        step_1_upload()
    elif st.session_state.current_step == 2:
        step_2_correction()
    elif st.session_state.current_step == 3:
        step_3_geocoding()
    elif st.session_state.current_step == 4:
        step_4_enrichment()
    elif st.session_state.current_step == 5:
        step_5_import()

if __name__ == "__main__":
    main()