"""
Script de correction des valeurs eligibilite_avp dans les CSV
Applique les vraies règles métier pour corriger les données existantes
"""
import pandas as pd
import sys
from pathlib import Path

# Ajouter le parent au path pour les imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from enrichment.eligibilite_rules import deduce_eligibilite_statut


def fix_eligibilite_in_csv(input_file: str, output_file: str = None):
    """
    Corriger les valeurs eligibilite_avp dans un CSV
    
    Args:
        input_file: Chemin du fichier CSV d'entrée
        output_file: Chemin du fichier CSV de sortie (si None, remplace l'original)
    """
    print(f"📁 Lecture du fichier: {input_file}")
    df = pd.read_csv(input_file)
    
    print(f"📊 {len(df)} établissements chargés")
    
    # Statistiques avant correction
    print("\n📈 AVANT CORRECTION:")
    if 'eligibilite_avp' in df.columns:
        print(df['eligibilite_avp'].value_counts())
    elif 'eligibilite_statut' in df.columns:
        print(df['eligibilite_statut'].value_counts())
    else:
        print("⚠️  Colonne d'éligibilité non trouvée")
        return
    
    # Détecter le nom de la colonne
    elig_col = 'eligibilite_avp' if 'eligibilite_avp' in df.columns else 'eligibilite_statut'
    sous_cat_col = 'sous_categories' if 'sous_categories' in df.columns else 'sous_categorie'
    
    # Appliquer les vraies règles
    corrections = 0
    details = []
    
    for idx, row in df.iterrows():
        sous_cat = str(row.get(sous_cat_col, '')).lower().strip()
        old_value = row.get(elig_col, 'a_verifier')
        
        # Détecter mention AVP dans les champs texte
        mention_avp = False
        for field in ['presentation', 'description', 'source']:
            text = str(row.get(field, '')).lower()
            if any(kw in text for kw in ['avp', 'aide à la vie partagée', 'aide a la vie partagee', 'conventionné avp', 'conventionne avp']):
                mention_avp = True
                break
        
        # Appliquer la règle
        new_value = deduce_eligibilite_statut(
            sous_cat,
            mention_avp_explicite=mention_avp,
            eligibilite_csv=old_value
        )
        
        # Mettre à jour si changement
        if new_value != old_value:
            df.at[idx, elig_col] = new_value
            corrections += 1
            details.append({
                'nom': row.get('nom', 'N/A'),
                'sous_categorie': sous_cat,
                'ancien': old_value,
                'nouveau': new_value,
                'raison': _get_correction_reason(sous_cat, old_value, new_value, mention_avp)
            })
    
    # Statistiques après correction
    print(f"\n✅ {corrections} corrections appliquées")
    
    print("\n📈 APRÈS CORRECTION:")
    print(df[elig_col].value_counts())
    
    # Afficher détails des corrections
    if details:
        print("\n📋 DÉTAIL DES CORRECTIONS:")
        for i, detail in enumerate(details[:10], 1):  # Limiter à 10 pour lisibilité
            print(f"\n{i}. {detail['nom']}")
            print(f"   Catégorie: {detail['sous_categorie']}")
            print(f"   {detail['ancien']} → {detail['nouveau']}")
            print(f"   Raison: {detail['raison']}")
        
        if len(details) > 10:
            print(f"\n   ... et {len(details) - 10} autres corrections")
    
    # Sauvegarder
    if output_file is None:
        output_file = input_file
    
    df.to_csv(output_file, index=False)
    print(f"\n💾 Fichier sauvegardé: {output_file}")
    
    # Créer un rapport détaillé
    if details:
        report_file = output_file.replace('.csv', '_corrections_report.csv')
        pd.DataFrame(details).to_csv(report_file, index=False)
        print(f"📄 Rapport détaillé: {report_file}")


def _get_correction_reason(sous_cat: str, old: str, new: str, mention_avp: bool) -> str:
    """Générer la raison de la correction"""
    
    # Béguinage, village seniors, etc. jamais éligibles
    jamais_eligibles = ['béguinage', 'beguinage', 'village seniors', 'résidence services', 'résidence autonomie', 'marpa', 'accueil familial']
    if any(cat in sous_cat for cat in jamais_eligibles):
        return f"Catégorie '{sous_cat}' JAMAIS éligible AVP"
    
    # Habitat inclusif
    if 'habitat inclusif' in sous_cat:
        if old == 'avp_eligible':
            return "Habitat inclusif déjà avp_eligible → préservé (déjà vérifié)"
        else:
            return "Habitat inclusif → a_verifier (sauf si déjà avp_eligible)"
    
    # Autres catégories
    if mention_avp:
        return f"Mention AVP détectée → {new}"
    else:
        return f"Pas de mention AVP → {new}"


def main():
    """Point d'entrée principal"""
    if len(sys.argv) < 2:
        print("Usage: python fix_eligibilite_csv.py <fichier_input.csv> [fichier_output.csv]")
        print("\nExemple:")
        print("  python scripts/fix_eligibilite_csv.py data/data_65.csv")
        print("  python scripts/fix_eligibilite_csv.py data/data_65.csv data/data_65_corrige.csv")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        fix_eligibilite_in_csv(input_file, output_file)
        print("\n✅ Correction terminée avec succès !")
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
