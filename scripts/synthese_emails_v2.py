"""
Script de synthèse et analyse des résultats de reconstruction d'emails V2
"""

import pandas as pd
from collections import Counter


def analyze_results(filepath: str):
    """Analyse complète des résultats de reconstruction d'emails"""
    
    df = pd.read_excel(filepath)
    
    print("=" * 80)
    print("📊 SYNTHÈSE RECONSTRUCTION EMAILS DIRIGEANTS V2")
    print("=" * 80)
    
    # === STATISTIQUES GLOBALES ===
    print("\n📈 STATISTIQUES GLOBALES")
    print("-" * 80)
    print(f"Total gestionnaires: {len(df)}")
    print(f"Emails dirigeants générés: {df['Email Dirigeant 1'].notna().sum()}")
    print(f"Emails organisation trouvés: {df['Email Organisation'].notna().sum()}")
    print(f"Civilités déterminées: {df['Civilité'].notna().sum()}")
    print(f"Adresses formatées: {df['Adresse Publipostage'].notna().sum()}")
    
    # === PATTERNS DÉTECTÉS ===
    print("\n📧 ANALYSE DES PATTERNS DÉTECTÉS")
    print("-" * 80)
    
    patterns_stats = df['Pattern Email'].value_counts()
    print(f"Patterns détectés: {len(patterns_stats)} types différents")
    for pattern, count in patterns_stats.items():
        pct = (count / len(df)) * 100
        print(f"  • {pattern:15s}: {count:3d} ({pct:5.1f}%)")
    
    # Confiance moyenne
    conf_positive = df[df['Conf. Email'] > 0]['Conf. Email']
    if len(conf_positive) > 0:
        print(f"\nConfiance moyenne (patterns détectés): {conf_positive.mean():.1f}%")
        print(f"Patterns avec confiance ≥ 80%: {(conf_positive >= 80).sum()}")
        print(f"Patterns avec confiance ≥ 50%: {(conf_positive >= 50).sum()}")
    
    # === TYPES D'EMAILS ORGANISATION ===
    print("\n🏢 TYPES D'EMAILS ORGANISATION")
    print("-" * 80)
    
    org_types = df['Type Email Org'].value_counts()
    for org_type, count in org_types.items():
        pct = (count / len(df)) * 100
        emoji = {
            'siege': '🏛️',
            'direction': '👔',
            'dg': '🎯',
            'contact': '📞',
            'fallback': '💼',
            'none': '❌'
        }.get(org_type, '📧')
        print(f"  {emoji} {org_type:12s}: {count:3d} ({pct:5.1f}%)")
    
    # === CIVILITÉS ===
    print("\n👤 RÉPARTITION DES CIVILITÉS")
    print("-" * 80)
    
    civilites = df['Civilité'].value_counts()
    for civ, count in civilites.items():
        pct = (count / len(df)) * 100
        print(f"  • {civ:20s}: {count:3d} ({pct:5.1f}%)")
    
    # === EXEMPLES DE SUCCÈS ===
    print("\n✅ EXEMPLES DE SUCCÈS (patterns détectés avec haute confiance)")
    print("-" * 80)
    
    success_df = df[df['Conf. Email'] >= 80].copy()
    if len(success_df) > 0:
        success_df = success_df.sort_values('Conf. Email', ascending=False).head(10)
        
        for idx, row in success_df.iterrows():
            print(f"\n{row['nom_public']}")
            print(f"  Dirigeant: {row['dirigeant_nom']}")
            print(f"  Pattern: {row['Pattern Email']} (confiance: {row['Conf. Email']:.0f}%)")
            print(f"  Email n°1: {row['Email Dirigeant 1']}")
            if pd.notna(row['Email Organisation']):
                print(f"  Email org: {row['Email Organisation']} ({row['Type Email Org']})")
            print(f"  Civilité: {row['Civilité']}")
    
    # === CAS AVEC EMAIL SIEGE/DIRECTION ===
    print("\n\n🏛️ CAS AVEC EMAIL SIEGE/DIRECTION/DG (prioritaires)")
    print("-" * 80)
    
    priority_emails = df[df['Type Email Org'].isin(['siege', 'direction', 'dg'])].copy()
    print(f"Total: {len(priority_emails)} gestionnaires avec email prioritaire")
    
    if len(priority_emails) > 0:
        for idx, row in priority_emails.head(10).iterrows():
            print(f"\n{row['nom_public']}")
            print(f"  Email org: {row['Email Organisation']} ({row['Type Email Org']})")
            print(f"  Email dirigeant: {row['Email Dirigeant 1']}")
    
    # === VALIDATION CONSERVATION DES DONNÉES ===
    print("\n\n✅ VALIDATION CONSERVATION DES COLONNES ORIGINALES")
    print("-" * 80)
    
    original_cols_check = {
        'LinkedIn': 'dirigeant_linkedin_url',
        'Type principal': 'dominante_type',
        'Top 5 types': 'dominante_top5',
        'Site web': 'site_web',
        'Nb ESSMS': 'nb_essms',
        'Sources web': 'sources_web'
    }
    
    for name, col in original_cols_check.items():
        if col in df.columns:
            count = df[col].notna().sum()
            pct = (count / len(df)) * 100
            print(f"  ✅ {name:20s}: {count:3d} présents ({pct:5.1f}%)")
        else:
            print(f"  ❌ {name:20s}: colonne manquante")
    
    print(f"\n📊 Total colonnes: {len(df.columns)}")
    
    # === ÉCHANTILLON POUR PUBLIPOSTAGE ===
    print("\n\n📬 ÉCHANTILLON POUR PUBLIPOSTAGE (5 premiers)")
    print("-" * 80)
    
    publi_cols = ['nom_public', 'Civilité', 'dirigeant_nom', 'Email Dirigeant 1', 
                  'Email Organisation', 'Adresse Publipostage']
    
    sample = df[publi_cols].head(5)
    print(sample.to_string(index=False))
    
    # === STATISTIQUES POUR CAMPAGNE EMAIL ===
    print("\n\n📨 STATISTIQUES POUR CAMPAGNE EMAIL")
    print("-" * 80)
    
    # Emails dirigeants valides
    emails_dir_valides = df[df['Email Dirigeant 1'].notna()]
    print(f"Emails dirigeants disponibles: {len(emails_dir_valides)}")
    
    # Emails avec pattern fiable (conf >= 50%)
    emails_fiables = df[df['Conf. Email'] >= 50]
    print(f"Emails avec pattern fiable (≥50%): {len(emails_fiables)}")
    
    # Emails organisation disponibles
    emails_org = df[df['Email Organisation'].notna()]
    print(f"Emails organisation disponibles: {len(emails_org)}")
    
    # Total contacts uniques (dirigeant OU org)
    total_contactable = df[(df['Email Dirigeant 1'].notna()) | (df['Email Organisation'].notna())]
    print(f"\n✅ Total gestionnaires contactables: {len(total_contactable)}")
    print(f"   Taux de couverture: {(len(total_contactable) / len(df)) * 100:.1f}%")
    
    # === EXPORT LISTE PUBLIPOSTAGE ===
    print("\n\n💾 EXPORT FICHIER PUBLIPOSTAGE")
    print("-" * 80)
    
    # Créer fichier optimisé pour publipostage
    publi_export = df[df['Email Dirigeant 1'].notna()].copy()
    
    publi_cols_export = [
        'nom_public',
        'Civilité',
        'dirigeant_nom',
        'dirigeant_titre',
        'Email Dirigeant 1',
        'Email Dirigeant 2',
        'Email Dirigeant 3',
        'Email Organisation',
        'Type Email Org',
        'Adresse Publipostage',
        'site_web',
        'nb_essms',
        'categorie_taille',
        'dominante_type',
        'Pattern Email',
        'Conf. Email'
    ]
    
    output_publi = filepath.replace('.xlsx', '_PUBLIPOSTAGE.xlsx')
    publi_export[publi_cols_export].to_excel(output_publi, index=False)
    
    print(f"Fichier créé: {output_publi}")
    print(f"Lignes: {len(publi_export)}")
    print(f"Colonnes: {len(publi_cols_export)}")
    
    # === RÉSUMÉ FINAL ===
    print("\n\n" + "=" * 80)
    print("✅ SYNTHÈSE TERMINÉE")
    print("=" * 80)
    
    return df


if __name__ == "__main__":
    import sys
    
    filepath = "outputs/prospection_250_FINAL_FORMATE_V2.xlsx"
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
    
    print(f"Analyse du fichier: {filepath}\n")
    
    try:
        df = analyze_results(filepath)
        print("\n✅ Analyse terminée avec succès !")
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
