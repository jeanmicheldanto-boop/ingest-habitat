"""
Script pour convertir les documents Word en PDF et fusionner tous les PDFs
du dossier TDS en un seul fichier.

Usage:
    python convert_and_merge_pdfs.py
"""

import os
from pathlib import Path
from datetime import datetime
from docx2pdf import convert
from PyPDF2 import PdfMerger
import sys


def convert_docx_to_pdf(docx_folder: Path) -> list[Path]:
    """
    Convertit tous les fichiers .docx d'un dossier en PDF.
    
    Args:
        docx_folder: Chemin vers le dossier contenant les fichiers .docx
        
    Returns:
        Liste des fichiers PDF créés
    """
    docx_files = list(docx_folder.glob("*.docx"))
    
    if not docx_files:
        print("Aucun fichier .docx trouvé dans le dossier TDS")
        return []
    
    print(f"Fichiers Word trouvés : {len(docx_files)}")
    converted_pdfs = []
    
    for docx_file in docx_files:
        pdf_file = docx_file.with_suffix('.pdf')
        print(f"Conversion de {docx_file.name} -> {pdf_file.name}...")
        
        try:
            convert(str(docx_file), str(pdf_file))
            converted_pdfs.append(pdf_file)
            print(f"  ✓ Converti avec succès")
        except Exception as e:
            print(f"  ✗ Erreur lors de la conversion : {e}")
    
    return converted_pdfs


def merge_all_pdfs(pdf_folder: Path, output_file: Path) -> bool:
    """
    Fusionne tous les fichiers PDF d'un dossier en un seul.
    
    Args:
        pdf_folder: Chemin vers le dossier contenant les PDFs
        output_file: Chemin du fichier PDF fusionné en sortie
        
    Returns:
        True si la fusion a réussi, False sinon
    """
    pdf_files = sorted(list(pdf_folder.glob("*.pdf")))
    
    if not pdf_files:
        print("Aucun fichier PDF trouvé dans le dossier TDS")
        return False
    
    print(f"\nFusion de {len(pdf_files)} fichiers PDF...")
    merger = PdfMerger()
    
    for pdf_file in pdf_files:
        print(f"  Ajout de {pdf_file.name}...")
        try:
            merger.append(str(pdf_file))
        except Exception as e:
            print(f"  ✗ Erreur avec {pdf_file.name} : {e}")
            continue
    
    try:
        merger.write(str(output_file))
        merger.close()
        print(f"\n✓ Fusion terminée : {output_file}")
        print(f"  Taille du fichier : {output_file.stat().st_size / (1024*1024):.2f} MB")
        return True
    except Exception as e:
        print(f"\n✗ Erreur lors de l'écriture du fichier fusionné : {e}")
        return False


def main():
    """Fonction principale."""
    # Dossier TDS
    tds_folder = Path(__file__).parent / "TDS"
    
    if not tds_folder.exists():
        print(f"Erreur : Le dossier TDS n'existe pas : {tds_folder}")
        sys.exit(1)
    
    print("="*60)
    print("CONVERSION ET FUSION DE PDFs - DOSSIER TDS")
    print("="*60)
    
    # Étape 1 : Conversion des fichiers Word en PDF
    print("\n[1/2] Conversion des fichiers Word en PDF...")
    converted_pdfs = convert_docx_to_pdf(tds_folder)
    
    if converted_pdfs:
        print(f"\n✓ {len(converted_pdfs)} fichier(s) converti(s)")
    
    # Étape 2 : Fusion de tous les PDFs
    print("\n[2/2] Fusion de tous les PDFs...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = tds_folder / f"TDS_COMPLET_{timestamp}.pdf"
    
    success = merge_all_pdfs(tds_folder, output_file)
    
    if success:
        print("\n" + "="*60)
        print("TRAITEMENT TERMINÉ AVEC SUCCÈS")
        print("="*60)
        return 0
    else:
        print("\n" + "="*60)
        print("TRAITEMENT TERMINÉ AVEC DES ERREURS")
        print("="*60)
        return 1


if __name__ == "__main__":
    sys.exit(main())
