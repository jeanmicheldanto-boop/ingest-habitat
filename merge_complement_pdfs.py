from pathlib import Path
import sys

def find_complement_dir(root: Path):
    cand = root / "complement"
    if cand.exists() and cand.is_dir():
        return cand
    for d in root.rglob("complement"):
        if d.is_dir():
            return d
    return None

def main():
    root = Path(__file__).resolve().parent
    comp = find_complement_dir(root)
    if comp is None:
        print("Aucun dossier 'complement' trouvé sous", root)
        return 1
    pdfs = sorted([p for p in comp.glob("*.pdf") if p.is_file()])
    if not pdfs:
        print("Aucun fichier PDF trouvé dans", comp)
        return 1
    try:
        from pypdf import PdfReader, PdfWriter
    except Exception as e:
        print("Impossible d'importer pypdf:", e)
        print("Installez-le dans le venv utilisé: python -m pip install pypdf")
        return 2
    writer = PdfWriter()
    for p in pdfs:
        reader = PdfReader(str(p))
        for page in reader.pages:
            writer.add_page(page)
    out = comp / "merged_complement.pdf"
    with open(out, "wb") as f:
        writer.write(f)
    print("Fichier créé:", out)
    return 0

if __name__ == "__main__":
    sys.exit(main())
