from pathlib import Path

def find_first_tds_variant(tds_dir: Path, keyword: str):
    matches = [p for p in sorted(tds_dir.glob("*.pdf")) if keyword.lower() in p.name.lower()]
    return matches[0] if matches else None


def merge_pdfs(paths, out_path: Path):
    from pypdf import PdfReader, PdfWriter
    writer = PdfWriter()
    for p in paths:
        reader = PdfReader(str(p))
        for page in reader.pages:
            writer.add_page(page)
    with open(out_path, "wb") as f:
        writer.write(f)


def main():
    base = Path(__file__).resolve().parents[1] / "TDS"
    ident_dir = base / "IDENTITE"
    if not ident_dir.exists():
        print("IDENTITE directory not found:", ident_dir)
        return

    id_pdfs = sorted([p for p in ident_dir.glob("*.pdf") if p.is_file()])
    if not id_pdfs:
        print("No PDFs found in IDENTITE")
        return

    # Find COULEUR and NOIR source files in TDS
    couleur_src = find_first_tds_variant(base, "couleur")
    noir_src = find_first_tds_variant(base, "noir")

    if not couleur_src:
        print("No source COULEUR PDF found in TDS")
    else:
        couleur_out = base / "TDS_COULEUR.pdf"
        # merge original couleur_src then identity PDFs
        merge_pdfs([couleur_src] + id_pdfs, couleur_out)
        print("WROTE", couleur_out)

    if not noir_src:
        print("No source NOIR PDF found in TDS")
    else:
        noir_out = base / "TDS_NOIR.pdf"
        merge_pdfs([noir_src] + id_pdfs, noir_out)
        print("WROTE", noir_out)


if __name__ == "__main__":
    main()
