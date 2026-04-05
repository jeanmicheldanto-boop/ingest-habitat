import sys
from pathlib import Path

# Pages are 1-based and inclusive
COULEUR_PAGES = [
    (11, 12),
    (16, 43),
    (63, 71),
    (54, 54),
    (56, 56),
    (74, 110),
]

NOIR_PAGES = [
    (1, 1),
    (3, 3),
    (5, 5),
    (7, 7),
    (8, 8),
    (13, 15),
]


def load_pdf_modules():
    try:
        from pypdf import PdfReader, PdfWriter
    except Exception:
        try:
            from PyPDF2 import PdfReader, PdfWriter
        except Exception:
            raise
    return PdfReader, PdfWriter


def ranges_to_indexes(ranges):
    idxs = []
    for a, b in ranges:
        for i in range(a - 1, b):
            idxs.append(i)
    return idxs


def extract_pages(reader, indexes):
    PdfReader, PdfWriter = load_pdf_modules()
    writer = PdfWriter()
    total = len(reader.pages)
    for i in indexes:
        if 0 <= i < total:
            writer.add_page(reader.pages[i])
    return writer


def write_writer(writer, out_path: Path):
    with open(out_path, "wb") as f:
        writer.write(f)


def main():
    if len(sys.argv) < 2:
        print("Usage: split_tds_custom.py <input-pdf>")
        sys.exit(2)
    input_pdf = Path(sys.argv[1])
    if not input_pdf.exists():
        print("Input not found:", input_pdf)
        sys.exit(1)

    PdfReader, _ = load_pdf_modules()
    reader = PdfReader(str(input_pdf))

    couleur_idxs = ranges_to_indexes(COULEUR_PAGES)
    noir_idxs = ranges_to_indexes(NOIR_PAGES)

    couleur_writer = extract_pages(reader, couleur_idxs)
    noir_writer = extract_pages(reader, noir_idxs)

    base = input_pdf.stem
    out_dir = input_pdf.parent
    couleur_path = out_dir / f"{base}_COULEUR.pdf"
    noir_path = out_dir / f"{base}_NOIR.pdf"

    write_writer(couleur_writer, couleur_path)
    write_writer(noir_writer, noir_path)

    print("WROTE", couleur_path)
    print("WROTE", noir_path)


if __name__ == "__main__":
    main()
