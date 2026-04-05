import sys
from pathlib import Path

RANGES_TO_KEEP = [
    (11, 12),
    (16, 43),
    (63, 71),
    (74, 110),
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


def pages_to_indexes(ranges):
    # Convert 1-based inclusive ranges to 0-based indexes
    idxs = set()
    for a, b in ranges:
        for i in range(a - 1, b):
            idxs.add(i)
    return idxs


def is_page_blank(page):
    try:
        text = page.extract_text()
    except Exception:
        text = None
    if not text:
        return True
    return not text.strip()


def split_pdf(input_path: Path):
    PdfReader, PdfWriter = load_pdf_modules()
    reader = PdfReader(str(input_path))
    total = len(reader.pages)

    keep_idxs = pages_to_indexes(RANGES_TO_KEEP)

    kept_writer = PdfWriter()
    other_writer = PdfWriter()

    for i in range(total):
        page = reader.pages[i]
        if i in keep_idxs:
            kept_writer.add_page(page)
        else:
            if not is_page_blank(page):
                other_writer.add_page(page)

    base = input_path.stem
    out_dir = input_path.parent
    kept_name = f"{base}_kept_pages.pdf"
    other_name = f"{base}_other_nonempty.pdf"
    kept_path = out_dir / kept_name
    other_path = out_dir / other_name

    with open(kept_path, "wb") as f:
        kept_writer.write(f)

    with open(other_path, "wb") as f:
        other_writer.write(f)

    print("WROTE", kept_path)
    print("WROTE", other_path)


def main():
    if len(sys.argv) < 2:
        print("Usage: split_tds.py <input-pdf-path>")
        sys.exit(2)
    input_pdf = Path(sys.argv[1])
    if not input_pdf.exists():
        print("Input PDF not found:", input_pdf)
        sys.exit(1)
    split_pdf(input_pdf)


if __name__ == "__main__":
    main()
