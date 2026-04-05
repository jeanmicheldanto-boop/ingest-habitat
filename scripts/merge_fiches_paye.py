from pathlib import Path
import subprocess
import sys

def find_pdfs(dirpath: Path):
    return sorted([p for p in dirpath.glob("*.pdf") if p.is_file()])


def merge_pdfs(pdf_paths, out_path: Path):
    from pypdf import PdfReader, PdfWriter
    writer = PdfWriter()
    for p in pdf_paths:
        reader = PdfReader(str(p))
        for page in reader.pages:
            writer.add_page(page)
    with open(out_path, "wb") as f:
        writer.write(f)


def try_ghostscript_to_grayscale(input_path: Path, output_path: Path):
    # Try common ghostscript executables
    gs_cmds = ["gswin64c", "gswin32c", "gs"]
    for gs in gs_cmds:
        try:
            cmd = [
                gs,
                "-dNOPAUSE",
                "-dBATCH",
                "-sDEVICE=pdfwrite",
                "-dCompatibilityLevel=1.4",
                "-dProcessColorModel=/DeviceGray",
                "-dColorConversionStrategy=/Gray",
                "-dColorConversionStrategyForImages=/Gray",
                f"-sOutputFile={str(output_path)}",
                str(input_path),
            ]
            res = subprocess.run(cmd, check=True, capture_output=True)
            return True, res
        except FileNotFoundError:
            continue
        except subprocess.CalledProcessError as e:
            return False, e
    return False, "ghostscript not found"


def main():
    base_dir = Path(__file__).resolve().parents[1] / "TDS" / "Fiches_Paye"
    if len(sys.argv) > 1:
        base_dir = Path(sys.argv[1])
    if not base_dir.exists():
        print("Fiches_Paye directory not found:", base_dir)
        sys.exit(1)

    pdfs = find_pdfs(base_dir)
    if not pdfs:
        print("No PDFs found in", base_dir)
        sys.exit(1)

    print("Found PDFs:")
    for p in pdfs:
        print(" -", p.name)

    couleur_out = base_dir / "Fiches_Paye_COULEUR.pdf"
    noir_out = base_dir / "Fiches_Paye_NOIR_et_blanc.pdf"

    merge_pdfs(pdfs, couleur_out)
    print("WROTE", couleur_out)

    ok, info = try_ghostscript_to_grayscale(couleur_out, noir_out)
    if ok:
        print("WROTE (grayscale)", noir_out)
    else:
        print("Could not produce grayscale PDF:", info)
        print("You can install Ghostscript or I can provide instructions.")


if __name__ == "__main__":
    main()
