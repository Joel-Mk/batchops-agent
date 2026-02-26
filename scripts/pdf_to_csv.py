import os
import pytesseract
from pdf2image import convert_from_path
import pandas as pd

# Windows: Set Tesseract path
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

# Poppler path
POPPLER_PATH = r"C:\poppler\Library\bin"


def pdf_to_csv(pdf_path, output_csv):

    if not os.path.exists(pdf_path):
        print(f"PDF file not found: {pdf_path}")
        return

    images = convert_from_path(
        pdf_path,
        poppler_path=POPPLER_PATH
    )

    extracted_text = ""

    for img in images:
        text = pytesseract.image_to_string(img)
        extracted_text += text + "\n"

    lines = [line.strip() for line in extracted_text.split("\n") if line.strip()]
    rows = [line.split(",") for line in lines]

    df = pd.DataFrame(rows)

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    df.to_csv(output_csv, index=False)

    print(f"OCR extraction complete. CSV saved to: {output_csv}")


if __name__ == "__main__":
    pdf_to_csv(
        "data/ocr_input/sample_input.pdf",
        "data/ocr_input/ocr_output.csv"
    )