from pdf2image import convert_from_path
import pytesseract

def extract_text_with_ocr(file_path: str, lang: str = 'heb+eng') -> str:
    """
    Convert each PDF page to an image and run OCR (Hebrew + English).
    Returns plain text.
    """
    try:
        pages = convert_from_path(file_path)
    except Exception as e:
        print(f"Debug: Failed to convert PDF to images: {e}")
        return ""

    chunks = []
    for i, page in enumerate(pages):
        try:
            config = '--psm 6'  # Assume a single uniform block of text
            txt = pytesseract.image_to_string(page, lang=lang, config=config)
            if txt:
                chunks.append(txt)
            else:
                print(f"Debug: No text extracted from page {i+1}")
        except Exception as e:
            print(f"Debug: OCR failed on page {i+1}: {e}")

    return "\n".join(chunks).strip()