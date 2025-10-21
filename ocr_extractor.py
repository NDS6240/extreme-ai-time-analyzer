from pdf2image import convert_from_path
import pytesseract

def extract_text_with_ocr(file_path: str) -> str:
    """
    Convert each PDF page to an image and run OCR (Hebrew + English).
    Returns plain text.
    """
    pages = convert_from_path(file_path)
    chunks = []
    for page in pages:
        txt = pytesseract.image_to_string(page, lang='heb+eng')
        if txt:
            chunks.append(txt)
    return "\n".join(chunks).strip()